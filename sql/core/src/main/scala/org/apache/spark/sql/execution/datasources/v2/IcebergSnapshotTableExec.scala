/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import com.netflix.bdp.Events
import com.netflix.iceberg.metacat.MetacatTables
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{ManifestFile, ManifestWriter, PartitionSpec}
import org.apache.iceberg.hadoop.HadoopFileIO

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, TableCatalog}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.IcebergUtil.SparkDataFile
import org.apache.spark.sql.functions._
import org.apache.spark.util.{SerializableConfiguration, Utils}

case class IcebergSnapshotTableExec(
    catalog: TableCatalog,
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier) extends LeafExecNode {

  import CatalogV2Implicits._

  @transient private lazy val spark = sqlContext.sparkSession

  override protected def doExecute(): RDD[InternalRow] = {
    import spark.implicits._

    val conf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val version = spark.version
    val applicationId = spark.sparkContext.applicationId
    val hiveEnv = spark.sparkContext.hadoopConfiguration.get("spark.sql.hive.env", "prod")
    val mcCatalog = spark.sparkContext.conf.get("spark.sql.metacat.write.catalog", hiveEnv + "hive")
    val db = targetTable.database.get
    val name = targetTable.table

    // use the default table catalog
    val sourceCatalog = V2Util.catalog(conf.value)
    val source = spark.catalog(None).asTableCatalog.loadTable(sourceTable)
    val sourceName = s"${sourceTable.database.get}.${sourceTable.table}"
    val partitions: DataFrame = IcebergUtil.partitionDF(spark, sourceName)

    // send a scan event for the entire source table
    Events.sendScan(s"$sourceCatalog.$sourceName",
      "true",
      V2Util.columns(source.schema).asJava,
      Map("context" -> "snapshot_table").asJava)

    val nonParquetPartitions = partitions.filter(not($"format".contains("parquet"))).count()
    if (nonParquetPartitions > 0) {
      throw new SparkException(s"Cannot convert table with non-Parquet partitions: $sourceName")
    }

    val files = partitions.repartition(1000).as[TablePartition].flatMap { p =>
      // list the partition and read Parquet footers to get metrics
      IcebergUtil.listPartition(conf.value, p.partition, p.uri, p.format)
    }

    logInfo(s"Creating new snapshot Iceberg table $targetTable from $sourceTable")
    val properties = source.properties.asScala.toMap --
        Seq("path", "transient_lastDdlTime", "serialization.format") +
        ("provider" -> "iceberg") +
        ("spark.behavior.compatibility" -> "true")
    catalog.createTable(targetTable, source.schema, source.partitioning, properties.asJava)

    val tables = new MetacatTables(conf.value, "spark-" + version, applicationId, mcCatalog)
    val table = tables.load(db, name)

    // create a temp path in HDFS for manifests. these will be rewritten.
    val tempPath = new Path(s"hdfs:/tmp/iceberg-conversions/$applicationId")
    tempPath.getFileSystem(conf.value).deleteOnExit(tempPath)

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      logInfo(s"Creating Iceberg table metadata for data files in $sourceTable")

      val writeManifest =
        IcebergSnapshotTableExec.writeManifest(conf, table.spec, tempPath.toString)
      val manifests: Seq[ManifestFile] = files
          .repartition(1000)
          .orderBy($"path")
          .mapPartitions(writeManifest)
          .collect()
          .map(_.toManifestFile)

      val append = table.newAppend

      manifests.foreach(append.appendManifest)

      append.commit()

    })(catchBlock = {
      catalog.dropTable(targetTable)
    })

    Events.sendAppend(s"$mcCatalog.$db.$name",
      V2Util.columns(source.schema).asJava,
      Map("context" -> "snapshot_table").asJava)

    logInfo(s"Finished loading data into $targetTable")

    spark.sparkContext.parallelize(Seq.empty, 1)
  }

  override def output: Seq[Attribute] = Seq.empty
}

private[sql] case class Manifest(location: String, fileLength: Long, specId: Int) {
  def toManifestFile: ManifestFile = new ManifestFile {
    override def path: String = location

    override def length: Long = fileLength

    override def partitionSpecId: Int = specId

    override def snapshotId: java.lang.Long = null

    override def addedFilesCount: Integer = null

    override def existingFilesCount: Integer = null

    override def deletedFilesCount: Integer = null

    override def partitions: java.util.List[ManifestFile.PartitionFieldSummary] = null

    override def copy: ManifestFile = this
  }
}

private[sql] case class TablePartition(
    partition: Map[String, String],
    uri: String,
    format: String)

private[sql] object IcebergSnapshotTableExec {
  def writeManifest(
      conf: SerializableConfiguration,
      spec: PartitionSpec,
      basePath: String): Iterator[SparkDataFile] => Iterator[Manifest] = {
    files =>
      if (files.hasNext) {
        val ctx = TaskContext.get()
        val manifestLocation = new Path(basePath,
          s"stage-${ctx.stageId()}-task-${ctx.taskAttemptId()}-manifest.avro").toString
        val io = new HadoopFileIO(conf.value)
        val writer = ManifestWriter.write(spec, io.newOutputFile(manifestLocation))

        try {
          files.foreach { file =>
            writer.add(file.toDataFile(spec))
          }
        } finally {
          writer.close()
        }

        val manifest = writer.toManifestFile
        Seq(Manifest(manifest.path, manifest.length, manifest.partitionSpecId)).iterator

      } else {
        Seq.empty.iterator
      }
  }
}
