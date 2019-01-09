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

import com.netflix.iceberg.metacat.MetacatTables
import com.netflix.iceberg.spark.SparkTableUtil
import com.netflix.iceberg.spark.SparkTableUtil.SparkDataFile

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, TableCatalog}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.functions._
import org.apache.spark.util.{SerializableConfiguration, Utils}

case class IcebergSnapshotTableExec(
    catalog: TableCatalog,
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier) extends LeafExecNode {

  import IcebergSnapshotTableExec.addFiles
  import CatalogV2Implicits._

  @transient private lazy val spark = sqlContext.sparkSession

  override protected def doExecute(): RDD[InternalRow] = {
    import spark.implicits._

    val serializableConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val applicationId = spark.sparkContext.applicationId
    val hiveEnv = spark.sparkContext.hadoopConfiguration.get("spark.sql.hive.env", "prod")
    val mcCatalog = spark.sparkContext.conf.get("spark.sql.metacat.write.catalog", hiveEnv + "hive")
    val db = targetTable.database.get
    val name = targetTable.table

    // use the default table catalog
    val source = spark.catalog(None).asTableCatalog.loadTable(sourceTable)
    val sourceName = s"${sourceTable.database.get}.${sourceTable.table}"
    val partitions: DataFrame = SparkTableUtil.partitionDF(spark, sourceName)

    val nonParquetPartitions = partitions.filter(not($"format".contains("parquet"))).count()
    if (nonParquetPartitions > 0) {
      throw new SparkException(s"Cannot convert table with non-Parquet partitions: $sourceName")
    }

    val files = partitions.repartition(1000).as[TablePartition].flatMap { p =>
      // list the partition and read Parquet footers to get metrics
      SparkTableUtil.listPartition(p.partition, p.uri, p.format)
    }

    logInfo(s"Creating new snapshot Iceberg table $targetTable from $sourceTable")
    val properties = source.properties.asScala.toMap +
        ("provider" -> "iceberg") +
        ("spark.behavior.compatibility" -> "true")
    catalog.createTable(targetTable, source.schema(), source.partitioning(), properties.asJava)

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      logInfo(s"Creating Iceberg table metadata for data files in $sourceTable")
      files.repartition(10)
          .foreachPartition(addFiles(serializableConf, applicationId, mcCatalog, db, name))
    })(catchBlock = {
      catalog.dropTable(targetTable)
    })

    logInfo(s"Finished loading data into $targetTable")

    spark.sparkContext.parallelize(Seq.empty, 1)
  }

  override def output: Seq[Attribute] = Seq.empty
}

private[sql] case class TablePartition(
    partition: Map[String, String],
    uri: String,
    format: String)

private[sql] object IcebergSnapshotTableExec {
  def addFiles(
      conf: SerializableConfiguration,
      appId: String,
      catalog: String,
      dbName: String,
      tableName: String): Iterator[SparkDataFile] => Unit = {
    files =>
      // open the table and append the files from this partition
      val tables = new MetacatTables(conf.value, appId, catalog)
      val table = tables.load(dbName, tableName)

      // fast appends will create a manifest for the new files
      val append = table.newFastAppend
      files.foreach { file =>
        append.appendFile(file.toDataFile(table.spec))
      }
      append.commit()
  }
}
