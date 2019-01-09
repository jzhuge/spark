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

import com.netflix.iceberg.spark.SparkTableUtil

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, TableCatalog, TableChange, V1MetadataTable}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.functions.not
import org.apache.spark.util.{SerializableConfiguration, Utils}

case class IcebergMigrateTableExec(
    catalog: TableCatalog,
    ident: TableIdentifier) extends LeafExecNode {

  private val HasBatchID = """(.*)/batchid=\d+""".r
  private val HasPartitionFolder = """.*/[^/]+=[^/]+/?""".r

  import IcebergSnapshotTableExec.addFiles
  import CatalogV2Implicits._

  @transient private lazy val spark = sqlContext.sparkSession

  override protected def doExecute(): RDD[InternalRow] = {
    import spark.implicits._

    val serializableConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val applicationId = spark.sparkContext.applicationId
    val hiveEnv = spark.sparkContext.hadoopConfiguration.get("spark.sql.hive.env", "prod")
    val mcCatalog = spark.sparkContext.conf.get("spark.sql.metacat.write.catalog", hiveEnv + "hive")
    val db = ident.database.get
    val tempName = ident.table + "_iceberg"
    val sourceName = s"${ident.database.get}.${ident.table}"
    val hiveIdent = TableIdentifier(ident.table + "_hive", Some(db))
    val tempIdent = TableIdentifier(tempName, Some(db))

    // use the default table catalog
    val source = spark.catalog(None).asTableCatalog.loadTable(ident).asInstanceOf[V1MetadataTable]
    val location: String = source.catalogTable.location.toString match {
      case HasBatchID(parent) => parent
      case withoutBatchID => withoutBatchID
    }

    // do not allow locations that look like partitions
    location match {
      case HasPartitionFolder() =>
        throw new SparkException(
          s"Cannot migrate table $sourceName: location appears to be a table partition: $location")
      case _ =>
    }

    val partitions: DataFrame = SparkTableUtil.partitionDF(spark, sourceName)

    if (spark.sessionState.catalog.tableExists(hiveIdent)) {
      throw new SparkException(s"Cannot create backup table $hiveIdent: already exists")
    }

    if (spark.sessionState.catalog.tableExists(tempIdent)) {
      throw new SparkException(s"Cannot create temporary table $tempIdent: already exists")
    }

    val nonParquetPartitions = partitions.filter(not($"format".contains("parquet"))).count()
    if (nonParquetPartitions > 0) {
      throw new SparkException(s"Cannot convert table with non-Parquet partitions: $sourceName")
    }

    val files = partitions.repartition(1000).as[TablePartition].flatMap { p =>
      // list the partition and read Parquet footers to get metrics
      SparkTableUtil.listPartition(p.partition, p.uri, p.format)
    }

    logInfo(s"Renaming $ident to $hiveIdent")
    spark.sessionState.catalog.renameTable(ident, hiveIdent)

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      logInfo(s"Creating new Iceberg table $tempIdent")
      val properties = source.properties.asScala.toMap --
          Seq("path", "transient_lastDdlTime", "serialization.format") +
          ("provider" -> "iceberg") +
          ("spark.behavior.compatibility" -> "true") +
          ("write.metadata.path" -> (location + "/metadata")) +
          ("migrated-from-hive" -> "true")
      catalog.createTable(tempIdent, source.schema, source.partitioning, properties.asJava)

      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        logInfo(s"Creating Iceberg table metadata for data files in $sourceName")
        files.orderBy($"path")
            .coalesce(10)
            .foreachPartition(addFiles(serializableConf, applicationId, mcCatalog, db, tempName))

        logInfo(s"Finished loading data into $tempIdent")
        logInfo(s"Updating location of table $tempIdent to $location")
        catalog.alterTable(tempIdent, TableChange.setProperty("location", location))

      })(catchBlock = {
        catalog.dropTable(ident)
      })

      logInfo(s"Renaming $tempIdent to $ident")
      spark.sessionState.catalog.renameTable(tempIdent, ident)

    })(catchBlock = {
      logInfo(s"Restoring original table: renaming $hiveIdent to $ident")
      spark.sessionState.catalog.renameTable(hiveIdent, ident)
    })

    // clear the cache for the original table because it changed.
    spark.sessionState.catalog.refreshTable(ident)

    spark.sparkContext.parallelize(Seq.empty, 1)
  }

  override def output: Seq[Attribute] = Seq.empty
}
