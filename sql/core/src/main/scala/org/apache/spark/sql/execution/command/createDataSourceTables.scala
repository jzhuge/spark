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

package org.apache.spark.sql.execution.command

import java.net.URI

import com.netflix.iceberg.PartitionSpec
import com.netflix.iceberg.metacat.MetacatTables
import com.netflix.iceberg.spark.SparkSchemaUtil

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

/**
 * A command used to create a data source table.
 *
 * Note: This is different from [[CreateTableCommand]]. Please check the syntax for difference.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   [(col1 data_type [COMMENT col_comment], ...)]
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 * }}}
 */
case class CreateDataSourceTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand {

  import CreateDataSourceTableCommand._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    if (sessionState.catalog.tableExists(table.identifier)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table ${table.identifier.unquotedString} already exists.")
      }
    }

    if (table.provider.get == "iceberg") {
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val hiveEnv = hadoopConf.get("spark.sql.hive.env", "prod")
      val catalog = conf.getConfString("spark.sql.metacat.write.catalog", hiveEnv + "hive")
      val db = table.identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
      val tableName = table.identifier.table

      val tables = new MetacatTables(
        hadoopConf, sparkSession.sparkContext.applicationId, catalog)

      val nonIdentityPartitions = table.partitionColumnNames.filter {
        case Year(name) if table.schema.fieldNames.contains(name) => true
        case Month(name) if table.schema.fieldNames.contains(name) => true
        case Day(name) if table.schema.fieldNames.contains(name) => true
        case Hour(name) if table.schema.fieldNames.contains(name) => true
        case Bucket(name, _) if table.schema.fieldNames.contains(name) => true
        case Truncate(name, _) if table.schema.fieldNames.contains(name) => true
        case _ => false
      }.toSet

      // filter our the partition columns, except for identity partitions
      val baseSchema = table.schema.toAttributes
          .filterNot(a => nonIdentityPartitions.contains(a.name))
          .toStructType

      val schema = SparkSchemaUtil.convert(baseSchema)
      val specBuilder = PartitionSpec.builderFor(schema)
      table.partitionColumnNames.foreach {
        case Year(name) if schema.findField(name) != null =>
          specBuilder.year(name)
        case Month(name) if schema.findField(name) != null =>
          specBuilder.month(name)
        case Day(name) if schema.findField(name) != null =>
          specBuilder.day(name)
        case Hour(name) if schema.findField(name) != null =>
          specBuilder.hour(name)
        case Bucket(name, numBuckets) if schema.findField(name) != null =>
          specBuilder.bucket(name, numBuckets.toInt)
        case Truncate(name, width) if schema.findField(name) != null =>
          specBuilder.truncate(name, width.toInt)
        case Identity(name) if schema.findField(name) != null =>
          specBuilder.identity(name)
        case name: String =>
          specBuilder.identity(name)
        case other =>
          throw new SparkException(s"Cannot determine partition type: $other")
      }

      tables.create(schema, specBuilder.build, db, tableName)
    } else {
      // Create the relation to validate the arguments before writing the metadata to metastore,
      // and infer the table schema and partition if users didn't specify schema in CREATE TABLE.
      val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      // Fill in some default table options from the session conf
      val tableWithDefaultOptions = table.copy(
        identifier = table.identifier.copy(
          database = Some(
            table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
        tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)
      val dataSource: BaseRelation =
        DataSource(
          sparkSession = sparkSession,
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          className = table.provider.get,
          bucketSpec = table.bucketSpec,
          options = table.storage.properties ++ pathOption,
          // As discussed in SPARK-19583, we don't check if the location is existed
          catalogTable = Some(tableWithDefaultOptions)).resolveRelation(checkFilesExist = false)

      val partitionColumnNames = if (table.schema.nonEmpty) {
        table.partitionColumnNames
      } else {
        // This is guaranteed in `PreprocessDDL`.
        assert(table.partitionColumnNames.isEmpty)
        dataSource match {
          case r: HadoopFsRelation => r.partitionSchema.fieldNames.toSeq
          case _ => Nil
        }
      }

      val newTable = dataSource match {
        // Since Spark 2.1, we store the inferred schema of data source in metastore, to avoid
        // inferring the schema again at read path. However if data source has overlapped columns
        // between data and partition schema, we can't store it in metastore as it breaks the
        // assumption of table schema. Here we fallback to the behavior of Spark prior to 2.1, store
        // empty schema in metastore and infer it at runtime. Note that this also means the new
        // scalable partitioning handling feature(introduced at Spark 2.1) is disabled in this case.
        case r: HadoopFsRelation if r.overlappedPartCols.nonEmpty =>
          logWarning("It is not recommended to create a table with overlapped data and partition " +
            "columns, as Spark cannot store a valid table schema and has to infer it at runtime, " +
            "which hurts performance. Please check your data files and remove the partition " +
            "columns in it.")
          table.copy(schema = new StructType(), partitionColumnNames = Nil)

        case _ =>
          table.copy(
            schema = dataSource.schema,
            partitionColumnNames = partitionColumnNames,
            // If metastore partition mgmt for file source tables is enabled, we start off with
            // partition provider hive, but no partitions in the metastore. The user has to call
            // `msck repair table` to populate the table partitions.
            tracksPartitionsInCatalog = partitionColumnNames.nonEmpty &&
              sessionState.conf.manageFilesourcePartitions)

      }

      // We will return Nil or throw exception at the beginning if the table already exists, so when
      // we reach here, the table should not exist and we should set `ignoreIfExists` to false.
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }

    Seq.empty[Row]
  }
}

object CreateDataSourceTableCommand {
  lazy val Identity = "^(\\w+)_identity$".r
  lazy val Year = "^(\\w+)_year$".r
  lazy val Month = "^(\\w+)_month$".r
  lazy val Day = "^(\\w+)_day$".r
  lazy val Hour = "^(\\w+)_hour$".r
  lazy val Bucket = "^(\\w+)_bucket_(\\d+)$".r
  lazy val Truncate = "^(\\w+)_truncate_(\\d+)$".r
}

/**
 * A command used to create a data source table using the result of a query.
 *
 * Note: This is different from `CreateHiveTableAsSelectCommand`. Please check the syntax for
 * difference. This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 *   AS SELECT ...
 * }}}
 */
case class CreateDataSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan,
    outputColumnNames: Seq[String])
  extends DataWritingCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, child, SaveMode.Append, tableExists = true)
    } else {
      assert(table.schema.isEmpty)

      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, child, SaveMode.Overwrite, tableExists = false)
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)

      result match {
        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
            sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
        case _ =>
      }
    }

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      physicalPlan: SparkPlan,
      mode: SaveMode,
      tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `query`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) Some(table) else None)

    try {
      dataSource.writeAndRead(mode, query, outputColumnNames, physicalPlan)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
        throw ex
    }
  }
}
