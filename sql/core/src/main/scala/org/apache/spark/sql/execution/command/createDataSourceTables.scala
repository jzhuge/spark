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

import com.netflix.iceberg.PartitionSpec
import com.netflix.iceberg.metacat.MetacatTables
import com.netflix.iceberg.spark.SparkSchemaUtil
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.v2.DataSourceV2

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

    val cls = DataSource.lookupDataSource(table.provider.get)
    if (classOf[DataSourceV2].isAssignableFrom(cls)) {
      if (table.provider.get == "iceberg") {
        val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
        val hiveEnv = hadoopConf.get("spark.sql.hive.env", "prod")
        val catalog = conf.getConfString("spark.sql.metacat.write.catalog", hiveEnv + "hive")
        val db = table.identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
        val tableName = table.identifier.table

        val tables = new MetacatTables(
          hadoopConf, sparkSession.sparkContext.applicationId, catalog)

        val nonIdentityPartitions = table.partitionColumnNames.filter {
          case Year(_) => true
          case Month(_) => true
          case Day(_) => true
          case Hour(_) => true
          case Bucket(_, _) => true
          case Truncate(_, _) => true
          case _ => false
        }.toSet

        // filter our the partition columns, except for identity partitions
        val baseSchema = table.schema.toAttributes
            .filterNot(a => nonIdentityPartitions.contains(a.name))
            .toStructType

        val schema = SparkSchemaUtil.convert(baseSchema)
        val specBuilder = PartitionSpec.builderFor(schema)
        table.partitionColumnNames.foreach {
          case Year(name) =>
            specBuilder.year(name)
          case Month(name) =>
            specBuilder.month(name)
          case Day(name) =>
            specBuilder.day(name)
          case Hour(name) =>
            specBuilder.hour(name)
          case Bucket(name, numBuckets) =>
            specBuilder.bucket(name, numBuckets.toInt)
          case Truncate(name, width) =>
            specBuilder.truncate(name, width.toInt)
          case name: String =>
            specBuilder.identity(name)
          case other =>
            throw new SparkException(s"Cannot determine partition type: $other")
        }

        tables.create(schema, specBuilder.build, db, tableName)
      }

    } else {
      // Create the relation to validate the arguments before writing the metadata to the
      // metastore, and infer the table schema and partition if users didn't specify schema in
      // CREATE TABLE.
      val pathOption = table.storage.locationUri.map("path" -> _)
      // Fill in some default table options from the session conf
      val tableWithDefaultOptions = table.copy(
        identifier = table.identifier.copy(
          database = Some(
            table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
        tracksPartitionsInCatalog = sparkSession.sessionState.conf.manageFilesourcePartitions)
      val dataSource: BaseRelation =
        DataSource(
          sparkSession = sparkSession,
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          className = table.provider.get,
          bucketSpec = table.bucketSpec,
          options = table.storage.properties ++ pathOption,
          catalogTable = Some(tableWithDefaultOptions)).resolveRelation()

      dataSource match {
        case fs: HadoopFsRelation =>
          if (table.tableType == CatalogTableType.EXTERNAL && fs.location.rootPaths.isEmpty) {
            throw new AnalysisException(
              "Cannot create a file-based external data source table without path")
          }
        case _ =>
      }

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

      val newTable = table.copy(
        schema = dataSource.schema,
        partitionColumnNames = partitionColumnNames,
        // If metastore partition management for file source tables is enabled, we start off with
        // partition provider hive, but no partitions in the metastore. The user has to call
        // `msck repair table` to populate the table partitions.
        tracksPartitionsInCatalog = partitionColumnNames.nonEmpty &&
            sparkSession.sessionState.conf.manageFilesourcePartitions)
      // We will return Nil or throw exception at the beginning if the table already exists, so
      // when we reach here, the table should not exist and we should set `ignoreIfExists` to
      // false.
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }

    Seq.empty[Row]
  }
}

object CreateDataSourceTableCommand {
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
    query: LogicalPlan)
  extends RunnableCommand {

  override def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    assert(table.schema.isEmpty)

    val provider = table.provider.get
    val specifiedProvider = DataSource.lookupDataSource(provider)
    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString

    val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
      Some(sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }

    val isS3 = tableLocation.map(new Path(_).toUri)
        .flatMap(uri => Option(uri.getScheme))
        .exists(_.startsWith("s3"))
    val willUseS3Committer = isS3 && sparkSession.sessionState.conf.useS3OutputCommitter

    var createMetastoreTable = false
    // We may need to reorder the columns of the query to match the existing table.
    var reorderedColumns = Option.empty[Seq[NamedExpression]]
    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          val existingTable = sessionState.catalog.getTableMetadata(tableIdentWithDB)

          if (existingTable.provider.get == DDLUtils.HIVE_PROVIDER) {
            throw new AnalysisException(s"Saving data in the Hive serde table $tableName is " +
              "not supported yet. Please use the insertInto() API as an alternative.")
          }

          // Check if the specified data source match the data source of the existing table.
          val existingProvider = DataSource.lookupDataSource(existingTable.provider.get)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).
          if (existingProvider != specifiedProvider) {
            throw new AnalysisException(s"The format of the existing table $tableName is " +
              s"`${existingProvider.getSimpleName}`. It doesn't match the specified format " +
              s"`${specifiedProvider.getSimpleName}`.")
          }

          if (query.schema.length != existingTable.schema.length) {
            throw new AnalysisException(
              s"The column number of the existing table $tableName" +
                s"(${existingTable.schema.catalogString}) doesn't match the data schema" +
                s"(${query.schema.catalogString})")
          }

          val resolver = sessionState.conf.resolver
          val tableCols = existingTable.schema.map(_.name)

          reorderedColumns = Some(existingTable.schema.map { f =>
            query.resolve(Seq(f.name), resolver).getOrElse {
              val inputColumns = query.schema.map(_.name).mkString(", ")
              throw new AnalysisException(
                s"cannot resolve '${f.name}' given input columns: [$inputColumns]")
            }
          })

          // In `AnalyzeCreateTable`, we verified the consistency between the user-specified table
          // definition(partition columns, bucketing) and the SELECT query, here we also need to
          // verify the the consistency between the user-specified table definition and the existing
          // table definition.

          // Check if the specified partition columns match the existing table.
          val specifiedPartCols = CatalogUtils.normalizePartCols(
            tableName, tableCols, table.partitionColumnNames, resolver)
          if (specifiedPartCols != existingTable.partitionColumnNames) {
            throw new AnalysisException(
              s"""
                |Specified partitioning does not match that of the existing table $tableName.
                |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
                |Existing partition columns: [${existingTable.partitionColumnNames.mkString(", ")}]
              """.stripMargin)
          }

          // Check if the specified bucketing match the existing table.
          val specifiedBucketSpec = table.bucketSpec.map { bucketSpec =>
            CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
          }
          if (specifiedBucketSpec != existingTable.bucketSpec) {
            val specifiedBucketString =
              specifiedBucketSpec.map(_.toString).getOrElse("not bucketed")
            val existingBucketString =
              existingTable.bucketSpec.map(_.toString).getOrElse("not bucketed")
            throw new AnalysisException(
              s"""
                |Specified bucketing does not match that of the existing table $tableName.
                |Specified bucketing: $specifiedBucketString
                |Existing bucketing: $existingBucketString
              """.stripMargin)
          }

        case SaveMode.Overwrite =>
          if (!willUseS3Committer) {
            sessionState.catalog.dropTable(
              tableIdentWithDB, ignoreIfNotExists = true, purge = false)
            // Need to create the table again.
            createMetastoreTable = true
          }
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = Dataset.ofRows(sparkSession, query)
    val df = reorderedColumns match {
      // Reorder the columns of the query to match the existing table.
      case Some(cols) => data.select(cols.map(Column(_)): _*)
      case None => data
    }

    // Create the relation based on the data of df.
    val pathOption = tableLocation.map("path" -> _)
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = Some(table))

    val finalSchema = specifiedProvider.newInstance() match {
      case format: FileFormat =>
        df.schema.asNullable
      case _ =>
        df.schema
    }

    if (createMetastoreTable && willUseS3Committer) {
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        schema = finalSchema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }

    val result = try {
      dataSource.writeAndRead(mode, df.logicalPlan)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table $tableName in $mode mode", ex)
        throw ex
    }

    // if not using the S3 committer, the table must be created after the insert runs.
    // this is because it uses InsertIntoHadoopFsRelationCommand because the call to
    // writeAndRead passes the logical plan.
    if (createMetastoreTable && !willUseS3Committer) {
      val newTable = table.copy(
        storage = table.storage.copy(locationUri = tableLocation),
        // We will use the schema of resolved.relation as the schema of the table (instead of
        // the schema of df). It is important since the nullability may be changed by the relation
        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
        schema = result.schema)
      sessionState.catalog.createTable(newTable, ignoreIfExists = false)
    }

    result match {
      case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
          sparkSession.sqlContext.conf.manageFilesourcePartitions &&
          !willUseS3Committer =>
        // Need to recover partitions into the metastore so our saved data is visible.
        sparkSession.sessionState.executePlan(
          AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
      case _ =>
    }

    // Refresh the cache of the table in the catalog.
    sessionState.catalog.refreshTable(tableIdentWithDB)
    Seq.empty[Row]
  }
}
