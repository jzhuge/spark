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

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalog.v2.{PartitionUtil, Table, TableChange, V1MetadataTable}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, AppendData, CreateTable, CreateTableAsSelect, InsertIntoTable, LogicalPlan, OverwritePartitionsDynamic, Project, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetPropertiesCommand}
import org.apache.spark.sql.execution.datasources
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.types.StructType

case class V2AsBaseRelation(
    sqlContext: SQLContext,
    v2relation: NamedRelation,
    catalogTable: CatalogTable) extends BaseRelation {
  override lazy val schema: StructType = v2relation.output.toStructType
}

/**
 * Rules to convert from plans produced by the SQL parser to v2 logical plans.
 */
class DataSourceV2Analysis(spark: SparkSession) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
  import DataSourceV2Analysis._

  // TODO: after identifier is extended to include catalog, load the correct catalog for each ident
  private val catalog = spark.catalog(None).asTableCatalog

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case alter @ AlterTableAddColumnsCommand(ident, columns) =>
      val identifier = fixIdent(ident)
      val table = catalog.loadTable(identifier)

      // create a relation so that the alter table command can be validated
      lazy val relation = DataSourceV2Relation.create(
        catalog.name, identifier, table,
        Map("database" -> identifier.database.get, "table" -> identifier.table))
      lazy val changes = columns.map { field =>
        val (parent, name) = field.name match {
          case NestedFieldName(path, fieldName) =>
            (path, fieldName)
          case fieldName =>
            (null, fieldName)
        }
        TableChange.addColumn(parent, name, field.dataType)
      }
      lazy val converted = AlterTable(catalog, relation, changes)

      table match {
        case table: V1MetadataTable if isV2Source(table.catalogTable, spark) =>
          converted

        case _: V1MetadataTable =>
          alter

        case _ =>
          converted
      }

    case a @ AlterTableSetPropertiesCommand(ident, properties, false /* isView */ ) =>
      a

    case datasources.CreateTable(catalogTable, mode, None)
      if isV2Source(catalogTable, spark) =>
      val options = catalogTable.storage.properties + ("provider" -> catalogTable.provider.get)

      val partitioning = PartitionUtil.convertToTransforms(
        catalogTable.partitionColumnNames, catalogTable.bucketSpec)

      mode match {
        case SaveMode.ErrorIfExists =>
          CreateTable(catalog, identifier(catalogTable), catalogTable.schema, partitioning, options,
            ignoreIfExists = false)

        case SaveMode.Ignore =>
          CreateTable(catalog, identifier(catalogTable), catalogTable.schema, partitioning, options,
            ignoreIfExists = true)

        case _ =>
          throw new AnalysisException(s"$mode cannot be used for table creation in v2 data sources")
      }

    case datasources.CreateTable(catalogTable, mode, Some(query))
        if isV2Source(catalogTable, spark) =>
      val options = catalogTable.storage.properties + ("provider" -> catalogTable.provider.get)

      if (catalogTable.partitionColumnNames.nonEmpty) {
        throw new AnalysisException("CTAS with partitions is not yet supported.")
      }

      mode match {
        case SaveMode.Append =>
          throw new AnalysisException("Append mode cannot be used with CTAS for v2 data sources")

        case SaveMode.ErrorIfExists =>
          CreateTableAsSelect(catalog, identifier(catalogTable), Seq.empty, query, options,
            ignoreIfExists = false)

        case SaveMode.Ignore =>
          CreateTableAsSelect(catalog, identifier(catalogTable), Seq.empty, query, options,
            ignoreIfExists = true)

        case SaveMode.Overwrite =>
          ReplaceTableAsSelect(catalog, identifier(catalogTable), Seq.empty, query, options)
      }

    case insert @ InsertIntoTable(LogicalRelation(v2: V2AsBaseRelation, _, _, _), _, _, _, _, _) =>
      // the DataFrame API doesn't create INSERT INTO plans for v2 tables, so this must be
      // SQL and should match columns by position, not by name.

      // ifNotExists is append with validation, but validation is not supported
      if (insert.ifPartitionNotExists) {
        throw new AnalysisException(s"Cannot write, IF NOT EXISTS is not supported for table: $v2")
      }

      val staticPartitionValues: Map[String, String] =
        insert.partition.filter(_._2.isDefined).mapValues(_.get)

      val query = if (staticPartitionValues.isEmpty) {
        insert.query

      } else {
        // add any static value as a literal column
        val output = v2.v2relation.output

        // check that the data column counts match
        val numColumns = output.size
        if (numColumns > staticPartitionValues.size + insert.query.output.size) {
          throw new AnalysisException(s"Cannot write: too many columns")
        } else if (numColumns < staticPartitionValues.size + insert.query.output.size) {
          throw new AnalysisException(s"Cannot write: not enough columns")
        }

        val resolver = spark.sessionState.analyzer.resolver
        val staticNames = staticPartitionValues.keySet

        // for each static name, find the column name it will replace and check for unknowns.
        val outputNameToStaticName = staticNames.map(staticName =>
          output.find(col => resolver(col.name, staticName)) match {
            case Some(attr) =>
              attr.name -> staticName
            case _ =>
              throw new AnalysisException(
                s"Cannot add static value for unknown column: $staticName")
          }).toMap

        // for each output column, add the static value as a literal or use the next input column
        val queryColumns = insert.query.output.iterator
        val projectExprs = output.map { col =>
          outputNameToStaticName.get(col.name).flatMap(staticPartitionValues.get) match {
            case Some(staticValue) =>
              Alias(Literal(staticValue), col.name)()
            case _ =>
              queryColumns.next
          }
        }

        Project(projectExprs, insert.query)
      }

      v2.v2relation match {
        case TableV2Relation(_, _, table, _, _) if useCompatBehavior(table) =>
          // NETFLIX: use batch pattern behavior for compat tables
          OverwritePartitionsDynamic.byPosition(v2.v2relation, query)

        case _ if insert.overwrite =>
          // always use dynamic overwrite to match Hive's behavior
          OverwritePartitionsDynamic.byPosition(v2.v2relation, query)

        case _ =>
          AppendData.byPosition(v2.v2relation, query)
      }

    case LogicalRelation(v2: V2AsBaseRelation, _, _, _) =>
      // unwrap v2 relation
      v2.v2relation
  }

  private def fixIdent(identifier: TableIdentifier): TableIdentifier = {
    identifier.database match {
      case Some(_) =>
        identifier
      case _ =>
        TableIdentifier(identifier.table, Some(spark.catalog.currentDatabase))
    }
  }

  private def identifier(catalogTable: CatalogTable): TableIdentifier = {
    fixIdent(catalogTable.identifier)
  }
}

object DataSourceV2Analysis {
  def apply(spark: SparkSession): DataSourceV2Analysis = new DataSourceV2Analysis(spark)

  val NestedFieldName = """([\w\.]+)\.(\w+)""".r

  private def useCompatBehavior(table: Table): Boolean = {
    table.properties().getOrDefault("spark.behavior.compatibility", "false").toBoolean
  }

  private def isV2Source(catalogTable: CatalogTable, spark: SparkSession): Boolean = {
    catalogTable.provider.exists(provider =>
      !"hive".equalsIgnoreCase(provider) &&
          classOf[DataSourceV2].isAssignableFrom(
            DataSource.lookupDataSource(provider, spark.sqlContext.conf)))
  }
}
