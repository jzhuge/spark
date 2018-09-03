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

import org.apache.spark.sql.catalog.v2.{Table, V2Relation}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics, SupportsPhysicalStats}
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Implicits, DataSourceV2TableProvider}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * A logical plan representing a data source v2 scan.
 *
 * @param source An instance of a [[DataSourceV2]] implementation.
 * @param options The options for this scan. Used to create fresh [[DataSourceReader]].
 * @param userSpecifiedSchema The user-specified schema for this scan. Used to create fresh
 *                            [[DataSourceReader]].
 */
case class DataSourceV2Relation(
    source: DataSourceV2,
    output: Seq[AttributeReference],
    options: Map[String, String],
    tableIdent: Option[TableIdentifier] = None,
    userSpecifiedSchema: Option[StructType] = None)
  extends LeafNode with MultiInstanceRelation with NamedRelation with CatalogRelation
      with SupportsPhysicalStats with V2Relation {

  import DataSourceV2Implicits._

  override def simpleString: String = {
    s"DataSourceV2Relation(source=$source.name, schema=$schema, options=$options)"
  }

  override def name: String = {
    tableIdent.map(_.unquotedString).getOrElse(s"${source.name}:unknown")
  }

  def newReader(): DataSourceReader = source.createReader(options, userSpecifiedSchema)

  def newWriter(): DataSourceWriter = source.createWriter(options, schema)

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }

  private lazy val tableIdentifier: TableIdentifier = {
    tableIdent.orElse(options.get("table").map(TableIdentifier(_, options.get("database"))))
        .orElse(options.get("path").map(p => TableIdentifier(p.substring(p.lastIndexOf("/") + 1))))
        .getOrElse(TableIdentifier("unknown"))
  }

  lazy val catalogTable: CatalogTable = CatalogTable(
    tableIdentifier,
    CatalogTableType.EXTERNAL,
    CatalogStorageFormat(options.get("path"), None, None, None, compressed = true, options),
    schema,
    Some(source.name)
  )

  /**
   * Used to return statistics when filters and projection are available.
   */
  override def computeStats(
      projection: Seq[NamedExpression],
      filters: Seq[Expression]): Statistics = newReader() match {
    case r: SupportsReportStatistics =>
      DataSourceV2Strategy.pushFilters(r, filters)
      val stats = r.getStatistics

      if (stats.numRows.isPresent) {
        val numRows = stats.numRows.getAsLong
        val rowSizeEstimate = projection.map(_.dataType.defaultSize).sum + 8
        val sizeEstimate = numRows * rowSizeEstimate
        logInfo(s"Row-based size estimate for ${catalogTable.identifier}: " +
            s"$numRows rows * $rowSizeEstimate bytes = $sizeEstimate bytes")
        Statistics(sizeInBytes = sizeEstimate, rowCount = Some(numRows))
      } else {
        logInfo(s"Fallback size estimate for ${catalogTable.identifier}: ${stats.sizeInBytes}")
        Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
      }

    case _ =>
      logInfo(s"Default size estimate for ${catalogTable.identifier}: ${conf.defaultSizeInBytes}")
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  override def statistics: Statistics = newReader() match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

/**
 * A logical plan representing a data source v2 table.
 *
 * @param ident The table's TableIdentifier.
 * @param table The table.
 * @param output The output attributes of the table.
 * @param options The options for this scan or write.
 */
case class TableV2Relation(
    catalogName: String,
    ident: TableIdentifier,
    table: Table,
    output: Seq[AttributeReference],
    options: Map[String, String])
    extends LeafNode with MultiInstanceRelation with NamedRelation with SupportsPhysicalStats {

  import DataSourceV2Implicits._

  override def name: String = ident.unquotedString

  override def simpleString: String =
    s"RelationV2 $name ${Utils.truncatedString(output, "[", ", ", "]")}"

  def newReader(): DataSourceReader = table.createReader(options, None)

  /**
   * Used to return statistics when filters and projection are available.
   */
  override def computeStats(
      projection: Seq[NamedExpression],
      filters: Seq[Expression]): Statistics = newReader() match {
    case r: SupportsReportStatistics =>
      DataSourceV2Strategy.pushFilters(r, filters)
      val stats = r.getStatistics

      if (stats.numRows.isPresent) {
        val numRows = stats.numRows.getAsLong
        val rowSizeEstimate = projection.map(_.dataType.defaultSize).sum + 8
        val sizeEstimate = numRows * rowSizeEstimate
        logInfo(s"Row-based size estimate for $ident: " +
            s"$numRows rows * $rowSizeEstimate bytes = $sizeEstimate bytes")
        Statistics(sizeInBytes = sizeEstimate, rowCount = Some(numRows))
      } else {
        logInfo(s"Fallback size estimate for $ident: ${stats.sizeInBytes}")
        Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
      }

    case _ =>
      logInfo(s"Default size estimate for $ident: ${conf.defaultSizeInBytes}")
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  override def newInstance(): TableV2Relation = {
    copy(output = output.map(_.newInstance()))
  }
}

object DataSourceV2Relation {

  import DataSourceV2Implicits._

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      tableIdent: Option[TableIdentifier] = None,
      userSpecifiedSchema: Option[StructType] = None): NamedRelation = {
    val ident = tableIdent.orElse(options.table)
    source match {
      case tableProvider: DataSourceV2TableProvider =>
        val identifier = ident.getOrElse(TableIdentifier("anonymous"))
        val table = tableProvider.createTable(options.asDataSourceOptions)
        TableV2Relation(
          source.name, identifier, table, table.schema.toAttributes, options)

      case _ =>
        val reader = source.createReader(options, userSpecifiedSchema)
        DataSourceV2Relation(
          source, reader.readSchema().toAttributes, options, ident, userSpecifiedSchema)
    }
  }

  def create(
      catalogName: String,
      ident: TableIdentifier,
      table: Table,
      options: Map[String, String]): NamedRelation = {
    TableV2Relation(catalogName, ident, table, table.schema.toAttributes, options)
  }
}
