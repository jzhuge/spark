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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2.V2Relation
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics, SupportsPhysicalStats}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsReportStatistics}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

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
  extends LeafNode
    with MultiInstanceRelation
    with NamedRelation
    with CatalogRelation
    with V2Relation
    with SupportsPhysicalStats
    with DataSourceV2StringFormat {

  import DataSourceV2Relation._

  override def name: String = {
    tableIdent.map(_.unquotedString).getOrElse(s"${source.name}:unknown")
  }

  override def pushedFilters: Seq[Expression] = Seq.empty

  override def simpleString: String = "RelationV2 " + metadataString

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
    CatalogStorageFormat(options.get("path").map(CatalogUtils.stringToURI), None, None, None,
      compressed = true, options),
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

  override def computeStats(): Statistics = newReader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

/**
 * A specialization of [[DataSourceV2Relation]] with the streaming bit set to true.
 *
 * Note that, this plan has a mutable reader, so Spark won't apply operator push-down for this plan,
 * to avoid making the plan mutable. We should consolidate this plan and [[DataSourceV2Relation]]
 * after we figure out how to apply operator push-down for streaming data sources.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    reader: DataSourceReader)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString: String = "Streaming RelationV2 " + metadataString

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  implicit class SourceHelpers(source: DataSourceV2) {
    def asReadSupport: ReadSupport = {
      source match {
        case support: ReadSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
      }
    }

    def asWriteSupport: WriteSupport = {
      source match {
        case support: WriteSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not writable: $name")
      }
    }

    def name: String = {
      source match {
        case registered: DataSourceRegister =>
          registered.shortName()
        case _ =>
          source.getClass.getSimpleName
      }
    }

    def createReader(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      val v2Options = new DataSourceOptions(options.asJava)
      userSpecifiedSchema match {
        case Some(s) =>
          asReadSupport.createReader(s, v2Options)
        case _ =>
          asReadSupport.createReader(v2Options)
      }
    }

    def createWriter(
        options: Map[String, String],
        schema: StructType): DataSourceWriter = {
      val v2Options = new DataSourceOptions(options.asJava)
      asWriteSupport.createWriter(UUID.randomUUID.toString, schema, SaveMode.Append, v2Options).get
    }
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      tableIdent: Option[TableIdentifier] = None,
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val reader = source.createReader(options, userSpecifiedSchema)
    val ident = tableIdent.orElse(tableFromOptions(options))
    DataSourceV2Relation(
      source, reader.readSchema().toAttributes, options, ident, userSpecifiedSchema)
  }

  private def tableFromOptions(options: Map[String, String]): Option[TableIdentifier] = {
    options
        .get(DataSourceOptions.TABLE_KEY)
        .map(TableIdentifier(_, options.get(DataSourceOptions.DATABASE_KEY)))
  }
}
