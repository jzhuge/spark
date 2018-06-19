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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics, SupportsPhysicalStats}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema, WriteSupport}
import org.apache.spark.sql.sources.v2.reader._
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
    userSpecifiedSchema: Option[StructType])
  extends LeafNode with MultiInstanceRelation with CatalogRelation with SupportsPhysicalStats {

  import DataSourceV2Relation._

  override def simpleString: String = {
    s"DataSourceV2Relation(source=$source.name, schema=$schema, options=$options)"
  }

  def newReader(): DataSourceReader = source.createReader(options, userSpecifiedSchema)

  def newWriter(dfSchema: StructType, mode: SaveMode): Option[DataSourceWriter] = {
    val writer = source.asWriteSupport.createWriter(
      UUID.randomUUID.toString, dfSchema, mode, new DataSourceOptions(options.asJava))
    if (writer.isPresent) Some(writer.get()) else None
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }

  lazy val tableIdentifier: TableIdentifier = {
    options.get("table").map(TableIdentifier(_, options.get("database")))
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

object DataSourceV2Relation {
  private implicit class SourceHelpers(source: DataSourceV2) {
    def asReadSupport: ReadSupport = {
      source match {
        case support: ReadSupport =>
          support
        case _: ReadSupportWithSchema =>
          // this method is only called if there is no user-supplied schema. if there is no
          // user-supplied schema and ReadSupport was not implemented, throw a helpful exception.
          throw new AnalysisException(s"Data source requires a user-supplied schema: $name")
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
      }
    }

    def asReadSupportWithSchema: ReadSupportWithSchema = {
      source match {
        case support: ReadSupportWithSchema =>
          support
        case _: ReadSupport =>
          throw new AnalysisException(
            s"Data source does not support user-supplied schema: $name")
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
          asReadSupportWithSchema.createReader(s, v2Options)
        case _ =>
          asReadSupport.createReader(v2Options)
      }
    }
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val reader = source.createReader(options, userSpecifiedSchema)
    DataSourceV2Relation(source, reader.readSchema().toAttributes, options, userSpecifiedSchema)
  }
}
