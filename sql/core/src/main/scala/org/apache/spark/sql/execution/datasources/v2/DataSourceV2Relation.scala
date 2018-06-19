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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsReportStatistics}
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
    path: Option[String] = None,
    table: Option[TableIdentifier] = None,
    userSpecifiedSchema: Option[StructType] = None)
  extends LeafNode with MultiInstanceRelation with CatalogRelation with DataSourceV2StringFormat {

  import DataSourceV2Relation._

  override def pushedFilters: Seq[Expression] = Seq.empty

  override def simpleString: String = "RelationV2 " + metadataString

  lazy val v2Options: DataSourceOptions = makeV2Options(options, path, table)

  def newReader(): DataSourceReader = source.createReader(options, v2Options, userSpecifiedSchema)

  lazy val writer = (dfSchema: StructType, mode: SaveMode) => {
    val writer =
      source.asWriteSupport.createWriter(UUID.randomUUID.toString, dfSchema, mode, v2Options)
    if (writer.isPresent) Some(writer.get()) else None
  }

  override def computeStats(): Statistics = newReader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  override def catalogTable: CatalogTable = CatalogTable(
    table.getOrElse(path
        .map(p => TableIdentifier(p.substring(p.lastIndexOf("/") + 1)))
        .getOrElse(TableIdentifier("unknown"))),
    CatalogTableType.EXTERNAL,
    CatalogStorageFormat(options.get("path").map(CatalogUtils.stringToURI), None, None, None,
      compressed = true, options),
    schema,
    Some(source.name)
  )

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
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
        v2Options: DataSourceOptions,
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      userSpecifiedSchema match {
        case Some(s) =>
          asReadSupportWithSchema.createReader(s, v2Options)
        case _ =>
          asReadSupport.createReader(v2Options)
      }
    }
  }

  private def makeV2Options(options: Map[String, String], path: Option[String],
                            table: Option[TableIdentifier]): DataSourceOptions = {
    // ensure path and table options are set correctly
    val updatedOptions = new mutable.HashMap[String, String]

    updatedOptions ++= options

    path.foreach(p => updatedOptions.put("path", p))
    table.foreach { ident =>
      updatedOptions.put("table", ident.table)
      ident.database.map(db => updatedOptions.put("database", db))
    }

    new DataSourceOptions(updatedOptions.asJava)
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      path: Option[String] = None,
      table: Option[TableIdentifier] = None,
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val v2Options = makeV2Options(options, path, table)
    val reader = source.createReader(options, v2Options, userSpecifiedSchema)
    DataSourceV2Relation(
      source, reader.readSchema().toAttributes, options, path, table, userSpecifiedSchema)
  }
}
