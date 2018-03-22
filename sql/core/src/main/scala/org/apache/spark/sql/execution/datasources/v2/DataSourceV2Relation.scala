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
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema, WriteSupport}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

case class DataSourceV2Relation(
    source: DataSourceV2,
    options: Map[String, String],
    path: Option[String] = None,
    table: Option[TableIdentifier] = None,
    projection: Option[Seq[AttributeReference]] = None,
    filters: Option[Seq[Expression]] = None,
    userSchema: Option[StructType] = None) extends LeafNode with CatalogRelation {

  override def simpleString: String = {
    s"DataSourceV2Relation(source=$sourceName, schema=$schema, options=$options)"
  }

  override lazy val schema: StructType = reader.readSchema()

  override lazy val output: Seq[AttributeReference] = {
    projection match {
      case Some(attrs) =>
        // use the projection attributes to avoid assigning new ids. fields that are not projected
        // will be assigned new ids, which is okay because they are not projected.
        val attrMap = attrs.map(a => a.name -> a).toMap
        schema.map(f => attrMap.getOrElse(f.name,
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
      case _ =>
        schema.toAttributes
    }
  }

  private lazy val v2Options: DataSourceOptions = {
    // ensure path and table options are set correctly
    val updatedOptions = new mutable.HashMap[String, String]

    updatedOptions ++= options

    path.map(p => updatedOptions.put("path", p))
    table.map { ident =>
      updatedOptions.put("table", ident.table)
      ident.database.map(db => updatedOptions.put("database", db))
    }

    new  DataSourceOptions(updatedOptions.asJava)
  }

  private val sourceName: String = {
    source match {
      case registered: DataSourceRegister =>
        registered.shortName()
      case _ =>
        source.getClass.getSimpleName
    }
  }

  lazy val (reader: DataSourceReader, unsupportedFilters: Seq[Expression]) = {
    val newReader = userSchema match {
      case Some(s) =>
        asReadSupportWithSchema.createReader(s, v2Options)
      case _ =>
        asReadSupport.createReader(v2Options)
    }

    projection.foreach { attrs =>
      DataSourceV2Relation.pushRequiredColumns(newReader, attrs.toStructType)
    }

    val remainingFilters = filters match {
      case Some(filterSeq) =>
        DataSourceV2Relation.pushFilters(newReader, filterSeq)
      case _ =>
        Nil
    }

    (newReader, remainingFilters)
  }

  def writer(dfSchema: StructType, mode: SaveMode): Option[DataSourceWriter] = {
    val writer = asWriteSupport.createWriter(UUID.randomUUID.toString, dfSchema, mode, v2Options)
    if (writer.isPresent) Some(writer.get()) else None
  }

  private lazy val asReadSupport: ReadSupport = {
    source match {
      case support: ReadSupport =>
        support
      case _: ReadSupportWithSchema =>
        // this method is only called if there is no user-supplied schema. if there is no
        // user-supplied schema and ReadSupport was not implemented, throw a helpful exception.
        throw new AnalysisException(s"Data source requires a user-supplied schema: $sourceName")
      case _ =>
        throw new AnalysisException(s"Data source is not readable: $sourceName")
    }
  }

  private lazy val asReadSupportWithSchema: ReadSupportWithSchema = {
    source match {
      case support: ReadSupportWithSchema =>
        support
      case _: ReadSupport =>
        throw new AnalysisException(
          s"Data source does not support user-supplied schema: $sourceName")
      case _ =>
        throw new AnalysisException(s"Data source is not readable: $sourceName")
    }
  }

  private lazy val asWriteSupport: WriteSupport = {
    source match {
      case support: WriteSupport =>
        support
      case _ =>
        throw new AnalysisException(s"Data source is not writable: $sourceName")
    }
  }

  override def catalogTable: CatalogTable = CatalogTable(
    table.getOrElse(path
        .map(p => TableIdentifier(p.substring(p.lastIndexOf("/") + 1)))
        .getOrElse(TableIdentifier("unknown"))),
    CatalogTableType.EXTERNAL,
    CatalogStorageFormat(options.get("path"), None, None, None, compressed = true, options),
    schema,
    Some(sourceName)
  )

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataSourceV2Relation]

  // this overrides computeStats added in b1d719e7c9faeb5661a7e712b3ecefca56bf356f (not backported)
  def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  private def pushRequiredColumns(reader: DataSourceReader, struct: StructType): Unit = {
    reader match {
      case projectionSupport: SupportsPushDownRequiredColumns =>
        projectionSupport.pruneColumns(struct)
      case _ =>
    }
  }

  private def pushFilters(reader: DataSourceReader, filters: Seq[Expression]): Seq[Expression] = {
    reader match {
      case catalystFilterSupport: SupportsPushDownCatalystFilters =>
        catalystFilterSupport.pushCatalystFilters(filters.toArray)

      case filterSupport: SupportsPushDownFilters =>
        // A map from original Catalyst expressions to corresponding translated data source
        // filters. If a predicate is not in this map, it means it cannot be pushed down.
        val translatedMap: Map[Expression, Filter] = filters.flatMap { p =>
          DataSourceStrategy.translateFilter(p).map(f => p -> f)
        }.toMap

        // Catalyst predicate expressions that cannot be converted to data source filters.
        val nonConvertiblePredicates = filters.filterNot(translatedMap.contains)

        // Data source filters that cannot be pushed down. An unhandled filter means
        // the data source cannot guarantee the rows returned can pass the filter.
        // As a result we must return it so Spark can plan an extra filter operator.
        val unhandledFilters = filterSupport.pushFilters(translatedMap.values.toArray).toSet
        val unhandledPredicates = translatedMap.filter { case (_, f) =>
          unhandledFilters.contains(f)
        }.keys

        nonConvertiblePredicates ++ unhandledPredicates

      case _ => filters
    }
  }
}
