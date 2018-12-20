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

package org.apache.spark.sql.hive

import scala.util.Try

import com.netflix.iceberg.spark.source.IcebergMetacatSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, TableCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, CreateTable, CreateTableAsSelect, DropTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, TableV2Relation, V2AsBaseRelation}
import org.apache.spark.sql.sources.v2.DataSourceV2

/**
 * Analysis rules specific to Netflix.
 */
class NetflixAnalysis(spark: SparkSession) extends Rule[LogicalPlan] {
  import CatalogV2Implicits._
  import NetflixAnalysis._

  private lazy val icebergCatalog: TableCatalog =
    Try(spark.catalog(Some("iceberg"))).getOrElse(spark.catalog(None)).asTableCatalog
  private lazy val icebergTables: DataSourceV2 = new IcebergMetacatSource()

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // replace the default v2 catalog with one for Iceberg tables
    case alter @ AlterTable(cat, rel: TableV2Relation, _)
        if cat != icebergCatalog && rel.table.getClass.getName.contains("iceberg") =>
      alter.copy(catalog = icebergCatalog)

    case create @ CreateTable(catalog, _, _, _, options, _)
        if shouldReplaceCatalog(catalog, options.get("provider")) =>
      create.copy(catalog = icebergCatalog)

    case ctas @ CreateTableAsSelect(catalog, _, _, _, options, _)
        if shouldReplaceCatalog(catalog, options.get("provider")) =>
      ctas.copy(catalog = icebergCatalog)

    case drop @ DropTable(catalog, identifier, _)
        if shouldReplaceCatalog(catalog,
          Option(catalog.loadTable(identifier).properties.get("provider"))) =>
      drop.copy(catalog = icebergCatalog)

    // this case is only used for older iceberg tables that don't have the provider set
    case rel: LogicalRelation if rel.catalogTable.map(isIcebergTable).getOrElse(false) =>
      toLogicalRelation(rel)
  }

  def toLogicalRelation(rel: LogicalRelation): LogicalRelation = {
    val ident = rel.catalogTable.get.identifier
    val relation = DataSourceV2Relation.create(icebergTables,
      Map("database" -> ident.database.get, "table" -> ident.table), Some(ident))

    LogicalRelation(V2AsBaseRelation(spark.sqlContext, relation, rel.catalogTable.get))
  }

  def shouldReplaceCatalog(catalog: TableCatalog, provider: Option[String]): Boolean = {
    catalog != icebergCatalog && provider.exists("iceberg".equalsIgnoreCase)
  }
}

object NetflixAnalysis {
  def apply(spark: SparkSession): NetflixAnalysis = new NetflixAnalysis(spark)

  def isIcebergTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.get("table_type").exists("iceberg".equalsIgnoreCase)
  }
}
