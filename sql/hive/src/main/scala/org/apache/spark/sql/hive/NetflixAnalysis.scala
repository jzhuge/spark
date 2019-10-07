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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, CreateTable, CreateTableAsSelect, DropTable, LogicalPlan, MigrateTable, RefreshTable, SnapshotTable, SubqueryAlias}
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
    Try(spark.catalog("iceberg")).getOrElse(spark.v1CatalogAsV2).asTableCatalog

  private lazy val icebergTables: DataSourceV2 = new IcebergMetacatSource()

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case unresolved: UnresolvedRelation =>
      val identifier = ensureDatabaseIsSet(unresolved.tableIdentifier)
      try {
        val table = icebergCatalog.loadTable(identifier)
        DataSourceV2Relation.create(icebergCatalog.name, identifier, table,
          Map("database" -> identifier.database.get, "table" -> identifier.table))
      } catch {
        case _: NoSuchTableException =>
          unresolved
        case e: IllegalArgumentException if e.getMessage.contains("not Iceberg") =>
          unresolved
      }

    case migrate @ MigrateTable(identifier, _) =>
      val newIdent = ensureDatabaseIsSet(identifier)
      if (newIdent != identifier) {
        migrate.copy(
          identifier = TableIdentifier(identifier.table, Some(spark.catalog.currentDatabase)))
      } else {
        migrate
      }

    case snapshot @ SnapshotTable(target, source, _) =>
      val newTarget = ensureDatabaseIsSet(target)
      val newSource = ensureDatabaseIsSet(source)
      if (newTarget != target || newSource != source) {
        snapshot.copy(targetTable = newTarget, sourceTable = newSource)
      } else {
        snapshot
      }

    // replace the default v2 catalog with one for Iceberg tables
    case refresh @ RefreshTable(cat, ident)
        if cat != icebergCatalog && cat.loadTable(ident).getClass.getName.contains("iceberg") =>
      refresh.copy(catalog = icebergCatalog)

    case alter @ AlterTable(cat, rel: TableV2Relation, _)
        if cat != icebergCatalog && rel.table.getClass.getName.contains("iceberg") =>
      alter.copy(catalog = icebergCatalog)

    case create @ CreateTable(catalog, _, _, _, _, options, _)
        if shouldReplaceCatalog(catalog, options.get("provider")) =>
      create.copy(catalog = icebergCatalog)

    case ctas @ CreateTableAsSelect(catalog, _, _, _, _, options, _)
        if shouldReplaceCatalog(catalog, options.get("provider")) =>
      ctas.copy(catalog = icebergCatalog)

    case drop @ DropTable(cat, ident, _)
        if cat != icebergCatalog && cat.loadTable(ident).getClass.getName.contains("iceberg") =>
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

  private def ensureDatabaseIsSet(identifier: TableIdentifier): TableIdentifier = {
    identifier.database match {
      case Some(_) =>
        identifier
      case _ =>
        TableIdentifier(identifier.table, Some(spark.catalog.currentDatabase))
    }
  }
}

object NetflixAnalysis {
  def apply(spark: SparkSession): NetflixAnalysis = new NetflixAnalysis(spark)

  def isIcebergTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.properties.get("table_type").exists("iceberg".equalsIgnoreCase)
  }
}
