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
package org.apache.spark.sql.execution.datasources

import java.net.URI

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogStatistics, CatalogTable, MaybeCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation
import org.apache.spark.sql.execution.datasources.v2.V2AsBaseRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 */
case class LogicalRelation(
    relation: BaseRelation,
    output: Seq[AttributeReference],
    catalogTable: Option[CatalogTable],
    override val isStreaming: Boolean)
  extends LeafNode with MultiInstanceRelation with MaybeCatalogRelation {

  def name: String = {
    val catalogTableName = catalogTable.map(_.identifier).map { ident =>
      ident.database match {
        case Some(db) =>
          s"$db.${ident.table}"
        case _ =>
          ident.table
      }
    }

    catalogTableName.getOrElse(relation match {
      case fsRel: HadoopFsRelation =>
        fsRel.location.rootPaths.mkString(",")
      case jdbcRel: JDBCRelation =>
        (Option(jdbcRel.jdbcOptions.table), Option(jdbcRel.jdbcOptions.url)) match {
          case (Some(table), Some(url)) =>
            addTableToURI(table, url)
          case (None, Some(url)) =>
            url
          case (Some(table), None) =>
            table
          case _ =>
            "jdbc:unknown"
        }
      case _ =>
        "unknown"
    })
  }

  def addTableToURI(table: String, url: String): String = {
    try {
      val uri = URI.create(url)
      Option(uri.getPath) match {
        case Some(path) =>
          new URI(
            uri.getScheme,
            null,
            uri.getHost,
            uri.getPort,
            s"$path/$table",
            null,
            uri.getFragment).toString
        case _ =>
          Option(uri.getScheme) match {
            case Some(scheme) =>
              val inner = URI.create(addTableToURI(table, uri.getRawSchemeSpecificPart))
              new URI(
                scheme,
                s"${inner.getScheme}:${inner.getSchemeSpecificPart}",
                uri.getFragment).toString
            case _ =>
              new URI(
                null,
                s"${uri.getSchemeSpecificPart}/$table",
                uri.getFragment).toString
          }
      }
    } catch {
      case NonFatal(_) =>
        table
    }
  }

  override def asCatalogRelation: Option[CatalogRelation] = {
    relation match {
      case c: CatalogRelation =>
        Some(c)
      case m: MaybeCatalogRelation =>
        m.asCatalogRelation
      case _ =>
        None
    }
  }

  override lazy val resolved: Boolean = relation match {
    case _: V2AsBaseRelation => false // avoid rules that will change the plan before conversion
    case _ => expressions.forall(_.resolved) && childrenResolved
  }

  // Only care about relation when canonicalizing.
  override def doCanonicalize(): LogicalPlan = copy(
    output = output.map(QueryPlan.normalizeExprId(_, output)),
    catalogTable = None)

  override def computeStats(): Statistics = {
    // Build a CatalogStatistics from relation
    // if catalogTable does not exist or it doesn't have stats
    val stats = catalogTable.flatMap(_.stats)
      .getOrElse(CatalogStatistics(relation.sizeInBytes, relation.rowCount))
    val tableName = catalogTable.map(_.identifier).getOrElse(relation).toString
    // Override rowCount in catalogTable stats if the relation has rowCount
    stats.toPlanStats(output, tableName, knownRowCount = relation.rowCount)
  }

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): LogicalRelation = {
    this.copy(output = output.map(_.newInstance()))
  }

  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.location.refresh()
    case _ =>  // Do nothing.
  }

  override def simpleString: String = s"Relation[${Utils.truncatedString(output, ",")}] $relation"
}

object LogicalRelation {
  def apply(relation: BaseRelation, isStreaming: Boolean = false): LogicalRelation =
    LogicalRelation(relation, relation.schema.toAttributes, None, isStreaming)

  def apply(relation: BaseRelation, table: CatalogTable): LogicalRelation =
    LogicalRelation(relation, relation.schema.toAttributes, Some(table), false)
}
