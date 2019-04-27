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

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable, MaybeCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
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
    if (conf.cboEnabled || !conf.rowCountStatsEnabled) {
      catalogTable
        .flatMap(_.stats.map(_.toPlanStats(output, conf.cboEnabled)))
        .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
    } else {
      val numRows = relation.rowCount
      val tableName = catalogTable.map(_.identifier).getOrElse(relation).toString
      val sizeInBytes: BigInt = numRows match {
        case Some(rowEstimate) =>
          // if the row count is present, use it to estimate size because size on disk may be
          // columnar but the size that matters is the size in memory
          val rowSizeEstimate = output.map(_.dataType.defaultSize).sum + 8
          val sizeEstimate = rowEstimate * rowSizeEstimate
          logInfo(s"Row-based size estimate for $tableName: " +
            s"$rowEstimate rows * $rowSizeEstimate bytes = $sizeEstimate bytes")
          sizeEstimate
        case _ =>
          // fall back to the size on disk
          logInfo(s"Fallback size estimate for $tableName: ${relation.sizeInBytes}")
          relation.sizeInBytes
      }

      catalogTable
        .flatMap(_.stats.map(_.toPlanStats(output, conf.cboEnabled).copy(sizeInBytes, numRows)))
        .getOrElse(Statistics(sizeInBytes = sizeInBytes, rowCount = numRows))
    }
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
