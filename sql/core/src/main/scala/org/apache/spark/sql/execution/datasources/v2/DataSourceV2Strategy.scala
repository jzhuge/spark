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

import scala.collection.mutable

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, AppendData, CreateTable, CreateTableAsSelect, DeleteFrom, DropTable, LogicalPlan, OverwritePartitionsDynamic, ReplaceTableAsSelect}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownCatalystFilters, SupportsPushDownFilters, SupportsPushDownRequiredColumns}

object DataSourceV2Strategy extends Strategy {

  import org.apache.spark.sql.sources.v2.DataSourceV2Implicits._

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  def pushFilters(
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownCatalystFilters =>
        val postScanFilters = r.pushCatalystFilters(filters.toArray)
        val pushedFilters = r.pushedCatalystFilters()
        (pushedFilters, postScanFilters)

      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = mutable.HashMap.empty[Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilterToExpr.keys.toArray)
          .map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return new output attributes after column pruning.
   */
  // TODO: nested column pruning.
  private def pruneColumns(
      reader: DataSourceReader,
      relation: NamedRelation,
      exprs: Seq[Expression]): Seq[AttributeReference] = {
    reader match {
      case r: SupportsPushDownRequiredColumns =>
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val neededOutput = relation.output.filter(requiredColumns.contains)
        if (neededOutput != relation.output) {
          r.pruneColumns(neededOutput.toStructType)
          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          r.readSchema().toAttributes.map {
            // We have to keep the attribute id during transformation.
            a => a.withExprId(nameToAttr(a.name).exprId)
          }
        } else {
          relation.output
        }

      case _ => relation.output
    }
  }

  import DataSourceV2Relation._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case cmd: ShowCreateTable =>
      ExecutedCommandExec(cmd, Nil) :: Nil

    case cmd: ShowProperties =>
      ExecutedCommandExec(cmd, Nil) :: Nil

    case cmd: DescribeTable =>
      ExecutedCommandExec(cmd, Nil) :: Nil

    case DropTable(catalog, identifier, ifExists) =>
      DropTableExec(catalog, identifier, ifExists) :: Nil

    case PhysicalOperation(project, filters, relation: NamedRelation)
        if relation.isInstanceOf[DataSourceV2Relation] || relation.isInstanceOf[TableV2Relation] =>

      val (reader, options, sourceName) = relation match {
        case r: DataSourceV2Relation => (r.newReader(), r.options, r.source.name)
        case r: TableV2Relation => (r.newReader(), r.options, r.catalogName)
      }

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFilters) = pushFilters(reader, filters)
      val output = pruneColumns(reader, relation, project ++ postScanFilters)
      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val scan = DataSourceV2ScanExec(
        output, sourceName, options, pushedFilters, reader)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

      // always add the projection, which will produce unsafe rows required by some operators
      ProjectExec(project, withFilter) :: Nil

    case AppendData(r: DataSourceV2Relation, query, _) =>
      WriteToDataSourceV2Exec(r.newWriter(), planLater(query)) :: Nil

    case AppendData(r: TableV2Relation, query, _) =>
      AppendDataExec(r.table, r.options, planLater(query)) :: Nil

    case OverwritePartitionsDynamic(r: TableV2Relation, query, _) =>
      OverwritePartitionsDynamicExec(r.table, r.options, planLater(query)) :: Nil

    case AlterTable(catalog, rel: TableV2Relation, changes) =>
      AlterTableExec(catalog, rel.ident, changes) :: Nil

    case CreateTable(catalog, ident, schema, partitioning, options, ignoreIfExists) =>
      CreateTableExec(catalog, ident, schema, partitioning, options, ignoreIfExists) :: Nil

    case CreateTableAsSelect(catalog, ident, partitioning, query, writeOptions, ignoreIfExists) =>
      CreateTableAsSelectExec(catalog, ident, partitioning, Map.empty, writeOptions,
        planLater(query), ignoreIfExists) :: Nil

    case ReplaceTableAsSelect(catalog, ident, partitioning, query, writeOptions) =>
      ReplaceTableAsSelectExec(catalog, ident, partitioning, Map.empty, writeOptions,
        planLater(query)) :: Nil

    case DeleteFrom(rel: TableV2Relation, expr) =>
      DeleteFromV2Exec(rel, expr) :: Nil

    case _ => Nil
  }
}
