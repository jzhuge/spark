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

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, InsertIntoTable, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.types.StructType

case class V2AsBaseRelation(
    sqlContext: SQLContext,
    v2relation: NamedRelation,
    catalogTable: CatalogTable) extends BaseRelation {
  override lazy val schema: StructType = v2relation.output.toStructType
}

/**
 * Rules to convert from plans produced by the SQL parser to v2 logical plans.
 */
class DataSourceV2Analysis(spark: SparkSession) extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._
  import DataSourceV2Analysis._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case create @ CreateTable(catalogTable, _, None) if isV2CatalogTable(catalogTable) =>
      create

    case ctas @ CreateTable(catalogTable, mode, Some(query)) if isV2CatalogTable(catalogTable) =>
      // TODO: after identifier is extended to include catalog, load the correct catalog
      val catalog = spark.catalog(None).asTableCatalog
      val options = catalogTable.storage.properties

      if (catalogTable.partitionColumnNames.nonEmpty) {
        throw new AnalysisException("CTAS with partitions is not yet supported.")
      }

      mode match {
        case SaveMode.Append =>
          throw new AnalysisException("Append mode cannot be used with CTAS for v2 data sources")

        case SaveMode.ErrorIfExists =>
          CreateTableAsSelect(catalog, catalogTable.identifier, Seq.empty, query, options,
            ignoreIfExists = false)

        case SaveMode.Ignore =>
          CreateTableAsSelect(catalog, catalogTable.identifier, Seq.empty, query, options,
            ignoreIfExists = true)

        case SaveMode.Overwrite =>
          ReplaceTableAsSelect(catalog, catalogTable.identifier, Seq.empty, query, options)
      }

    case insert @ InsertIntoTable(LogicalRelation(v2: V2AsBaseRelation, _, _), _, _, _, _, _) =>
      // the DataFrame API doesn't create INSERT INTO plans for v2 tables, so this must be
      // SQL and should match columns by position, not by name.

      // ifNotExists is append with validation, but validation is not supported
      if (insert.ifNotExists) {
        throw new AnalysisException(s"Cannot write, IF NOT EXISTS is not supported for table: $v2")
      }

      if (insert.overwrite.enabled) {
        // TODO: Support overwrite
        throw new AnalysisException(s"Cannot overwrite with table: $v2")
      }

      // TODO: Handle partition values
      assert(insert.partition.isEmpty, s"Cannot write static partitions into table: $v2")

      AppendData.byPosition(v2.v2relation, insert.child)

    case rel @ LogicalRelation(v2: V2AsBaseRelation, _, _) =>
      // unwrap v2 relation
      v2.v2relation
  }
}

object DataSourceV2Analysis {
  def apply(spark: SparkSession): DataSourceV2Analysis = new DataSourceV2Analysis(spark)

  private def isV2CatalogTable(catalogTable: CatalogTable): Boolean = {
    catalogTable.provider.exists(
      provider => classOf[DataSourceV2].isAssignableFrom(DataSource.lookupDataSource(provider)))
  }
}
