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

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalog.v2.{CatalogV2Implicits, TableCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MigrateTable, SnapshotTable}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{IcebergMigrateTableExec, IcebergSnapshotTableExec}

class NetflixStrategy(spark: SparkSession) extends Strategy {
  import CatalogV2Implicits._

  private lazy val icebergCatalog: TableCatalog =
    Try(spark.catalog(Some("iceberg"))).getOrElse(spark.catalog(None)).asTableCatalog

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case MigrateTable(identifier, provider) if provider.equalsIgnoreCase("iceberg") =>
      IcebergMigrateTableExec(icebergCatalog, identifier) :: Nil

    case SnapshotTable(target, source, provider) if provider.equalsIgnoreCase("iceberg") =>
      IcebergSnapshotTableExec(icebergCatalog, target, source) :: Nil

    case _ => Nil
  }
}

object NetflixStrategy {
  def apply(spark: SparkSession): NetflixStrategy = new NetflixStrategy(spark)
}
