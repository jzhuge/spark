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

package org.apache.spark.sql.catalyst.plans.logical.sql

import org.apache.spark.sql.catalog.v2.PartitionTransform
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

case class CreateTable(
    table: TableIdentifier,
    provider: String,
    tableSchema: StructType,
    partitioning: Seq[PartitionTransform],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    ifNotExists: Boolean) extends ParsedLogicalPlan {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

case class CreateTableAsSelect(
    table: TableIdentifier,
    provider: String,
    partitioning: Seq[PartitionTransform],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    asSelect: LogicalPlan,
    ifNotExists: Boolean) extends ParsedLogicalPlan {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}
