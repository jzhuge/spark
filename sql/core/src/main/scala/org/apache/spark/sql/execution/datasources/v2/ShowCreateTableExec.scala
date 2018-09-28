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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.Table
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

case class ShowCreateTableExec(
    ident: TableIdentifier,
    table: Table,
    output: Seq[Attribute]) extends SparkPlan {

  private lazy val schemaFragment = table.schema
      .map { field =>
        val comment = field.getComment().map(comment => s" COMMENT '$comment'")
        s"${field.name} ${field.dataType.simpleString}$comment"
      }
      .mkString("\n    ", ",\n    ", "")

  private lazy val partFragment = if (table.partitioning.isEmpty) {
    ""
  } else {
    table.partitioning.asScala.map(_.toString).mkString("\nPARTITIONED BY (", ", ", ")")
  }

  private lazy val usingFragment = Option(table.properties.get("provider"))
      .map(p => s"\nUSING $p")
      .getOrElse("")

  override protected def doExecute(): RDD[InternalRow] = {
    val create = s"CREATE TABLE ${ident.quotedString} ($schemaFragment)$usingFragment$partFragment"
    sparkContext.parallelize(Seq(InternalRow(create)), 1)
  }

  override def children: Seq[SparkPlan] = Seq.empty
}
