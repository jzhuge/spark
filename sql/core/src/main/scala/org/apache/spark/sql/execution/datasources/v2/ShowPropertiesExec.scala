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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.unsafe.types.UTF8String

case class ShowPropertiesExec(
    table: Table,
    property: Option[String],
    output: Seq[Attribute]) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    val rows = property match {
      case Some(key) =>
        val value = Option(table.properties.get(key)).getOrElse("(not set)")
        Seq(InternalRow(UTF8String.fromString(key), UTF8String.fromString(value)))

      case _ =>
        table.properties.asScala.map {
          case (key, value) =>
            InternalRow(UTF8String.fromString(key), UTF8String.fromString(value))
        }.toSeq
    }

    sparkContext.parallelize(rows, 1)
  }

  override def children: Seq[SparkPlan] = Seq.empty
}
