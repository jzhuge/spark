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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.Table
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.unsafe.types.UTF8String

case class DescribeTableExec(
    table: Table,
    isExtended: Boolean,
    output: Seq[Attribute]) extends SparkPlan {
  import DescribeTableExec._

  override protected def doExecute(): RDD[InternalRow] = {
    val rows: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]()

    addSchema(rows)

    if (rows.nonEmpty) {
      addPartitioning(rows)
    }

    if (isExtended) {
      addProperties(rows)
    }

    sparkContext.parallelize(rows, 1)
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.schema.map{ column =>
      column.getComment() match {
        case Some(comment) =>
          row(column.name, column.dataType.simpleString, comment)
        case _ =>
          row(column.name, column.dataType.simpleString)
      }
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += EMPTY_ROW
    rows += row("Partitioning", "")
    rows ++= table.partitioning.asScala.zipWithIndex.map {
      case (transform, index) => row(index.toString, transform.describe())
    }
  }

  private def addProperties(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += EMPTY_ROW
    rows += row("Table Property", "Value")
    rows ++= table.properties.asScala.map {
      case (key, value) => row(key, value)
    }
  }

  override def children: Seq[SparkPlan] = Seq.empty
}

object DescribeTableExec {
  private val EMPTY_STRING = UTF8String.fromString("")
  private val EMPTY_ROW = InternalRow(EMPTY_STRING, EMPTY_STRING, EMPTY_STRING)

  private def row(colName: String, dataType: String): InternalRow = {
    InternalRow(UTF8String.fromString(colName), UTF8String.fromString(dataType), EMPTY_STRING)
  }

  private def row(colName: String, dataType: String, comment: String): InternalRow = {
    InternalRow(
      UTF8String.fromString(colName),
      UTF8String.fromString(dataType),
      UTF8String.fromString(comment))
  }
}
