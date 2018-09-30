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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalog.v2.Table
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

case class DescribeTable(
    table: Table,
    isExtended: Boolean,
    isFormatted: Boolean) extends RunnableCommand {
  import DescribeTable._

  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive. These are used in some testing and should not be changed.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows: ArrayBuffer[Row] = new ArrayBuffer[Row]()

    addSchema(rows)

    if (isFormatted) {
      addPartitioning(rows)
    }

    if (isExtended || isFormatted) {
      addProperties(rows)
    }

    rows
  }

  private def addSchema(rows: ArrayBuffer[Row]): Unit = {
    rows ++= table.schema.map{ column =>
      Row(column.name, column.dataType.simpleString, column.getComment().getOrElse(""))
    }
  }

  private def addPartitioning(rows: ArrayBuffer[Row]): Unit = {
    rows += EMPTY_ROW
    rows += Row(" Partitioning", "", "")
    rows += Row("--------------", "", "")
    if (table.partitioning.isEmpty) {
      rows += Row("Not partitioned", "", "")
    } else {
      rows ++= table.partitioning.asScala.zipWithIndex.map {
        case (transform, index) => Row(s"Part $index", transform.describe(), "")
      }
    }
  }

  private def addProperties(rows: ArrayBuffer[Row]): Unit = {
    rows += EMPTY_ROW
    rows += Row(" Table Property", " Value", "")
    rows += Row("----------------", "-------", "")
    rows ++= table.properties.asScala.map {
      case (key, value) => Row(key, value, "")
    }
  }
}

object DescribeTable {
  private val EMPTY_ROW = Row("", "", "")
}
