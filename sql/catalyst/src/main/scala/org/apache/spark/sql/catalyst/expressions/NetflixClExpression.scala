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

package org.apache.spark.sql.catalyst.expressions
import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.NetflixClUtils._
import org.apache.spark.sql.catalyst.util.NetflixJsonUtils._
import org.apache.spark.sql.types.{
  AbstractDataType,
  ArrayType,
  DataType,
  StringType
}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Extracts values from a cl snapshot based on clType, extractCriteria, and filterCriteria
  * and returns array of strings. It will return null if the input parameters are invalid.
  */
@ExpressionDescription(
  usage =
    "_FUNC_(json, clType, filterCriteria, extractCriteria) - " +
      "Extracts values from cl snapshot, http://go/cludfs."
)
case class ClSnapshotExtract(snapshot: Expression,
                             clType: Expression,
                             extractCriteria: Expression,
                             filterCriteria: Expression)
    extends ExpectsInputTypes
    with CodegenFallback {
  override def children: Seq[Expression] =
    Seq(snapshot, clType, extractCriteria, filterCriteria)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, StringType, StringType)
  override def dataType: DataType = ArrayType(StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "cl_snapshot_extract"

  override def eval(input: InternalRow): Any = {
    val extractedValue =
      snapshotExtract(input, snapshot, clType, extractCriteria, filterCriteria)
    val result = new ArrayBuffer[UTF8String]
    if (extractedValue.isInstanceOf[ArrayList[Any]]) {
      val matchesArray = extractedValue.asInstanceOf[ArrayList[Any]]
      for (data <- matchesArray.asScala) {
        result.append(getJsonAsString(data))
      }
    } else {
      result.append(getJsonAsString(extractedValue))
    }
    ArrayData.toArrayData(result.toArray)
  }
}
