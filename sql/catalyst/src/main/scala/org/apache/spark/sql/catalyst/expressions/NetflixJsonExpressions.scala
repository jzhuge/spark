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
import org.apache.spark.sql.catalyst.util.NetflixJsonUtils._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extracts json object from a json string based on json path specified, and returns json string
 * of the extracted json object. It will return null if the input json string is invalid.
 */
@ExpressionDescription(
  usage = "_FUNC_(json, jsonPath) - Extracts a json object from `jsonPath`."
 )
case class NfJsonExtract(json: Expression, jsonPath: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {
  override def left: Expression = json
  override def right: Expression = jsonPath
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def prettyName: String = "nf_json_extract"

  override def eval(input: InternalRow): Any = {
    val path = if (jsonPath.foldable) {
      jsonPath.eval().asInstanceOf[UTF8String]
    } else {
      jsonPath.eval(input).asInstanceOf[UTF8String]
    }
      val extractedValue = extractJsonFromInternalRow(input, json, path)
      if (extractedValue == null) {
        return null
      }
      getJsonAsString(extractedValue)
  }
}

/**
 * Evaluates the json path expression on the input col and returns the result as a string as
 * opposed to a json encoded string. The value referenced by json path must be a scalar.
 * Returns null if there the value referenced is not a scalar or there is a json parse exception
 */
@ExpressionDescription(
  usage = "_FUNC_(json, jsonPath) - Extracts a json object from `jsonPath`."
)
case class NfJsonExtractScalar(json: Expression, jsonPath: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {
  override def left: Expression = json
  override def right: Expression = jsonPath
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def prettyName: String = "nf_json_extract_scalar"

  override def eval(input: InternalRow): Any = {
    val path = if (jsonPath.foldable) {
      jsonPath.eval().asInstanceOf[UTF8String]
    } else {
      jsonPath.eval(input).asInstanceOf[UTF8String]
    }
    val extractedValue = extractJsonFromInternalRow(input, json, path)
    if (extractedValue != null) {
      if (extractedValue.isInstanceOf[java.lang.Number] ||
        extractedValue.isInstanceOf[java.lang.String] ||
        extractedValue.isInstanceOf[java.lang.Boolean]) {
        return UTF8String.fromString(extractedValue.toString)
      }
    }
    null
  }
}

/**
 * Extracts json object from a json string based on json path specified, and returns
 * array of json strings of the extracted json object. It will return null if the
 * input json string is invalid.
 */
@ExpressionDescription(
  usage = "_FUNC_(json, jsonPath) - Extracts a json object from `jsonPath`."
)
case class NfJsonExtractArray(json: Expression, jsonPath: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {
  override def left: Expression = json
  override def right: Expression = jsonPath
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def dataType: DataType = ArrayType(StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "nf_json_extract_array"

  override def eval(input: InternalRow): Any = {
    val path = if (jsonPath.foldable) {
      jsonPath.eval().asInstanceOf[UTF8String]
    } else {
      jsonPath.eval(input).asInstanceOf[UTF8String]
    }
    val extractedValue = extractJsonFromInternalRow(input, json, path)
    val result = new ArrayBuffer[UTF8String]
    if (extractedValue == null) {
      return null
    }
    if (extractedValue.isInstanceOf[ArrayList[Any]]) {
      val matchesArray = extractedValue.asInstanceOf[ArrayList[Any]]
      if (matchesArray.isEmpty) {
        result.append(getJsonAsString(""))
      }
      for (data <- matchesArray.asScala) {
        result.append(getJsonAsString(data))
      }
      }
    else {
      result.append(getJsonAsString(extractedValue))
    }
    ArrayData.toArrayData(result.toArray)
  }
}
