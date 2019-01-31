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

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.JsonPathException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StringType}
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
    val jsonStr = json.eval(input).asInstanceOf[UTF8String]
    if (jsonStr == null) {
      return null
    }
    val utf8JsonPath = jsonPath.eval().asInstanceOf[UTF8String]
    val output = new ByteArrayOutputStream()
    val sortedMapper = new ObjectMapper().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    try {
      val parsedData = JsonPath.parse(jsonStr.toString)
      sortedMapper.writeValue(output, parsedData.read(jsonPath.toString))
      UTF8String.fromBytes(output.toByteArray)
    } catch {
      case _: JsonPathException => null
    }
  }
}
