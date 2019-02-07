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

package org.apache.spark.sql.catalyst.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS
import com.jayway.jsonpath.{JsonPath, JsonPathException}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String

object NetflixJsonUtils {

  @throws(classOf[JsonPathException])
  def extractJsonFromInternalRow(input: InternalRow, json: Expression,
                                 jsonPath: Expression): Any = {
    val jsonStr = json.eval(input).asInstanceOf[UTF8String]
    if (jsonStr == null) {
      return null
    }
    val utf8JsonPath = jsonPath.eval().asInstanceOf[UTF8String]
    val parsedData = JsonPath.parse(jsonStr.toString)
    parsedData.read(jsonPath.toString)
  }

  def getJsonAsString(input: Any): UTF8String = {
    val sortedMapper = new ObjectMapper().configure(ORDER_MAP_ENTRIES_BY_KEYS, true)
    UTF8String.fromBytes(sortedMapper.writeValueAsString(input).getBytes())
  }
}
