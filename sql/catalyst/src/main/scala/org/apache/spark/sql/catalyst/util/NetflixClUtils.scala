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

import scala.collection.immutable.HashSet

import com.jayway.jsonpath.{JsonPath, JsonPathException}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String

object NetflixClUtils {

  private val relationalOperators = HashSet("==", "!=", ">=", "<=", ">", "<")

  def generateClSnapshotJaywayJsonPath(clType: String,
                                       extractCriteria: String,
                                       filterCriteria: String): String = {
    val jsonPathifiedExtractCriteria =
      Option(extractCriteria).getOrElse("") match {
        case x if x.nonEmpty => s".$x"
        case _               => ""
      }

    val jsonPathifiedFilterCriteria =
      Option(filterCriteria).getOrElse("") match {
        case x if x.nonEmpty =>
          val tokens = filterCriteria
            .split(
              "((?<===|>|<|>=|<=|!=|&&|\\|\\||\\(|\\)) *|(?===|>|<|>=|<=|!=|&&|\\|\\||\\(|\\))) *")
            .toList
          " && (" + tokens
            .sliding(2)
            .toList
            .map(e =>
              if (relationalOperators
                    .contains(e.last)) { s"@.${e.head.trim}" } else {
                s"${e.head.trim}"
            })
            .mkString + tokens.last.trim + ")"
        case _ => ""
      }

    "$[?(\"" + Option(clType).getOrElse("") +
      "\" in @.type" + jsonPathifiedFilterCriteria + ")]" + jsonPathifiedExtractCriteria
  }

  @throws(classOf[JsonPathException])
  def snapshotExtract(input: InternalRow,
                      json: Expression,
                      clType: Expression,
                      extractCriteria: Expression,
                      filterCriteria: Expression): Any = {
    val snapshot = json.eval(input).asInstanceOf[UTF8String]
    if (snapshot == null) {
      return null
    }

    if (clType.eval().asInstanceOf[UTF8String] == null) {
      throw new IllegalArgumentException(
        "`cl_snapshot_extract` must be supplied with " +
          "a valid `clType` from http://go/cl, refer http://go/cludfs")
    }

    val jsonPath = generateClSnapshotJaywayJsonPath(
      clType.eval().asInstanceOf[UTF8String].toString,
      extractCriteria.eval().asInstanceOf[UTF8String].toString,
      filterCriteria.eval().asInstanceOf[UTF8String].toString
    )
    JsonPath.parse(snapshot.toString).read(jsonPath.toString)
  }
}
