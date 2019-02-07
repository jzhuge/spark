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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class NetflixJsonFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("function nf_json_extract") {
    val df: DataFrame = Seq(
      """
        |{"store": {"book": [{"category": "reference","author": "Nigel Rees","title":
        |"Sayings of the Century","price": 8.95}, {"category": "fiction","author":"Evelyn Waugh",
        |"title": "Sword of Honour","price": 12.99},{"category": "fiction",
        |"author": "Herman Melville","title":"Moby Dick","isbn": "0-553-21311-3","price": 8.99},
        |{"category": "fiction","author": "J. R. R. Tolkien","title":"The Lord of the Rings",
        |"isbn":"0-395-19395-8","price": 22.99}],"object": {"inner_object":
        |{"array":[{"inner_array": [{"x": "y"}]}]}}}}""".stripMargin).toDF("a");
    checkAnswer(df.selectExpr("nf_json_extract(a, '$..book.length()')"), Row("[4]"))
    checkAnswer(df.selectExpr("nf_json_extract(a, " +
      "'$.store.object.inner_object.array[0].inner_array[0].x')"), Row("\"y\""))
    checkAnswer(df.selectExpr("nf_json_extract(a, '$.store.book[*].category')"),
      Row("[\"reference\",\"fiction\",\"fiction\",\"fiction\"]"))
    checkAnswer(df.selectExpr("nf_json_extract(a, '$.store.bicycle.price')"), Row(null))
    val df1: DataFrame = Seq("""{s:}""").toDF("b")
    checkAnswer(df1.selectExpr("nf_json_extract(b, '$')"), Row(null))
  }

  test("function nf_get_as_json_extract") {
    val df: DataFrame = Seq(
      """
        ["a", "b", "c"]""".stripMargin).toDF("a");
    checkAnswer(df.selectExpr("size(nf_json_extract_array(a, '$'))"), Row(3))
  }
}
