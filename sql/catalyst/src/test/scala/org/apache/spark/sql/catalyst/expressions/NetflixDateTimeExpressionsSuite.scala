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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.NetflixDateTimeUtils.TypeException
import org.apache.spark.sql.types.{IntegerType, LongType}

class NetflixDateTimeExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("NfDateAdd") {
    checkEvaluation(NfDateAdd(Literal(20180531), Literal(2)), 20180602)
    checkEvaluation(NfDateAdd(Literal(20180531L), Literal(2)), 20180602L)
    checkEvaluation(NfDateAdd(Literal(1531373400000L), Literal(2)), 1531546200000L)

    checkEvaluation(NfDateAdd(Literal("20180531"), Literal(-2)), "20180529")
    checkEvaluation(NfDateAdd(Literal("2018-05-31"), Literal(-2)), "2018-05-29")
    checkEvaluation(
      NfDateAdd(Literal("2018-05-31 12:20:21.010"), Literal(-2)),
      "2018-05-29 12:20:21.01")
    checkEvaluation(
      NfDateAdd(Literal(Date.valueOf("2018-05-31")), Literal(-2)),
      Date.valueOf("2018-05-29"))
    checkEvaluation(
      NfDateAdd(
        Literal(Timestamp.valueOf("2018-05-31 12:20:21.010")),
        Literal(-2)),
      Timestamp.valueOf("2018-05-29 12:20:21.01"))
    checkEvaluation(NfDateAdd(Literal("20180531"), Literal("2M")), "20180731")
    checkEvaluation(
      NfDateAdd(
        Literal(Timestamp.valueOf("2018-05-31 12:20:21.010")),
        Literal(null)),
      null)
    checkEvaluation(NfDateAdd(Literal("day"), Literal(5), Literal("20180531")), "20180605")
    checkEvaluation(
      NfDateAdd(Literal("week"), Literal(-10), Literal("2018-05-31 12:20:21.010")),
      "2018-03-22 12:20:21.01")
    checkEvaluation(
      NfDateAdd(
        Literal("hour"),
        Literal(2),
        Literal(Timestamp.valueOf("2018-03-01 02:00:00"))),
      Timestamp.valueOf("2018-03-01 04:00:00"))

    checkEvaluation(
      NfDateAdd(
        Literal("day"),
        Literal(2),
        BoundReference(0, IntegerType, true)),
      20180709,
      InternalRow(20180707))

    checkEvaluation(
      NfDateAdd(
        Literal("day"),
        Literal(2L, LongType),
        BoundReference(0, LongType, true)),
      20180709L,
      InternalRow(20180707.toLong))
  }

  test("NfDateDiff") {
    // Int or Long input
    checkEvaluation(NfDateDiff(Literal(20180531), Literal(20180604)), 4L)
    checkEvaluation(NfDateDiff(Literal("day"), Literal(20180101L), Literal(20171225L)), -7L)
    checkEvaluation(NfDateDiff(Literal(20180604), Literal(20180531L)), -4L)
    checkEvaluation(NfDateDiff(Literal(20160229L), Literal(20160301)), 1L)
    checkEvaluation(NfDateDiff(Literal(1531373400000L), Literal(1531546200000L)), 2L)

    // String input
    checkEvaluation(NfDateDiff(Literal("week"), Literal("20180501"), Literal("20180531")), 4L)
    checkEvaluation(NfDateDiff(Literal("2018-06-04"), Literal("2018-05-31")), -4L)
    checkEvaluation(
      NfDateDiff(
        Literal("hour"),
        Literal("2018-03-01 05:00:00"),
        Literal("2018-03-01 02:00:00")),
      -3L)

    // Date input
    checkEvaluation(
      NfDateDiff(
        Literal(Date.valueOf("2018-06-04")),
        Literal(Date.valueOf("2018-05-31"))),
      -4L)

    // Timestamp input
    checkEvaluation(
      NfDateDiff(
        Literal("second"),
        Literal(Timestamp.valueOf("2018-05-31 12:20:21")),
        Literal(Timestamp.valueOf("2018-05-31 02:20:21"))),
      -36000L)

    // BoundReference
    checkEvaluation(
      NfDateDiff(
        Literal("day"),
        Literal(20180701),
        BoundReference(0, LongType, true)),
      6L,
      InternalRow(20180707L))
    checkEvaluation(
      NfDateDiff(
        Literal("day"),
        BoundReference(0, LongType, true),
        Literal(20180701)),
      -6L,
      InternalRow(20180707L))

    checkEvaluation(NfDateDiff(Literal(1527806973000L), Literal(null)), null)

    assertEvalError(
      NfDateDiff(Literal("minute"), Literal(20180531), Literal(20180604)),
      "'minute' is not a valid Date field" :: Nil)
    assertEvalError(
      NfDateDiff(Literal("days"), Literal(1000000000), Literal(1000000000)),
      "'days' is not a valid Timestamp field" :: Nil)
    assertEvalError(
      NfDateDiff(Literal(Date.valueOf("2016-03-14")), Literal(20160307)),
      "Both inputs should have the same type" :: Nil
    )
    assertEvalError(
      NfDateDiff(Literal(Date.valueOf("2016-03-14")), Literal("20160307")),
      "Both inputs should have the same type" :: Nil
    )
    assertEvalError(
      NfDateDiff(Literal("20180531"), Literal(20180604)),
      "Both inputs should have the same type" :: Nil)
    assertEvalError(
      NfDateDiff(
        Literal("second"),
        Literal(Date.valueOf("2016-03-14")),
        Literal(Timestamp.valueOf("2018-05-31 02:20:21"))),
      "Both inputs should have the same type" :: Nil)
  }

  private def assertEvalError(
    expr: Expression,
    expectedErrors: Seq[String],
    caseSensitive: Boolean = true): Unit = {
    val e = intercept[TypeException] {
      evaluate(expr)
    }

    if (!expectedErrors.map(_.toLowerCase).forall(e.getMessage.toLowerCase.contains)) {
      fail(
        s"""Exception message should contain the following substrings:
           |
           |  ${expectedErrors.mkString("\n  ")}
           |
           |Actual exception message:
           |
           |  ${e.getMessage}
         """.stripMargin)
    }
  }
}
