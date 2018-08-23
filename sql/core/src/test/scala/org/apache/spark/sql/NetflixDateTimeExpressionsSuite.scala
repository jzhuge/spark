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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils}
import org.apache.spark.sql.test.SharedSQLContext

class NetflixDateTimeExpressionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("nf_dateadd") {
    checkSqlAnswer("SELECT nf_dateadd(20180531, 2)", 20180602)
    checkSqlAnswer("SELECT nf_dateadd(20180531L, 2)", 20180602L)
    checkSqlAnswer("SELECT nf_dateadd('second', -1, 1000001000)", 1000000000)
    checkSqlAnswer("SELECT nf_dateadd(1531373400000L, 2)", 1531546200000L)

    checkSqlAnswer("SELECT nf_dateadd('20180531', -2)", "20180529")
    checkSqlAnswer("SELECT nf_dateadd('2018-05-31', -2)", "2018-05-29")
    checkSqlAnswer(
      "SELECT nf_dateadd('2018-05-31 12:20:21.010', -2)",
      "2018-05-29 12:20:21.01")
    checkSqlAnswer("SELECT nf_dateadd(date '2018-05-31', -2)", Date.valueOf("2018-05-29"))
    checkSqlAnswer("SELECT nf_dateadd(timestamp '2018-05-31 12:20:21.010', -2)",
      Timestamp.valueOf("2018-05-29 12:20:21.01"))
    checkSqlAnswer("SELECT nf_dateadd('20180531', '2M')", "20180731")
    checkSqlAnswer("SELECT nf_dateadd(timestamp '2018-05-31 12:20:21.010', null)", null)
    checkSqlAnswer("SELECT nf_dateadd('day', 5, '20180531')", "20180605")
    checkSqlAnswer(
      "SELECT nf_dateadd('week', -10, '2018-05-31 12:20:21.010')",
      "2018-03-22 12:20:21.01")
    checkSqlAnswer(
      "SELECT nf_dateadd('hour', 2, timestamp '2018-03-01 02:00:00')",
      Timestamp.valueOf("2018-03-01 04:00:00"))

    withTempView("t") {
      Seq(20180707).toDF("dateint").createTempView("t")
      checkSqlAnswer("select nf_dateadd('day', 2, dateint) from t", 20180709)
    }

    withTempView("t") {
      Seq(20180707L).toDF("long_dateint").createTempView("t")
      checkSqlAnswer("select nf_dateadd('day', 2, long_dateint) from t", 20180709L)
    }
  }

  test("nf_datediff") {
    checkSqlAnswer("select nf_datediff(20180531, 20180604)", 4)
    checkSqlAnswer("select nf_datediff(20180101L, 20171225L)", -7)
    checkSqlAnswer("select nf_datediff(20180604, 20180531L)", -4)
    checkSqlAnswer("select nf_datediff(20160229L, 20160301)", 1)
    checkSqlAnswer("select nf_datediff('second', 1000000000, 1000001000)", 1)
    checkSqlAnswer("select nf_datediff(1531373400000L, 1531546200000L)", 2)

    checkSqlAnswer("select nf_datediff('week', '20180501', '20180531')", 4)
    checkSqlAnswer("select nf_datediff('2018-06-04', '2018-05-31')", -4)
    checkSqlAnswer("select nf_datediff('hour', '2018-03-01 05:00:00', '2018-03-01 02:00:00')", -3)

    checkSqlAnswer("select nf_datediff(date '2018-06-04', date '2018-05-31')", -4)

    val (ts1, ts2) = ("2018-05-31 12:20:21", "2018-05-31 02:20:21")
    checkSqlAnswer(s"select nf_datediff('second', timestamp '$ts1', timestamp '$ts2')", -36000)

    withTempView("t") {
      Seq(20180707L).toDF("long_dateint").createTempView("t")
      checkSqlAnswer("select nf_datediff('day', 20180701, long_dateint) from t", 6)
      checkSqlAnswer("select nf_datediff('day', long_dateint, 20180701) from t", -6)
    }

    checkSqlAnswer("select nf_datediff(1527806973000, null)", null)
  }

  test("nf_from_unixtime") {
    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("UTC")) {
      val ts = Timestamp.from(Instant.ofEpochMilli(1527745543000L))
      val date = new Date(1527745543000L)
      val sdformat = new SimpleDateFormat("yyyy/MM/dd", Locale.US)
      var res = sdformat.format(date)
      checkAnswer(sql("select nf_from_unixtime(1527745543)"), Row(ts) :: Nil)
      checkAnswer(sql("select nf_from_unixtime_ms(1527745543000)"), Row(ts) :: Nil)
      checkAnswer(sql("select nf_from_unixtime(1527745543, 'yyyy/MM/dd')"), Row(res) :: Nil)
      checkAnswer(sql("select nf_from_unixtime_tz(1527745543, 'GMT+05:00')"),
        Row(Timestamp.from(Instant.ofEpochMilli(DateTimeUtils.convertTz(1527745543000000L,
          TimeZone.getTimeZone("UTC"),
          TimeZone.getTimeZone("GMT+05:00")) / 1000))) :: Nil)
      checkAnswer(sql("select nf_from_unixtime_ms_tz(1527745543000, 'Europe/Paris')"),
        Row(Timestamp.from(Instant.ofEpochMilli(DateTimeUtils.convertTz(1527745543000000L,
          TimeZone.getTimeZone("UTC"),
          TimeZone.getTimeZone("Europe/Paris")) / 1000))) :: Nil)
    }
  }

  private def checkSqlAnswer(sqlString: String, expectedResult: Any): Unit = {
    checkAnswer(sql(sqlString), Row(expectedResult))
  }
}
