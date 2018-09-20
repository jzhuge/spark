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
import java.time.{Instant, LocalDate}
import java.util.{Locale, TimeZone}

import org.scalatest._

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, NetflixDateTimeUtils}
import org.apache.spark.sql.catalyst.util.NetflixDateTimeUtils.TypeException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class NetflixDateTimeFunctionsSuite extends QueryTest with SharedSQLContext with Matchers {

  import testImplicits._

  /** [[QueryTest]] sets the default time zone to America/Los_Angeles. */
  TimeZone.getDefault.getID shouldBe "America/Los_Angeles"

  test("nf_dateadd") {
    checkSqlAnswer("SELECT nf_dateadd(20180531, 2)", 20180602)
    checkSqlAnswer("SELECT nf_dateadd(20180531L, 2)", 20180602L)
    checkSqlAnswer("SELECT nf_dateadd(1531373400000L, 2)", 1531546200000L)
    checkSqlAnswer("SELECT nf_dateadd(1531373400, 2)", 1531546200)
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
    checkSqlAnswer("SELECT nf_dateadd('quarter', 5, '20180531')", "20190831")
    checkSqlAnswer(
      "SELECT nf_dateadd('week', -10, '2018-05-31 12:20:21.010')",
      "2018-03-22 12:20:21.01")
    checkSqlAnswer(
      "SELECT nf_dateadd('hour', 2, timestamp '2018-03-11 02:00:00')",
      Timestamp.valueOf("2018-03-11 05:00:00"))
    withTimeZone("UTC") {
      checkSqlAnswer(
        "SELECT nf_dateadd('hour', 2, timestamp '2018-03-11 02:00:00')",
        Timestamp.valueOf("2018-03-11 04:00:00"))
    }

    withTempView("t") {
      Seq(20180707).toDF("dateint").createTempView("t")
      checkSqlAnswer("select nf_dateadd('day', 2, dateint) from t", 20180709)
    }

    withTempView("t") {
      Seq(20180707L).toDF("long_dateint").createTempView("t")
      checkSqlAnswer("select nf_dateadd('day', 2, long_dateint) from t", 20180709L)
    }

    withTimeZone("UTC") {
      checkSqlAnswer(
        "SELECT nf_dateadd('2018-07-12 05:30:00', 2)",
        "2018-07-14 05:30:00")
      checkSqlAnswer(
        "SELECT nf_dateadd('2018-07-11 22:30:00', 2)",
        "2018-07-13 22:30:00")
    }

    val df = spark.range(1)
    checkAnswer(df.select(nf_dateadd(lit(20180531), lit(2))), Seq(Row(20180602)))
    checkAnswer(
      df.select(nf_dateadd(lit("week"), lit(-10), lit("2018-05-31 12:20:21.010"))),
      Seq(Row("2018-03-22 12:20:21.01")))
    intercept[TypeException](df.select(nf_dateadd(lit(1.0f), lit(3))).collect())
  }

  test("nf_datediff") {
    checkSqlAnswer("select nf_datediff(20180531, 20180604)", 4)
    checkSqlAnswer("select nf_datediff(20180101L, 20171225L)", -7)
    checkSqlAnswer("select nf_datediff(20180604, 20180531L)", -4)
    checkSqlAnswer("select nf_datediff(20160229L, 20160301)", 1)
    checkSqlAnswer("select nf_datediff(1531373400000L, 1531546200000L)", 2)
    checkSqlAnswer("select nf_datediff(1531373400L, 1531546200L)", 2)

    checkSqlAnswer("select nf_datediff('week', '20180501', '20180531')", 4)
    checkSqlAnswer("select nf_datediff('2018-06-04', '2018-05-31')", -4)
    checkSqlAnswer("select nf_datediff('hour', '2018-03-11 05:00:00', '2018-03-11 02:00:00')", -2)
    withTimeZone("UTC") {
      checkSqlAnswer("select nf_datediff('hour', '2018-03-11 05:00:00', '2018-03-11 02:00:00')", -3)
    }
    checkSqlAnswer("select nf_datediff(date '2018-06-04', date '2018-05-31')", -4)

    val (ts1, ts2) = ("2018-05-31 12:20:21", "2018-05-31 02:20:21")
    checkSqlAnswer(s"select nf_datediff('second', timestamp '$ts1', timestamp '$ts2')", -36000)

    withTempView("t") {
      Seq(20180707L).toDF("long_dateint").createTempView("t")
      checkSqlAnswer("select nf_datediff('day', 20180701, long_dateint) from t", 6)
      checkSqlAnswer("select nf_datediff('day', long_dateint, 20180701) from t", -6)
    }

    checkSqlAnswer("select nf_datediff(1527806973000, null)", null)

    val df = spark.range(1)
    checkAnswer(df.select(nf_datediff(lit(20180531), lit(20180604))), Seq(Row(4)))
    checkAnswer(
      df.select(
        nf_datediff(
          lit("hour"),
          lit("2018-03-01 05:00:00"),
          lit("2018-03-01 02:00:00"))),
      Seq(Row(-3)))
    intercept[TypeException](
      df.select(nf_datediff(lit("minute"), lit(20180531), lit(20180604))).collect())
    intercept[TypeException](
      df.select(nf_datediff(lit("days"), lit(1000000000), lit(1000000000))).collect())
    intercept[TypeException](
      df.select(nf_datediff(lit(Date.valueOf("2016-03-14")), lit(20160307))).collect())
    intercept[TypeException](
      df.select(nf_datediff(lit(Date.valueOf("2016-03-14")), lit("20160307"))).collect())
    intercept[TypeException](df.select(nf_datediff(lit("20180531"), lit(20180604))).collect())
    intercept[TypeException](
      df.select(
        nf_datediff(
          lit("second"),
          lit(Date.valueOf("2016-03-14")),
          lit(Timestamp.valueOf("2018-05-31 02:20:21"))))
        .collect())
  }

  test("nf_from_unixtime") {
    val ts = Timestamp.from(Instant.ofEpochMilli(1527745543000L))
    val date = new Date(1527745543000L)
    val sdformat = new SimpleDateFormat("yyyy/MM/dd", Locale.US)
    var res = sdformat.format(date)
    checkSqlAnswer("select nf_from_unixtime(1527745543)", ts)
    checkSqlAnswer("select nf_from_unixtime(1527745543000)", ts)
    checkSqlAnswer("select nf_from_unixtime_ms(1527745543000)", ts)
    checkSqlAnswer("select nf_from_unixtime(1527745543, 'yyyy/MM/dd')", res)
    checkSqlAnswer("select nf_from_unixtime_tz(1527745543, 'GMT+05:00')",
      Timestamp.from(Instant.ofEpochMilli(DateTimeUtils.convertTz(1527745543000000L,
        DateTimeUtils.defaultTimeZone,
        TimeZone.getTimeZone("GMT+05:00")) / 1000)))
    checkSqlAnswer("select nf_from_unixtime_tz(1527745543000, 'GMT+05:00')",
      Timestamp.from(Instant.ofEpochMilli(DateTimeUtils.convertTz(1527745543000000L,
        DateTimeUtils.defaultTimeZone,
        TimeZone.getTimeZone("GMT+05:00")) / 1000)))
    checkSqlAnswer("select nf_from_unixtime_ms_tz(1527745543000, 'Europe/Paris')",
      Timestamp.from(Instant.ofEpochMilli(DateTimeUtils.convertTz(1527745543000000L,
        DateTimeUtils.defaultTimeZone,
        TimeZone.getTimeZone("Europe/Paris")) / 1000)))
  }

  test("nf_dateint & nf_dateint_today") {
    checkSqlAnswer("SELECT nf_dateint(20180531)", 20180531)
    checkSqlAnswer("SELECT nf_dateint(20181531)", null)
    checkSqlAnswer("SELECT nf_dateint('20180531')", 20180531)
    checkSqlAnswer("SELECT nf_dateint('2018-05-31')", 20180531)
    checkSqlAnswer("SELECT nf_dateint(1527806973000)", 20180531)
    checkSqlAnswer("SELECT nf_dateint(1527806973)", 20180531)
    checkSqlAnswer("SELECT nf_dateint(date '2018-05-31')", 20180531)
    checkSqlAnswer("SELECT nf_dateint(timestamp '2018-05-31 12:20:21.010')",
      20180531)
    checkSqlAnswer("SELECT nf_dateint('2018-05-31T12:20:21.010')", 20180531)
    checkSqlAnswer("SELECT nf_dateint('2018-05-31 12:20:21.010')", 20180531)
    checkSqlAnswer("SELECT nf_dateint(null)", null)
    checkSqlAnswer("SELECT nf_dateint('20183105', 'yyyyddMM')", 20180531)
    checkSqlAnswer("SELECT nf_dateint('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')",
      20180531)
    checkSqlAnswer("SELECT nf_dateint('20183105', null)", null)
    val d0 = NetflixDateTimeUtils.toLocalDate(
      sql("""SELECT nf_dateint_today()""").collect().head.getInt(0)).toEpochDay
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 - d0 <= 1)
  }

  test("nf_datestr & nf_datestr_today") {
    checkSqlAnswer("SELECT nf_datestr(20180531)", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr(20181531)", null)
    checkSqlAnswer("SELECT nf_datestr(20181531)", null)
    checkSqlAnswer("SELECT nf_datestr('20180531')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('2018-05-31')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr(1527806973000)", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr(1527806973)", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr(date '2018-05-31')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr(timestamp '2018-05-31 12:20:21.010')",
      "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('2018-05-31T12:20:21.010')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('2018-05-31 12:20:21.010')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('20183105', 'yyyyddMM')", "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')",
      "2018-05-31")
    checkSqlAnswer("SELECT nf_datestr('20183105', null)", null)
    val d0 = LocalDate.parse(
      sql("""SELECT nf_datestr_today()""").collect().head.getString(0)).toEpochDay
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 - d0 <= 1)
  }

  test("nf_to_unixtime & nf_unixtime_now") {
    val d0 = sql("""SELECT nf_unixtime_now()""").collect().head.getLong(0)
    val d1 = System.currentTimeMillis() / 1000
    assert(d0 <= d1)
    val d2 = sql("""SELECT nf_unixtime_now_ms()""").collect().head.getLong(0)
    val d3 = System.currentTimeMillis()
    assert(d2 <= d3)

    var ts = Timestamp.valueOf("2018-05-31 00:00:00").getTime / 1000L

    checkSqlAnswer("SELECT nf_to_unixtime(20180531)", ts)
    checkSqlAnswer("SELECT nf_to_unixtime('2018-05-31')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime('2018-31-05', 'yyyy-dd-MM')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime('20180531')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime(date '2018-05-31')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime(" + ts + ")", ts)
    checkSqlAnswer("SELECT nf_to_unixtime(" + ts * 1000 + ")", ts)

    ts = Timestamp.valueOf("2018-05-31 12:20:21.010").getTime / 1000L
    checkSqlAnswer("SELECT nf_to_unixtime(timestamp '2018-05-31 12:20:21.010')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime('2018-05-31 12:20:21.010')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime('2018-05-31T12:20:21.010')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime(null)", null)
    checkSqlAnswer("SELECT nf_to_unixtime('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')",
      ts)

    ts = Timestamp.valueOf("2018-05-31 00:00:00").getTime
    checkSqlAnswer("SELECT nf_to_unixtime_ms(20180531)", ts)
    checkSqlAnswer("SELECT nf_to_unixtime_ms(20181531)", null)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('2018-05-31')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('2018-31-05', 'yyyy-dd-MM')",
      ts)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('20180531')", ts)
    checkSqlAnswer("SELECT nf_to_unixtime_ms(date '2018-05-31')", ts)

    val ts_ms = Timestamp.valueOf("2018-05-31 12:20:21.010").getTime
    checkSqlAnswer("SELECT nf_to_unixtime_ms(timestamp '2018-05-31 12:20:21.010')",
      ts_ms)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('2018-05-31 12:20:21.010')",
      ts_ms)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('2018-05-31T12:20:21.010')",
      ts_ms)
    checkSqlAnswer("SELECT nf_to_unixtime_ms(null)", null)
    checkSqlAnswer("SELECT nf_to_unixtime_ms('2018-31-05 12:20:21.010'," +
      "'yyyy-dd-MM HH:mm:ss.SSS')", ts_ms)
  }

  test("nf_date & nf_date_today") {
    val res = Date.valueOf("2018-05-31")
    checkSqlAnswer("SELECT nf_date(20180531)", res)
    checkSqlAnswer("SELECT nf_date(20181531)", null)
    checkSqlAnswer("SELECT nf_date('20180531')", res)
    checkSqlAnswer("SELECT nf_date('2018-05-31')", res)
    checkSqlAnswer("SELECT nf_date(1527806973000)", res)
    checkSqlAnswer("SELECT nf_date(1527806973)", res)
    checkSqlAnswer("SELECT nf_date(date '2018-05-31')", res)
    checkSqlAnswer("SELECT nf_date(timestamp '2018-05-31 12:20:21.010')",
      res)
    checkSqlAnswer("SELECT nf_date('2018-05-31T12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_date('2018-05-31 12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_date('20183105', 'yyyyddMM')", res)
    checkSqlAnswer("SELECT nf_date('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_date('20183105', null)", null)
    val d0 = DateTimeUtils.fromJavaDate(
      sql("""SELECT nf_date_today()""").collect().head.getDate(0))
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 - d0 <= 1)
  }

  test("nf_timestamp & nf_timestamp_now") {
    var res = Timestamp.valueOf("2018-05-31 00:00:00.000")
    checkSqlAnswer("SELECT nf_timestamp(20180531)", res)
    checkSqlAnswer("SELECT nf_timestamp(20181531)", null)
    checkSqlAnswer("SELECT nf_timestamp('20180531')", res)
    checkSqlAnswer("SELECT nf_timestamp('20181531')", null)
    checkSqlAnswer("SELECT nf_timestamp('2018-05-31')", res)
    checkSqlAnswer("SELECT nf_timestamp(date '2018-05-31')", res)
    checkSqlAnswer("SELECT nf_timestamp('20183105', 'yyyyddMM')", res)
    checkSqlAnswer("SELECT nf_timestamp(" + (res.getTime / 1000) + ")", res)
    checkSqlAnswer("SELECT nf_timestamp(" + (res.getTime) + ")", res)
    res = Timestamp.valueOf("2018-05-31 12:20:21.010")
    checkSqlAnswer("SELECT nf_timestamp(1527794421010)", res)
    checkSqlAnswer("SELECT nf_timestamp(timestamp '2018-05-31 12:20:21.010')",
      res)
    checkSqlAnswer("SELECT nf_timestamp('2018-05-31T12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_timestamp('2018-05-31 12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_timestamp('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_timestamp('20183105', null)", null)
    val d0 = sql("""SELECT nf_timestamp_now()""").collect().head.getTimestamp(0).getTime
    val d1 = System.currentTimeMillis()
    assert(d0 <= d1)
  }

  test("nf_datetrunc") {
    checkSqlAnswer("SELECT nf_datetrunc('month', 20180531)", 20180501)
    checkSqlAnswer("SELECT nf_datetrunc('month','20180531')", "20180501")
    checkSqlAnswer("SELECT nf_datetrunc('year','2018-05-31')", "2018-01-01")
    checkSqlAnswer("SELECT nf_datetrunc('quarter','2018-05-31')", "2018-04-01")
    // Truncate week to Monday according to ISO 8601. Same as Presto.
    checkSqlAnswer("SELECT nf_datetrunc('week','2018-05-31')", "2018-05-28")
    withLocale(Locale.FRENCH.toString) {
      checkSqlAnswer("SELECT nf_datetrunc('week','2018-05-31')", "2018-05-28")
    }
    checkSqlAnswer("SELECT nf_datetrunc('day', timestamp '2018-05-31 12:20:21.010')",
      Timestamp.valueOf("2018-05-31 00:00:00.0"))
    checkSqlAnswer("SELECT nf_datetrunc('millisecond', '2018-05-31T12:20:21.010')",
      "2018-05-31 12:20:21.01")
    checkSqlAnswer("SELECT nf_datetrunc('minute','2018-05-31 12:20:21.010')",
      "2018-05-31 12:20:00")
    checkSqlAnswer("SELECT nf_datetrunc(20180531, null)", null)
  }

  test("nf_dateformat") {
    var res = "2018-05-31 00:00:00.000"
    checkSqlAnswer("SELECT nf_dateformat(20180531, 'yyyy-MM-dd HH:mm:ss.SSS')", res)
    checkSqlAnswer("SELECT nf_dateformat('20180531', 'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_dateformat('2018-05-31', 'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_dateformat(date '2018-05-31', 'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    res = "2018-05-31 12:20:21.010"
    checkSqlAnswer("SELECT nf_dateformat(timestamp '2018-05-31 12:20:21.010'," +
      "'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_dateformat('2018-05-31T12:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_dateformat('2018-05-31 12:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')",
      res)
    checkSqlAnswer("SELECT nf_dateformat('20180531', null)", null)

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      checkSqlAnswer(
        "SELECT nf_dateformat(timestamp '2018-05-31 12:20:21.010', 'yyyy-MM-dd HH:mm:ss.SSS')",
        "2018-05-31 19:20:21.010")
    }
  }

  test("nf_year") {
    var res = Integer.valueOf(2018)
    checkSqlAnswer("SELECT nf_year(20180531)", res)
    checkSqlAnswer("SELECT nf_year(20181531)", null)
    checkSqlAnswer("SELECT nf_year('20180531')", res)
    checkSqlAnswer("SELECT nf_year('2018-05-31')", res)
    checkSqlAnswer("SELECT nf_year(date '2018-05-31')", res)
    checkSqlAnswer("SELECT nf_year(timestamp '2018-01-01 00:00:00.000')", res)
    checkSqlAnswer("SELECT nf_year('2018-05-31T12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_year('2018-05-31 12:20:21.010')", res)
    checkSqlAnswer("SELECT nf_year(null)", null)
  }

  test("nf_month") {
    checkSqlAnswer("SELECT nf_month(20180601)", 6)
    checkSqlAnswer("SELECT nf_month(20181501)", null)
    checkSqlAnswer("SELECT nf_month('20180531')", 5)
    checkSqlAnswer("SELECT nf_month('2018-05-31')", 5)
    checkSqlAnswer("SELECT nf_month(date '2018-05-31')", 5)
    checkSqlAnswer("SELECT nf_month(timestamp '2018-01-01 00:00:00.000')", 1)
    checkSqlAnswer("SELECT nf_month('2018-05-31T12:20:21.010')", 5)
    checkSqlAnswer("SELECT nf_month('2018-05-31 12:20:21.010')", 5)
    checkSqlAnswer("SELECT nf_month(null)", null)
  }

  test("nf_day") {
    checkSqlAnswer("SELECT nf_day(20180531)", 31)
    checkSqlAnswer("SELECT nf_day(20181531)", null)
    checkSqlAnswer("SELECT nf_day('20180531')", 31)
    checkSqlAnswer("SELECT nf_day('2018-05-31')", 31)
    checkSqlAnswer("SELECT nf_day(date '2018-05-31')", 31)
    checkSqlAnswer("SELECT nf_day(timestamp '2018-01-01 00:00:00.000')", 1)
    checkSqlAnswer("SELECT nf_day('2018-05-31T12:20:21.010')", 31)
    checkSqlAnswer("SELECT nf_day('2018-05-31 12:20:21.010')", 31)
    checkSqlAnswer("SELECT nf_day(null)", null)
  }

  test("nf_week") {
    checkSqlAnswer("SELECT nf_week(20180531)", 22)
    checkSqlAnswer("SELECT nf_week(20181531)", null)
    checkSqlAnswer("SELECT nf_week('20180531')", 22)
    checkSqlAnswer("SELECT nf_week('2018-05-31')", 22)
    checkSqlAnswer("SELECT nf_week(date '2018-05-31')", 22)
    checkSqlAnswer("SELECT nf_week(timestamp '2018-01-01 00:00:00.000')", 1)
    checkSqlAnswer("SELECT nf_week('2018-05-31T12:20:21.010')", 22)
    checkSqlAnswer("SELECT nf_week('2018-05-31 12:20:21.010')", 22)
    checkSqlAnswer("SELECT nf_week(null)", null)
  }

  test("nf_quarter") {
    checkSqlAnswer("SELECT nf_quarter(20180531)", 2)
    checkSqlAnswer("SELECT nf_quarter(20181531)", null)
    checkSqlAnswer("SELECT nf_quarter('20180531')", 2)
    checkSqlAnswer("SELECT nf_quarter('2018-05-31')", 2)
    checkSqlAnswer("SELECT nf_quarter(date '2018-05-31')", 2)
    checkSqlAnswer("SELECT nf_quarter(timestamp '2018-01-01 00:00:00.000')", 1)
    checkSqlAnswer("SELECT nf_quarter('2018-05-31T12:20:21.010')", 2)
    checkSqlAnswer("SELECT nf_quarter('2018-05-31 12:20:21.010')", 2)
    checkSqlAnswer("SELECT nf_quarter(null)", null)
  }

  test("nf_hour") {
    checkSqlAnswer("SELECT nf_hour(20180531)", 0)
    checkSqlAnswer("SELECT nf_hour(20181531)", null)
    checkSqlAnswer("SELECT nf_hour('20180531')", 0)
    checkSqlAnswer("SELECT nf_hour('2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_hour(date '2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_hour(timestamp '2018-01-01 21:00:00.000')", 21)
    checkSqlAnswer("SELECT nf_hour('2018-05-31T12:20:21.010')", 12)
    checkSqlAnswer("SELECT nf_hour('2018-05-31 12:20:21.010')", 12)
    checkSqlAnswer("SELECT nf_hour(null)", null)
  }

  test("nf_minute") {
    checkSqlAnswer("SELECT nf_minute(20180531)", 0)
    checkSqlAnswer("SELECT nf_minute(20181531)", null)
    checkSqlAnswer("SELECT nf_minute('20180531')", 0)
    checkSqlAnswer("SELECT nf_minute('2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_minute(date '2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_minute(timestamp '2018-01-01 21:05:00.000')", 5)
    checkSqlAnswer("SELECT nf_minute('2018-05-31T12:20:21.010')", 20)
    checkSqlAnswer("SELECT nf_minute('2018-05-31 12:20:21.010')", 20)
    checkSqlAnswer("SELECT nf_minute(null)", null)
  }

  test("nf_second") {
    checkSqlAnswer("SELECT nf_second(20180531)", 0)
    checkSqlAnswer("SELECT nf_second(20181531)", null)
    checkSqlAnswer("SELECT nf_second('20180531')", 0)
    checkSqlAnswer("SELECT nf_second('2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_second(date '2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_second(timestamp '2018-01-01 21:00:10.000')", 10)
    checkSqlAnswer("SELECT nf_second('2018-05-31T12:20:21.010')", 21)
    checkSqlAnswer("SELECT nf_second('2018-05-31 12:20:21.010')", 21)
    checkSqlAnswer("SELECT nf_second(null)", null)
  }

  test("nf_millisecond") {
    checkSqlAnswer("SELECT nf_millisecond(20180531)", 0)
    checkSqlAnswer("SELECT nf_millisecond(20181531)", null)
    checkSqlAnswer("SELECT nf_millisecond('20180531')", 0)
    checkSqlAnswer("SELECT nf_millisecond('2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_millisecond(date '2018-05-31')", 0)
    checkSqlAnswer("SELECT nf_millisecond(timestamp '2018-01-01 21:00:00.210')", 210)
    checkSqlAnswer("SELECT nf_millisecond('2018-05-31T12:20:21.010')", 10)
    checkSqlAnswer("SELECT nf_millisecond('2018-05-31 12:20:21.010')", 10)
    checkSqlAnswer("SELECT nf_millisecond(null)", null)
  }

  private def checkSqlAnswer(sqlString: String, expectedResult: Any): Unit = {
    checkAnswer(sql(sqlString), Row(expectedResult))
  }
}
