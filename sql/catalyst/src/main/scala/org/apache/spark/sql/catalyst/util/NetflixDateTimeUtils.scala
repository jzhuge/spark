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

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.Locale.ENGLISH
import java.util.TimeZone
import java.util.concurrent.TimeUnit.MILLISECONDS

import org.joda.time.{DateTimeField, DateTimeZone}
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.catalyst.util.QuarterOfYearDateTimeField.QUARTER_OF_YEAR
import org.apache.spark.sql.internal.SQLConf

object NetflixDateTimeUtils {

  val DATE_INT_MAX_THRESHOLD = 100000000L
  val DATE_INT_MIN_THRESHOLD = 10000000L
  val DATE_INT_FORMAT = "yyyyMMdd"
  val DATE_INT_10_FORMAT = "yyyy-MM-dd"
  val DEFAULT_DUMMY_ARGUMENT = "-"

  // Convert a dateint in the format 'yyyyMMdd' to Java local date
  def toLocalDate(dateInt: Int): LocalDate = {
    if (dateInt >= 10000000 && dateInt < DATE_INT_MAX_THRESHOLD) {
      LocalDate.of(dateInt / 10000, dateInt / 100 % 100, dateInt % 100)
    } else {
      throw new IllegalArgumentException("Input must have eight digits in the format 'yyyyMMdd'")
    }
  }

  def getEpochMs(value: Long): Long = {
    val length = (Math.log10(value) + 1).toInt
    if (length == 10) {
      value * 1000L;
    } else if (length == 13) {
      value
    } else {
      throw new IllegalArgumentException("Only 10 (epoch) or 13 (epochMs) " +
        "digit numbers are accepted.")
    }
  }

  def toDateInt(localDate: LocalDate): Int = {
    localDate.getYear * 10000 + localDate.getMonthValue * 100 + localDate.getDayOfMonth
  }

  def toLocalDate(epochMs: Long, timezone: String): LocalDate = {
    Instant.ofEpochMilli(epochMs).atZone(ZoneId.of(timezone)).toLocalDate
  }

  def toLocalDate(dateStr: String, format: String): LocalDate = {
    val dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern(format)
    LocalDate.parse(dateStr, dateTimeFormatter)
  }

  def toLocalDateTime(dateStr: String, format: String): LocalDateTime = {
    val dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern(format)
    LocalDateTime.parse(dateStr, dateTimeFormatter)
  }

  def toUnixTime(localDate: LocalDate, timezone: String): Long = {
    localDate.atStartOfDay(ZoneId.of(timezone)).toEpochSecond
  }

  def toUnixTimeMs(localDateTime: LocalDateTime, timezone: String): Long = {
    localDateTime.atZone(ZoneId.of(timezone)).toInstant.toEpochMilli
  }

   def getDateField(chronology: ISOChronology, unit: String): DateTimeField = {
    val unitString = unit.toLowerCase(ENGLISH)
    unitString match {
      case "day" =>
         chronology.dayOfMonth
      case "week" =>
         chronology.weekOfWeekyear
      case "quarter" =>
        QUARTER_OF_YEAR.getField(chronology)
      case "month" =>
         chronology.monthOfYear
      case "year" =>
         chronology.year
      case _ => throw new TypeException("Invalid Argument: '" + unitString +
        "' is not a valid Date field")
    }
  }

   def getTimestampField(chronology: ISOChronology, unit: String): DateTimeField = {
    val unitString = unit.toLowerCase(ENGLISH)
    unitString match {
      case "millisecond" =>
         chronology.millisOfSecond
      case "second" =>
         chronology.secondOfMinute
      case "minute" =>
         chronology.minuteOfHour
      case "hour" =>
         chronology.hourOfDay
      case "day" =>
         chronology.dayOfMonth
      case "week" =>
         chronology.weekOfWeekyear
      case "quarter" =>
        QUARTER_OF_YEAR.getField(chronology)
      case "month" =>
         chronology.monthOfYear
      case "year" =>
         chronology.year()
      case _ => throw new TypeException("Invalid Argument: '" + unitString +
        "' is not a valid Timestamp field")
    }
  }

  def truncateDate(unit: String, date: LocalDate, timezone: TimeZone): Long = {
    val millis = getDateField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timezone)),
      unit).roundFloor(toUnixTime(date, timezone.getID) * 1000L)
    MILLISECONDS.toDays(millis)
  }

  def truncateTimestamp(unit: String, timestamp: Long, timezone: TimeZone): Long = {
   getTimestampField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timezone)),
     unit).roundFloor(timestamp)
 }

  def dateIntEpochTrunc(unit: String, timestamp: Long, timezone: TimeZone): Long = {
    val epochMs = getEpochMs(timestamp)
    var isEpoch = false
    if (epochMs != timestamp) isEpoch = true
    val res = truncateTimestamp(unit, timestamp, timezone)
    if (isEpoch)  {
      res / 1000L
    } else {
      res
    }
  }

  def formatDatetime(timestamp: Long, formatString: String, timeZone: TimeZone): String = {
   try {
     DateTimeFormat.forPattern(formatString).withChronology(
       ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)))
       .withLocale(ENGLISH).print(timestamp)
   }
   catch {
     case e: IllegalArgumentException =>
       throw new IllegalArgumentException(e)
     case e: Exception => throw e
   }
 }

  def yearFromDate(date: LocalDate, timeZone: TimeZone): Int = {
   ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).year().
     get(toUnixTime(date, timeZone.getID) * 1000L)
  }

  def yearFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).year.get(timestamp)
  }
  def monthFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).monthOfYear.get(timestamp)
  }
  def monthFromDate(date: LocalDate, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).monthOfYear()
      .get(toUnixTime(date, timeZone.getID) * 1000L)
  }
  def dayFromDate(date: LocalDate, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).dayOfMonth()
      .get(toUnixTime(date, timeZone.getID) * 1000L)
  }
  def dayFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).dayOfMonth.get(timestamp)
  }
  def hourFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).hourOfDay().get(timestamp)
  }
  def minuteFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).minuteOfHour.get(timestamp)
  }
  def secondFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).secondOfMinute().get(timestamp)
  }
  def millisecondFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).millisOfSecond.get(timestamp)
  }
  def weekFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).weekOfWeekyear.get(timestamp)
  }
  def weekFromDate(date: LocalDate, timeZone: TimeZone): Int = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)).weekOfWeekyear()
      .get(toUnixTime(date, timeZone.getID) * 1000L)
  }
  def quarterFromTimestamp(timestamp: Long, timeZone: TimeZone): Int = {
    QUARTER_OF_YEAR.getField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)))
      .get(timestamp)
  }
  def quarterFromDate(date: LocalDate, timeZone: TimeZone): Int = {
    QUARTER_OF_YEAR.getField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)))
      .get(toUnixTime(date, timeZone.getID) * 1000L)
  }

  def returnNullForEx[T](udf: => T): T = handleExceptions(() => udf, null.asInstanceOf[T])

  def handleExceptions[T](udf: () => T, defaultValue: T): T = {
    try {
      udf()
    }
    catch {
      case e: Exception =>
        if (SQLConf.get.enableNetflixUdfStrictEval) {
          throw e
        } else if (e.isInstanceOf[TypeException]) {
          throw e
        } else {
          defaultValue
        }
    }
  }

  case class TypeException(s: String)  extends Exception(s)
}
