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
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{DAYS, MILLISECONDS}

import org.joda.time.chrono.ISOChronology
import org.joda.time.DateTimeField
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.catalyst.util.QuarterOfYearDateTimeField.QUARTER_OF_YEAR

object NetflixDateTimeUtils {

  val DATE_INT_FORMAT = "yyyyMMdd"
  val DEFAULT_DUMMY_ARGUMENT = "-"
  val UTC_CHRONOLOGY = ISOChronology.getInstance(UTC)
  private val UTC_ZONE_ID = "UTC"

  // Convert a dateint in the format 'yyyyMMdd' to Java local date
  def toLocalDate(dateInt: Int): LocalDate = {
    if (dateInt >= 10000000 && dateInt < 100000000) {
      LocalDate.of(dateInt / 10000, dateInt / 100 % 100, dateInt % 100)
    } else {
      throw new IllegalArgumentException("Input must have eight digits in the format 'yyyyMMdd'")
    }
  }

  def toDateInt(localDate: LocalDate): Int = {
    localDate.getYear * 10000 + localDate.getMonthValue * 100 + localDate.getDayOfMonth
  }

  def toLocalDate(epochMs: Long): LocalDate = {
   Instant.ofEpochMilli(epochMs).atZone(ZoneId.of(UTC_ZONE_ID)).toLocalDate
  }

  def toLocalDate(dateStr: String, format: String): LocalDate = {
    val dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern(format)
    LocalDate.parse(dateStr, dateTimeFormatter)
  }

  def toLocalDateTime(dateStr: String, format: String): LocalDateTime = {
    val dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern(format)
    LocalDateTime.parse(dateStr, dateTimeFormatter)
  }

  def toUnixTime(localDate: LocalDate): Long = {
    localDate.atStartOfDay(ZoneId.of(UTC_ZONE_ID)).toEpochSecond
  }

  def toUnixTimeMs(localDateTime: LocalDateTime): Long = {
    localDateTime.atZone(ZoneId.of(UTC_ZONE_ID)).toInstant.toEpochMilli
  }

   def getDateField(chronology: ISOChronology, unit: String): DateTimeField = {
    val unitString = unit.toLowerCase(ENGLISH)
    unitString match {
      case "day" =>
         chronology.dayOfMonth
      case "week" =>
         chronology.weekOfWeekyear
      case "quarter" =>
        QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY)
      case "month" =>
         chronology.monthOfYear
      case "year" =>
         chronology.year
      case _ => throw new Exception("Invalid Argument: '" + unitString +
        "' is not a valid DATE field")
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
        QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY)
      case "month" =>
         chronology.monthOfYear
      case "year" =>
         chronology.year()
      case _ => throw new Exception("Invalid Argument: '" + unitString +
        "' is not a valid Timestamp field")
    }
  }

  def addFieldValueTimestamp(unit: String, value: Long, timestamp: Long): Long = {
    getTimestampField(ISOChronology.getInstance(), unit).add(timestamp, value.toInt)
  }

  def addFieldValueDate(unit: String, value: Long, date: Long): Long = {
    TimeUnit.MILLISECONDS.toDays(getDateField(ISOChronology.getInstance(), unit)
      .add(TimeUnit.DAYS.toMillis(date), value.toInt))
  }

  def diffDate(unit: String, date1: Long, date2: Long): Long = {
    getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2),
      DAYS.toMillis(date1))
  }

  def diffTimestamp(unit: String, timestamp1: Long, timestamp2: Long): Long = {
    getTimestampField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(timestamp2, timestamp1)
  }

  def truncateDate(unit: String, date: Long): Long = {
    val millis = getDateField(UTC_CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date))
    MILLISECONDS.toDays(millis)
  }

  def truncateTimestamp(unit: String, timestamp: Long): Long = {
   getTimestampField(UTC_CHRONOLOGY, unit).roundFloor(timestamp)
 }

  def formatDatetime(timestamp: Long, formatString: String): String = {
   try {
     DateTimeFormat.forPattern(formatString).withChronology(UTC_CHRONOLOGY)
       .withLocale(ENGLISH).print(timestamp)
   }
   catch {
     case e: IllegalArgumentException =>
       throw new IllegalArgumentException(e)
     case e: Exception => throw e
   }
 }

  def yearFromDate(date: Long): Int = {
   UTC_CHRONOLOGY.year().get(DAYS.toMillis(date))
  }

  def yearFromTimestamp(timestamp: Long): Int = {
   UTC_CHRONOLOGY.year.get(timestamp)
  }
  def monthFromTimestamp(timestamp: Long): Int = {
   UTC_CHRONOLOGY.monthOfYear.get(timestamp)
  }
  def monthFromDate(date: Long): Int = {
   UTC_CHRONOLOGY.monthOfYear().get(DAYS.toMillis(date))
  }
  def dayFromDate(date: Long): Int = {
   UTC_CHRONOLOGY.dayOfMonth().get(DAYS.toMillis(date))
  }
  def dayFromTimestamp(timestamp: Long): Int = {
   UTC_CHRONOLOGY.dayOfMonth.get(timestamp)
  }
  def hourFromTimestamp(timestamp: Long): Int = {
   UTC_CHRONOLOGY.hourOfDay().get(timestamp)
  }
  def minuteFromTimestamp(timestamp: Long): Int = {
   UTC_CHRONOLOGY.minuteOfHour.get(timestamp)
  }
  def secondFromTimestamp(timestamp: Long): Int = {
    UTC_CHRONOLOGY.secondOfMinute().get(timestamp)
  }
  def millisecondFromTimestamp(timestamp: Long): Int = {
    UTC_CHRONOLOGY.millisOfSecond.get(timestamp)
  }
  def weekFromTimestamp(timestamp: Long): Int = {
    UTC_CHRONOLOGY.weekOfWeekyear.get(timestamp)
  }
  def weekFromDate(date: Long): Int = {
    UTC_CHRONOLOGY.weekOfWeekyear().get(DAYS.toMillis(date))
  }
  def quarterFromTimestamp(timestamp: Long): Int = {
    QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY).get(timestamp)
  }
  def quarterFromDate(date: Long): Int = {
    QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY).get(DAYS.toMillis(date))
  }
}
