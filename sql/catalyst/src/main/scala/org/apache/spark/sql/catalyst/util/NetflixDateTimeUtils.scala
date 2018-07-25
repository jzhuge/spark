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
import java.util.concurrent.TimeUnit.{DAYS, MILLISECONDS, SECONDS}

import org.joda.time.{DateTimeField, DateTimeZone}
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.catalyst.util.QuarterOfYearDateTimeField.QUARTER_OF_YEAR
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object NetflixDateTimeUtils {

  val DATE_INT_MAX_THRESHOLD = 100000000L
  val DATE_INT_MIN_THRESHOLD = 10000000L
  val DATE_INT_FORMAT = "yyyyMMdd"
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

  abstract class EpochMillis(var epochMillisUTC: Long) {
    def add(value: Long, unit: String): Any = {
      epochMillisUTC = getField(unit).add(epochMillisUTC, value)
      toOutput
    }

    def diff(that: EpochMillis, unit: String): Long = {
      getField(unit).getDifferenceAsLong(that.epochMillisUTC, epochMillisUTC)
    }

    protected def getField(unit: String): DateTimeField
    protected def toOutput: Any

    override def toString: String = s"Output: ${toOutput.toString}, Epoch (ms): ${epochMillisUTC}"
  }

  abstract class EpochDate(epochDay: Long, timeZone: TimeZone)
    extends EpochMillis(DAYS.toMillis(epochDay)) {
    protected def epochDays: Long = MILLISECONDS.toDays(epochMillisUTC)
    override def getField(unit: String): DateTimeField =
      getDateField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)), unit)
  }

  class EpochDateInteger(dateint: Long, timeZone: TimeZone)
    extends EpochDate(
      SECONDS.toDays(toUnixTime(toLocalDate(dateint.toInt), timeZone.getID)),
      timeZone) {
    def toOutput: Any = toDateInt(LocalDate.ofEpochDay(epochDays))
  }

  class EpochDateLong(dateint: Long, timeZone: TimeZone)
    extends EpochDate(
      SECONDS.toDays(toUnixTime(toLocalDate(dateint.toInt), timeZone.getID)),
      timeZone) {
    def toOutput: Any = toDateInt(LocalDate.ofEpochDay(epochDays)).toLong
  }

  class EpochDateString8(dateint: String, timeZone: TimeZone)
    extends EpochDate(toLocalDate(dateint, DATE_INT_FORMAT).toEpochDay, timeZone) {
    def toOutput: Any = UTF8String.fromString(toDateInt(LocalDate.ofEpochDay(epochDays)).toString)
  }

  class EpochDateString10(dateint: String, timeZone: TimeZone)
    extends EpochDate(LocalDate.parse(dateint).toEpochDay, timeZone) {
    def toOutput: Any = UTF8String.fromString(LocalDate.ofEpochDay(epochDays).toString)
  }

  class EpochDateObject(epochDay2: Int, timeZone: TimeZone)
    extends EpochDate(epochDay2, timeZone) {
    def toOutput: Any = epochDays.toInt
  }

  abstract class EpochTimestamp(val epochInTs: Long, timeZone: TimeZone)
    extends EpochMillis(getEpochMs(epochInTs)) {
    private def isEpoch: Boolean = getEpochMs(epochInTs) != epochInTs
    private def epochMillis: Long = epochMillisUTC
    protected def epochOutTs = if (isEpoch) epochMillis / 1000L else epochMillis
    override def getField(unit: String): DateTimeField =
      getTimestampField(ISOChronology.getInstance(DateTimeZone.forTimeZone(timeZone)), unit)
  }

  class EpochTimestampInteger(epochInTs: Long, timeZone: TimeZone)
    extends EpochTimestamp(epochInTs, timeZone) {
    def toOutput: Any = epochOutTs.toInt
  }

  class EpochTimestampLong(epochInTs: Long, timeZone: TimeZone)
    extends EpochTimestamp(epochInTs, timeZone) {
    def toOutput: Any = epochOutTs
  }

  class EpochTimestampString(timestamp: UTF8String, timeZone: TimeZone)
    extends EpochTimestamp(
      DateTimeUtils.stringToTimestamp(timestamp, timeZone).map(_ / 1000L).getOrElse(
        throw new IllegalArgumentException("Could not convert string to timestamp")),
      timeZone) {
    def toOutput: Any =
      UTF8String.fromString(DateTimeUtils.timestampToString(epochOutTs * 1000L, timeZone))
  }

  class EpochTimestampObject(epochUs: Long, timeZone: TimeZone)
    extends EpochTimestamp(epochUs / 1000L, timeZone) {
    def toOutput: Any = epochOutTs * 1000L
  }

  def fromInput(input: Any, inputDataType: DataType, timeZone: TimeZone): Any =
    (input, inputDataType) match {
      case (ts: Int, IntegerType) if ts > DATE_INT_MAX_THRESHOLD =>
        new EpochTimestampInteger(ts, timeZone)
      case (dateint: Int, IntegerType) if dateint <= DATE_INT_MAX_THRESHOLD =>
        new EpochDateInteger(dateint, timeZone)
      case (ts: Long, LongType) if ts > DATE_INT_MAX_THRESHOLD =>
        new EpochTimestampLong(ts, timeZone)
      case (dateint: Long, LongType) if dateint <= DATE_INT_MAX_THRESHOLD =>
        new EpochDateLong(dateint, timeZone)
      case (u8s: UTF8String, StringType) =>
        val s = u8s.toString
        s.length match {
          case 8 =>
            new EpochDateString8(s, timeZone)
          case 10 =>
            new EpochDateString10(s, timeZone)
          case _ =>
            // Assume its timestamp represented as a string
            new EpochTimestampString(u8s, timeZone)
        }
      case (d: Int, DateType) =>
        new EpochDateObject(d, timeZone)
      case (us: Long, TimestampType) =>
        new EpochTimestampObject(us, timeZone)
      case _ =>
        throw TypeException("Invalid input type")
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
