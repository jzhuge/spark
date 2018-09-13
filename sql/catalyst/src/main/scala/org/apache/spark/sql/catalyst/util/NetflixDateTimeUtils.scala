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
import java.util.concurrent.TimeUnit.{DAYS, MILLISECONDS}

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
    if (dateInt >= DATE_INT_MIN_THRESHOLD && dateInt < DATE_INT_MAX_THRESHOLD) {
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

  def toLocalDate(epochMs: Long): LocalDate = {
   Instant.ofEpochMilli(epochMs).atZone(ZoneId.of(
     DateTimeUtils.defaultTimeZone.getID)).toLocalDate
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
    localDate.atStartOfDay(ZoneId.of(DateTimeUtils.defaultTimeZone.getID)).toEpochSecond
  }

  def toUnixTimeMs(localDateTime: LocalDateTime): Long = {
    localDateTime.atZone(ZoneId.of(DateTimeUtils.defaultTimeZone.getID)).toInstant.toEpochMilli
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

  abstract class EpochMillis(var epochMillis: Long) {
    def add(value: Long, unit: String): Any = {
      epochMillis = getField(unit).add(epochMillis, value)
      toOutput
    }

    def diff(that: EpochMillis, unit: String): Long = {
      getField(unit).getDifferenceAsLong(that.epochMillis, epochMillis)
    }

    protected def getField(unit: String): DateTimeField
    protected def toOutput: Any

    override def toString: String = s"Output: ${toOutput.toString}, Epoch (ms): ${epochMillis}"
  }

  abstract class EpochDate(epochDay: Long) extends EpochMillis(DAYS.toMillis(epochDay)) {
    protected def epochDays: Long = MILLISECONDS.toDays(epochMillis)
    override def getField(unit: String): DateTimeField =
      getDateField(getDefaultChronology(), unit)
  }

  class EpochDateInteger(dateint: Long)
    extends EpochDate(toLocalDate(dateint.toInt).toEpochDay) {
    def toOutput: Any = toDateInt(LocalDate.ofEpochDay(epochDays))
  }

  class EpochDateLong(dateint: Long)
    extends EpochDate(toLocalDate(dateint.toInt).toEpochDay) {
    def toOutput: Any = toDateInt(LocalDate.ofEpochDay(epochDays)).toLong
  }

  class EpochDateString8(dateint: String)
    extends EpochDate(toLocalDate(dateint, DATE_INT_FORMAT).toEpochDay) {
    def toOutput: Any = UTF8String.fromString(toDateInt(LocalDate.ofEpochDay(epochDays)).toString)
  }

  class EpochDateString10(dateint: String)
    extends EpochDate(LocalDate.parse(dateint).toEpochDay) {
    def toOutput: Any = UTF8String.fromString(LocalDate.ofEpochDay(epochDays).toString)
  }

  class EpochDateObject(epochDay2: Int)
    extends EpochDate(epochDay2) {
    def toOutput: Any = epochDays.toInt
  }

  abstract class EpochTimestamp(epochMs: Long) extends EpochMillis(epochMs) {
    override def getField(unit: String): DateTimeField =
      getTimestampField(getDefaultChronology(), unit)
  }

  class EpochTimestampInteger(epochMs: Long, isEpoch: Boolean) extends EpochTimestamp(epochMs) {
    def toOutput: Any = if (isEpoch) (epochMillis/ 1000L).toInt else epochMillis.toInt
  }

  class EpochTimestampLong(epochMs2: Long, isEpoch: Boolean)
    extends EpochTimestamp(epochMs2) {
    def toOutput: Any = if (isEpoch) (epochMillis/ 1000L) else epochMillis
  }

  class EpochTimestampString(timestamp: UTF8String)
    extends EpochTimestamp(DateTimeUtils.stringToTimestamp(timestamp).map(_ / 1000L).getOrElse(
      throw new IllegalArgumentException("Could not convert string to timestamp"))) {
    def toOutput: Any = UTF8String.fromString(DateTimeUtils.timestampToString(epochMillis * 1000L))
  }

  class EpochTimestampObject(epochUs: Long)
    extends EpochTimestamp(epochUs / 1000L) {
    def toOutput: Any = epochMillis * 1000L
  }

  def fromInput(input: Any, inputDataType: DataType): Any = (input, inputDataType) match {
    case (ts: Int, IntegerType) if ts > DATE_INT_MAX_THRESHOLD =>
      val ms = getEpochMs(ts)
      new EpochTimestampInteger(ms, !(ms == ts))
    case (dateint: Int, IntegerType) if dateint <= DATE_INT_MAX_THRESHOLD =>
      new EpochDateInteger(dateint)
    case (ts: Long, LongType) if ts > DATE_INT_MAX_THRESHOLD =>
      val ms = getEpochMs(ts)
      new EpochTimestampLong(ms, !(ms == ts))
    case (dateint: Long, LongType) if dateint <= DATE_INT_MAX_THRESHOLD =>
      new EpochDateLong(dateint)
    case (u8s: UTF8String, StringType) =>
      val s = u8s.toString
      s.length match {
        case 8 =>
          new EpochDateString8(s)
        case 10 =>
          new EpochDateString10(s)
        case _ =>
          // Assume its timestamp represented as a string
          new EpochTimestampString(u8s)
      }
    case (d: Int, DateType) =>
      new EpochDateObject(d)
    case (us: Long, TimestampType) =>
      new EpochTimestampObject(us)
    case _ =>
      throw TypeException("Invalid input type")
  }

  def truncateDate(unit: String, date: Long): Long = {
    val millis = getDateField(getDefaultChronology(), unit).roundFloor(DAYS.toMillis(date))
    MILLISECONDS.toDays(millis)
  }

  def truncateTimestamp(unit: String, timestamp: Long): Long = {
   getTimestampField(ISOChronology.getInstance(DateTimeZone.forTimeZone(
     DateTimeUtils.defaultTimeZone)),
     unit).roundFloor(timestamp)
 }

  def dateIntEpochTrunc(unit: String, timestamp: Long): Long = {
    val epochMs = getEpochMs(timestamp)
    var isEpoch = false
    if (epochMs != timestamp) isEpoch = true
    val res = truncateTimestamp(unit, timestamp)
    if (isEpoch)  {
      res / 1000L
    } else {
      res
    }
  }

  def formatDatetime(timestamp: Long, formatString: String): String = {
   try {
     DateTimeFormat.forPattern(formatString).withChronology(getDefaultChronology())
       .withLocale(ENGLISH).print(timestamp)
   }
   catch {
     case e: IllegalArgumentException =>
       throw new IllegalArgumentException(e)
     case e: Exception => throw e
   }
 }

  def yearFromDate(date: Long): Int = {
    getDefaultChronology().year().get(toUnixTime(LocalDate.ofEpochDay(date)) * 1000L)
  }

  def yearFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().year.get(timestamp)
  }
  def monthFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().monthOfYear.get(timestamp)
  }
  def monthFromDate(date: Long): Int = {
    getDefaultChronology().monthOfYear().get(toUnixTime(LocalDate.ofEpochDay(date)) * 1000L)
  }
  def dayFromDate(date: Long): Int = {
    getDefaultChronology().dayOfMonth().get(toUnixTime(LocalDate.ofEpochDay(date)) * 1000L)
  }
  def dayFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().dayOfMonth.get(timestamp)
  }
  def hourFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().hourOfDay().get(timestamp)
  }
  def minuteFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().minuteOfHour.get(timestamp)
  }
  def secondFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().secondOfMinute().get(timestamp)
  }
  def millisecondFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().millisOfSecond.get(timestamp)
  }
  def weekFromTimestamp(timestamp: Long): Int = {
    getDefaultChronology().weekOfWeekyear.get(timestamp)
  }
  def weekFromDate(date: Long): Int = {
    getDefaultChronology().weekOfWeekyear()
      .get(toUnixTime(LocalDate.ofEpochDay(date)) * 1000L)
  }
  def quarterFromTimestamp(timestamp: Long): Int = {
    QUARTER_OF_YEAR.getField(getDefaultChronology())
      .get(timestamp)
  }
  def quarterFromDate(date: Long): Int = {
    QUARTER_OF_YEAR.getField(getDefaultChronology())
      .get(toUnixTime(LocalDate.ofEpochDay(date)) * 1000L)
  }

  def getDefaultChronology(): ISOChronology = {
    ISOChronology.getInstance(DateTimeZone.forTimeZone(DateTimeUtils.defaultTimeZone));
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
