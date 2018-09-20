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

import java.time.{Instant, LocalDate, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal._
import java.util.{Locale, TimeZone}

import org.apache.spark.sql.catalyst.util.NetflixDateTimeUtils.TypeException
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


object NetflixDateTime {
  final lazy val dateint_formatter =
    DateTimeFormatter.ofPattern(NetflixDateTimeUtils.DATE_INT_FORMAT)
  final lazy val dateint_10_formatter =
    DateTimeFormatter.ofPattern(NetflixDateTimeUtils.DATE_INT_10_FORMAT)

  def fromLiteral(value: Any, dataType: DataType): NetflixDateTime = {
    val (temporal: Temporal, nfDateTimestampType) = (value, dataType) match {
      case (s: Int, IntegerType) if isEpoch(s) =>
        (Instant.ofEpochSecond(s), NfTimestampSecondIntegerType)

      case (dateint: Int, IntegerType) if isDateInt(dateint) =>
        (NetflixDateTimeUtils.toLocalDate(dateint), NfDateIntIntegerType)

      case (ms: Long, LongType) if isEpochMilli(ms) =>
        (Instant.ofEpochMilli(ms), NfTimestampLongType)

      case (s: Long, LongType) if isEpoch(s) =>
        (Instant.ofEpochSecond(s), NfTimestampSecondLongType)

      case (dateint: Long, LongType) if isDateInt(dateint) =>
        (NetflixDateTimeUtils.toLocalDate(dateint.toInt), NfDateIntLongType)

      case (u8s: UTF8String, StringType) =>
        val str = u8s.toString
        str.length match {
          case 8 =>
            (LocalDate.parse(str, dateint_formatter), NfDateString8Type)
          case 10 =>
            (LocalDate.parse(str, dateint_10_formatter), NfDateString10Type)
          case _ =>
            // Assume it's timestamp represented as a string
            val us = DateTimeUtils.stringToTimestamp(u8s)
              .getOrElse(throw TypeException("Could not convert string to timestamp"))
            (Instant.ofEpochMilli(us / 1000L), NfTimestampStringType)
        }

      case (days: DateTimeUtils.SQLDate, DateType) =>
        (LocalDate.ofEpochDay(days), NfDateType)

      case (us: DateTimeUtils.SQLTimestamp, TimestampType) =>
        (Instant.ofEpochMilli(us / 1000L), NfTimestampType)

      case _ => throw TypeException("Invalid data type")
    }

    temporal match {
      case _: LocalDate =>
        NetflixDateTime(temporal, nfDateTimestampType)
      case instant: Instant =>
        val dateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault)
        NetflixDateTime(dateTime, nfDateTimestampType)
    }
  }

  def toLiteralValue(obj: NetflixDateTime): Any = obj.temporal match {
    case localDate: LocalDate => obj.nfDateTimestampType match {
      case NfDateIntIntegerType => NetflixDateTimeUtils.toDateInt(localDate)
      case NfDateIntLongType => NetflixDateTimeUtils.toDateInt(localDate).toLong
      case NfDateString8Type => UTF8String.fromString(localDate.format(dateint_formatter))
      case NfDateString10Type => UTF8String.fromString(localDate.format(dateint_10_formatter))
      case NfDateType => localDate.toEpochDay.toInt
    }
    case dateTime: ZonedDateTime => obj.nfDateTimestampType match {
      case NfTimestampIntegerType => getEpochMilli(dateTime)
      case NfTimestampSecondIntegerType => getEpochMilli(dateTime) / 1000L
      case NfTimestampLongType => getEpochMilli(dateTime)
      case NfTimestampSecondLongType => getEpochMilli(dateTime) / 1000L
      case NfTimestampStringType =>
        UTF8String.fromString(DateTimeUtils.timestampToString(getEpochMilli(dateTime) * 1000L))
      case NfTimestampType => getEpochMilli(dateTime) * 1000L
    }
  }

  def isEpoch(s: Long): Boolean =
    s > NetflixDateTimeUtils.DATE_INT_MAX_THRESHOLD && NetflixDateTimeUtils.getEpochMs(s) != s

  def isEpochMilli(ms: Long): Boolean =
    ms > NetflixDateTimeUtils.DATE_INT_MAX_THRESHOLD && NetflixDateTimeUtils.getEpochMs(ms) == ms

  def isDateInt(dateint: Long): Boolean = dateint <= NetflixDateTimeUtils.DATE_INT_MAX_THRESHOLD

  def canDiff(type1: NetflixDateTimestampType, type2: NetflixDateTimestampType): Boolean =
    (type1, type2) match {
      case (t1, t2) if t1 == t2 => true
      case (NfDateIntIntegerType, NfDateIntLongType) => true
      case (NfDateIntLongType, NfDateIntIntegerType) => true
      case (NfTimestampLongType, NfTimestampIntegerType) => true
      case (NfTimestampIntegerType, NfTimestampLongType) => true
      case _ => false
    }

  def parseUnit(unitString: String): TemporalUnit = unitString.toLowerCase(Locale.ENGLISH) match {
    case "millisecond" => ChronoUnit.MILLIS
    case "second" => ChronoUnit.SECONDS
    case "minute" => ChronoUnit.MINUTES
    case "hour" => ChronoUnit.HOURS
    case "day" => ChronoUnit.DAYS
    case "week" => ChronoUnit.WEEKS
    case "quarter" => IsoFields.QUARTER_YEARS
    case "month" => ChronoUnit.MONTHS
    case "year" => ChronoUnit.YEARS
    case _ => throw TypeException(s"'${unitString}' is not a valid unit")
  }

  private def getEpochMilli(dateTime: ZonedDateTime): Long = dateTime.toInstant.toEpochMilli
}

case class NetflixDateTime(
    temporal: Temporal,
    nfDateTimestampType: NetflixDateTimestampType) {
  import NetflixDateTime._

  /** Clone with a new [[Temporal]]. */
  def withTemporal(newTemporal: Temporal): NetflixDateTime =
    if (newTemporal == temporal) this else NetflixDateTime(newTemporal, nfDateTimestampType)

  /** Clone with a new [[TimeZone]]. */
  def withTimeZone(timeZone: TimeZone): NetflixDateTime = temporal match {
    case dateTime: ZonedDateTime =>
      withTemporal(dateTime.withZoneSameInstant(timeZone.toZoneId))
    case _ =>
      this
  }

  def toLiteralValue: Any = NetflixDateTime.toLiteralValue(this)

  def add(value: Long, unit: String): NetflixDateTime =
    withTemporal(temporal.plus(value, parseUnit(unit)))

  def diff(that: NetflixDateTime, unit: String): Long =
    if (canDiff(nfDateTimestampType, that.nfDateTimestampType)) {
      temporal.until(that.temporal, parseUnit(unit))
    } else {
      throw TypeException("Can't diff these two types")
    }

  def format(format: String): UTF8String = {
    val formatter = DateTimeFormatter.ofPattern(format)
    val str = temporal match {
      case localDate: LocalDate => localDate.atStartOfDay.format(formatter)
      case dateTime: ZonedDateTime => dateTime.format(formatter)
    }
    UTF8String.fromString(str)
  }

  def trunc(unitStr: String): NetflixDateTime = {
    val unit = parseUnit(unitStr)
    temporal match {
      case localDate: LocalDate =>
        val newLocalDate = unit match {
          case ChronoUnit.DAYS => localDate
          case ChronoUnit.WEEKS =>
            // Truncate week to Monday according to ISO 8601. Same as Presto.
            localDate.`with`(WeekFields.ISO.dayOfWeek(), 1)
          case IsoFields.QUARTER_YEARS => localDate.`with`(IsoFields.DAY_OF_QUARTER, 1)
          case ChronoUnit.MONTHS => localDate.withDayOfMonth(1)
          case ChronoUnit.YEARS => localDate.withDayOfYear(1)
        }
        withTemporal(newLocalDate)

      case dateTime: ZonedDateTime =>
        withTemporal(dateTime.truncatedTo(unit))
    }
  }

  private def parseUnit(unitString: String): TemporalUnit = {
    val unit = NetflixDateTime.parseUnit(unitString)
    temporal match {
      case _: LocalDate if !unit.isDateBased =>
        throw TypeException(s"'${unitString}' is not a valid Date unit")
      case _ => unit
    }
  }

  override def toString: String =
    s"Type: ${nfDateTimestampType}, Expr: ${toLiteralValue}" +
      (temporal match {
        case dateTime: ZonedDateTime => s", TZ: ${dateTime.getZone}"
      })
}

trait NetflixDateTimestampType

private case object NfDateIntIntegerType extends NetflixDateTimestampType
private case object NfDateIntLongType extends NetflixDateTimestampType
private case object NfDateString8Type extends NetflixDateTimestampType
private case object NfDateString10Type extends NetflixDateTimestampType
private case object NfDateType extends NetflixDateTimestampType

private case object NfTimestampIntegerType extends NetflixDateTimestampType
private case object NfTimestampSecondIntegerType extends NetflixDateTimestampType
private case object NfTimestampLongType extends NetflixDateTimestampType
private case object NfTimestampSecondLongType extends NetflixDateTimestampType
private case object NfTimestampStringType extends NetflixDateTimestampType
private case object NfTimestampType extends NetflixDateTimestampType
