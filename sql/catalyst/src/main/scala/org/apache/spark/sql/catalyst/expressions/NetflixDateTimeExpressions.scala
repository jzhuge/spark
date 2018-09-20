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

import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.TimeZone
import java.util.regex.Pattern

import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.catalyst.util.NetflixDateTime
import org.apache.spark.sql.catalyst.util.NetflixDateTimeUtils._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - " +
    "Returns the date as an integer in the format yyyyMMdd.")
case class NfDateInt(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          toDateInt(toLocalDate(getEpochMs(dateLong)))
        } else {
          toLocalDate(dateLong.toInt)
          dateLong.toInt
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
             toDateInt(toLocalDate(dateString, DATE_INT_FORMAT))
          } else if (dateString.length() == 10) {
            toDateInt(LocalDate.parse(dateString))
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }

            val timestamp: SQLTimestamp = timestampOption.get
            val epochMicros = timestamp.asInstanceOf[Long]
            toDateInt(toLocalDate(epochMicros / 1000L))
          }
        } else {
          right.dataType match {
            case StringType =>
              toDateInt(toLocalDate(dateString, inputDateFormat))
            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        toDateInt(LocalDate.ofEpochDay(epochDays))

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        toDateInt(toLocalDate(epochMicros / 1000L))

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_dateint"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the current date as an integer in the format yyyyMMdd.")
case class NfDateIntToday() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override def eval(input: InternalRow): Any = {
    toDateInt(toLocalDate(System.currentTimeMillis()))
  }

  override def prettyName: String = "nf_dateint_today"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - " +
    "Returns the date as a string in the format 'yyyy-MM-dd'.")
case class NfDateString(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = StringType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          UTF8String.fromString(toLocalDate(getEpochMs(dateLong)).toString)
        } else {
          UTF8String.fromString(toLocalDate(dateLong.toInt).toString)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
              UTF8String.fromString(toLocalDate(dateString, DATE_INT_FORMAT).toString)
          } else if (dateString.length() == 10) {
            LocalDate.parse(dateString)
            UTF8String.fromString(dateString)
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }
            val timestamp: SQLTimestamp = timestampOption.get
            val epochMicros = timestamp.asInstanceOf[Long]
            UTF8String.fromString(toLocalDate(epochMicros / 1000L).toString)
          }
        } else {
          right.dataType match {
            case StringType =>
              UTF8String.fromString(toLocalDate(dateString, inputDateFormat).toString)
            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        UTF8String.fromString(LocalDate.ofEpochDay(epochDays).toString)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        UTF8String.fromString(toLocalDate(epochMicros / 1000L).toString)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_datestring"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the date as an integer in the format yyyyMMdd.")
case class NfDateStringToday() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    UTF8String.fromString(toLocalDate(System.currentTimeMillis()).toString)
  }

  override def prettyName: String = "nf_datestring_today"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns current unix time (epoch seconds).")
case class NfUnixTimeNow() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() / 1000L
  }

  override def prettyName: String = "nf_unixtime_now"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns current unix time ms (epoch milliseconds).")
case class NfUnixTimeNowMs() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis()
  }

  override def prettyName: String = "nf_unixtime_now_ms"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - Converts the input to unix time /epoch Seconds.")
case class NfToUnixTime(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = LongType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          getEpochMs(dateLong) / 1000L
        } else {
          toUnixTime(toLocalDate(dateLong.toInt))
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
            toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT))
          } else if (dateString.length() == 10) {
            toUnixTime(LocalDate.parse(dateString))
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }
            val timestamp: SQLTimestamp = timestampOption.get
            val epochMicros = timestamp.asInstanceOf[Long]
            epochMicros / 1000000L
          }
        } else {
          right.dataType match {
            case StringType =>
              try {
                toUnixTimeMs(toLocalDateTime(dateString, inputDateFormat)) / 1000L
              } catch {
                case e: DateTimeParseException =>
                   toUnixTime(toLocalDate(dateString, inputDateFormat))
                case e: Exception => throw e
              }

            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        toUnixTime(LocalDate.ofEpochDay(epochDays))

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        epochMicros / 1000000L

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_to_unixtime"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - " +
    "Converts the input to unix time /epoch milliseconds.")
case class NfToUnixTimeMs(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = LongType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > 100000000) {
          dateLong
        } else {
          toUnixTime(toLocalDate(dateLong.toInt)) * 1000L
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
            toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000L
          } else if (dateString.length() == 10) {
            toUnixTime(LocalDate.parse(dateString)) * 1000L
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }
            val timestamp: SQLTimestamp = timestampOption.get
            val epochMicros = timestamp.asInstanceOf[Long]
            epochMicros / 1000L
          }
        } else {
          right.dataType match {
            case StringType =>
              try {
                toUnixTimeMs(toLocalDateTime(dateString, inputDateFormat))
              } catch {
                case e: DateTimeParseException =>
                  toUnixTime(toLocalDate(dateString, inputDateFormat)) * 1000L
                case e: Exception => throw e
              }
            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000L

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        epochMicros / 1000L

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_to_unixtime_ms"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - Converts epoch seconds to timestamp. " +
    "Also allows formatting the timestamp in a given format")
case class NfFromUnixTime(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback with ImplicitCastInputTypes {
  override def dataType: DataType = right match {
    case Literal(u8s: UTF8String, _) if u8s.toString == DEFAULT_DUMMY_ARGUMENT =>
      TimestampType
    case _ =>
      StringType
    }
  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any, dateFormat: Any): Any = {
    handleExceptions(() => { val epochLong = input.asInstanceOf[Long]
      val epochMs = getEpochMs(epochLong)
    val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
    if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
      epochMs * 1000L
    } else {
      val df = DateTimeFormat.forPattern(inputDateFormat).withChronology(getDefaultChronology())
      UTF8String.fromString(df.print(epochMs))
    }}, null)
  }
  override def prettyName: String = "nf_from_unixtime"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - Converts epoch milliseconds to timestamp. " +
    "Also allows formatting the timestamp in a given format")
case class NfFromUnixTimeMs(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback with ImplicitCastInputTypes {
  override def dataType: DataType = right match {
    case Literal(u8s: UTF8String, _) if u8s.toString == DEFAULT_DUMMY_ARGUMENT =>
      TimestampType
    case _ =>
      StringType
  }
  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any, dateFormat: Any): Any = {
    handleExceptions(() => { val epochMs = input.asInstanceOf[Long]
    val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
    if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
      epochMs * 1000L
    } else {
      val df = DateTimeFormat.forPattern(inputDateFormat).withChronology(getDefaultChronology())
      UTF8String.fromString(df.print(epochMs))
    }
    }, null)
  }
  override def prettyName: String = "nf_from_unixtime_ms"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, timezone) - Converts epoch seconds to timestamp " +
    "in a given timezone.")
case class NfFromUnixTimeTz(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback with ImplicitCastInputTypes {
  override def dataType: DataType = TimestampType

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any, timezone: Any): Any = {
    handleExceptions(() => {
      val epochLong = input.asInstanceOf[Long]
      val epochMs = getEpochMs(epochLong)
    val tz: String = timezone.asInstanceOf[UTF8String].toString
    DateTimeUtils.convertTz(epochMs * 1000L, DateTimeUtils.defaultTimeZone,
      TimeZone.getTimeZone(tz))
    }, null)
  }
  override def prettyName: String = "nf_from_unixtime_tz"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, timezone) - Converts epoch ms to timestamp in a given timezone.")
case class NfFromUnixTimeMsTz(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback with ImplicitCastInputTypes {
  override def dataType: DataType = TimestampType

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any, timezone: Any): Any = {
    handleExceptions(() => {
      val epochMillis = input.asInstanceOf[Long]
    val tz: String = timezone.asInstanceOf[UTF8String].toString
    DateTimeUtils.convertTz(epochMillis * 1000L, DateTimeUtils.defaultTimeZone,
      TimeZone.getTimeZone(tz))
  }, null)
  }
  override def prettyName: String = "nf_from_unixtime_ms_tz"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - " +
    "Converts the input into a date in the format 'yyyy-MM-dd'.")
case class NfDate(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = DateType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => { def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          toLocalDate(getEpochMs(dateLong)).toEpochDay.toInt
        } else {
          toLocalDate(dateLong.toInt).toEpochDay.toInt
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
            toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay
          } else if (dateString.length() == 10) {
            LocalDate.parse(dateString).toEpochDay.toInt
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }
            val timestamp: SQLTimestamp = timestampOption.get
            val epochMicros = timestamp.asInstanceOf[Long]
            toLocalDate(epochMicros / 1000L).toEpochDay.toInt
          }
        } else {
          right.dataType match {
            case StringType =>
              toLocalDate(dateString, inputDateFormat).toEpochDay.toInt
            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        inputDate.asInstanceOf[Int]

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        toLocalDate(epochMicros / 1000L).toEpochDay.toInt

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_date"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the current date in the format 'yyyy-MM-dd'.")
case class NfDateToday() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    toLocalDate(System.currentTimeMillis()).toEpochDay.toInt
  }

  override def prettyName: String = "nf_date_today"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, optional format) - " +
    "Converts the input into timestamp in the format 'yyyy-MM-dd HH:mm:ss.SSS'.")
case class NfTimestamp(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {
  override def dataType: DataType = TimestampType

  def this(date: Expression) = {
    this(date, Literal(DEFAULT_DUMMY_ARGUMENT))
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any, dateFormat: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = left.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          getEpochMs(dateLong) * 1000L
        } else {
          toUnixTime(toLocalDate(dateLong.toInt)) * 1000000L
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        val inputDateFormat: String = dateFormat.asInstanceOf[UTF8String].toString
        if (inputDateFormat.equals(DEFAULT_DUMMY_ARGUMENT)) {
          if (dateString.length() == 8) {
            toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000000L
          } else if (dateString.length() == 10) {
            toUnixTime(LocalDate.parse(dateString)) * 1000000L
          } else {
            // Assume its timestamp represented as a string
            val timestampOption = DateTimeUtils.stringToTimestamp(
              inputDate.asInstanceOf[UTF8String])
            if (timestampOption.isEmpty) {
              throw new IllegalArgumentException("Could not convert string to timestamp")
            }
            val timestamp: SQLTimestamp = timestampOption.get
            timestamp.asInstanceOf[Long]
          }
        } else {
          right.dataType match {
            case StringType =>
              try {
                toUnixTimeMs(toLocalDateTime(dateString, inputDateFormat)) * 1000L
              } catch {
                case e: DateTimeParseException =>
                  toUnixTime(toLocalDate(dateString, inputDateFormat)) * 1000000L
                case e: Exception => throw e
              }

            case _ => throw new TypeException(
              "Invalid input type of second parameter " + inputDataType)
          }
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000000L

      case TimestampType =>
        inputDate.asInstanceOf[Long]

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_timestamp"
}

@ExpressionDescription(
  usage = "_FUNC_() - Returns the current timestamp.")
case class NfTimestampNow() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }

  override def prettyName: String = "nf_timestamp_now"
}

@ExpressionDescription(
  usage = "_FUNC_(input date, numdays), _FUNC_(input date, offsetExpression)," +
    "_FUNC_(unit, value, input) - Adds the given num days / offset expression / specified unit" +
    "to the input date. The result returned is of the same type as input.")
case class NfDateAdd(param1: Expression, param2: Expression, param3: Expression)
  extends TernaryExpression with CodegenFallback {

  override def children: Seq[Expression] = Seq(param1, param2, param3)

  override def dataType: DataType = param3.dataType

  def this(param1: Expression, param2: Expression) = {
    this(Literal("day"), param2, param1)
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(first: Any, second: Any, third: Any): Any = returnNullForEx {
    val unitFirst = first.asInstanceOf[UTF8String].toString

    val (value, unit) = (second, param2.dataType) match {
      case (i: Int, IntegerType) =>
        (i.toLong, unitFirst)
      case (l: Long, LongType) =>
        (l, unitFirst)
      case (u8s: UTF8String, StringType) =>
        // Second argument is an offset expression
        def offsetExpression = u8s.toString

        val offsetPattern = Pattern.compile("([+-]?\\d+)(['yMd'])")
        val matcher = offsetPattern.matcher(offsetExpression)
        if (!matcher.matches) {
          throw new TypeException("Invalid offset expression " + offsetExpression)
        }
        val v = matcher.group(1).toLong
        matcher.group(2) match {
          case "y" =>
            (v, "year")
          case "M" =>
            (v, "month")
          case "d" =>
            (v, "day")
          case _ =>
            throw new TypeException("Invalid offset expression " + offsetExpression)
        }
      case _ => throw new TypeException("Invalid offset input type")
    }

    NetflixDateTime.fromLiteral(third, param3.dataType)
      .add(value, unit)
      .toLiteralValue
  }

  override def prettyName: String = "nf_dateadd"
}

object NfDateAdd {
  def apply(param1: Expression, param2: Expression): NfDateAdd =
    new NfDateAdd(param1, param2)
}

@ExpressionDescription(
  usage = "_FUNC_(input1, input2), _FUNC_(unit, input1, input2) - " +
    "Number of days between input1 and input2 (input2-input1)" +
    "or difference between input1 and input2 in terms of the specified unit")
case class NfDateDiff(param1: Expression, param2: Expression, param3: Expression)
  extends TernaryExpression with CodegenFallback {

  override def children: Seq[Expression] = Seq(param1, param2, param3)

  override def dataType: DataType = LongType

  def this(param1: Expression, param2: Expression) = {
    this(Literal("day"), param1, param2)
  }

  override def nullable: Boolean = true

  protected override def nullSafeEval(first: Any, second: Any, third: Any): Any = returnNullForEx {
    val unit = first.asInstanceOf[UTF8String].toString
    NetflixDateTime.fromLiteral(second, param2.dataType)
      .diff(NetflixDateTime.fromLiteral(third, param3.dataType), unit)
  }

  override def prettyName: String = "nf_datediff"
}

object NfDateDiff {
  def apply(param1: Expression, param2: Expression): NfDateDiff =
    new NfDateDiff(param1, param2)
}

@ExpressionDescription(
  usage = "_FUNC_(unit, input) - Returns the input truncated to the given unit")
case class NfDateTrunc(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {

  override def dataType: DataType = right.dataType

  override def nullable: Boolean = true

  protected override def nullSafeEval(first: Any, second: Any): Any = returnNullForEx {
    val unit = first.asInstanceOf[UTF8String].toString
    NetflixDateTime.fromLiteral(second, right.dataType)
      .trunc(unit)
      .toLiteralValue
  }

  override def prettyName: String = "nf_datetrunc"
}

@ExpressionDescription(
usage = "_FUNC_(input, format) - Returns a string representing the input in the given format")
case class NfDateFormat(left: Expression, right: Expression) extends BinaryExpression
  with CodegenFallback {

  override def dataType: DataType = StringType

  override def nullable: Boolean = true

  protected override def nullSafeEval(first: Any, second: Any): Any = returnNullForEx {
    val format = second.asInstanceOf[UTF8String].toString
    NetflixDateTime.fromLiteral(first, left.dataType).format(format)
  }

  override def prettyName: String = "nf_dateformat"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts year as an integer from the input")
case class NfYear(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          yearFromTimestamp(getEpochMs(dateLong))
        } else {
          yearFromDate(toLocalDate(dateLong.toInt).toEpochDay)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          yearFromDate(toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay)
        } else if (dateString.length() == 10) {
          yearFromDate(LocalDate.parse(dateString).toEpochDay)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          yearFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        yearFromDate(epochDays)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        yearFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_year"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts month of year as an integer from the input (1 to 12)")
case class NfMonth(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          monthFromTimestamp(getEpochMs(dateLong))
        } else {
          monthFromDate(toLocalDate(dateLong.toInt).toEpochDay)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          monthFromDate(toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay)
        } else if (dateString.length() == 10) {
          monthFromDate(LocalDate.parse(dateString).toEpochDay)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          monthFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        monthFromDate(epochDays)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        monthFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_month"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts day of month as an integer from the input (1 to 31)")
case class NfDay(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          dayFromTimestamp(getEpochMs(dateLong))
        } else {
          dayFromDate(toLocalDate(dateLong.toInt).toEpochDay)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          dayFromDate(toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay)
        } else if (dateString.length() == 10) {
          dayFromDate(LocalDate.parse(dateString).toEpochDay)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          dayFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        dayFromDate(epochDays)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        dayFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_day"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts hour of day as an integer from the input (0 to 23)")
case class NfHour(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          hourFromTimestamp(getEpochMs(dateLong))
        } else {
          hourFromTimestamp(toUnixTime(toLocalDate(dateLong.toInt)) * 1000L)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          hourFromTimestamp(toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000L)
        } else if (dateString.length() == 10) {
          hourFromTimestamp(toUnixTime(LocalDate.parse(dateString)) * 1000L)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          hourFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        hourFromTimestamp(toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000L)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        hourFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_hour"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts minute of hour as an integer from the input (0 to 59)")
case class NfMinute(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          minuteFromTimestamp(getEpochMs(dateLong))
        } else {
          minuteFromTimestamp(toUnixTime(toLocalDate(dateLong.toInt)) * 1000L)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          minuteFromTimestamp(toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000L)
        } else if (dateString.length() == 10) {
          minuteFromTimestamp(toUnixTime(LocalDate.parse(dateString)) * 1000L)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          minuteFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        minuteFromTimestamp(toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000L)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        minuteFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_minute"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts second of minute as an integer from the input (0 to 59)")
case class NfSecond(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          secondFromTimestamp(getEpochMs(dateLong))
        } else {
          secondFromTimestamp(toUnixTime(toLocalDate(dateLong.toInt)) * 1000L)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          secondFromTimestamp(toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000L)
        } else if (dateString.length() == 10) {
          secondFromTimestamp(toUnixTime(LocalDate.parse(dateString)) * 1000L)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          secondFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        secondFromTimestamp(toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000L)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        secondFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_second"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts millisecond of second as an integer " +
    "from the input (0 to 999)")
case class NfMillisecond(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          millisecondFromTimestamp(getEpochMs(dateLong))
        } else {
          millisecondFromTimestamp(toUnixTime(toLocalDate(dateLong.toInt)) * 1000L)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          millisecondFromTimestamp(toUnixTime(toLocalDate(dateString, DATE_INT_FORMAT)) * 1000L)
        } else if (dateString.length() == 10) {
          millisecondFromTimestamp(toUnixTime(LocalDate.parse(dateString)) * 1000L)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          millisecondFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        millisecondFromTimestamp(toUnixTime(LocalDate.ofEpochDay(epochDays)) * 1000L)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        millisecondFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_millisecond"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts week of year as an integer from the input (1 to 53)")
case class NfWeek(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          weekFromTimestamp(getEpochMs(dateLong))
        } else {
          weekFromDate(toLocalDate(dateLong.toInt).toEpochDay)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          weekFromDate(toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay)
        } else if (dateString.length() == 10) {
          weekFromDate(LocalDate.parse(dateString).toEpochDay)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          weekFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        weekFromDate(epochDays)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        weekFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_week"
}

@ExpressionDescription(
  usage = "_FUNC_(input) - Extracts quarter of year as an integer from the input (1 to 4)")
case class NfQuarter(child: Expression) extends UnaryExpression
  with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  protected override def nullSafeEval(inputDate: Any): Any = {
    handleExceptions(() => {def inputDataType: DataType = child.dataType
    inputDataType match {
      case LongType | IntegerType =>
        var dateLong: Long = -1
        if (inputDataType.equals(IntegerType)) {
          dateLong = inputDate.asInstanceOf[Int].toLong
        } else {
          dateLong = inputDate.asInstanceOf[Long]
        }
        if (dateLong > DATE_INT_MAX_THRESHOLD) {
          quarterFromTimestamp(getEpochMs(dateLong))
        } else {
          quarterFromDate(toLocalDate(dateLong.toInt).toEpochDay)
        }

      case StringType =>
        val dateString: String = inputDate.asInstanceOf[UTF8String].toString
        if (dateString.length() == 8) {
          quarterFromDate(toLocalDate(dateString, DATE_INT_FORMAT).toEpochDay)
        } else if (dateString.length() == 10) {
          quarterFromDate(LocalDate.parse(dateString).toEpochDay)
        } else {
          // Assume its timestamp represented as a string
          val timestampOption = DateTimeUtils.stringToTimestamp(
            inputDate.asInstanceOf[UTF8String])
          if (timestampOption.isEmpty) {
            throw new IllegalArgumentException("Could not convert string to timestamp")
          }
          val timestamp: SQLTimestamp = timestampOption.get
          val epochMicros = timestamp.asInstanceOf[Long]
          quarterFromTimestamp(epochMicros / 1000L)
        }

      case DateType =>
        val epochDays: Int = inputDate.asInstanceOf[Int]
        quarterFromDate(epochDays)

      case TimestampType =>
        val epochMicros: Long = inputDate.asInstanceOf[Long]
        quarterFromTimestamp(epochMicros / 1000L)

      case _ =>
        throw new TypeException("Invalid input type " + inputDataType)
    }}, null)
  }
  override def prettyName: String = "nf_quarter"
}
