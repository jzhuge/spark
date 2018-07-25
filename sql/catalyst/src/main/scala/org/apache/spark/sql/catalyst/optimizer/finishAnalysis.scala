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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.NetflixDateTimeUtils.{toDateInt, toLocalDate}
import org.apache.spark.sql.types._


/**
 * Finds all [[RuntimeReplaceable]] expressions and replace them with the expressions that can
 * be evaluated. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case e: RuntimeReplaceable => e.child
  }
}


/**
 * Computes the current date and time to make sure we return the same result in a single query.
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val currentDates = mutable.Map.empty[String, Literal]
    val currentDateInts = mutable.Map.empty[String, Literal]
    val currentDateStrings = mutable.Map.empty[String, Literal]
    val currentTimestamps = mutable.Map.empty[String, Literal]
    val timeExpr = CurrentTimestamp()
    val timestamp = timeExpr.eval(EmptyRow).asInstanceOf[Long]
    val currentTime = Literal.create(timestamp, timeExpr.dataType)
    val nfTimestampExpr = NfTimestampNow()
    val nfUnixTimeExpr = NfUnixTimeNow()
    val nfUnixTimeMsExpr = NfUnixTimeNowMs()
    val nfUnixTimeNow = Literal.create(nfUnixTimeExpr.eval(EmptyRow), nfUnixTimeExpr.dataType)
    val nfUnixTimeNowMs = Literal.create(nfUnixTimeMsExpr.eval(EmptyRow), nfUnixTimeMsExpr.dataType)

    plan transformAllExpressions {
      case CurrentDate(Some(timeZoneId)) =>
        currentDates.getOrElseUpdate(timeZoneId, {
          Literal.create(
            DateTimeUtils.millisToDays(timestamp / 1000L, DateTimeUtils.getTimeZone(timeZoneId)),
            DateType)
        })
      case CurrentTimestamp() => currentTime
      case NfDateIntToday(Some(timeZoneId)) =>
        currentDateInts.getOrElseUpdate(timeZoneId, {
          Literal.create(toDateInt(toLocalDate(System.currentTimeMillis(),
            DateTimeUtils.getTimeZone(timeZoneId).getID)), IntegerType)
        })
      case NfDateStringToday(Some(timeZoneId)) =>
        currentDateStrings.getOrElseUpdate(timeZoneId, {
          Literal.create(toLocalDate(System.currentTimeMillis(),
            DateTimeUtils.getTimeZone(timeZoneId).getID).toString, StringType)
        })
      case NfDateToday(Some(timeZoneId)) =>
        currentDates.getOrElseUpdate(timeZoneId, {
          Literal.create(
            DateTimeUtils.millisToDays(timestamp / 1000L, DateTimeUtils.getTimeZone(timeZoneId)),
            DateType)
        })

      case NfTimestampNow(Some(timeZoneId)) =>
        currentTimestamps.getOrElseUpdate(timeZoneId, {
          Literal.create(
            DateTimeUtils.convertTz(System.currentTimeMillis() * 1000L,
              DateTimeUtils.defaultTimeZone(), DateTimeUtils.getTimeZone(timeZoneId)),
            TimestampType)
        })
      case NfUnixTimeNow() => nfUnixTimeNow
      case NfUnixTimeNowMs() => nfUnixTimeNowMs

    }
  }
}

/** Replaces the expression of CurrentDatabase with the current database name. */
case class GetCurrentDatabase(sessionCatalog: SessionCatalog) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case CurrentDatabase() =>
        Literal.create(sessionCatalog.getCurrentDatabase, StringType)
    }
  }
}
