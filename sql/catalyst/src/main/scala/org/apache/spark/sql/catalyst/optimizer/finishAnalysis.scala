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

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
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
    val dateExpr = CurrentDate()
    val timeExpr = CurrentTimestamp()
    val nfDateIntExpr = NfDateIntToday()
    val nfDateStringExpr = NfDateStringToday()
    val nfDateExpr = NfDateToday()
    val nfTimestampExpr = NfTimestampNow()
    val nfUnixTimeExpr = NfUnixTimeNow()
    val nfUnixTimeMsExpr = NfUnixTimeNowMs()
    val currentDate = Literal.create(dateExpr.eval(EmptyRow), dateExpr.dataType)
    val currentTime = Literal.create(timeExpr.eval(EmptyRow), timeExpr.dataType)
    val nfDateIntToday = Literal.create(nfDateIntExpr.eval(EmptyRow), nfDateIntExpr.dataType)
    val nfDateStringToday = Literal.create(nfDateStringExpr.eval(EmptyRow),
      nfDateStringExpr.dataType)
    val nfDateToday = Literal.create(nfDateExpr.eval(EmptyRow), nfDateExpr.dataType)
    val nfUnixTimeNow = Literal.create(nfUnixTimeExpr.eval(EmptyRow), nfUnixTimeExpr.dataType)
    val nfUnixTimeNowMs = Literal.create(nfUnixTimeMsExpr.eval(EmptyRow), nfUnixTimeMsExpr.dataType)
    val nfTimestampNow = Literal.create(nfTimestampExpr.eval(EmptyRow), nfTimestampExpr.dataType)

    plan transformAllExpressions {
      case CurrentDate() => currentDate
      case CurrentTimestamp() => currentTime
      case NfDateIntToday() => nfDateIntToday
      case NfDateStringToday() => nfDateStringToday
      case NfDateToday() => nfDateToday
      case NfUnixTimeNow() => nfUnixTimeNow
      case NfUnixTimeNowMs() => nfUnixTimeNowMs
      case NfTimestampNow() => nfTimestampNow
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
