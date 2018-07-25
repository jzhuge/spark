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

import org.joda.time.Chronology
import org.joda.time.DateTimeField
import org.joda.time.DateTimeFieldType
import org.joda.time.DurationField
import org.joda.time.DurationFieldType
import org.joda.time.field.DividedDateTimeField
import org.joda.time.field.OffsetDateTimeField
import org.joda.time.field.ScaledDurationField

// Forked from org.elasticsearch.common.joda.Joda
@SerialVersionUID(-5677872459807379123L)
object QuarterOfYearDateTimeField {
  private val QUARTER_OF_YEAR_DURATION_FIELD_TYPE =
    new QuarterOfYearDateTimeField.QuarterOfYearDurationFieldType
  val QUARTER_OF_YEAR = new QuarterOfYearDateTimeField

  @SerialVersionUID(-8167713675442491871L)
  private class QuarterOfYearDurationFieldType() extends DurationFieldType("quarters") {
    override def getField(chronology: Chronology): DurationField =
      new ScaledDurationField(chronology.months, QUARTER_OF_YEAR_DURATION_FIELD_TYPE, 3)
  }
}

@SerialVersionUID(-5677872459807379123L)
final class QuarterOfYearDateTimeField private() extends DateTimeFieldType("quarterOfYear") {
  override def getDurationType: DurationFieldType =
    QuarterOfYearDateTimeField.QUARTER_OF_YEAR_DURATION_FIELD_TYPE

  override def getRangeDurationType: DurationFieldType = DurationFieldType.years

  override def getField(chronology: Chronology): DateTimeField =
    new OffsetDateTimeField(new DividedDateTimeField(
      new OffsetDateTimeField(chronology.monthOfYear, -1),
      QuarterOfYearDateTimeField.QUARTER_OF_YEAR, 3), 1)
}
