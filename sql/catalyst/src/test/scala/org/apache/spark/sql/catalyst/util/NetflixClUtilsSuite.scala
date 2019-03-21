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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper

class NetflixClUtilsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("jsonpath with only `clType`") {
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath("Focus", "", ""))(
      "$[?(\"Focus\" in @.type)]")
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath("Session", "", ""))(
      "$[?(\"Session\" in @.type)]")
  }

  test("jsonpath with `clType` and `extractCriteria`") {
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath("Focus", "view", null))(
      "$[?(\"Focus\" in @.type)].view")
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath(
        "Focus",
        "trackingInfo.surveyResponse",
        null))("$[?(\"Focus\" in @.type)].trackingInfo.surveyResponse")
  }

  test("jsonpath with all arguments") {
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath("Focus",
                                                      "trackingInfo",
                                                      "view==\"homeTab\""))(
      "$[?(\"Focus\" in @.type && (@.view==\"homeTab\"))].trackingInfo"
    )
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath(
        "Focus",
        "trackingInfo.surveyIdentifier",
        "view==\"homeTab\" && trackingInfo.surveyResponse==1"))(
      "$[?(\"Focus\" in @.type && (@.view==\"homeTab\"&&@.trackingInfo.surveyResponse==1))].trackingInfo.surveyIdentifier"
    )
    assertResult(
      NetflixClUtils.generateClSnapshotJaywayJsonPath(
        "Presentation",
        "trackingInfo.imageKey",
        "view==\"browseTitles\" && ( trackingInfo.trackId==9087 || trackingInfo.videoId == 12345 )"))(
      "$[?(\"Presentation\" in @.type && (@.view==\"browseTitles\"&&(@.trackingInfo.trackId==9087||@.trackingInfo.videoId==12345)))].trackingInfo.imageKey"
    )
  }
}
