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

import org.apache.spark.SparkFunSuite

class NetflixClExpressionsSuit extends SparkFunSuite with ExpressionEvalHelper {

  private val snapshot =
    """
      |[{"sequence":1,"id":757294327146741,"source":"iOS","schema":{"name":"App","version":"1.26.0"},"type":["Log","Session"],"time":559440041005},
      |{"type":["ProcessState"],"id":757294397275504,"computation":"none","allocation":"none","interaction":"none"},
      |{"type":["ProcessState"],"id":757294425125683,"computation":"normal","allocation":"normal","interaction":"direct"},
      |{"sequence":3,"id":757294439218544,"type":["ProcessStateTransition","Action","Session"],"time":559440041006},
      |{"nfvdid":"blahblablb","id":757294485523660,"sequence":4,"type":["VisitorDeviceId","AccountIdentity","Session"],"time":559440041008},
      |{"sequence":5,"id":757294513373839,"type":["UserInteraction","Session"],"time":559440041008},
      |{"model":"APPLE_iPhone9-1","type":["Device"],"id":757294562698854},
      |{"type":["OsVersion"],"osVersion":"11.4.1","id":757294575785082},
      |{"appVersion":"11.2.0","type":["AppVersion"],"id":757294615379312},
      |{"uiVersion":"ios11.2.0 (2265)","type":["UiVersion"],"id":757294643565035},
      |{"esn":"","type":["Esn"],"id":757294732484280},
      |{"type":["NrdLib"],"id":757294743557242,"appVersion":"11.2.0 (2265)","sdkVersion":"2012.4","libVersion":"2012.4"},
      |{"userAgent":" App/11.2.0 ( iOS 11.4.1 )","type":["UserAgent"],"id":757294786171371},
      |{"utcOffset":-18000,"type":["TimeZone"],"id":757294829121044},
      |{"muting":false,"id":757294867037552,"level":0.875,"type":["Volume"]},
      |{"type":["WifiConnection","NetworkConnection"],"id":757294904954060},
      |{"type":["UiLocale"],"uiLocale":"en","id":757294954950164},
      |{"cells":{"7972":1,"10953":4},"type":["TestAllocations"],"id":757294995551027},
      |{"trackingInfo":{"videoId":12345,"trackId":9087,"imageKey":"test1"},"sequence":9,"id":757295011213817,"view":"browseTitles","type":["Presentation","Session"],"time":559440043683},
      |{"trackingInfo":{"surveyResponse":1,"surveyIdentifier":"IO_80203147"},"sequence":11,"id":757295111313817,"view":"homeTab","type":["Focus","Session"],"time":559440043683}]
    """.stripMargin.replace("\n", "")

  test("function with only `clType`") {
    val result = List(
      "{\"id\":757295111313817,\"sequence\":11,\"time\":559440043683,\"trackingInfo\":{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1},\"type\":[\"Focus\",\"Session\"],\"view\":\"homeTab\"}"
    )
    checkEvaluation(ClSnapshotExtract(Literal(snapshot),
                                      Literal("Focus"),
                                      Literal(""),
                                      Literal("")),
                    result)
    val result1 = List(
      "{\"id\":757294327146741,\"schema\":{\"name\":\"App\",\"version\":\"1.26.0\"},\"sequence\":1,\"source\":\"iOS\",\"time\":559440041005,\"type\":[\"Log\",\"Session\"]}",
      "{\"id\":757294439218544,\"sequence\":3,\"time\":559440041006,\"type\":[\"ProcessStateTransition\",\"Action\",\"Session\"]}",
      "{\"id\":757294485523660,\"nfvdid\":\"blahblablb\",\"sequence\":4,\"time\":559440041008,\"type\":[\"VisitorDeviceId\",\"AccountIdentity\",\"Session\"]}",
      "{\"id\":757294513373839,\"sequence\":5,\"time\":559440041008,\"type\":[\"UserInteraction\",\"Session\"]}",
      "{\"id\":757295011213817,\"sequence\":9,\"time\":559440043683,\"trackingInfo\":{\"imageKey\":\"test1\",\"trackId\":9087,\"videoId\":12345},\"type\":[\"Presentation\",\"Session\"],\"view\":\"browseTitles\"}",
      "{\"id\":757295111313817,\"sequence\":11,\"time\":559440043683,\"trackingInfo\":{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1},\"type\":[\"Focus\",\"Session\"],\"view\":\"homeTab\"}"
    )
    checkEvaluation(ClSnapshotExtract(Literal(snapshot),
                                      Literal("Session"),
                                      Literal(""),
                                      Literal("")),
                    result1)
  }

  test("function with `clType` and `extractCriteria`") {
    val result = List("\"homeTab\"")
    checkEvaluation(ClSnapshotExtract(Literal(snapshot),
                                      Literal("Focus"),
                                      Literal("view"),
                                      Literal("")),
                    result)
    val result1 = List("1")
    checkEvaluation(ClSnapshotExtract(Literal(snapshot),
                                      Literal("Focus"),
                                      Literal("trackingInfo.surveyResponse"),
                                      Literal("")),
                    result1)
  }

  test("function with all the arguments present") {
    val result =
      List("{\"surveyIdentifier\":\"IO_80203147\",\"surveyResponse\":1}")
    checkEvaluation(ClSnapshotExtract(Literal(snapshot),
                                      Literal("Focus"),
                                      Literal("trackingInfo"),
                                      Literal("view==\"homeTab\"")),
                    result)
    val result1 = List("\"IO_80203147\"")
    checkEvaluation(
      ClSnapshotExtract(
        Literal(snapshot),
        Literal("Focus"),
        Literal("trackingInfo.surveyIdentifier"),
        Literal("view==\"homeTab\" && trackingInfo.surveyResponse==1")),
      result1
    )
    val result3 = List()
    checkEvaluation(
      ClSnapshotExtract(
        Literal(snapshot),
        Literal("Focus"),
        Literal("trackingInfo.surveyIdentifier"),
        Literal("view==\"browseTitles\" && trackingInfo.surveyResponse==1")),
      result3
    )
    val result4 = List("\"test1\"")
    checkEvaluation(
      ClSnapshotExtract(
        Literal(snapshot),
        Literal("Presentation"),
        Literal("trackingInfo.imageKey"),
        Literal(
          "view==\"browseTitles\" && ( trackingInfo.trackId==9087 || trackingInfo.videoId == 12345 )")
      ),
      result4
    )
  }

  test("function without `clType` parameter") {
    try {
      ClSnapshotExtract(
        Literal(snapshot),
        Literal(null),
        Literal("trackingInfo.imageKey"),
        Literal(
          "view==\"browseTitles\" && ( trackingInfo.trackId==9087 || trackingInfo.videoId == 12345 )")
      )
    } catch {
      case _: IllegalArgumentException =>
    }
  }

}
