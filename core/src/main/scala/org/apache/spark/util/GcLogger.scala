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

package org.apache.spark.util

import com.netflix.bdp

import org.apache.spark.SparkConf

object GcLogger {
  def start(conf: SparkConf): Unit = {
    if (conf.getBoolean("spark.report.gc.metrics", true)) {
      bdp.TaskMetrics.addTag("app", conf.getAppId)
      conf.getOption("spark.genie.id").foreach(bdp.TaskMetrics.addTag("job", _))
      conf.getOption("spark.genie.name").foreach(bdp.TaskMetrics.addTag("jobName", _))
      bdp.metrics.GcLogger.start(bdp.TaskMetrics.groupFactory())
    }
  }
}
