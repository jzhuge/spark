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

package org.apache.spark.sql

import org.apache.spark.TaskContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics

// from 9c419698fe110a805570031cac3387a51957d9d1
private[sql]
object SparkHadoopWriterUtils {

  private val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 1024

  // return type: (output metrics, bytes written callback), defined only if the latter is defined
  def initHadoopOutputMetrics(
      context: TaskContext): Option[(OutputMetrics, () => Long)] = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    bytesWrittenCallback.map { b =>
      (context.taskMetrics().outputMetrics, b)
    }
  }

  def maybeUpdateOutputMetrics(
      outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)],
      recordsWritten: Long, force: Boolean = false): Unit = {
    if (force || recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      outputMetricsAndBytesWrittenCallback.foreach {
        case (om, callback) =>
          om.setBytesWritten(callback())
          om.setRecordsWritten(recordsWritten)
      }
    }
  }
}
