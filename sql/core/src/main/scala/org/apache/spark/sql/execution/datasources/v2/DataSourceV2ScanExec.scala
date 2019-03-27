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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.sources.v2.reader._

/**
 * Physical plan node for scanning data from a data source.
 */
case class DataSourceV2ScanExec(
    output: Seq[AttributeReference],
    @transient sourceName: String,
    @transient options: Map[String, String],
    @transient pushedFilters: Seq[Expression],
    @transient reader: DataSourceReader)
  extends LeafExecNode with DataSourceReaderHolder {

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataSourceV2ScanExec]

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: DataSourceV2ScanExec =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, sourceName, options).hashCode()
  }

  private lazy val partitions: Seq[InputPartition[InternalRow]] = {
    reader.planInputPartitions().asScala
  }

  private lazy val inputRDD: RDD[InternalRow] = reader match {
    case _ =>
      new DataSourceRDD(sparkContext, partitions).asInstanceOf[RDD[InternalRow]]
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    val metricsHandler = new MetricsHandler(longMetric("numOutputRows"))

    inputRDD.mapPartitions { iter =>
      try {
        iter.map { r =>
          metricsHandler.updateMetrics()
          r
        }

      } finally {
        metricsHandler.updateMetrics(force = true)
      }
    }
  }

  class MetricsHandler(numOutputRows: SQLMetric) extends Logging {
    private lazy val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
    private lazy val startingBytesRead = inputMetrics.bytesRead
    private lazy val maybeGetBytesRead: Option[() => Long] =
      SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

    def updateMetrics(force: Boolean = false): Unit = {
      maybeGetBytesRead match {
        case Some(getBytesRead) =>
          val shouldUpdateBytesRead =
            inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0
          if (shouldUpdateBytesRead || force) {
            inputMetrics.setBytesRead(startingBytesRead + getBytesRead())
          }
        case _ =>
      }
      inputMetrics.incRecordsRead(1)
      numOutputRows += 1
    }
  }
}
