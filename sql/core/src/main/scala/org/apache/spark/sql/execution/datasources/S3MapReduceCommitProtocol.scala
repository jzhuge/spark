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

package org.apache.spark.sql.execution.datasources

import java.util.Date

import com.netflix.bdp.Committers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.JobContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol, SparkHadoopWriterUtils}

class S3MapReduceCommitProtocol(jobId: String, path: String, options: Map[String, String])
  extends HadoopMapReduceCommitProtocol(jobId, path) with Serializable with Logging {
  private val isAppend = options.get("spark.sql.commit-protocol.append").exists(_.toBoolean)
  private val isPartitioned = options.get("spark.sql.s3committer.is-partitioned")
      .exists(_.toBoolean)
  private val useBatch = options.get("spark.sql.s3committer.use-batch").exists(_.toBoolean)
  private val batchId: Long = System.currentTimeMillis

  private def addCommitterOptions(conf: Configuration): Unit = {
    conf.set("s3.multipart.committer.uuid", jobId)
    conf.set("s3.multipart.committer.conflict-mode", if (isAppend) "append" else "replace")
  }

  private def addBatchCommitterOptions(conf: Configuration): Unit = {
    Seq(
      "s3.multipart.committer.catalog",
      "s3.multipart.committer.database",
      "s3.multipart.committer.table"
    ).foreach(prop => conf.set(prop, options(prop)))
    conf.set("s3.multipart.committer.batch-id", batchId.toString)
  }

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val conf = context.getConfiguration

    addCommitterOptions(conf)
    val committer = if (useBatch) {
      addBatchCommitterOptions(conf)
      Committers.newBatchCommitter(new Path(path), context, isPartitioned)
    } else {
      Committers.newS3Committer(new Path(path), context, isPartitioned)
    }

    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")

    committer
  }

  override protected def setupCommitter(context: JobContext): OutputCommitter = {
    val conf = context.getConfiguration

    // Setup IDs
    val jobId = SparkHadoopWriterUtils.createJobID(new Date(batchId), 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    context match {
      case impl: JobContextImpl =>
        impl.setJobID(jobId)
      case _ =>
    }
    conf.set("mapred.job.id", jobId.toString)
    conf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    conf.set("mapred.task.id", taskAttemptId.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", 0)

    addCommitterOptions(conf)

    val committer = if (useBatch) {
      addBatchCommitterOptions(conf)
      Committers.newBatchCommitter(new Path(path), context, isPartitioned)
    } else {
      Committers.newS3Committer(new Path(path), context, isPartitioned)
    }

    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")

    committer
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    // absolute paths are used when the location of a partition doesn't match the default location.
    // in those cases, the default commit protocol will create a temp folder with the files and
    // rename them to the absolute location at the end. this is incompatible with the S3 committer
    // because there currently no way to instruct the committer where files should be uploaded to.
    throw new UnsupportedOperationException("S3 committer cannot be used with absolute paths.")
  }

  override def commitJob(
      jobContext: JobContext,
      taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    // commit the job without moving absolute files
    committer.commitJob(jobContext)
  }
}
