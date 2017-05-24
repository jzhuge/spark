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

package org.apache.spark

import java.io.IOException
import java.text.NumberFormat
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType

import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.util.SerializableJobConf

/**
 * Internal helper class that saves an RDD using a Hadoop OutputFormat.
 *
 * Saves the RDD using a JobConf, which should contain an output key class, an output value class,
 * a filename to write to, etc, exactly like in a Hadoop MapReduce job.
 */
private[spark]
class SparkHadoopWriter(jobConf: JobConf) extends Logging with Serializable {

  private val now = new Date()
  private val conf = new SerializableJobConf(jobConf)

  private var jobID = 0
  private var stageID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: RecordWriter[AnyRef, AnyRef] = null
  @transient private var format: OutputFormat[AnyRef, AnyRef] = null
  @transient private var committer: OutputCommitter = null
  @transient private var jobContext: JobContext = null
  @transient private var taskContext: TaskAttemptContext = null

  def preSetup(jobId: Int) {
    setIDs(jobId, 0, 0, 0)
    HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(now),
      jobId, 0, 0, conf.value)

    val jCtxt = getJobContext()
    getOutputCommitter().setupJob(jCtxt)
  }


  def setup(jobid: Int, stageid: Int, splitid: Int, attemptid: Int) {
    setIDs(jobid, stageid, splitid, attemptid)
    HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(now),
      jobid, splitID, attemptID, conf.value)
  }

  def open() {
    val numfmt = NumberFormat.getInstance(Locale.US)
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    val outputName = "part-"  + numfmt.format(splitID)
    val path = FileOutputFormat.getOutputPath(conf.value)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(conf.value)
      } else {
        FileSystem.get(conf.value)
      }
    }

    getOutputCommitter().setupTask(getTaskContext())
    writer = getOutputFormat().getRecordWriter(fs, conf.value, outputName, Reporter.NULL)
  }

  def write(key: AnyRef, value: AnyRef) {
    if (writer != null) {
      writer.write(key, value)
    } else {
      throw new IOException("Writer is null, open() has not been called")
    }
  }

  def close() {
    writer.close(Reporter.NULL)
  }

  def commit() {
    SparkHadoopMapRedUtil.commitTask(getOutputCommitter(), getTaskContext(), stageID, splitID)
  }

  def commitJob() {
    val cmtr = getOutputCommitter()
    cmtr.commitJob(getJobContext())
  }

  // ********* Private Functions *********

  private def getOutputFormat(): OutputFormat[AnyRef, AnyRef] = {
    if (format == null) {
      format = conf.value.getOutputFormat()
        .asInstanceOf[OutputFormat[AnyRef, AnyRef]]
    }
    format
  }

  private def getOutputCommitter(): OutputCommitter = {
    if (committer == null) {
      committer = conf.value.getOutputCommitter
    }
    committer
  }

  private def getJobContext(): JobContext = {
    if (jobContext == null) {
      jobContext = new JobContextImpl(conf.value, jID.value)
    }
    jobContext
  }

  private def getTaskContext(): TaskAttemptContext = {
    if (taskContext == null) {
      taskContext = newTaskAttemptContext(conf.value, taID.value)
    }
    taskContext
  }

  protected def newTaskAttemptContext(
      conf: JobConf,
      attemptId: TaskAttemptID): TaskAttemptContext = {
    new TaskAttemptContextImpl(conf, attemptId)
  }

  private def setIDs(jobid: Int, stageid: Int, splitid: Int, attemptid: Int) {
    jobID = jobid
    stageID = stageid
    splitID = splitid
    attemptID = attemptid

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobid))
    taID = new SerializableWritable[TaskAttemptID](
        new TaskAttemptID(new TaskID(jID.value, TaskType.MAP, splitID), attemptID))
  }
}

private[spark]
object SparkHadoopWriter {
  def createJobID(time: Date, id: Int): JobID = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    val jobtrackerID = formatter.format(time)
    new JobID(jobtrackerID, id)
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }
}
