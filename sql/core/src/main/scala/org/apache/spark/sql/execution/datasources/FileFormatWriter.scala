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

import java.util.{Date, UUID}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark._
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.{SparkHadoopWriterUtils, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SortExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.{SerializableConfiguration, Utils}


/** A helper object for writing FileFormat data out to a location. */
object FileFormatWriter extends Logging {

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
    outputPath: String, customPartitionLocations: Map[TablePartitionSpec, String])

  /** A shared job description for all the write tasks. */
  private class WriteJobDescription(
      val uuid: String,  // prevent collision between different (appending) write jobs
      val serializableHadoopConf: SerializableConfiguration,
      val outputWriterFactory: OutputWriterFactory,
      val allColumns: Seq[Attribute],
      val dataColumns: Seq[Attribute],
      val partitionColumns: Seq[Attribute],
      val bucketIdExpression: Option[Expression],
      val path: String,
      val customPartitionLocations: Map[TablePartitionSpec, String])
    extends Serializable {

    assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
      s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Non-partition columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
  }

  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, including output committer initialization and data source specific
   *    preparation work for the write job to be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   *    rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   *    exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   *    thrown during job commitment, also aborts the job.
   */
  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: FileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      refreshFunction: (Seq[TablePartitionSpec]) => Unit,
      options: Map[String, String]): Unit = {

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val allColumns = plan.output
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = allColumns.filterNot(partitionSet.contains)

    val bucketIdExpression = bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      // Use `HashPartitioning.partitionIdExpression` as our bucket id expression, so that we can
      // guarantee the data distribution is same between shuffle and bucketed data source, which
      // enables us to only shuffle one side when join a bucketed table and a normal one.
      HashPartitioning(bucketColumns, spec.numBuckets).partitionIdExpression
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, options, dataColumns.toStructType)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketIdExpression = bucketIdExpression,
      path = outputSpec.outputPath,
      customPartitionLocations = outputSpec.customPartitionLocations)

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering = partitionColumns ++ bucketIdExpression ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = plan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    try {
      val rdd = if (orderingMatched || sparkSession.sqlContext.conf.hiveForceSortedWrite) {
        plan.execute()
      } else {
        SortExec(
          requiredOrdering.map(SortOrder(_, Ascending)),
          global = false,
          child = plan).execute()
      }

      val ret = sparkSession.sparkContext.runJob(rdd,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            taskContext,
            committer,
            iterator = iter)
        })

      val commitMsgs = ret.map(_._1)
      val updatedPartitions = ret.flatMap(_._2).distinct.map(PartitioningUtils.parsePathFragment)

      committer.commitJob(job, commitMsgs)
      logInfo(s"Job ${job.getJobID} committed.")
      if (!committer.isInstanceOf[S3MapReduceCommitProtocol]) {
        // the S3 committer handles partition updates. running the refresh function clobbers them
        refreshFunction(updatedPartitions)
      }
    } catch { case cause: Throwable =>
      logError(s"Aborting job ${job.getJobID}.", cause)
      committer.abortJob(job)
      throw new SparkException("Job aborted.", cause)
    }
  }

  /** Writes data out in a single Spark task. */
  private def executeTask(
      description: WriteJobDescription,
      context: TaskContext,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]): (TaskCommitMessage, Set[String]) = {

    val jobId = SparkHadoopWriter.createJobID(new Date, context.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, context.partitionId())
    val taskAttemptId = new TaskAttemptID(taskId, context.attemptNumber())

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapred.job.id", jobId.toString)
      hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapred.task.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapred.task.is.map", true)
      hadoopConf.setInt("mapred.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val writeTask =
      if (description.partitionColumns.isEmpty && description.bucketIdExpression.isEmpty) {
        new SingleDirectoryWriteTask(description, taskAttemptContext, committer)
      } else {
        new DynamicPartitionWriteTask(description, taskAttemptContext, committer)
      }

    val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
      SparkHadoopWriterUtils.initHadoopOutputMetrics(context)

    def updateMetrics(recordsWritten: Long, force: Boolean = false): Unit = {
      SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
        outputMetricsAndBytesWrittenCallback, recordsWritten, force = force)
    }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        val outputPartitions = writeTask.execute(iterator, updateMetrics)
        writeTask.releaseResources()
        (committer.commitTask(taskAttemptContext), outputPartitions)
      })(catchBlock = {
        // If there is an error, release resource and then abort the task
        try {
          writeTask.releaseResources()
        } finally {
          committer.abortTask(taskAttemptContext)
          logError(s"Job $jobId aborted.")
        }
      })
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }

  /**
   * A simple trait for writing out data in a single Spark task, without any concerns about how
   * to commit or abort tasks. Exceptions thrown by the implementation of this trait will
   * automatically trigger task aborts.
   */
  private trait ExecuteWriteTask {
    /**
     * Writes data out to files, and then returns the list of partition strings written out.
     * The list of partitions is sent back to the driver and used to update the catalog.
     */
    def execute(
        iterator: Iterator[InternalRow],
        maybeUpdateMetrics: (Long, Boolean) => Unit): Set[String]
    def releaseResources(): Unit
  }

  /** Writes data to a single directory (used for non-dynamic-partition writes). */
  private class SingleDirectoryWriteTask(
      description: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    private[this] var outputWriter: OutputWriter = {
      val tmpFilePath = committer.newTaskTempFile(
        taskAttemptContext,
        None,
        description.outputWriterFactory.getFileExtension(taskAttemptContext))

      val outputWriter = description.outputWriterFactory.newInstance(
        path = tmpFilePath,
        dataSchema = description.dataColumns.toStructType,
        context = taskAttemptContext)
      outputWriter.initConverter(dataSchema = description.dataColumns.toStructType)
      outputWriter
    }

    override def execute(
        iter: Iterator[InternalRow],
        maybeUpdateMetrics: (Long, Boolean) => Unit): Set[String] = {
      var recordsWritten: Long = 0L
      while (iter.hasNext) {
        val internalRow = iter.next()
        outputWriter.writeInternal(internalRow)
        recordsWritten += 1
        maybeUpdateMetrics(recordsWritten, false)
      }
      releaseResources() // release resources so that the bytes written for metrics are correct
      maybeUpdateMetrics(recordsWritten, true)
      Set.empty
    }

    override def releaseResources(): Unit = {
      if (outputWriter != null) {
        outputWriter.close()
        outputWriter = null
      }
    }
  }

  /**
   * Writes data to using dynamic partition writes, meaning this single function can write to
   * multiple directories (partitions) or files (bucketing).
   */
  private class DynamicPartitionWriteTask(
      desc: WriteJobDescription,
      taskAttemptContext: TaskAttemptContext,
      committer: FileCommitProtocol) extends ExecuteWriteTask {

    // currentWriter is initialized whenever we see a new key
    private var currentWriter: OutputWriter = _

    /** Expressions that given partition columns build a path string like: col1=val/col2=val/... */
    private def partitionPathExpression: Seq[Expression] = {
      desc.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
        // TODO: use correct timezone for partition values.
        val partitionName = ScalaUDF(
          ExternalCatalogUtils.getPartitionPathString _,
          StringType,
          Seq(Literal(c.name), Cast(c, StringType)))
        if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
      }
    }

    /**
     * Opens a new OutputWriter given a partition key and optional bucket id.
     * If bucket id is specified, we will append it to the end of the file name, but before the
     * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
     *
     * @param partColsAndBucketId a row consisting of partition columns and a bucket id for the
     *                            current row.
     * @param getPartitionPath a function that projects the partition values into a path string.
     * @param fileCounter the number of files that have been written in the past for this specific
     *                    partition. This is used to limit the max number of records written for a
     *                    single file. The value should start from 0.
     * @param updatedPartitions the set of updated partition paths, we should add the new partition
     *                          path of this writer to it.
     */
    private def newOutputWriter(
        partColsAndBucketId: InternalRow,
        getPartitionPath: UnsafeProjection,
        fileCounter: Int,
        updatedPartitions: mutable.Set[String]): Unit = {
      val partDir = if (desc.partitionColumns.isEmpty) {
        None
      } else {
        Option(getPartitionPath(partColsAndBucketId).getString(0))
      }
      partDir.foreach(updatedPartitions.add)

      // If the bucketId expression is defined, the bucketId column is right after the partition
      // columns.
      val bucketId = if (desc.bucketIdExpression.isDefined) {
        BucketingUtils.bucketIdToString(partColsAndBucketId.getInt(desc.partitionColumns.length))
      } else {
        ""
      }

      // This must be in a form that matches our bucketing format. See BucketingUtils.
      val ext = f"$bucketId.c$fileCounter%03d" +
        desc.outputWriterFactory.getFileExtension(taskAttemptContext)

      val customPath = partDir match {
        case Some(dir) =>
          desc.customPartitionLocations.get(PartitioningUtils.parsePathFragment(dir))
        case _ =>
          None
      }
      val path = if (customPath.isDefined) {
        committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
      } else {
        committer.newTaskTempFile(taskAttemptContext, partDir, ext)
      }

      currentWriter = desc.outputWriterFactory.newInstance(
        path = path,
        dataSchema = desc.dataColumns.toStructType,
        context = taskAttemptContext)
      currentWriter.initConverter(desc.dataColumns.toStructType)
    }

    override def execute(
        iter: Iterator[InternalRow],
        maybeUpdateMetrics: (Long, Boolean) => Unit): Set[String] = {
      val getPartitionColsAndBucketId = UnsafeProjection.create(
        desc.partitionColumns ++ desc.bucketIdExpression, desc.allColumns)

      // Generates the partition path given the row generated by `getPartitionColsAndBucketId`.
      val getPartPath = UnsafeProjection.create(
        Seq(Concat(partitionPathExpression)), desc.partitionColumns)

      // Returns the data columns to be written given an input row
      val getOutputRow = UnsafeProjection.create(desc.dataColumns, desc.allColumns)

      // If anything below fails, we should abort the task.
      var recordsInFile: Long = 0L
      var recordsWritten: Long = 0L
      var fileCounter = 0
      var currentPartColsAndBucketId: UnsafeRow = null
      val updatedPartitions = mutable.Set[String]()
      for (row <- iter) {
        val nextPartColsAndBucketId = getPartitionColsAndBucketId(row)
        if (currentPartColsAndBucketId != nextPartColsAndBucketId) {
          // See a new partition or bucket - write to a new partition dir (or a new bucket file).
          currentPartColsAndBucketId = nextPartColsAndBucketId.copy()
          logDebug(s"Writing partition: $currentPartColsAndBucketId")

          recordsInFile = 0
          fileCounter = 0

          releaseResources()
          newOutputWriter(currentPartColsAndBucketId, getPartPath, fileCounter, updatedPartitions)
        }

        currentWriter.writeInternal(getOutputRow(row))
        recordsInFile += 1
        recordsWritten += 1
        maybeUpdateMetrics(recordsWritten, false)
      }
      releaseResources()
      maybeUpdateMetrics(recordsWritten, true)
      updatedPartitions.toSet
    }

    override def releaseResources(): Unit = {
      if (currentWriter != null) {
        currentWriter.close()
        currentWriter = null
      }
    }
  }
}
