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

package org.apache.spark.sql.hive

import java.text.NumberFormat
import java.util.{Date, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.netflix.dse.mds.PartitionMetrics
import com.netflix.dse.mds.data.{DataField, DataTuple}
import org.apache.commons.configuration.{BaseConfiguration => CommonsConf}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator, Utilities}
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.SparkHadoopWriterUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.SerializableJobConf
import org.apache.spark.util.Utils.tryWithSafeFinallyAndFailureCallbacks
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

private[hive] trait HiveWriterContainer {
  def newSerializer(table: TableDesc): Serializer

  def driverSideSetup(): Unit

  def commitJob(taskCommits: Seq[TaskCommitMessage]): Unit

  def abortJob(): Unit

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int): Unit

  def writeToFile(
      context: TaskContext,
      iterator: Iterator[InternalRow]
  ): (TaskCommitMessage, Set[String])

  def close(): Unit

  private[hive] def deSerializeValue(value: String, dataType: DataType): Object = {
    if (value == null) return null
    dataType match {
      case _: NullType =>
        null
      case _: BooleanType =>
        java.lang.Boolean.valueOf(value)
      case _: ByteType =>
        java.lang.Byte.valueOf(value)
      case _: ShortType =>
        java.lang.Short.valueOf(value)
      case _: IntegerType =>
        java.lang.Integer.valueOf(value)
      case _: LongType | TimestampType =>
        java.lang.Long.valueOf(value)
      case _: FloatType =>
        java.lang.Float.valueOf(value)
      case _: DoubleType =>
        java.lang.Double.valueOf(value)
      case _: DecimalType =>
        java.lang.Double.valueOf(value)
      case _: DateType =>
        DateTimeUtils.stringToTime(value)
      case _: BinaryType =>
        value.getBytes
      case _: StringType =>
        UTF8String.fromString(value)
      case _: CalendarIntervalType =>
        CalendarInterval.fromString(value)
      case _ =>
        throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString)
    }
  }
}

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on [[SparkHadoopWriter]].
 */
private[hive] class SparkHiveWriterContainer(
    @transient private val jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    inputSchema: Seq[Attribute],
    committer: FileCommitProtocol)
  extends Logging
  with HiveInspectors
  with Serializable
  with HiveWriterContainer {

  import HivePartitionMetricHelper._

  private val now = new Date()
  private val tableDesc: TableDesc = fileSinkConf.getTableInfo
  // Add table properties from storage handler to jobConf, so any custom storage
  // handler settings can be set to jobConf
  if (tableDesc != null) {
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
  }
  protected val conf = new SerializableJobConf(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  val tableProperties = fileSinkConf.getTableInfo.getProperties
  val storeDefaults = conf.value.getBoolean("dse.store.default", false)
  val fieldDefaultValues: Array[Object] = inputSchema.map { attr =>
    deSerializeValue(tableProperties.getProperty(s"dse.field.default.${attr.name}"), attr.dataType)
  }.toArray

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient private var path: Path = null
  @transient protected lazy val jobContext = new JobContextImpl(conf.value, jID.value)
  @transient protected lazy val taskContext = new TaskAttemptContextImpl(conf.value, taID.value)
  @transient private lazy val outputFormat =
    conf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

  @transient protected lazy val metricClasses = PartitionMetrics
      .metricClasses(conf.value.get("dse.storage.partition.metrics"))
  @transient protected lazy val fieldMetricClasses = PartitionMetrics
      .fieldMetricClasses(conf.value.get("dse.storage.field.metrics"))
  @transient protected lazy val metricHelper = new HivePartitionMetricHelper()

  protected def newMetrics(schema: Seq[Attribute]): PartitionMetrics = {
    if (conf.value.getBoolean("spark.sql.partition.metrics.enabled", true)) {
      val pm = new PartitionMetrics(metricClasses, fieldMetricClasses, metricHelper)
      pm.setSchema(schema.map(c => new DataField(c.name, c.dataType.simpleString)).asJava)
      pm.initialize(SparkHiveWriterContainer.toCommonsConf(conf.value))
      pm
    } else {
      new DummyMetrics()
    }
  }

  def driverSideSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    committer.setupJob(jobContext)
  }

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int) {
    setIDs(jobId, splitId, attemptId)
    setConfParams()
    committer.setupTask(taskContext)
    initWriters()
  }

  protected lazy val extension: String = Utilities.getFileExtension(
    conf.value, fileSinkConf.getCompressed, outputFormat)

  protected def getOutputName: String = {
    val numberFormat = NumberFormat.getInstance(Locale.US)
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf.value, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def close() {
    // Seems the boolean value passed into close does not matter.
    if (writer != null) {
      writer.close(false)
    }
  }

  def commitJob(taskCommits: Seq[TaskCommitMessage]) {
    committer.commitJob(jobContext, taskCommits)
  }

  def abortJob(): Unit = {
    committer.abortJob(jobContext)
  }

  protected def initWriters() {
    // NOTE this method is executed at the executor side.
    // For Hive tables without partitions or with only static partitions, only 1 writer is needed.
    path = new Path(committer.newTaskTempFile(taskContext, None, extension))
    writer = HiveFileFormatUtils.getHiveRecordWriter(
      conf.value,
      fileSinkConf.getTableInfo,
      conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
      fileSinkConf,
      path,
      Reporter.NULL)
  }

  protected def commit(): TaskCommitMessage = {
    // TODO: Make sure SparkHadoopMapRedUtil.commitTask is called somewhere?
    // SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID)
    committer.commitTask(taskContext)
  }

  def abortTask(): Unit = {
    committer.abortTask(taskContext)
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobId))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, TaskType.MAP, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
  }

  def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  protected def prepareForWrite() = {
    val serializer = newSerializer(fileSinkConf.getTableInfo)
    val standardOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val fieldOIs = standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
    val dataTypes = inputSchema.map(_.dataType)
    val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
    val outputData = new Array[Any](fieldOIs.length)

    val serializeFunc = (data: InternalRow) => {
      var i = 0
      while (i < fieldOIs.length) {
        outputData(i) = if (data.isNullAt(i)) {
          if (storeDefaults) fieldDefaultValues(i) else null
        } else {
          wrappers(i)(data.get(i, dataTypes(i)))
        }
        i += 1
      }
      serializer.serialize(outputData, standardOI)
    }

    (serializeFunc, fieldOIs)
  }

  // this function is executed on executor side
  def writeToFile(
      context: TaskContext,
      iterator: Iterator[InternalRow]
  ): (TaskCommitMessage, Set[String]) = {
    executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

    if (!iterator.hasNext) {
      return (commit(), Set.empty)
    }

    val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
      SparkHadoopWriterUtils.initHadoopOutputMetrics(context)
    var recordsWritten = 0

    val (serialize, _) = prepareForWrite()
    val pm = newMetrics(inputSchema)

    tryWithSafeFinallyAndFailureCallbacks(block = {
      iterator.foreach { row =>
        pm.update(HiveDataTuple.wrap(row, inputSchema))
        writer.write(serialize(row))

        // Update bytes written metric every few records
        SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
          outputMetricsAndBytesWrittenCallback, recordsWritten)
        recordsWritten += 1
      }

      // write metrics before calling close because close will commit
      writeMetrics(pm, metricsPath(path), conf.value)
      close()

      (commit(), Set.empty[String])

    })(catchBlock = {
      abortTask()
      logError(s"Job ${taskContext.getTaskAttemptID} aborted.")
    }, finallyBlock = {
      // Update bytes written metric every few records
      SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
        outputMetricsAndBytesWrittenCallback, recordsWritten, force = true)
    })
  }
}

private[hive] object SparkHiveWriterContainer {
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  def toCommonsConf(conf: JobConf): CommonsConf = {
    val commonsConf = new CommonsConf()
    conf.iterator.asScala.foreach(entry => commonsConf.addProperty(entry.getKey, entry.getValue))
    commonsConf
  }
}

private[spark] object SparkHiveDynamicPartitionWriterContainer {
  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
}

private[spark] class SparkHiveDynamicPartitionWriterContainer(
    jobConf: JobConf,
    fileSinkConf: FileSinkDesc,
    staticPartitionPart: Option[String],
    dynamicPartColNames: Array[String],
    isSorted: Boolean,
    inputSchema: Seq[Attribute],
    committer: FileCommitProtocol)
  extends SparkHiveWriterContainer(jobConf, fileSinkConf, inputSchema, committer) {

  import SparkHiveDynamicPartitionWriterContainer._
  import HivePartitionMetricHelper._

  private val defaultPartName = jobConf.get(
    ConfVars.DEFAULTPARTITIONNAME.varname, ConfVars.DEFAULTPARTITIONNAME.defaultStrVal)

  override protected def initWriters(): Unit = {
    // do nothing
  }

  override def close(): Unit = {
    // do nothing
  }

  override def commitJob(taskCommits: Seq[TaskCommitMessage]): Unit = {
    // This is a hack to avoid writing _SUCCESS mark file. In lower versions of Hadoop (e.g. 1.0.4),
    // semantics of FileSystem.globStatus() is different from higher versions (e.g. 2.4.1) and will
    // include _SUCCESS file when glob'ing for dynamic partition data files.
    //
    // Better solution is to add a step similar to what Hive FileSinkOperator.jobCloseOp does:
    // calling something like Utilities.mvFileToFinalPath to cleanup the output directory and then
    // load it with loadDynamicPartitions/loadPartition/loadTable.
    val oldMarker = conf.value.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    super.commitJob(taskCommits)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  // this function is executed on executor side
  override def writeToFile(
      context: TaskContext,
      iterator: Iterator[InternalRow]
  ): (TaskCommitMessage, Set[String]) = {
    val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
      SparkHadoopWriterUtils.initHadoopOutputMetrics(context)
    var recordsWritten = 0

    val (serialize, fieldOIs) = prepareForWrite()
    executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

    val partitionOutput = inputSchema.takeRight(dynamicPartColNames.length)
    val dataOutput = inputSchema.take(fieldOIs.length)
    // Returns the partition key given an input row
    val getPartitionKey = UnsafeProjection.create(partitionOutput, inputSchema)
    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataOutput, inputSchema)

    val fun: AnyRef = (pathString: String) => FileUtils.escapePathName(pathString, defaultPartName)
    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionOutput.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(fun, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartName), escaped)
      val partitionName = Literal(dynamicPartColNames(i) + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR_CHAR.toString) :: partitionName
    }

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionOutput)

    def sortAndWrite(iterator: Iterator[InternalRow]): Set[String] = {
      val sorter: UnsafeKVExternalSorter = new UnsafeKVExternalSorter(
        StructType.fromAttributes(partitionOutput),
        StructType.fromAttributes(dataOutput),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        TaskContext.get().taskMemoryManager().pageSizeBytes,
        SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
          UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD))

      while (iterator.hasNext) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        sorter.insertKV(currentKey, getOutputRow(inputRow))
      }

      logInfo(s"Sorting complete. Writing out partition files one at a time.")
      val sortedIterator = sorter.sortedIterator()
      var currentKey: InternalRow = null
      var currentWriter: FileSinkOperator.RecordWriter = null
      var currentMetrics: PartitionMetrics = null
      var currentMetricsPath: Path = null
      try {
        val updatedPartitions = mutable.Set[String]()
        while (sortedIterator.next()) {
          if (currentKey != sortedIterator.getKey) {
            if (currentWriter != null) {
              currentWriter.close(false)
              writeMetrics(currentMetrics, currentMetricsPath, conf.value)
            }
            currentKey = sortedIterator.getKey.copy()
            logDebug(s"Writing partition: $currentKey")
            val (partition, path) = partitionAndPath(currentKey)
            updatedPartitions.add(partition)
            currentWriter = newOutputWriter(partition, path)
            currentMetricsPath = metricsPath(path)
            currentMetrics = newMetrics(dataOutput)
          }

          val outputRow = sortedIterator.getValue
          currentWriter.write(serialize(outputRow))
          currentMetrics.update(HiveDataTuple.wrap(outputRow, dataOutput))

          // Update bytes written metric every few records
          SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
            outputMetricsAndBytesWrittenCallback, recordsWritten)
          recordsWritten += 1
        }
        updatedPartitions.toSet
      } finally {
        if (currentWriter != null) {
          currentWriter.close(false)
          writeMetrics(currentMetrics, currentMetricsPath, conf.value)
        }
        SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
          outputMetricsAndBytesWrittenCallback, recordsWritten, force = true)
      }
    }

    def sortedWrite(iterator: Iterator[InternalRow]): Set[String] = {
      var currentKey: InternalRow = null
      var currentWriter: FileSinkOperator.RecordWriter = null
      var currentMetrics: PartitionMetrics = null
      var currentMetricsPath: Path = null
      try {
        val updatedPartitions = mutable.Set[String]()
        while (iterator.hasNext) {
          val inputRow = iterator.next()
          val rowKey = getPartitionKey(inputRow)
          if (currentKey != rowKey) {
            if (currentWriter != null) {
              currentWriter.close(false)
              writeMetrics(currentMetrics, currentMetricsPath, conf.value)
            }
            currentKey = rowKey.copy()
            logDebug(s"Writing partition: $currentKey")
            val (partition, path) = partitionAndPath(currentKey)
            updatedPartitions.add(partition)
            currentWriter = newOutputWriter(partition, path)
            currentMetricsPath = metricsPath(path)
            currentMetrics = newMetrics(dataOutput)
          }

          val outputRow = getOutputRow(inputRow)
          currentWriter.write(serialize(outputRow))
          currentMetrics.update(HiveDataTuple.wrap(outputRow, dataOutput))

          // Update bytes written metric every few records
          SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
            outputMetricsAndBytesWrittenCallback, recordsWritten)
          recordsWritten += 1
        }
        updatedPartitions.toSet
      } finally {
        if (currentWriter != null) {
          currentWriter.close(false)
          writeMetrics(currentMetrics, currentMetricsPath, conf.value)
        }
        SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
          outputMetricsAndBytesWrittenCallback, recordsWritten, force = true)
      }
    }

    def partitionAndPath(key: InternalRow): (String, Path) = {
      val partitionPath = getPartitionString(key).getString(0)
      val partPath = staticPartitionPart match {
        case Some(staticPart) => staticPart + Path.SEPARATOR_CHAR + partitionPath
        case None => partitionPath
      }
      val path = new Path(committer.newTaskTempFile(
        taskContext, if (partPath.nonEmpty) Some(partPath) else None, extension))

      (partitionPath, path)
    }

    /** Open and returns a new OutputWriter given a partition key. */
    def newOutputWriter(
        partitionPath: String,
        filePath: Path): FileSinkOperator.RecordWriter = {
      val newFileSinkDesc = new FileSinkDesc(
        fileSinkConf.getDirName + partitionPath,
        fileSinkConf.getTableInfo,
        fileSinkConf.getCompressed)
      newFileSinkDesc.setCompressCodec(fileSinkConf.getCompressCodec)
      newFileSinkDesc.setCompressType(fileSinkConf.getCompressType)

      HiveFileFormatUtils.getHiveRecordWriter(
        conf.value,
        fileSinkConf.getTableInfo,
        conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        filePath,
        Reporter.NULL)
    }

    // If anything below fails, we should abort the task.
    tryWithSafeFinallyAndFailureCallbacks(block = {
      val partitions = if (isSorted) {
        sortedWrite(iterator)
      } else {
        sortAndWrite(iterator)
      }
      (commit(), partitions)
    })(catchBlock = {
      abortTask()
      logError(s"Job ${taskContext.getTaskAttemptID} aborted.")
    }, finallyBlock = {
      // Update bytes written metric every few records
      SparkHadoopWriterUtils.maybeUpdateOutputMetrics(
        outputMetricsAndBytesWrittenCallback, recordsWritten, force = true)
    })
  }
}

private class DummyMetrics extends PartitionMetrics {
  override def update(t: DataTuple): Unit = {}
}
