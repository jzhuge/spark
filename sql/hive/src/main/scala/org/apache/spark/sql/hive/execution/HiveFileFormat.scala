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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import com.netflix.dse.mds.PartitionMetrics
import com.netflix.dse.mds.data.{DataField, DataTuple}
import org.apache.commons.configuration.{BaseConfiguration => CommonsConf}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.SerializableJobConf

/**
 * `FileFormat` for writing Hive tables.
 *
 * TODO: implement the read logic.
 */
class HiveFileFormat(fileSinkConf: FileSinkDesc)
  extends FileFormat with DataSourceRegister with Logging {

  def this() = this(null)

  override def shortName(): String = "hive"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw new UnsupportedOperationException(s"inferSchema is not supported for hive data source.")
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val tableDesc = fileSinkConf.getTableInfo
    conf.set("mapred.output.format.class", tableDesc.getOutputFileFormatClassName)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sparkSession.sparkContext.conf.getBoolean("spark.speculation", false)
    val outputCommitterClass = conf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    // Add table properties from storage handler to hadoopConf, so any custom storage
    // handler settings can be set to hadoopConf
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, conf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, conf)

    // Avoid referencing the outer object.
    val fileSinkConfSer = fileSinkConf
    new OutputWriterFactory {
      private val jobConf = new SerializableJobConf(new JobConf(conf))
      @transient private lazy val outputFormat =
        jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

      override def getFileExtension(context: TaskAttemptContext): String = {
        Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new HiveOutputWriter(path, fileSinkConfSer, jobConf.value, dataSchema)
      }
    }
  }
}

class HiveOutputWriter(
    path: String,
    fileSinkConf: FileSinkDesc,
    jobConf: JobConf,
    dataSchema: StructType) extends OutputWriter with HiveInspectors {

  private def tableDesc = fileSinkConf.getTableInfo

  private val serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(jobConf, tableDesc.getProperties)
    serializer
  }

  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
    jobConf,
    tableDesc,
    serializer.getSerializedClass,
    fileSinkConf,
    new Path(path),
    Reporter.NULL)

  private val standardOI = ObjectInspectorUtils
    .getStandardObjectInspector(
      tableDesc.getDeserializer(jobConf).getObjectInspector,
      ObjectInspectorCopyOption.JAVA)
    .asInstanceOf[StructObjectInspector]

  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = dataSchema.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  private val outputData = new Array[Any](fieldOIs.length)

  private def deSerializeValue(value: String, dataType: DataType): Object = {
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

  val tableProperties = fileSinkConf.getTableInfo.getProperties
  val storeDefaults = jobConf.getBoolean("dse.store.default", false)
  val fieldDefaultValues: Array[Object] = dataSchema.map { field =>
    deSerializeValue(tableProperties.getProperty(s"dse.field.default.${field.name}"),
      field.dataType)
  }.toArray

  import HivePartitionMetricHelper._

  @transient protected lazy val metricClasses = PartitionMetrics
    .metricClasses(jobConf.get("dse.storage.partition.metrics"))
  @transient protected lazy val fieldMetricClasses = PartitionMetrics
    .fieldMetricClasses(jobConf.get("dse.storage.field.metrics"))
  @transient protected lazy val metricHelper = new HivePartitionMetricHelper()

  private def toCommonsConf(conf: JobConf): CommonsConf = {
    val commonsConf = new CommonsConf()
    conf.iterator.asScala.foreach(entry => commonsConf.addProperty(entry.getKey, entry.getValue))
    commonsConf
  }

  private def newMetrics(schema: StructType): PartitionMetrics = {
    if (jobConf.getBoolean("spark.sql.partition.metrics.enabled", true)) {
      val pm = new PartitionMetrics(metricClasses, fieldMetricClasses, metricHelper)
      pm.setSchema(schema.map(c => new DataField(c.name, c.dataType.simpleString)).asJava)
      pm.initialize(toCommonsConf(jobConf))
      pm
    } else {
      new DummyMetrics()
    }
  }

  val pm = newMetrics(dataSchema)

  override def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) {
        if (storeDefaults) fieldDefaultValues(i) else null
      } else {
        wrappers(i)(row.get(i, dataTypes(i)))
      }
      i += 1
    }
    hiveWriter.write(serializer.serialize(outputData, standardOI))

    pm.update(HiveDataTuple.wrap(row, dataSchema.toAttributes))
  }

  override def close(): Unit = {
    // write metrics before calling close because close will commit
    writeMetrics(pm, metricsPath(new Path(path)), jobConf)

    // Seems the boolean value passed into close does not matter.
    hiveWriter.close(false)
  }
}

private class DummyMetrics extends PartitionMetrics {
  override def update(t: DataTuple): Unit = {}
}
