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

import java.io.IOException
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.dse.mds.PartitionMetrics
import com.netflix.dse.mds.data.{DataField, DataTuple}
import com.netflix.dse.mds.metric.PartitionMetricHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.DataTypes

private[hive] class HivePartitionMetricHelper extends PartitionMetricHelper {
  override def newTuple(value: scala.AnyRef): DataTuple = SingletonTuple.wrap(value)
  override def isStringType(field: DataField): Boolean =
    DataTypes.StringType.simpleString.equals(field.getType)
  override def isNumeric(field: DataField): Boolean =
    ( DataTypes.IntegerType.simpleString.equals(field.getType)
        || DataTypes.LongType.simpleString.equals(field.getType)
        || DataTypes.DoubleType.simpleString.equals(field.getType)
        || DataTypes.FloatType.simpleString.equals(field.getType)
        || DataTypes.ShortType.simpleString.equals(field.getType))
}

private[hive] object HivePartitionMetricHelper {
  def metricsPath(path: Path): Path = {
    // metrics for each file are written in a predictable location: .<filename>.metrics
    // this allows the output committer to find the metrics and update metacat
    new Path(path.getParent, "." + path.getName + ".metrics")
  }

  def writeMetrics(pm: PartitionMetrics, path: Path, conf: Configuration): Unit = {
    if (pm != null) {
      val mapper: ObjectMapper = new ObjectMapper()
      val fs = path.getFileSystem(conf)
      val os = fs.create(path)
      var threw = true
      try {
        os.write(mapper.writeValueAsString(pm).getBytes(StandardCharsets.UTF_8))
        threw = false
      } catch {
        case e: IOException =>
        // don't write metrics
      } finally {
        os.close()
        if (threw) {
          fs.delete(path)
        }
      }
    }
  }
}

private[hive] class SingletonTuple extends DataTuple {
  private var value: AnyRef = _

  def this(value: AnyRef) = {
    this()
    setValue(value)
  }

  def setValue(value: AnyRef): SingletonTuple = {
    this.value = value
    this
  }

  override def toDelimitedString(s: String): String = String.valueOf(value)

  override def get(i: Int): AnyRef = value

  override def size(): Int = 1

  override def isNull(i: Int): Boolean = value == null
}

private[hive] object SingletonTuple {
  private lazy val tuples: ThreadLocal[SingletonTuple] = new ThreadLocal[SingletonTuple] {
    override def initialValue(): SingletonTuple = new SingletonTuple()
  }

  def wrap(value: AnyRef): SingletonTuple = {
    tuples.get().setValue(value)
  }
}

private[hive] class HiveDataTuple extends DataTuple {
  private var row: InternalRow = _
  private var schema: Seq[Attribute] = _

  def setRow(r: InternalRow, schema: Seq[Attribute]): HiveDataTuple = {
    this.row = r
    this.schema = schema
    this
  }

  override def size(): Int = row.numFields

  override def toDelimitedString(p1: String): String = row.toString

  override def get(p1: Int): AnyRef = row.get(p1, schema(p1).dataType)

  override def isNull(p1: Int): Boolean = row.isNullAt(p1)
}

private[hive] object HiveDataTuple {
  private lazy val tuples: ThreadLocal[HiveDataTuple] = new ThreadLocal[HiveDataTuple] {
    override def initialValue(): HiveDataTuple = new HiveDataTuple()
  }

  def wrap(row: InternalRow, schema: Seq[Attribute]): HiveDataTuple = {
    tuples.get().setRow(row, schema)
  }
}

private class DummyMetrics extends PartitionMetrics {
  override def update(t: DataTuple): Unit = {}
}
