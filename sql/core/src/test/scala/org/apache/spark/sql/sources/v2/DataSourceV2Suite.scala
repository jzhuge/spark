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

package org.apache.spark.sql.sources.v2

import java.util.{ArrayList, List => JList}

import test.org.apache.spark.sql.sources.v2._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class DataSourceV2Suite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("simplest implementation") {
    Seq(classOf[SimpleDataSourceV2], classOf[JavaSimpleDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("advanced implementation") {
    def getReader(query: DataFrame): AdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[AdvancedDataSourceV2#Reader]
      }.head
    }

    def getJavaReader(query: DataFrame): JavaAdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[JavaAdvancedDataSourceV2#Reader]
      }.head
    }

    Seq(classOf[AdvancedDataSourceV2], classOf[JavaAdvancedDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))

        val q1 = df.select('j)
        checkAnswer(q1, (0 until 10).map(i => Row(-i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q1)
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        } else {
          val reader = getJavaReader(q1)
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        }

        val q2 = df.filter('i > 3)
        checkAnswer(q2, (4 until 10).map(i => Row(i, -i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q2)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i", "j"))
        } else {
          val reader = getJavaReader(q2)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i", "j"))
        }

        val q3 = df.select('i).filter('i > 6)
        checkAnswer(q3, (7 until 10).map(i => Row(i)))
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q3)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i"))
        } else {
          val reader = getJavaReader(q3)
          assert(reader.filters.flatMap(_.references).toSet == Set("i"))
          assert(reader.requiredSchema.fieldNames === Seq("i"))
        }

        val q4 = df.select('j).filter('j < -10)
        checkAnswer(q4, Nil)
        if (cls == classOf[AdvancedDataSourceV2]) {
          val reader = getReader(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        } else {
          val reader = getJavaReader(q4)
          // 'j < 10 is not supported by the testing data source.
          assert(reader.filters.isEmpty)
          assert(reader.requiredSchema.fieldNames === Seq("j"))
        }
      }
    }
  }

  test("unsafe row implementation") {
    Seq(classOf[UnsafeRowDataSourceV2], classOf[JavaUnsafeRowDataSourceV2]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }

  test("schema required data source") {
    Seq(classOf[SchemaRequiredDataSource], classOf[JavaSchemaRequiredDataSource]).foreach { cls =>
      withClue(cls.getName) {
        val e = intercept[AnalysisException](spark.read.format(cls.getName).load())
        assert(e.message.contains("requires a user-supplied schema"))

        val schema = new StructType().add("i", "int").add("s", "string")
        val df = spark.read.format(cls.getName).schema(schema).load()

        assert(df.schema == schema)
        assert(df.collect().isEmpty)
      }
    }
  }

  test("simple writable data source") {
    // TODO: java implementation.
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        // test with different save modes
        spark.range(10).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("append").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).union(spark.range(10)).select('id, -'id))

        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).mode("ignore").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))

        val e = intercept[Exception] {
          spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
            .option("path", path).mode("error").save()
        }
        assert(e.getMessage.contains("data already exists"))

        // test transaction
        val failingUdf = org.apache.spark.sql.functions.udf {
          var count = 0
          (id: Long) => {
            if (count > 5) {
              throw new RuntimeException("testing error")
            }
            count += 1
            id
          }
        }
        // this input data will fail to read middle way.
        val input = spark.range(10).select(failingUdf('id).as('i)).select('i as 'i, -'i as 'j)
        val e2 = intercept[SparkException] {
          input.write.format(cls.getName).option("path", path).mode("overwrite").save()
        }
        assert(e2.getMessage.contains("Writing job aborted"))
        // make sure we don't have partial data.
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        // test internal row writer
        spark.range(5).select('id as 'i, -'id as 'j).write.format(cls.getName)
          .option("path", path).option("internal", "true").mode("overwrite").save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(5).select('id, -'id))
      }
    }
  }

  test("SPARK-23301: column pruning with arbitrary expressions") {
    def getReader(query: DataFrame): AdvancedDataSourceV2#Reader = {
      query.queryExecution.executedPlan.collect {
        case d: DataSourceV2ScanExec => d.reader.asInstanceOf[AdvancedDataSourceV2#Reader]
      }.head
    }

    val df = spark.read.format(classOf[AdvancedDataSourceV2].getName).load()

    val q1 = df.select('i + 1)
    checkAnswer(q1, (1 until 11).map(i => Row(i)))
    val reader1 = getReader(q1)
    assert(reader1.requiredSchema.fieldNames === Seq("i"))

    val q2 = df.select(lit(1))
    checkAnswer(q2, (0 until 10).map(i => Row(1)))
    val reader2 = getReader(q2)
    assert(reader2.requiredSchema.isEmpty)

    // 'j === 1 can't be pushed down, but we should still be able do column pruning
    val q3 = df.filter('j === -1).select('j * 2)
    checkAnswer(q3, Row(-2))
    val reader3 = getReader(q3)
    assert(reader3.filters.isEmpty)
    assert(reader3.requiredSchema.fieldNames === Seq("j"))

    // column pruning should work with other operators.
    val q4 = df.sort('i).limit(1).select('i + 1)
    checkAnswer(q4, Row(1))
    val reader4 = getReader(q4)
    assert(reader4.requiredSchema.fieldNames === Seq("i"))
  }

  test("simple counter in writer with onDataWriterCommit") {
    Seq(classOf[SimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)

        val numPartition = 6
        spark.range(0, 10, 1, numPartition).select('id, -'id).write.format(cls.getName)
          .option("path", path).save()
        checkAnswer(
          spark.read.format(cls.getName).option("path", path).load(),
          spark.range(10).select('id, -'id))

        assert(SimpleCounter.getCounter == numPartition,
          "method onDataWriterCommit should be called as many as the number of partitions")
      }
    }
  }
}

class SimpleDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def planInputPartitions(): JList[InputPartition[Row]] = {
      java.util.Arrays.asList(new SimpleInputPartition(0, 5), new SimpleInputPartition(5, 10))
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class SimpleInputPartition(start: Int, end: Int)
  extends InputPartition[Row]
  with InputPartitionReader[Row] {
  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[Row] =
    new SimpleInputPartition(start, end)

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = Row(current, -current)

  override def close(): Unit = {}
}



class AdvancedDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader
    with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

    var requiredSchema = new StructType().add("i", "int").add("j", "int")
    var filters = Array.empty[Filter]

    override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val (supported, unsupported) = filters.partition {
        case GreaterThan("i", _: Int) => true
        case _ => false
      }
      this.filters = supported
      unsupported
    }

    override def pushedFilters(): Array[Filter] = filters

    override def readSchema(): StructType = {
      requiredSchema
    }

    override def planInputPartitions(): JList[InputPartition[Row]] = {
      val lowerBound = filters.collect {
        case GreaterThan("i", v: Int) => v
      }.headOption

      val res = new ArrayList[InputPartition[Row]]

      if (lowerBound.isEmpty) {
        res.add(new AdvancedInputPartition(0, 5, requiredSchema))
        res.add(new AdvancedInputPartition(5, 10, requiredSchema))
      } else if (lowerBound.get < 4) {
        res.add(new AdvancedInputPartition(lowerBound.get + 1, 5, requiredSchema))
        res.add(new AdvancedInputPartition(5, 10, requiredSchema))
      } else if (lowerBound.get < 9) {
        res.add(new AdvancedInputPartition(lowerBound.get + 1, 10, requiredSchema))
      }

      res
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class AdvancedInputPartition(start: Int, end: Int, requiredSchema: StructType)
  extends InputPartition[Row] with InputPartitionReader[Row] {

  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[Row] = {
    new AdvancedInputPartition(start, end, requiredSchema)
  }

  override def close(): Unit = {}

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = {
    val values = requiredSchema.map(_.name).map {
      case "i" => current
      case "j" => -current
    }
    Row.fromSeq(values)
  }
}


class UnsafeRowDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader extends DataSourceReader with SupportsScanUnsafeRow {
    override def readSchema(): StructType = new StructType().add("i", "int").add("j", "int")

    override def planUnsafeInputPartitions(): JList[InputPartition[UnsafeRow]] = {
      java.util.Arrays.asList(new UnsafeRowInputPartitionReader(0, 5),
        new UnsafeRowInputPartitionReader(5, 10))
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = new Reader
}

class UnsafeRowInputPartitionReader(start: Int, end: Int)
  extends InputPartition[UnsafeRow] with InputPartitionReader[UnsafeRow] {

  private val row = new UnsafeRow(2)
  row.pointTo(new Array[Byte](8 * 3), 8 * 3)

  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[UnsafeRow] = this

  override def next(): Boolean = {
    current += 1
    current < end
  }
  override def get(): UnsafeRow = {
    row.setInt(0, current)
    row.setInt(1, -current)
    row
  }

  override def close(): Unit = {}
}

class SchemaRequiredDataSource extends DataSourceV2 with ReadSupportWithSchema {

  class Reader(val readSchema: StructType) extends DataSourceReader {
    override def planInputPartitions(): JList[InputPartition[Row]] =
      java.util.Collections.emptyList()
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    new Reader(schema)
}
