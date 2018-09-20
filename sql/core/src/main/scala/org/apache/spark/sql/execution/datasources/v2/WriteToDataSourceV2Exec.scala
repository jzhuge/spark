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

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{PartitionTransform, Table, TableCatalog}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.util.Utils

/**
 * Deprecated logical plan for writing data into data source v2. This is being replaced by more
 * specific logical plans, like [[org.apache.spark.sql.catalyst.plans.logical.AppendData]].
 */
@deprecated("Use specific logical plans like AppendData instead", "2.4.0")
case class WriteToDataSourceV2(writer: DataSourceWriter, query: LogicalPlan) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
}

case class AppendDataExec(
    table: Table,
    writeOptions: Map[String, String],
    plan: SparkPlan) extends V2TableWriteExec(writeOptions, plan) {

  override protected def doExecute(): RDD[InternalRow] = {
    appendToTable(table)
  }
}

case class OverwritePartitionsDynamicExec(
    table: Table,
    writeOptions: Map[String, String],
    plan: SparkPlan) extends V2TableWriteExec(writeOptions, plan) {
  import org.apache.spark.sql.sources.v2.DataSourceV2Implicits._

  override protected def doExecute(): RDD[InternalRow] = {
    val writer = table.createWriter(writeOptions, plan.schema)

    if (!writer.supportsReplacePartitions()) {
      throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
    }

    writer.replacePartitions()

    doAppend(writer)
  }
}

case class CreateTableAsSelectExec(
    catalog: TableCatalog,
    ident: TableIdentifier,
    partitioning: Seq[PartitionTransform],
    properties: Map[String, String],
    writeOptions: Map[String, String],
    plan: SparkPlan,
    ifNotExists: Boolean) extends V2TableWriteExec(writeOptions, plan) {

  override protected def doExecute(): RDD[InternalRow] = {
    if (catalog.tableExists(ident)) {
      if (ifNotExists) {
        return sparkContext.parallelize(Seq.empty, 1)
      }

      throw new TableAlreadyExistsException(ident.database.getOrElse("null"), ident.table)
    }

    Utils.tryWithSafeFinallyAndFailureCallbacks({
      val table = catalog.createTable(ident, plan.schema, partitioning.asJava, properties.asJava)
      appendToTable(table)
    })(catchBlock = {
      catalog.dropTable(ident)
    })
  }
}

case class ReplaceTableAsSelectExec(
    catalog: TableCatalog,
    ident: TableIdentifier,
    partitioning: Seq[PartitionTransform],
    properties: Map[String, String],
    writeOptions: Map[String, String],
    plan: SparkPlan) extends V2TableWriteExec(writeOptions, plan) {

  override protected def doExecute(): RDD[InternalRow] = {
    if (!catalog.tableExists(ident)) {
      throw new NoSuchTableException(ident.database.getOrElse("null"), ident.table)
    }

    catalog.dropTable(ident)

    Utils.tryWithSafeFinallyAndFailureCallbacks({
      val table = catalog.createTable(ident, plan.schema, partitioning.asJava, properties.asJava)
      appendToTable(table)
    })(catchBlock = {
      catalog.dropTable(ident)
    })
  }
}

case class WriteToDataSourceV2Exec(
    writer: DataSourceWriter,
    plan: SparkPlan) extends V2TableWriteExec(Map.empty, plan) {

  override protected def doExecute(): RDD[InternalRow] = {
    doAppend(writer)
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
abstract class V2TableWriteExec(
    options: Map[String, String],
    query: SparkPlan) extends SparkPlan {
  import org.apache.spark.sql.sources.v2.DataSourceV2Implicits._

  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  protected def appendToTable(table: Table): RDD[InternalRow] = {
    doAppend(table.createWriter(options, query.schema))
  }

  protected def doAppend(writer: DataSourceWriter): RDD[InternalRow] = {
    val writeTask = writer.createWriterFactory()
    val useCommitCoordinator = writer.useCommitCoordinator
    val rdd = query.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writeTask, context, iter, useCommitCoordinator),
        rdd.partitions.indices,
        (index, message: WriterCommitMessage) => {
          messages(index) = message
          writer.onDataWriterCommit(message)
        }
      )

      logInfo(s"Data source writer $writer is committing.")
      writer.commit(messages)
      logInfo(s"Data source writer $writer committed.")
    } catch {
      case cause: Throwable =>
        logError(s"Data source writer $writer is aborting.")
        try {
          writer.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source writer $writer failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source writer $writer aborted.")
        throw new SparkException("Writing job aborted.", cause)
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean): WriterCommitMessage = {
    val stageId = context.stageId()
    val partId = context.partitionId()
    val attemptId = context.attemptNumber()
    val dataWriter = writeTask.createDataWriter(
      context.partitionId(), context.taskAttemptId().toInt) // see SPARK-24552

    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      iter.foreach(dataWriter.write)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(context.stageId(),
          context.stageAttemptNumber(), partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Writer for stage $stageId, task $partId.$attemptId is authorized to commit.")
          dataWriter.commit()

        } else {
          val message = s"Stage $stageId, task $partId.$attemptId: driver did not authorize commit"
          logInfo(message)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw new CommitDeniedException(message, stageId, partId, attemptId)
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Writer for stage $stageId, task $partId.$attemptId committed.")

      msg

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Writer for stage $stageId, task $partId.$attemptId is aborting.")
      dataWriter.abort()
      logError(s"Writer for stage $stageId, task $partId.$attemptId aborted.")
    })
  }
}

