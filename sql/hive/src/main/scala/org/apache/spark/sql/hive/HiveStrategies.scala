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

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.{CreateTable, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.execution._

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, _, options)
          if isS3(table) && isParquet(table) && convertHiveParquetWrite(sparkSession) =>

        val location = table.hiveQlTable.getDataLocation
        val staticParts = partition.filter(kv => kv._2.isDefined).mapValues(_.get)

        // add literals to the plan for all static partition columns
        val static = table.catalogTable.partitionSchema
            .filter(c => staticParts.contains(c.name))
            .map(c => Column(Literal(staticParts(c.name))).cast(c.dataType).as(c.name).named)
        val allColumns = child.output ++ static
        val childWithStaticPartitions = Project(allColumns, child)

        val partitionAttrs = childWithStaticPartitions.resolve(
          table.catalogTable.partitionSchema, sparkSession.sessionState.conf.resolver)
        val mode = if (overwrite.enabled) SaveMode.Overwrite else SaveMode.Append
        val refreshFunc = (arg: Any) => {
          sparkSession.catalog.refreshTable(table.catalogTable.identifier.toString)
        }

        val cmd = InsertIntoHadoopFsRelationCommand(
          location, staticParts, Map.empty, partitionAttrs, None, new ParquetFileFormat(),
          refreshFunc, options, childWithStaticPartitions, mode, Some(table.catalogTable))

        ExecutedCommandExec(cmd, cmd.children.map(planLater)) :: Nil

      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists, _) =>
        InsertIntoHiveTable(
          table, partition, planLater(child), overwrite.enabled, ifNotExists) :: Nil

      case CreateTable(tableDesc, mode, Some(query)) if tableDesc.provider.get == "hive" =>
        val newTableDesc = if (tableDesc.storage.serde.isEmpty) {
          // add default serde
          tableDesc.withNewStorage(
            serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        } else {
          tableDesc
        }

        // Currently we will never hit this branch, as SQL string API can only use `Ignore` or
        // `ErrorIfExists` mode, and `DataFrameWriter.saveAsTable` doesn't support hive serde
        // tables yet.
        if ((mode == SaveMode.Append || mode == SaveMode.Overwrite) && !isS3(tableDesc.location)) {
          throw new AnalysisException(
            "CTAS for hive serde tables does not support append or overwrite semantics.")
        }

        val dbName = tableDesc.identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
        val cmd = CreateHiveTableAsSelectCommand(
          newTableDesc.copy(identifier = tableDesc.identifier.copy(database = Some(dbName))),
          query,
          mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd, cmd.children.map(planLater)) :: Nil

      case _ => Nil
    }

    private def isS3(table: MetastoreRelation): Boolean = {
      Option(table.hiveQlTable.getDataLocation.toUri.getScheme).exists(_.startsWith("s3"))
    }

    private def isS3(location: String): Boolean = {
      Option(new Path(location).toUri.getScheme).exists(_.startsWith("s3"))
    }

    def isParquet(table: MetastoreRelation): Boolean = {
      table.tableDesc.getSerdeClassName.toLowerCase.contains("parquet")
    }

    def convertHiveParquetWrite(sparkSession: SparkSession): Boolean = {
      sparkSession.sessionState.conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WRITE)
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(
            _, relation, pruningPredicates, filterPredicates = Some(otherPredicates)
          )(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }
}
