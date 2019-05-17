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

import com.netflix.bdp.Events
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import parquet.filter2.predicate.FilterApi
import parquet.hadoop.ParquetInputFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{BatchIdPathFilter, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.V2Util
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.parquet.HiveParquetFilters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}
import org.apache.spark.util.Utils

/**
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @param requestedAttributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
private[hive]
case class HiveTableScanExec(
    requestedAttributes: Seq[Attribute],
    relation: MetastoreRelation,
    partitionPruningPred: Seq[Expression],
    filterPredicates: Option[Seq[Expression]] = None)(
    @transient private val sparkSession: SparkSession)
  extends LeafExecNode {

  require(partitionPruningPred.isEmpty || relation.hiveQlTable.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  // Retrieve the original attributes based on expression ID so that capitalization matches.
  val attributes = requestedAttributes.map(relation.attributeMap)

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private[this] val boundPruningPred = partitionPruningPred.reduceLeftOption(And).map { pred =>
    require(
      pred.dataType == BooleanType,
      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")

    BindReferences.bindReference(pred, relation.partitionKeys)
  }

  private lazy val dataFilters = {
    if (filterPredicates.isDefined) {
      // This is adapted from FileSourceStrategy:
      // The attribute name of predicate could be different than the one in schema in case of
      // case insensitive, we should change them to match the one in schema, so we donot need to
      // worry about case sensitivity anymore.
      val normalizedFilters = filterPredicates.get.map { pred =>
        pred transform {
          case attr: AttributeReference =>
            attr.withName(requestedAttributes.find(_.semanticEquals(attr)).get.name)
        }
      }
      Some(normalizedFilters.flatMap(DataSourceStrategy.translateFilter))
    } else {
      None
    }
  }

  // This is adapted from ParquetFileFormat:
  // Try to push down filters when filter push-down is enabled.
  private lazy val parquetFilters = {
    if (sparkSession.conf.get(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key).toBoolean &&
        sparkSession.conf.get(SQLConf.PARQUET_HIVE_FILTER_PUSHDOWN_ENABLED.key).toBoolean) {
      val dataSchema = StructType.fromAttributes(requestedAttributes)
      dataFilters.flatMap { filters =>
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        filters.flatMap(HiveParquetFilters.createFilter(dataSchema, _)).reduceOption(FilterApi.and)
      }
    } else {
      None
    }
  }

  // Create a local copy of hadoopConf,so that scan specific modifications should not impact
  // other queries
  @transient
  private[this] val hadoopConf = sparkSession.sessionState.newHadoopConf()

  // for Parquet relations, push data filters
  if (relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
      parquetFilters.isDefined) {
    ParquetInputFormat.setFilterPredicate(hadoopConf, parquetFilters.get)
  }

  if (sparkSession.sqlContext.conf.ignoreBatchIdFolders) {
    hadoopConf.setClass("mapreduce.input.pathFilter.class",
      classOf[BatchIdPathFilter], classOf[PathFilter])
  }

  // append columns ids and names before broadcast
  addColumnMetadataToConf(hadoopConf)

  @transient
  private[this] val hadoopReader =
    new HadoopTableReader(attributes, relation, sparkSession, hadoopConf)

  private[this] def castFromString(value: String, dataType: DataType) = {
    Cast(Literal(value), dataType).eval(null)
  }

  private def addColumnMetadataToConf(hiveConf: Configuration) {
    // Specifies needed column IDs for those non-partitioning columns.
    val neededColumnIDs = attributes.flatMap(relation.columnOrdinals.get).map(o => o: Integer)

    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, attributes.map(_.name))

    val tableDesc = relation.tableDesc
    val deserializer = tableDesc.getDeserializerClass.newInstance
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, relation.attributes.map(_.name).mkString(","))
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionKeys.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val allPredicates = filterPredicates
        .map(partitionPruningPred ++ _)
        .getOrElse(partitionPruningPred)

    Events.sendScan(
      s"${relation.databaseName}.${relation.tableName}",
      if (allPredicates.nonEmpty) allPredicates.reduce(And).sql else "true",
      V2Util.columns(schema).asJava,
      Map.empty[String, String].asJava)

    // Using dummyCallSite, as getCallSite can turn out to be expensive with
    // with multiple partitions.
    val rdd = if (!relation.hiveQlTable.isPartitioned) {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForTable(relation.hiveQlTable)
      }
    } else {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        hadoopReader.makeRDDForPartitionedTable(
          prunePartitions(relation.getHiveQlPartitions(partitionPruningPred)))
      }
    }
    val numOutputRows = longMetric("numOutputRows")
    // Avoid to serialize MetastoreRelation because schema is lazy. (see SPARK-15649)
    val outputSchema = schema
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(outputSchema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def output: Seq[Attribute] = attributes

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case other: HiveTableScanExec =>
      val thisPredicates = partitionPruningPred.map(cleanExpression)
      val otherPredicates = other.partitionPruningPred.map(cleanExpression)

      val result = relation.sameResult(other.relation) &&
        output.length == other.output.length &&
          output.zip(other.output)
            .forall(p => p._1.name == p._2.name && p._1.dataType == p._2.dataType) &&
              thisPredicates.length == otherPredicates.length &&
                thisPredicates.zip(otherPredicates).forall(p => p._1.semanticEquals(p._2))
      result
    case _ => false
  }
}
