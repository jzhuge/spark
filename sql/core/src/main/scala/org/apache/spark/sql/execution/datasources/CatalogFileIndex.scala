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

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.parallel.ThreadPoolTaskSupport

import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{BatchIdPathFilter, Filters, HiddenPathFilter, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType


/**
 * A [[FileIndex]] for a metastore catalog table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param table the metadata of the table
 * @param sizeInBytes the table's data size in bytes
 */
class CatalogFileIndex(
    sparkSession: SparkSession,
    val table: CatalogTable,
    override val sizeInBytes: Long) extends FileIndex {

  protected val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  /** Globally shared (not exclusive to this table) cache for file statuses to speed up listing. */
  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  assert(table.identifier.database.isDefined,
    "The table identifier must be qualified in CatalogFileIndex")

  private val baseLocation: Option[URI] = table.storage.locationUri

  override def partitionSchema: StructType = table.partitionSchema

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    filterPartitions(partitionFilters).listFiles(Nil, dataFilters)
  }

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  /**
   * Returns a [[InMemoryFileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): FileIndex = {
    if (table.partitionColumnNames.nonEmpty) {
      val selectedPartitions = sparkSession.sessionState.catalog.listPartitionsByFilter(
        table.identifier, filters)
      val partitions = selectedPartitions.map { p =>
        val path = new Path(p.location)
        val fs = path.getFileSystem(hadoopConf)
        PartitionPath(
          p.toRow(partitionSchema, sparkSession.sessionState.conf.sessionLocalTimeZone),
          path.makeQualified(fs.getUri, fs.getWorkingDirectory))
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      new PrunedInMemoryFileIndex(
        sparkSession, new Path(baseLocation.get), fileStatusCache, partitionSpec)
    } else {
      new InMemoryFileIndex(
        sparkSession, rootPaths, table.storage.properties, partitionSchema = None)
    }
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  // `CatalogFileIndex` may be a member of `HadoopFsRelation`, `HadoopFsRelation` may be a member
  // of `LogicalRelation`, and `LogicalRelation` may be used as the cache key. So we need to
  // implement `equals` and `hashCode` here, to make it work with cache lookup.
  override def equals(o: Any): Boolean = o match {
    case other: CatalogFileIndex => this.table.identifier == other.table.identifier
    case _ => false
  }

  override def hashCode(): Int = table.identifier.hashCode()

  private lazy val maybeBatchIdFilter = if (sparkSession.sqlContext.conf.ignoreBatchIdFolders) {
    Some(BatchIdPathFilter.get)
  } else {
    None
  }

  private lazy val maybeCustomFilter = Option(FileInputFormat.getInputPathFilter(
    new JobConf(sparkSession.sparkContext.hadoopConfiguration)))

  private lazy val filter: PathFilter = Filters.from(HiddenPathFilter.get)
    .and(maybeCustomFilter)
    .and(maybeBatchIdFilter)

  private lazy val listingCache: LoadingCache[Path, Seq[FileStatus]] = CacheBuilder.newBuilder()
    .maximumSize(sparkSession.sqlContext.conf.filesourcePartitionFileCacheSize)
    .build(new CacheLoader[Path, Seq[FileStatus]]() {
      override def load(path: Path): Seq[FileStatus] = {
        try {
          val files = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
            .listStatus(path, filter)
            .toSeq
          HiveCatalogMetrics.incrementFilesDiscovered(files.size)
          files
        } catch {
          case e: FileNotFoundException =>
            Seq.empty
        }
      }
    })
}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 *
 * @param tableBasePath The default base path of the Hive metastore table
 * @param partitionSpec The partition specifications from Hive metastore
 */
private class PrunedInMemoryFileIndex(
    sparkSession: SparkSession,
    tableBasePath: Path,
    fileStatusCache: FileStatusCache,
    override val partitionSpec: PartitionSpec)
  extends InMemoryFileIndex(
    sparkSession,
    partitionSpec.partitions.map(_.path),
    Map.empty,
    Some(partitionSpec.partitionColumns),
    fileStatusCache)

private class PartitionSpecFileIndex(
    sparkSession: SparkSession,
    partitionSpec: PartitionSpec,
    listingCache: LoadingCache[Path, Seq[FileStatus]])
  extends FileIndex with Logging {

  import PartitionSpecFileIndex.parallelizeIfLarge

  /** Schema of the partitioning columns, or the empty schema if the table is not partitioned. */
  override def partitionSchema: StructType = partitionSpec.partitionColumns


  /**
   * Returns the list of root input paths from which the catalog will get files. There may be a
   * single root path from which partitions are discovered, or individual partitions may be
   * specified by each path.
   */
  override def rootPaths: Seq[Path] = partitionSpec.partitions.map(_.path)

  /**
   * Returns all valid files grouped into partitions when the data is partitioned. If the data is
   * unpartitioned, this will return a single partition with no partition values.
   *
   * @param filters The filters used to prune which partitions are returned.  These filters must
   *                only refer to partition columns and this method will only return files
   *                where these predicates are guaranteed to evaluate to `true`.  Thus, these
   *                filters will not need to be evaluated again on the returned data.
   */
  override def listFiles(filters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitions = PartitioningAwareFileIndex.prunePartitions(filters, partitionSpec)
    parallelizeIfLarge(partitions).map { partition =>
      val maybeFiles = Option(listingCache.getIfPresent(partition.path))
      maybeFiles.foreach(files => HiveCatalogMetrics.incrementFileCacheHits(files.size))
      PartitionDirectory(partition.values, maybeFiles.getOrElse(listingCache.get(partition.path)))
    }.seq
  }

  /** Refresh the file listing */
  override def refresh(): Unit = {
    listingCache.invalidateAll()
  }

  override def inputFiles: Array[String] = {
    listFiles(Seq(), Nil).flatMap(_.files).map(_.getPath.toString).toArray
  }

  /** Sum of table file sizes, in bytes */
  override def sizeInBytes: Long = 0L
}

private object PartitionSpecFileIndex {
  val PARALLEL_LISTING_THRESH = 16 // about 1 second of work for S3
  val WORKER_POOL: ThreadPoolTaskSupport = new ThreadPoolTaskSupport(new ThreadPoolExecutor(
    4, 16, 30, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](),
    new ThreadFactoryBuilder().setNameFormat("hive-liststatus-%s").setDaemon(true).build))

  private def parallelizeIfLarge[E](seq: Seq[E]) = {
    if (seq.size > PARALLEL_LISTING_THRESH) {
      val parSeq = seq.par
      parSeq.tasksupport = WORKER_POOL
      parSeq
    } else {
      seq
    }
  }
}
