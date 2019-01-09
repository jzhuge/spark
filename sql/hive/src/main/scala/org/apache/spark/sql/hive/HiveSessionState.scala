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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Analysis, DataSourceV2Strategy}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SessionState, SQLConf}


/**
 * A class that holds all session-specific state in a given [[SparkSession]] backed by Hive.
 */
private[hive] class HiveSessionState(sparkSession: SparkSession)
  extends SessionState(sparkSession) {

  self =>

  /**
   * A SQLConf that uses values from Hadoop environment configurations as defaults.
   *
   * @param hadoopConf a Hadoop Configuration
   */
  private[sql] class HiveSQLConf(@transient hadoopConf: Configuration) extends SQLConf {
    private def ensureS3Loaded(): Unit = {
      // temporarily work around for add jar. load the file system with the current classloader to
      // avoid any problems with commons logging.
      if (hadoopConf.get("fs.s3.impl") != null) {
        val p = new Path("s3://netflix-dataoven-prod/genie/jars/dataoven-hive-tools.jar")
        val fs = p.getFileSystem(hadoopConf)
        if (fs.exists(p)) {
          val in = fs.open(p)
          try {
            in.read()
          } finally {
            in.close()
          }
        }
      }
    }

    ensureS3Loaded()

    sparkSession.sparkContext.getConf.getAll
        .filter {
          case (key: String, value: String) => key.startsWith("spark.session.")
        }.foreach {
          case (key, value) => setConfString(key.replaceFirst("spark.session.", ""), value)
        }

    /** Return the value of Spark SQL configuration property for the given key. */
    override def getConfString(
        key: String): String = {
      Option(hadoopConf.get(key)) match {
        case Some(envDefault) =>
          super.getConfString(key, envDefault)
        case None =>
          super.getConfString(key)
      }
    }

    /**
     * Return the `string` value of Spark SQL configuration property for the given key. If the key
     * is not set yet, return `defaultValue`.
     */
    override def getConfString(key: String,
        defaultValue: String): String = {
      Option(hadoopConf.get(key)) match {
        case Some(envDefault) =>
          super.getConfString(key, envDefault)
        case None =>
          super.getConfString(key, defaultValue)
      }
    }
  }

  override lazy val conf: SQLConf = new HiveSQLConf(sparkSession.sparkContext.hadoopConfiguration)

  /**
   * A Hive client used for interacting with the metastore.
   */
  lazy val metadataHive: HiveClient =
    sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client.newSession()

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = {
    new HiveSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

  /**
   * An analyzer that uses the Hive metastore.
   */
  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        NetflixAnalysis(sparkSession) ::
        DataSourceV2Analysis(sparkSession) ::
        AnalyzeCreateTable(sparkSession) ::
        DataSourceAnalysis(conf) ::
        (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

      override val extendedCheckRules = Seq(PreWriteCheck(conf, catalog))
    }
  }

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override def planner: SparkPlanner = {
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)
      with HiveStrategies {
      override val sparkSession: SparkSession = self.sparkSession

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++ Seq(
          DataSourceV2Strategy,
          NetflixStrategy(sparkSession),
          FileSourceStrategy,
          DataSourceStrategy,
          DDLStrategy,
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          DataSinks,
          Scripts,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }


  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  override def addJar(path: String): Unit = {
    metadataHive.addJar(path)
    super.addJar(path)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreParquet: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET)
  }

  def convertMetastoreParquetWrite: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WRITE)
  }

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  def convertMetastoreParquetWithSchemaMerging: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the Orc SerDe
   * are automatically converted to use the Spark SQL ORC table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreOrc: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  /**
   * When true, Hive Thrift server will execute SQL queries asynchronously using a thread pool."
   */
  def hiveThriftServerAsync: Boolean = {
    conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
  }

  // TODO: why do we get this from SparkConf but not SQLConf?
  def hiveThriftServerSingleSession: Boolean = {
    sparkSession.sparkContext.conf.getBoolean(
      "spark.sql.hive.thriftServer.singleSession", defaultValue = false)
  }

}
