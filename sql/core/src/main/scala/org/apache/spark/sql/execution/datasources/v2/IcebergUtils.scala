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

import com.netflix.iceberg.spark.hacks.Hive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition


object IcebergUtils {
    /**
     * Returns a DataFrame with a row for each partition in the table.
     *
     * The DataFrame has 3 columns, partition key (a=1/b=2), partition location, and format
     * (avro or parquet).
     *
     * @param spark a Spark session
     * @param table a table name and (optional) database
     * @return a DataFrame of the table's partitions
     */
    def partitionDF(spark: SparkSession, table: String): DataFrame = {
      import spark.implicits._

      val partitions: Seq[(Map[String, String], Option[String], Option[String])] =
        Hive.partitions(spark, table).map { p: CatalogTablePartition =>
          (p.spec, p.storage.locationUri.map(String.valueOf(_)), p.storage.serde)
        }

      partitions.toDF("partition", "uri", "format")
    }
}
