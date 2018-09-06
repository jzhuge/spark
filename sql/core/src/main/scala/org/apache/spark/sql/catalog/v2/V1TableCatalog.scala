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

package org.apache.spark.sql.catalog.v2

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.v2.{DataSourceV2TableProvider, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * A [[TableCatalog]] that translates calls to a v1 SessionCatalog.
 */
class V1TableCatalog(sessionState: SessionState) extends TableCatalog {

  import org.apache.spark.sql.sources.v2.DataSourceV2Implicits.OptionsHelper

  private var _name: String = _

  override def name: String = _name

  private lazy val catalog: SessionCatalog = sessionState.catalog

  override def loadTable(ident: TableIdentifier): Table = {
    val catalogTable = catalog.getTableMetadata(ident)

    catalogTable.provider match {
      case Some(provider) if provider != "hive" =>
        DataSource.lookupDataSource(provider).newInstance() match {
          case tableProvider: DataSourceV2TableProvider =>
            val storageOptions = ident.database match {
              case Some(db) =>
                catalogTable.storage.properties +
                    ("provider" -> provider) +
                    ("database" -> db) +
                    ("table" -> ident.table)
              case None =>
                catalogTable.storage.properties +
                    ("provider" -> provider) +
                    ("table" -> ident.table)
            }
            tableProvider.createTable(storageOptions.asDataSourceOptions)

          case v2Source: ReadSupport if v2Source.isInstanceOf[WriteSupport] =>
            new V1MetadataTable(catalogTable, Some(v2Source))
                with DelegateReadSupport with DelegateWriteSupport

          case v2Source: ReadSupport =>
            new V1MetadataTable(catalogTable, Some(v2Source)) with DelegateReadSupport

          case v2Source: WriteSupport =>
            new V1MetadataTable(catalogTable, Some(v2Source)) with DelegateWriteSupport

          case _ =>
            new V1MetadataTable(catalogTable, None)
        }

      case _ =>
        new V1MetadataTable(catalogTable, None)
    }
  }

  override def createTable(ident: TableIdentifier,
      schema: StructType,
      partitions: util.List[PartitionTransform],
      properties: util.Map[String, String]): Table = {
    val (partitionColumns, maybeBucketSpec) = PartitionUtil.convertTransforms(partitions.asScala)
    val source = properties.getOrDefault("provider", sessionState.conf.defaultDataSourceName)
    val tableProperties = properties.asScala
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)

    val tableDesc = CatalogTable(
      identifier = ident.copy(
        database = Some(ident.database.getOrElse(sessionState.catalog.getCurrentDatabase))),
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = schema,
      provider = Some(source),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      tracksPartitionsInCatalog = sessionState.conf.manageFilesourcePartitions)

    catalog.createTable(tableDesc, ignoreIfExists = false)

    loadTable(ident)
  }

  override def alterTable(ident: TableIdentifier,
      changes: util.List[TableChange]): Table = {
    throw new UnsupportedOperationException("Alter table is not supported for this source")
  }

  /**
   * Drop a table in the catalog.
   *
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  override def dropTable(ident: TableIdentifier): Boolean = {
    try {
      if (loadTable(ident) != null) {
        catalog.dropTable(ident, ignoreIfNotExists = true, purge = true /* skip HDFS trash */)
        true
      } else {
        false
      }
    } catch {
      case _: NoSuchTableException =>
        false
    }
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this._name = name
  }
}
