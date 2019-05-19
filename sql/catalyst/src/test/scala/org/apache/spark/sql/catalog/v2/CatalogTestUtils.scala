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
import java.util.Collections

import scala.collection.mutable

import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.CatalogTableIdentifier
import org.apache.spark.sql.catalyst.analysis.V2TableIdentifierImplicits
import org.apache.spark.sql.catalyst.parser.{AstBuilder, CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.Table
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[sql] trait CatalogTestUtils {

  import CatalogV2Implicits._

  lazy val conf: SQLConf = new SQLConf

  lazy val catalogs = TestCatalogs(conf)

  def catalog(name: String): CatalogPlugin = synchronized {
    catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
  }

  catalogs.add("default", isDefault = true)
  catalogs.add("testcat")

  def findCatalog(name: String): CatalogPlugin = catalog(name)

  val v2TableIdentifierImplicits = V2TableIdentifierImplicits(conf, findCatalog)

  import v2TableIdentifierImplicits._

  lazy val sqlParser: ParserInterface = new CatalystSqlParser(conf) {
    override val astBuilder: AstBuilder = new AstBuilder(conf) {
      override lazy val tableIdentifierImplicits = v2TableIdentifierImplicits
    }
  }

  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val testSchema: StructType =
    new StructType()
      .add("id", IntegerType)
      .add("data", StringType)

  /**
   * Create a test table.
   */
  def createTable(
      name: String,
      schema: StructType = testSchema,
      partitions: Array[Transform] = Array.empty,
      properties: util.Map[String, String] = emptyProps): Table =
    sqlParser.parseMultipartIdentifier(name).asCatalogTableIdentifier match {
      case CatalogTableIdentifier(catalog, ident) =>
        catalog.asTableCatalog.createTable(ident, schema, partitions, properties)
    }

  /**
   * Drops tables after calling `f`.
   */
  protected def withCatalogTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        sqlParser.parseMultipartIdentifier(name).asCatalogTableIdentifier match {
          case CatalogTableIdentifier(catalog, ident) =>
            catalog.asTableCatalog.dropTable(ident)
        }
      }
    }
  }
}

private[sql] case class TestCatalogs(conf: SQLConf)
  extends mutable.HashMap[String, CatalogPlugin] {

  def add(name: String, pluginClassName: String = classOf[TestTableCatalog].getName,
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
      isDefault: Boolean = false): Unit = {
    Catalogs.add(name, pluginClassName, options, conf)
    if (isDefault) {
      conf.setConfString(SQLConf.DEFAULT_V2_CATALOG.key, name)
    }
  }
}
