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

import java.net.URI
import java.util.Collections

import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, Identifier, TableCatalog, TestTableCatalog}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EliminateSubqueryAliases, EmptyFunctionRegistry, TestRelations, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.execution.datasources.DataSourceResolution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2ResolutionSuite extends AnalysisTest {

  override protected def getAnalyzer(caseSensitive: Boolean) = analyzer

  private lazy val analyzer = makeAnalyzer(caseSensitive = false)

  private def makeAnalyzer(caseSensitive: Boolean) = {
    val conf = new SQLConf().copy(
      SQLConf.CASE_SENSITIVE -> caseSensitive,
      SQLConf.DEFAULT_V2_CATALOG -> "testcat")
    val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", new URI("loc"), Map.empty),
      ignoreIfExists = false)
    catalog.createTempView("v1tbl", testV1Relation, overrideIfExists = true)
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        DataSourceResolution(conf, lookupCatalog) :: EliminateSubqueryAliases :: Nil
    }
  }

  private val lookupCatalog: String => CatalogPlugin = {
    case "testcat" =>
      testCat
    case name =>
      throw new CatalogNotFoundException(s"No such catalog: $name")
  }

  private val testCat: TableCatalog = {
    val newCatalog = new TestTableCatalog
    newCatalog.initialize("testcat", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]
  private val schema: StructType = (new StructType)
    .add("id", IntegerType)
    .add("data", StringType)
  private val v2tbl = testCat.createTable(
    Identifier.of(Array("db"), "v2tbl"),
    schema,
    Array.empty,
    emptyProps)
  private val testV2Relation = DataSourceV2Relation.create(v2tbl, CaseInsensitiveStringMap.empty)

  private val testV1Relation = TestRelations.testRelation

  test("resolve V2 relations") {
    // V2 table in catalog
    checkAnalysis(UnresolvedV2Relation(Seq("testcat", "db", "v2tbl")), testV2Relation)
    // V2 table in default catalog
    checkAnalysis(UnresolvedV2Relation(Seq("db", "v2tbl")), testV2Relation)
    // V1 table in session catalog
    checkAnalysis(UnresolvedV2Relation(Seq("v1tbl")), testV1Relation)
  }
}
