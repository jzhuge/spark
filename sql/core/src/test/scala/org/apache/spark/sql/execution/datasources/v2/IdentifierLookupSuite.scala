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

import java.util.Collections

import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, Identifier, TableCatalog, TestTableCatalog}
import org.apache.spark.sql.catalyst.{CatalogTableIdentifier, TableIdentifier, TableIdentifierLike}
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer, EmptyFunctionRegistry, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class IdentifierLookupSuite extends AnalysisTest {

  override protected def getAnalyzer(caseSensitive: Boolean): Analyzer = analyzer

  private lazy val analyzer = makeAnalyzer(caseSensitive = false)

  private def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
    val conf = new SQLConf().copy(
      SQLConf.CASE_SENSITIVE -> caseSensitive,
      SQLConf.DEFAULT_V2_CATALOG -> "default")
    val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
    new Analyzer(catalog, conf) {
      override val identifierLookupRules: Seq[Rule[LogicalPlan]] =
        IdentifierLookup(conf, lookupCatalog) :: Nil
    }
  }

  private val lookupCatalog: String => CatalogPlugin = {
    case "default" =>
      defCat
    case "testcat" =>
      testCat
    case name =>
      throw new CatalogNotFoundException(s"No such catalog: $name")
  }

  private val defCat: TableCatalog = {
    val newCatalog = new TestTableCatalog
    newCatalog.initialize("default", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val testCat: TableCatalog = {
    val newCatalog = new TestTableCatalog
    newCatalog.initialize("testcat", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val schema: StructType = (new StructType)
    .add("id", IntegerType)
    .add("data", StringType)
  private val emptyProps: java.util.Map[String, String] = Collections.emptyMap[String, String]

  private val tbl = Identifier.of(Array("ns1", "ns2"), "tbl")
  defCat.createTable(tbl, schema, Array.empty, emptyProps)

  private val v2tbl = Identifier.of(Array.empty, "v2tbl")
  testCat.createTable(v2tbl, schema, Array.empty, emptyProps)

  private def checkIdentifierLookup(parts: Seq[String], ident: TableIdentifierLike) =
    comparePlans(analyzer.lookupIdentifiers(UnresolvedRelation(parts)), UnresolvedRelation(ident))

  test("Lookup multipart identifier") {
    // V2 table with catalog
    checkIdentifierLookup(Seq("testcat", "v2tbl"), CatalogTableIdentifier(testCat, v2tbl))
    // V2 table in default catalog
    checkIdentifierLookup(Seq("ns1", "ns2", "tbl"), CatalogTableIdentifier(defCat, tbl))
    // V1 table in session catalog
    checkIdentifierLookup(Seq("v1tbl"), TableIdentifier("v1tbl"))
  }
}
