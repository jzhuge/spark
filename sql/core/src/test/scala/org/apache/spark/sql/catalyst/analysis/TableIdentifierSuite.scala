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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalog.v2.{CatalogTestUtils, Identifier}
import org.apache.spark.sql.catalyst.{CatalogTableIdentifier, TableIdentifier}

class TableIdentifierSuite extends SparkFunSuite with CatalogTestUtils {

  test("lookup table identifier") {
    import v2TableIdentifierImplicits._

    // V2 table in non-default catalog
    assert(Seq("testcat", "v2tbl").asCatalogTableIdentifier ===
      CatalogTableIdentifier(catalog("testcat"), Identifier.of(Array.empty, "v2tbl")))
    // V2 table in default catalog
    withCatalogTable("default.ns1.ns2.tbl") {
      createTable("default.ns1.ns2.tbl")
      assert(Seq("ns1", "ns2", "tbl").asCatalogTableIdentifier ===
        CatalogTableIdentifier(catalog("default"), Identifier.of(Array("ns1", "ns2"), "tbl"))
      )
    }
    // V1 table
    assert(Seq("v1tbl").asCatalogTableIdentifier === TableIdentifier("v1tbl"))
    // V1 table with db
    assert(Seq("db", "tbl").asCatalogTableIdentifier === TableIdentifier("tbl", Some("db")))
  }

  test("lookup table identifier in V1 only mode") {
    import V1TableIdentifierImplicits._

    assert(Seq("db", "v1tbl").asCatalogTableIdentifier === TableIdentifier("v1tbl", Some("db")))
    assert(Seq("v1tbl").asCatalogTableIdentifier === TableIdentifier("v1tbl"))

    intercept[UnsupportedOperationException] {
      Seq("ns1", "ns2", "v2tbl").asCatalogTableIdentifier
    }.getMessage.contains("not a V1 table identifier")
  }
}
