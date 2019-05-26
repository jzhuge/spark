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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, Identifier, TableIdentifierHelper, TestTableCatalog}
import org.apache.spark.sql.catalyst.{CatalogTableIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TableIdentifierParserSuite extends SparkFunSuite with SQLHelper with TableIdentifierHelper {
  import CatalystSqlParser._

  private val testCat = new TestTableCatalog {
    initialize("testcat", CaseInsensitiveStringMap.empty())
  }

  private def findCatalog(name: String): CatalogPlugin = name match {
    case "testcat" =>
      testCat
    case _ =>
      throw new CatalogNotFoundException(s"$name not found")
  }

  override protected def lookupCatalog(name: String): CatalogPlugin = findCatalog(name)

  // Add "$elem$", "$value$" & "$key$"
  // It is recommended to list them in alphabetical order.
  val hiveNonReservedKeyword = Array(
    "add",
    "admin",
    "after",
    "all",
    "alter",
    "analyze",
    "any",
    "archive",
    "array",
    "as",
    "asc",
    "at",
    "authorization",
    "autocommit",
    "before",
    "between",
    "bigint",
    "binary",
    "boolean",
    "both",
    "bucket",
    "buckets",
    "by",
    "cascade",
    "change",
    "cluster",
    "clustered",
    "clusterstatus",
    "collection",
    "columns",
    "comment",
    "compact",
    "compactions",
    "compute",
    "concatenate",
    "continue",
    "cost",
    "create",
    "cube",
    "current_date",
    "current_timestamp",
    "cursor",
    "data",
    "databases",
    "date",
    "datetime",
    "day",
    "days",
    "dbproperties",
    "decimal",
    "deferred",
    "defined",
    "delete",
    "delimited",
    "dependency",
    "desc",
    "describe",
    "directories",
    "directory",
    "disable",
    "distribute",
    "double",
    "drop",
    "enable",
    "escaped",
    "exclusive",
    "exists",
    "explain",
    "export",
    "external",
    "extract",
    "false",
    "fetch",
    "fields",
    "file",
    "fileformat",
    "first",
    "float",
    "for",
    "format",
    "formatted",
    "functions",
    "grant",
    "group",
    "grouping",
    "hold_ddltime",
    "hour",
    "hours",
    "idxproperties",
    "ignore",
    "import",
    "in",
    "index",
    "indexes",
    "inpath",
    "inputdriver",
    "inputformat",
    "insert",
    "int",
    "into",
    "is",
    "isolation",
    "items",
    "jar",
    "key_type",
    "keys",
    "last",
    "lateral",
    "leading",
    "level",
    "like",
    "limit",
    "lines",
    "load",
    "local",
    "location",
    "lock",
    "locks",
    "logical",
    "long",
    "mapjoin",
    "materialized",
    "metadata",
    "microsecond",
    "microseconds",
    "millisecond",
    "milliseconds",
    "minus",
    "minute",
    "minutes",
    "month",
    "months",
    "msck",
    "no_drop",
    "none",
    "noscan",
    "null",
    "nulls",
    "of",
    "offline",
    "offset",
    "option",
    "order",
    "out",
    "outer",
    "outputdriver",
    "outputformat",
    "overwrite",
    "owner",
    "partition",
    "partitioned",
    "partitions",
    "percent",
    "pivot",
    "plus",
    "position",
    "pretty",
    "principals",
    "procedure",
    "protection",
    "purge",
    "query",
    "range",
    "read",
    "readonly",
    "reads",
    "rebuild",
    "recordreader",
    "recordwriter",
    "regexp",
    "reload",
    "rename",
    "repair",
    "replace",
    "replication",
    "restrict",
    "revoke",
    "rewrite",
    "rlike",
    "role",
    "roles",
    "rollup",
    "row",
    "rows",
    "schemas",
    "second",
    "seconds",
    "serde",
    "serdeproperties",
    "server",
    "set",
    "sets",
    "shared",
    "show",
    "show_database",
    "skewed",
    "smallint",
    "snapshot",
    "sort",
    "sorted",
    "ssl",
    "statistics",
    "stored",
    "streamtable",
    "string",
    "struct",
    "table",
    "tables",
    "tblproperties",
    "temporary",
    "terminated",
    "timestamp",
    "tinyint",
    "to",
    "touch",
    "trailing",
    "transaction",
    "transactions",
    "trigger",
    "true",
    "truncate",
    "unarchive",
    "undo",
    "uniontype",
    "unlock",
    "unset",
    "unsigned",
    "update",
    "uri",
    "use",
    "user",
    "utc",
    "utctimestamp",
    "values",
    "view",
    "week",
    "weeks",
    "while",
    "with",
    "work",
    "write",
    "year",
    "years")

  val hiveStrictNonReservedKeyword = Seq(
    "anti",
    "cross",
    "database",
    "except",
    "from",
    "full",
    "having",
    "inner",
    "intersect",
    "join",
    "left",
    "natural",
    "not",
    "on",
    "right",
    "select",
    "semi",
    "table",
    "to",
    "union",
    "where",
    "with")

  // All the keywords in `docs/sql-keywords.md` are listed below:
  val allCandidateKeywords = Set(
    "add",
    "after",
    "all",
    "alter",
    "analyze",
    "and",
    "anti",
    "any",
    "archive",
    "array",
    "as",
    "asc",
    "at",
    "authorization",
    "between",
    "both",
    "bucket",
    "buckets",
    "by",
    "cache",
    "cascade",
    "case",
    "cast",
    "change",
    "check",
    "clear",
    "cluster",
    "clustered",
    "codegen",
    "collate",
    "collection",
    "column",
    "columns",
    "comment",
    "commit",
    "compact",
    "compactions",
    "compute",
    "concatenate",
    "constraint",
    "cost",
    "create",
    "cross",
    "cube",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "data",
    "database",
    "databases",
    "day",
    "days",
    "dbproperties",
    "defined",
    "delete",
    "delimited",
    "desc",
    "describe",
    "dfs",
    "directories",
    "directory",
    "distinct",
    "distribute",
    "div",
    "drop",
    "else",
    "end",
    "escaped",
    "except",
    "exchange",
    "exists",
    "explain",
    "export",
    "extended",
    "external",
    "extract",
    "false",
    "fetch",
    "fields",
    "fileformat",
    "first",
    "following",
    "for",
    "foreign",
    "format",
    "formatted",
    "from",
    "full",
    "function",
    "functions",
    "global",
    "grant",
    "group",
    "grouping",
    "having",
    "hour",
    "hours",
    "if",
    "ignore",
    "import",
    "in",
    "index",
    "indexes",
    "inner",
    "inpath",
    "inputformat",
    "insert",
    "intersect",
    "interval",
    "into",
    "is",
    "items",
    "join",
    "keys",
    "last",
    "lateral",
    "lazy",
    "leading",
    "left",
    "like",
    "limit",
    "lines",
    "list",
    "load",
    "local",
    "location",
    "lock",
    "locks",
    "logical",
    "macro",
    "map",
    "microsecond",
    "microseconds",
    "millisecond",
    "milliseconds",
    "minus",
    "minute",
    "minutes",
    "month",
    "months",
    "msck",
    "natural",
    "no",
    "not",
    "null",
    "nulls",
    "of",
    "on",
    "only",
    "option",
    "options",
    "or",
    "order",
    "out",
    "outer",
    "outputformat",
    "over",
    "overlaps",
    "overwrite",
    "partition",
    "partitioned",
    "partitions",
    "percent",
    "pivot",
    "position",
    "preceding",
    "primary",
    "principals",
    "purge",
    "query",
    "range",
    "recordreader",
    "recordwriter",
    "recover",
    "reduce",
    "references",
    "refresh",
    "rename",
    "repair",
    "replace",
    "reset",
    "restrict",
    "revoke",
    "right",
    "rlike",
    "role",
    "roles",
    "rollback",
    "rollup",
    "row",
    "rows",
    "schema",
    "second",
    "seconds",
    "select",
    "semi",
    "separated",
    "serde",
    "serdeproperties",
    "session_user",
    "set",
    "sets",
    "show",
    "skewed",
    "some",
    "sort",
    "sorted",
    "start",
    "statistics",
    "stored",
    "stratify",
    "struct",
    "table",
    "tables",
    "tablesample",
    "tblproperties",
    "temporary",
    "terminated",
    "then",
    "to",
    "touch",
    "trailing",
    "transaction",
    "transactions",
    "transform",
    "true",
    "truncate",
    "unarchive",
    "unbounded",
    "uncache",
    "union",
    "unique",
    "unlock",
    "unset",
    "use",
    "user",
    "using",
    "values",
    "view",
    "week",
    "weeks",
    "when",
    "where",
    "window",
    "with",
    "year",
    "years")

  val reservedKeywordsInAnsiMode = Set(
    "all",
    "and",
    "anti",
    "any",
    "as",
    "authorization",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "cross",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "day",
    "distinct",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "hour",
    "in",
    "inner",
    "intersect",
    "into",
    "join",
    "is",
    "leading",
    "left",
    "minute",
    "month",
    "natural",
    "not",
    "null",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "primary",
    "references",
    "right",
    "select",
    "semi",
    "session_user",
    "minus",
    "second",
    "some",
    "table",
    "then",
    "to",
    "trailing",
    "union",
    "unique",
    "user",
    "using",
    "when",
    "where",
    "with",
    "year")

  val nonReservedKeywordsInAnsiMode = allCandidateKeywords -- reservedKeywordsInAnsiMode

  test("table identifier") {
    // Regular names.
    assert(TableIdentifier("q") === parseTableIdentifier("q"))
    assert(TableIdentifier("q", Option("d")) === parseTableIdentifier("d.q"))

    // Illegal names.
    Seq("", "d.q.g", "t:", "${some.var.x}", "tab:1").foreach { identifier =>
      intercept[ParseException](parseTableIdentifier(identifier))
    }
  }

  test("quoted identifiers") {
    assert(TableIdentifier("z", Some("x.y")) === parseTableIdentifier("`x.y`.z"))
    assert(TableIdentifier("y.z", Some("x")) === parseTableIdentifier("x.`y.z`"))
    assert(TableIdentifier("z", Some("`x.y`")) === parseTableIdentifier("```x.y```.z"))
    assert(TableIdentifier("`y.z`", Some("x")) === parseTableIdentifier("x.```y.z```"))
    assert(TableIdentifier("x.y.z", None) === parseTableIdentifier("`x.y.z`"))
  }

  test("table identifier - reserved/non-reserved keywords if ANSI mode enabled") {
    withSQLConf(SQLConf.ANSI_SQL_PARSER.key -> "true") {
      reservedKeywordsInAnsiMode.foreach { keyword =>
        val errMsg = intercept[ParseException] {
          parseTableIdentifier(keyword)
        }.getMessage
        assert(errMsg.contains("no viable alternative at input"))
        assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
        assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
      }
      nonReservedKeywordsInAnsiMode.foreach { keyword =>
        assert(TableIdentifier(keyword) === parseTableIdentifier(s"$keyword"))
        assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.$keyword"))
      }
    }
  }

  test("table identifier - strict keywords") {
    // SQL Keywords.
    hiveStrictNonReservedKeyword.foreach { keyword =>
      assert(TableIdentifier(keyword) === parseTableIdentifier(keyword))
      assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
      assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
    }
  }

  test("table identifier - non reserved keywords") {
    // Hive keywords are allowed.
    hiveNonReservedKeyword.foreach { nonReserved =>
      assert(TableIdentifier(nonReserved) === parseTableIdentifier(nonReserved))
    }
  }

  test("SPARK-17364 table identifier - contains number") {
    assert(parseTableIdentifier("123_") == TableIdentifier("123_"))
    assert(parseTableIdentifier("1a.123_") == TableIdentifier("123_", Some("1a")))
    // ".123" should not be treated as token of type DECIMAL_VALUE
    assert(parseTableIdentifier("a.123A") == TableIdentifier("123A", Some("a")))
    // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
    assert(parseTableIdentifier("a.123E3_LIST") == TableIdentifier("123E3_LIST", Some("a")))
    // ".123D" should not be treated as token of type DOUBLE_LITERAL
    assert(parseTableIdentifier("a.123D_LIST") == TableIdentifier("123D_LIST", Some("a")))
    // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
    assert(parseTableIdentifier("a.123BD_LIST") == TableIdentifier("123BD_LIST", Some("a")))
  }

  test("SPARK-17832 table identifier - contains backtick") {
    val complexName = TableIdentifier("`weird`table`name", Some("`d`b`1"))
    assert(complexName === parseTableIdentifier("```d``b``1`.```weird``table``name`"))
    assert(complexName === parseTableIdentifier(complexName.quotedString))
    intercept[ParseException](parseTableIdentifier(complexName.unquotedString))
    // Table identifier contains countious backticks should be treated correctly.
    val complexName2 = TableIdentifier("x``y", Some("d``b"))
    assert(complexName2 === parseTableIdentifier(complexName2.quotedString))
  }

  test("multipart table identifier") {
    assert(parseMultipartIdentifier("testcat.v2tbl").asCatalogTableIdentifier ===
      CatalogTableIdentifier(testCat, Identifier.of(Array.empty, "v2tbl")))
    assert(parseMultipartIdentifier("db.tbl").asCatalogTableIdentifier ===
      TableIdentifier("tbl", Some("db")))
  }
}
