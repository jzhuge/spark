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

import org.apache.spark.sql.catalog.v2.{CatalogPlugin, LookupCatalog}
import org.apache.spark.sql.catalyst.{CatalogTableIdentifier, TableIdentifierLike}
import org.apache.spark.sql.internal.SQLConf

sealed trait TableIdentifierImplicits {

  protected def lookup(parts: Seq[String]): TableIdentifierLike

  implicit class TableIdentifierLikeHelper(parts: Seq[String]) {
    val asCatalogTableIdentifier: TableIdentifierLike = lookup(parts)
  }
}

object V1TableIdentifierImplicits
  extends TableIdentifierImplicits with LookupCatalog {

  override protected def lookup(parts: Seq[String]): TableIdentifierLike = parts match {
    case AsTableIdentifier(ident) =>
      ident
    case _ =>
      throw new UnsupportedOperationException(
        s"${parts.mkString(".")} is not a V1 table identifier")
  }
}

case class V2TableIdentifierImplicits(
    conf: SQLConf,
    findCatalog: String => CatalogPlugin)
  extends TableIdentifierImplicits with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override def lookupCatalog: Option[String => CatalogPlugin] = Some(findCatalog)

  def defaultCatalog: Option[CatalogPlugin] = conf.defaultV2Catalog.map(findCatalog)

  override protected def lookup(parts: Seq[String]): TableIdentifierLike = parts match {
    // V2 table with catalog
    case CatalogObjectIdentifier(Some(catalog), ident) =>
      CatalogTableIdentifier(catalog, ident)

    // V2 table in default catalog
    case CatalogObjectIdentifier(None, ident)
      if defaultCatalog.exists(_.asTableCatalog.tableExists(ident)) =>
      CatalogTableIdentifier(defaultCatalog.get, ident)

    // V1 table
    case AsTableIdentifier(ident) =>
      ident
  }
}
