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

import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier, LookupCatalog}
import org.apache.spark.sql.catalyst.{CatalogTableIdentifier, MultipartIdentifier, TableIdentifierLike}
import org.apache.spark.sql.catalyst.analysis.{CastSupport, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

case class IdentifierLookup(
    conf: SQLConf,
    findCatalog: String => CatalogPlugin)
  extends Rule[LogicalPlan] with CastSupport with LookupCatalog {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  override def lookupCatalog: Option[String => CatalogPlugin] = Some(findCatalog)

  def defaultCatalog: Option[CatalogPlugin] = conf.defaultV2Catalog.map(findCatalog)

  def tableExistsInDefaultCatalog(ident: Identifier): Boolean =
    defaultCatalog.exists(_.asTableCatalog.tableExists(ident))

  private def resolveParts(parts: Seq[String]): TableIdentifierLike = parts match {
    // V2 table with catalog
    case CatalogObjectIdentifier(Some(catalog), ident) =>
      CatalogTableIdentifier(catalog, ident)

    // V2 table in default catalog
    case CatalogObjectIdentifier(None, ident) if tableExistsInDefaultCatalog(ident) =>
      CatalogTableIdentifier(defaultCatalog.get, ident)

    // V1 table
    case AsTableIdentifier(ident) =>
      ident
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedRelation(MultipartIdentifier(parts)) =>
      UnresolvedRelation(resolveParts(parts))
  }
}
