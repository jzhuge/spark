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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}

private[sql] object CatalogV2Implicits {
  implicit class CatalogHelper(catalog: CatalogPlugin) {
    def asTableCatalog: TableCatalog = catalog match {
      case tableCatalog: TableCatalog =>
        tableCatalog
      case _ =>
        throw new UnsupportedOperationException(s"Catalog $catalog does not support tables")
    }

    def asFunctionCatalog: FunctionCatalog = catalog match {
      case functionCatalog: FunctionCatalog =>
        functionCatalog
      case _ =>
        throw new UnsupportedOperationException(s"Catalog $catalog does not support functions")
    }
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quote).mkString(".")
  }

  implicit class IdentifierHelper(ident: Identifier) {
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quote).mkString(".") + "." + quote(ident.name)
      } else {
        quote(ident.name)
      }
    }
  }

  implicit class MultipartIdentifierHelper(namespace: Seq[String]) {
    def quoted: String = namespace.map(quote).mkString(".")
  }

  implicit class TableIdentifierHelper(ident: TableIdentifier) {
    def asMultipart: Seq[String] = {
      ident.database match {
        case Some(db) =>
          Seq(db, ident.table)
        case _ =>
          Seq(ident.table)
      }
    }
  }

  implicit class FunctionIdentifierHelper(ident: FunctionIdentifier) {
    def asMultipart: Seq[String] = {
      ident.database match {
        case Some(db) =>
          Seq(db, ident.funcName)
        case _ =>
          Seq(ident.funcName)
      }
    }
  }

  private def quote(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }
}
