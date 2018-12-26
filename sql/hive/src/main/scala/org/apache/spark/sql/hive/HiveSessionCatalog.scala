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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.{UDAF, UDF}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry => HiveFunctionRegistry}
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDF, GenericUDTF}

import org.apache.spark.sql.{AnalysisException, SparkSession, UDFLibrary}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchTableException}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionInfo, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.util.Utils


private[sql] class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    sparkSession: SparkSession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    conf: SQLConf,
    hadoopConf: Configuration)
  extends SessionCatalog(
    externalCatalog,
    globalTempViewManager,
    functionResourceLoader,
    functionRegistry,
    conf,
    hadoopConf) {

  private val nfRules = new NetflixAnalysis(sparkSession)

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    synchronized {
      val table = formatTableName(name.table)
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      if (db == globalTempViewManager.database) {
        val relationAlias = alias.getOrElse(table)
        globalTempViewManager.get(table).map { viewDef =>
          SubqueryAlias(relationAlias, viewDef, Some(name))
        }.getOrElse(throw new NoSuchTableException(db, table))
      } else if (name.database.isDefined || !tempTables.contains(table)) {
        val database = name.database.map(formatDatabaseName)
        val newName = name.copy(database = database, table = table)
        nfRules.apply(metastoreCatalog.lookupRelation(newName, alias))
      } else {
        val relation = tempTables(table)
        val tableWithQualifiers = SubqueryAlias(table, relation, None)
        // If an alias was specified by the lookup, wrap the plan in a subquery so that
        // attributes are properly qualified with this alias.
        alias.map(a => SubqueryAlias(a, tableWithQualifiers, None)).getOrElse(tableWithQualifiers)
      }
    }
  }

  // ----------------------------------------------------------------
  // | Methods and fields for interacting with HiveMetastoreCatalog |
  // ----------------------------------------------------------------

  // Catalog for handling data source tables. TODO: This really doesn't belong here since it is
  // essentially a cache for metastore tables. However, it relies on a lot of session-specific
  // things so it would be a lot of work to split its functionality between HiveSessionCatalog
  // and HiveCatalog. We should still do it at some point...
  private val metastoreCatalog = new HiveMetastoreCatalog(sparkSession)

  val ParquetConversions: Rule[LogicalPlan] = metastoreCatalog.ParquetConversions
  val OrcConversions: Rule[LogicalPlan] = metastoreCatalog.OrcConversions

  override def refreshTable(name: TableIdentifier): Unit = {
    super.refreshTable(name)
    metastoreCatalog.refreshTable(name)
  }

  def invalidateCache(): Unit = {
    metastoreCatalog.cachedDataSourceTables.invalidateAll()
  }

  def hiveDefaultTableFilePath(name: TableIdentifier): String = {
    metastoreCatalog.hiveDefaultTableFilePath(name)
  }

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = metastoreCatalog.getQualifiedTableName(table)
    metastoreCatalog.cachedDataSourceTables.getIfPresent(key)
  }

  override def makeFunctionBuilder(funcName: String, className: String): FunctionBuilder = {
    makeFunctionBuilder(funcName, Utils.classForName(className))
  }

  /**
   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
   */
  private def makeFunctionBuilder(name: String, clazz: Class[_]): FunctionBuilder = {
    // When we instantiate hive UDF wrapper class, we may throw exception if the input
    // expressions don't satisfy the hive UDF, such as type mismatch, input number
    // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
    (children: Seq[Expression]) => {
      try {
        if (classOf[UDFLibrary].isAssignableFrom(clazz)) {
          val plugin = clazz.newInstance.asInstanceOf[UDFLibrary]
          val udf = plugin.loadUDF(name, children.map(_.dataType).asJava)
          if (udf == null) {
            throw new AnalysisException(
              s"Cannot load UDF using ${clazz.getCanonicalName}: not found")
          }
          ScalaUDF(udf.f, udf.dataType, children, udf.inputTypes.getOrElse(Nil))
        } else if (classOf[UDF].isAssignableFrom(clazz)) {
          val udf = HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
          val udf = HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), children)
          udf.dataType // Force it to check input data types.
          udf
        } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), children)
          udaf.dataType // Force it to check input data types.
          udaf
        } else if (classOf[UDAF].isAssignableFrom(clazz)) {
          val udaf = HiveUDAFFunction(
            name,
            new HiveFunctionWrapper(clazz.getName),
            children,
            isUDAFBridgeRequired = true)
          udaf.dataType  // Force it to check input data types.
          udaf
        } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
          val udtf = HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), children)
          udtf.elementSchema // Force it to check input data types.
          udtf
        } else {
          // try to load the function with reflection
          val dataTypes = children.map(_.dataType)
          val javaTypes = dataTypes.map(dt => HiveSessionCatalog.JAVA_TYPES.getOrElse(dt,
            throw new AnalysisException(
              s"Cannot load UDF ${clazz.getCanonicalName}.$name: $dt is not supported")))

          try {
            val method = clazz.getDeclaredMethod(name, javaTypes: _*)

            if (!method.isAccessible) {
              method.setAccessible(true)
            }

//            if (!Modifier.isStatic(method.getModifiers)) {
//              throw new AnalysisException(
//                s"Cannot load non-static function as UDF: '$name' in ${clazz.getCanonicalName}")
//            }

            val javaReturn = method.getReturnType
            val returnType = HiveSessionCatalog.DATA_TYPES.getOrElse(javaReturn,
              throw new AnalysisException(s"Cannot load UDF ${clazz.getCanonicalName}.$name: " +
                  s"return type $javaReturn is not supported"))

            val f = javaTypes.size match {
              case 0 => () => method.invoke(null)
              case 1 => (a: AnyRef) => method.invoke(null, a)
              case 2 => (a: AnyRef, v: AnyRef) => method.invoke(null, a, v)
              case 3 => (a: AnyRef, b: AnyRef, c: AnyRef) => method.invoke(null, a, b, c)
              case 4 => (a: AnyRef, b: AnyRef, c: AnyRef, d: AnyRef) =>
                method.invoke(null, a, b, c, d)
              case _ =>
                throw new AnalysisException(s"Cannot load UDF ${clazz.getCanonicalName}.$name: " +
                    s"too many arguments (${javaTypes.size}, max 4)")
            }

            ScalaUDF(f, returnType, children, dataTypes)

          } catch {
            case e: SecurityException =>
              throw new AnalysisException(
                s"Cannot load UDF ${clazz.getCanonicalName}.$name", cause = Some(e))
            case _: NoSuchMethodException =>
              throw new AnalysisException(s"Cannot load UDF ${clazz.getCanonicalName}.$name: " +
                  s"cannot find $name(${javaTypes.mkString(", ")})")
          }
        }
      } catch {
        case ae: AnalysisException =>
          throw ae
        case NonFatal(e) =>
          val analysisException =
            new AnalysisException(s"No handler for Hive UDF '${clazz.getCanonicalName}': $e")
          analysisException.setStackTrace(e.getStackTrace)
          throw analysisException
      }
    }
  }

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    try {
      lookupFunction0(name, children)
    } catch {
      case NonFatal(_) =>
        // SPARK-16228 ExternalCatalog may recognize `double`-type only.
        val newChildren = children.map { child =>
          if (child.dataType.isInstanceOf[DecimalType]) Cast(child, DoubleType) else child
        }
        lookupFunction0(name, newChildren)
    }
  }

  private def lookupFunction0(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    // TODO: Once lookupFunction accepts a FunctionIdentifier, we should refactor this method to
    // if (super.functionExists(name)) {
    //   super.lookupFunction(name, children)
    // } else {
    //   // This function is a Hive builtin function.
    //   ...
    // }
    val database = name.database.map(formatDatabaseName)
    val funcName = name.copy(database = database)
    Try(super.lookupFunction(funcName, children)) match {
      case Success(expr) => expr
      case Failure(error) =>
        if (functionRegistry.functionExists(funcName.unquotedString)) {
          // If the function actually exists in functionRegistry, it means that there is an
          // error when we create the Expression using the given children.
          // We need to throw the original exception.
          throw error
        } else {
          // This function is not in functionRegistry, let's try to load it as a Hive's
          // built-in function.
          // Hive is case insensitive.
          val functionName = funcName.unquotedString.toLowerCase
          if (!hiveFunctions.contains(functionName)) {
            failFunctionLookup(funcName.unquotedString)
          }

          // TODO: Remove this fallback path once we implement the list of fallback functions
          // defined below in hiveFunctions.
          val functionInfo = {
            try {
              Option(HiveFunctionRegistry.getFunctionInfo(functionName)).getOrElse(
                failFunctionLookup(funcName.unquotedString))
            } catch {
              // If HiveFunctionRegistry.getFunctionInfo throws an exception,
              // we are failing to load a Hive builtin function, which means that
              // the given function is not a Hive builtin function.
              case NonFatal(e) => failFunctionLookup(funcName.unquotedString)
            }
          }
          val className = functionInfo.getFunctionClass.getName
          val builder = makeFunctionBuilder(functionName, className)
          // Put this Hive built-in function to our function registry.
          val info = new ExpressionInfo(className, functionName)
          createTempFunction(functionName, info, builder, ignoreIfExists = false)
          // Now, we need to create the Expression.
          functionRegistry.lookupFunction(functionName, children)
        }
    }
  }

  /** List of functions we pass over to Hive. Note that over time this list should go to 0. */
  // We have a list of Hive built-in functions that we do not support. So, we will check
  // Hive's function registry and lazily load needed functions into our own function registry.
  // List of functions we are explicitly not supporting are:
  // compute_stats, context_ngrams, create_union,
  // current_user, ewah_bitmap, ewah_bitmap_and, ewah_bitmap_empty, ewah_bitmap_or, field,
  // in_file, index, matchpath, ngrams, noop, noopstreaming, noopwithmap,
  // noopwithmapstreaming, parse_url_tuple, reflect2, windowingtablefunction.
  // Note: don't forget to update SessionCatalog.isTemporaryFunction
  private val hiveFunctions = Seq(
    "histogram_numeric"
  )
}

object HiveSessionCatalog {
  private val JAVA_TYPES: Map[DataType, Class[_]] = Map(
    BooleanType -> java.lang.Boolean.TYPE,
    ByteType -> java.lang.Byte.TYPE,
    ShortType -> java.lang.Short.TYPE,
    IntegerType -> java.lang.Integer.TYPE,
    DateType -> java.lang.Integer.TYPE,
    LongType -> java.lang.Long.TYPE,
    TimestampType -> java.lang.Long.TYPE,
    FloatType -> java.lang.Float.TYPE,
    DoubleType -> java.lang.Double.TYPE,
    StringType -> classOf[java.lang.String],
    BinaryType -> classOf[Array[Byte]])

  private val DATA_TYPES: Map[Class[_], DataType] = Map(
    java.lang.Boolean.TYPE -> BooleanType,
    classOf[java.lang.Boolean] -> BooleanType.asNullable,
    java.lang.Byte.TYPE -> ByteType,
    classOf[java.lang.Byte] -> ByteType.asNullable,
    java.lang.Short.TYPE -> ShortType,
    classOf[java.lang.Short] -> ShortType.asNullable,
    java.lang.Integer.TYPE -> IntegerType,
    classOf[java.lang.Integer] -> IntegerType.asNullable,
    java.lang.Long.TYPE -> LongType,
    classOf[java.lang.Long] -> LongType.asNullable,
    java.lang.Float.TYPE -> FloatType,
    classOf[java.lang.Float] -> FloatType.asNullable,
    java.lang.Double.TYPE -> DoubleType,
    classOf[java.lang.Double] -> DoubleType.asNullable,
    classOf[java.lang.String] -> StringType.asNullable,
    classOf[Array[Byte]] -> BinaryType)

}
