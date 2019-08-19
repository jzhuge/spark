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

package org.apache.spark.sql.catalyst.expressions

import java.nio.ByteBuffer

import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.transforms.{Transform, Transforms}
import org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, DataType, Decimal, DecimalType, IntegerType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

abstract class IcebergTransformExpression
    extends Expression with CodegenFallback with NullIntolerant {

  def child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  @transient lazy val icebergInputType: Type = SparkSchemaUtil.convert(child.dataType)
}

abstract class IcebergTimeTransform
    extends IcebergTransformExpression with ImplicitCastInputTypes {

  def child: Expression
  def transform: Transform[Any, Integer]

  override def eval(input: InternalRow): Any = transform(child.eval(input)).toInt

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)
}

case class IcebergYearTransform(child: Expression)
    extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.year[Any](icebergInputType)
}

case class IcebergMonthTransform(child: Expression)
    extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.month[Any](icebergInputType)
}

case class IcebergDayTransform(child: Expression)
    extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.day[Any](icebergInputType)
}

case class IcebergHourTransform(child: Expression)
    extends IcebergTimeTransform {

  @transient lazy val transform: Transform[Any, Integer] = Transforms.hour[Any](icebergInputType)
}

case class IcebergBucketTransform(
    numBuckets: Int,
    child: Expression) extends IcebergTransformExpression {

  override def children: Seq[Expression] = child :: Nil

  @transient lazy val bucketFunc: Any => Int = child.dataType match {
    case _: DecimalType =>
      val t = Transforms.bucket[Any](icebergInputType, numBuckets)
      d: Decimal => t(d.toBigDecimal)
    case _: StringType =>
      // the spec requires that the hash of a string is equal to the hash of its UTF-8 encoded bytes
      val t = Transforms.bucket[ByteBuffer](Types.BinaryType.get(), numBuckets)
      s: UTF8String => t(ByteBuffer.wrap(s.getBytes)) // TODO: pass bytes without a copy
    case _ =>
      val t = Transforms.bucket[Any](icebergInputType, numBuckets)
      a: Any => t(a)
  }

  override def eval(input: InternalRow): Any = bucketFunc(child.eval(input))

  override def dataType: DataType = IntegerType
}
