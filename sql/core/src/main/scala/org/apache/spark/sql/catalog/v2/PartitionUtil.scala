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

import org.apache.spark.sql.catalog.v2.PartitionTransforms.{Bucket, Identity}
import org.apache.spark.sql.catalyst.catalog.BucketSpec

object PartitionUtil {
  def convertToTransforms(
      identityCols: Seq[String], bucketSpec: Option[BucketSpec]): Seq[PartitionTransform] = {
    val bucketTransform = bucketSpec.map { spec =>
      if (spec.sortColumnNames.nonEmpty) {
        throw new UnsupportedOperationException(
          s"Cannot convert bucket spec with sort columns: $bucketSpec")
      }

      PartitionTransforms.bucket(spec.numBuckets, spec.bucketColumnNames: _*)
    }

    identityCols.map(PartitionTransforms.identity) ++ bucketTransform.toSeq
  }

  def convertTransforms(
      partitions: Seq[PartitionTransform]): (Seq[String], Option[BucketSpec]) = {
    val (identityTransforms, bucketTransforms) = partitions.partition(_.isInstanceOf[Identity])

    val nonBucketTransforms = bucketTransforms.filterNot(_.isInstanceOf[Bucket])
    if (nonBucketTransforms.nonEmpty) {
      throw new UnsupportedOperationException("SessionCatalog does not support partition " +
          s"transforms: ${nonBucketTransforms.mkString(", ")}")
    }

    val bucketSpec = bucketTransforms.size match {
      case 0 =>
        None
      case 1 =>
        val bucket = bucketTransforms.head.asInstanceOf[Bucket]
        Some(BucketSpec(bucket.numBuckets, bucket.references, Nil))
      case _ =>
        throw new UnsupportedOperationException("SessionCatalog does not support multiple " +
            s"clusterings: ${bucketTransforms.mkString(", ")}")
    }

    val identityCols = identityTransforms.map(_.references.head)

    (identityCols, bucketSpec)
  }
}
