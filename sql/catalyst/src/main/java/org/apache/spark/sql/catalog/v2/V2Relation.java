package org.apache.spark.sql.catalog.v2;

import org.apache.spark.sql.catalyst.analysis.NamedRelation;

/**
 * A marker interface for catalyst rules to use when matching v2 relations.
 */
public interface V2Relation extends NamedRelation {
}
