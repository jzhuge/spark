package org.apache.spark.sql;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A filter for batchid folders used to avoid FileInputFormat complaining about them.
 * This is in Java because it must have a no-arg constructor.
 */
public class BatchIdPathFilter implements PathFilter {
  public BatchIdPathFilter() {
  }

  @Override
  public boolean accept(Path path) {
    return !path.getName().matches("batch_?id=\\d+");
  }
}
