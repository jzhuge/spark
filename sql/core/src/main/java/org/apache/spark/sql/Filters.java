package org.apache.spark.sql;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

public class Filters {

  public static MultiPathFilter from(PathFilter filter) {
    return new MultiPathFilter().and(filter);
  }

  public static class MultiPathFilter implements PathFilter {
    private final List<PathFilter> filters;

    private MultiPathFilter() {
      this.filters = new ArrayList<>();
    }

    public MultiPathFilter and(PathFilter filter) {
      if (filter != null) {
        filters.add(filter);
      }
      return this;
    }

    public MultiPathFilter and(Option<PathFilter> filterOption) {
      if (filterOption.isDefined()) {
        filters.add(filterOption.get());
      }
      return this;
    }

    @Override
    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }
}
