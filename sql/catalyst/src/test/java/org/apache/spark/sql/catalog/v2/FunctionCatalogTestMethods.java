package org.apache.spark.sql.catalog.v2;

public class FunctionCatalogTestMethods {
  private FunctionCatalogTestMethods() {
  }

  public static int plus(int a, int b) {
    return a + b;
  }

  public static long plus(long a, long b) {
    return a + b;
  }

  public static String plus(String a, String b) {
    return a + b;
  }
}
