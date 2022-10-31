package org.apache.hadoop.fs.azurebfs.services;

public class AbfsClientThrottlingInterceptTestUtil {

  public static AbfsClientThrottlingIntercept get() {
    return AbfsClientThrottlingIntercept.getSingleton();
  }

  public static void setReadAnalyzer(final AbfsClientThrottlingIntercept intercept,
      final AbfsClientThrottlingAnalyzer abfsClientThrottlingAnalyzer) {
    intercept.setReadThrottler(abfsClientThrottlingAnalyzer);
  }
}
