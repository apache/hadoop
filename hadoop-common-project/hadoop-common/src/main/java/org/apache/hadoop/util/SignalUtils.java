package org.apache.hadoop.util;

public class SignalUtils {

  public static boolean nativeCodeLoaded = false;

  static {
    try {
      System.loadLibrary("hadoop");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
      // Ignore failure to load
    }
  }

  public static native boolean isSigIgnored(int sig);
}
