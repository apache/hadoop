package org.apache.hadoop.fs.azurebfs.utils;

public enum TracingContextFormat {
  SINGLE_ID_FORMAT,
  ALL_ID_FORMAT,
  TWO_ID_FORMAT;

  //save list as calling values() is expensive
  private static final TracingContextFormat[] formatValues =
      TracingContextFormat.values();
  private static final int FORMAT_COUNT = formatValues.length;

  public static TracingContextFormat valueOf(int number) {
    return number < FORMAT_COUNT? formatValues[number] :
        TracingContextFormat.ALL_ID_FORMAT;
  }
}
