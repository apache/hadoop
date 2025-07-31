package org.apache.hadoop.fs.azurebfs.utils;

public enum TracingHeaderVersion {

  V0("", 8),
  V1("v1", 13);

  private final String version;
  private final int fieldCount;

  TracingHeaderVersion(String version, int fieldCount) {
    this.version = version;
    this.fieldCount = fieldCount;
  }

  @Override
  public String toString() {
    return version;
  }

  public static TracingHeaderVersion getCurrentVersion() {
    return V1;
  }

  public int getFieldCount() {
    return V1.fieldCount;
  }

  public String getVersion() {
    return V1.version;
  }
}