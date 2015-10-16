// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
public final class IntelStorageTypeProto {
  private IntelStorageTypeProto() { }
  public static final int DISK = 1;
  public static final int SSD = 2;
  public static final int ARCHIVE = 3;
  public static final int RAM_DISK = 4;

  private static final String[] names = { "DISK", "SSD", "ARCHIVE", "RAM_DISK", };

  public static String name(int e) { return names[e - DISK]; }
};

