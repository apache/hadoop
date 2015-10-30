// automatically generated, do not modify
package org.apache.hadoop.hdfs.server.flatbuffer;
public final class IntelTypee {
  private IntelTypee() { }
  public static final int FILE = 1;
  public static final int DIRECTORY = 2;
  public static final int SYMLINK = 3;

  private static final String[] names = { "FILE", "DIRECTORY", "SYMLINK", };

  public static String name(int e) { return names[e - FILE]; }
};

