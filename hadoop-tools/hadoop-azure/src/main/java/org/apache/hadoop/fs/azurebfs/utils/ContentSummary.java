package org.apache.hadoop.fs.azurebfs.utils;

public class ContentSummary{
  public final long length;
  public final long directoryCount;
  public final long fileCount;
  public final long spaceConsumed;

  public ContentSummary(long length, long directoryCount, long fileCount, long spaceConsumed) {
    this.length = length;
    this.directoryCount = directoryCount;
    this.fileCount = fileCount;
    this.spaceConsumed = spaceConsumed;
  }
}
