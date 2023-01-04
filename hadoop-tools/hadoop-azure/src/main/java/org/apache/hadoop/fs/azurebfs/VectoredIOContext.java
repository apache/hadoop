package org.apache.hadoop.fs.azurebfs;

import java.util.List;
import java.util.function.IntFunction;

/**
 * Context related to vectored IO operation.
 */
public class VectoredIOContext {

  /**
   * What is the smallest reasonable seek that we should group
   * ranges together during vectored read operation.
   */
  private int minSeekForVectorReads;

  /**
   * What is the largest size that we should group ranges
   * together during vectored read operation.
   * Setting this value 0 will disable merging of ranges.
   */
  private int maxReadSizeForVectorReads;

  /**
   * Default no arg constructor.
   */
  public VectoredIOContext() {
  }

  public VectoredIOContext setMinSeekForVectoredReads(int minSeek) {
    this.minSeekForVectorReads = minSeek;
    return this;
  }

  public VectoredIOContext setMaxReadSizeForVectoredReads(int maxSize) {
    this.maxReadSizeForVectorReads = maxSize;
    return this;
  }

  public VectoredIOContext build() {
    return this;
  }

  public int getMinSeekForVectorReads() {
    return minSeekForVectorReads;
  }

  public int getMaxReadSizeForVectorReads() {
    return maxReadSizeForVectorReads;
  }

  @Override
  public String toString() {
    return "VectoredIOContext{" +
        "minSeekForVectorReads=" + minSeekForVectorReads +
        ", maxReadSizeForVectorReads=" + maxReadSizeForVectorReads +
        '}';
  }
}
