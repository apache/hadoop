/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.util.Preconditions.checkState;

/**
 * Configuration information for vectored IO.
 */
public final class VectoredIOContext {

  /**
   * What is the smallest reasonable seek that we should group
   * ranges together during vectored read operation.
   */
  private int minSeekForVectorReads;

  /**
   * What is the largest size that we should group ranges
   * together during vectored read operation.
   * Setting this value to 0 will disable merging of ranges.
   */
  private int maxReadSizeForVectorReads;

  /**
   * Maximum number of active range read operation a single
   * input stream can have.
   */
  private int vectoredActiveRangeReads;

  /**
   * Can this instance be updated?
   */
  private boolean immutable = false;

  /**
   * Default no arg constructor.
   */
  public VectoredIOContext() {
  }

  /**
   * Make immutable.
   * @return this instance.
   */
  public VectoredIOContext build() {
    immutable = true;
    return this;
  }

  /**
   * Verify this object is still mutable.
   * @throws IllegalStateException if not.
   */
  private void checkMutable() {
    checkState(!immutable, "Instance is immutable");
  }

  /**
   * What is the threshold at which a seek() to a new location
   * is initiated, rather than merging ranges?
   * Set to zero to disable range merging entirely.
   * @param minSeek minimum amount of data to skip.
   * @return this instance.
   */
  public VectoredIOContext setMinSeekForVectoredReads(int minSeek) {
    checkMutable();
    checkState(minSeek >= 0);
    this.minSeekForVectorReads = minSeek;
    return this;
  }

  /**
   * What is the threshold at which a seek() to a new location
   * is initiated, rather than merging ranges?
   * @return a number greater than or equal to zero.
   */
  public int getMinSeekForVectorReads() {
    return minSeekForVectorReads;
  }

  /**
   * What is the largest size that we should group ranges
   * together during vectored read operation?
   * @param maxSize maximum size
   * @return this instance.
   */
  public VectoredIOContext setMaxReadSizeForVectoredReads(int maxSize) {
    checkMutable();
    checkState(maxSize >= 0);
    this.maxReadSizeForVectorReads = maxSize;
    return this;
  }

  /**
   * The largest size that we should group ranges.
   * together during vectored read operation
   * @return a number greater than or equal to zero.
   */
  public int getMaxReadSizeForVectorReads() {
    return maxReadSizeForVectorReads;
  }

  /**
   * Maximum number of active range read operation a single
   * input stream can have.
   * @return number of extra threads for reading, or zero.
   */
  public int getVectoredActiveRangeReads() {
    return vectoredActiveRangeReads;
  }

  /**
   * Maximum number of active range read operation a single
   * input stream can have.
   * @param activeReads number of extra threads for reading, or zero.
   * @return this instance.
   * number of extra threads for reading, or zero.
   */
  public VectoredIOContext setVectoredActiveRangeReads(
      final int activeReads) {
    checkMutable();
    checkState(activeReads >= 0);
    this.vectoredActiveRangeReads = activeReads;
    return this;
  }

  @Override
  public String toString() {
    return "VectoredIOContext{" +
        "minSeekForVectorReads=" + minSeekForVectorReads +
        ", maxReadSizeForVectorReads=" + maxReadSizeForVectorReads +
        ", vectoredActiveRangeReads=" + vectoredActiveRangeReads +
        '}';
  }
}
