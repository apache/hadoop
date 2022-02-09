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

import java.util.List;
import java.util.function.IntFunction;

/**
 * Context related to vectored IO operation.
 * See {@link S3AInputStream#readVectored(List, IntFunction)}.
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
