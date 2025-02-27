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

package org.apache.hadoop.fs.s3a.prefetch;

import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Options for the prefetch stream which are built up in {@link PrefetchingInputStreamFactory}
 * and passed down.
 */
public class PrefetchOptions {

  /** Size in bytes of a single prefetch block. */
  private final int prefetchBlockSize;

  /** Size of prefetch queue (in number of blocks). */
  private final int prefetchBlockCount;

  /**
   * Constructor.
   * @param prefetchBlockSize the size (in number of bytes) of each prefetched block.
   * @param prefetchBlockCount maximum number of prefetched blocks.
   */
  public PrefetchOptions(final int prefetchBlockSize, final int prefetchBlockCount) {

    checkArgument(
        prefetchBlockSize > 0, "invalid prefetchBlockSize %d", prefetchBlockSize);
    this.prefetchBlockSize = prefetchBlockSize;
    checkArgument(
        prefetchBlockCount > 0, "invalid prefetchBlockCount %d", prefetchBlockCount);
    this.prefetchBlockCount = prefetchBlockCount;
  }

  /**
   * Gets the size in bytes of a single prefetch block.
   *
   * @return the size in bytes of a single prefetch block.
   */
  public int getPrefetchBlockSize() {
    return prefetchBlockSize;
  }

  /**
   * Gets the size of prefetch queue (in number of blocks).
   *
   * @return the size of prefetch queue (in number of blocks).
   */
  public int getPrefetchBlockCount() {
    return prefetchBlockCount;
  }
}
