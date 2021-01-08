/**
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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Keeps statistics for the memory cache.
 */
class CacheStats {

  /**
   * The approximate amount of cache space in use.
   *
   * This number is an overestimate, counting bytes that will be used only if
   * pending caching operations succeed. It does not take into account pending
   * uncaching operations.
   *
   * This overestimate is more useful to the NameNode than an underestimate,
   * since we don't want the NameNode to assign us more replicas than we can
   * cache, because of the current batch of operations.
   */
  private final UsedBytesCount usedBytesCount;

  /**
   * The total cache capacity in bytes.
   */
  private final long maxBytes;

  CacheStats(long maxBytes) {
    this.usedBytesCount = new UsedBytesCount();
    this.maxBytes = maxBytes;
  }

  /**
   * Used to count operating system page size.
   */
  @VisibleForTesting
  static class PageRounder {
    private final long osPageSize = NativeIO.POSIX.getCacheManipulator()
        .getOperatingSystemPageSize();

    /**
     * Round up a number to the operating system page size.
     */
    public long roundUp(long count) {
      return (count + osPageSize - 1) & (~(osPageSize - 1));
    }

    /**
     * Round down a number to the operating system page size.
     */
    public long roundDown(long count) {
      return count & (~(osPageSize - 1));
    }
  }

  /**
   * Counts used bytes for memory.
   */
  private class UsedBytesCount {
    private final AtomicLong usedBytes = new AtomicLong(0);

    private CacheStats.PageRounder rounder = new PageRounder();

    /**
     * Try to reserve more bytes.
     *
     * @param count
     *          The number of bytes to add. We will round this up to the page
     *          size.
     *
     * @return The new number of usedBytes if we succeeded; -1 if we failed.
     */
    long reserve(long count) {
      count = rounder.roundUp(count);
      while (true) {
        long cur = usedBytes.get();
        long next = cur + count;
        if (next > getCacheCapacity()) {
          return -1;
        }
        if (usedBytes.compareAndSet(cur, next)) {
          return next;
        }
      }
    }

    /**
     * Release some bytes that we're using.
     *
     * @param count
     *          The number of bytes to release. We will round this up to the
     *          page size.
     *
     * @return The new number of usedBytes.
     */
    long release(long count) {
      count = rounder.roundUp(count);
      return usedBytes.addAndGet(-count);
    }

    /**
     * Release some bytes that we're using rounded down to the page size.
     *
     * @param count
     *          The number of bytes to release. We will round this down to the
     *          page size.
     *
     * @return The new number of usedBytes.
     */
    long releaseRoundDown(long count) {
      count = rounder.roundDown(count);
      return usedBytes.addAndGet(-count);
    }

    long get() {
      return usedBytes.get();
    }
  }

  // Stats related methods for FSDatasetMBean

  /**
   * Get the approximate amount of cache space used.
   */
  public long getCacheUsed() {
    return usedBytesCount.get();
  }

  /**
   * Get the maximum amount of bytes we can cache. This is a constant.
   */
  public long getCacheCapacity() {
    return maxBytes;
  }

  /**
   * Try to reserve more bytes.
   *
   * @param count
   *          The number of bytes to add. We will round this up to the page
   *          size.
   *
   * @return The new number of usedBytes if we succeeded; -1 if we failed.
   */
  long reserve(long count) {
    return usedBytesCount.reserve(count);
  }

  /**
   * Release some bytes that we're using.
   *
   * @param count
   *          The number of bytes to release. We will round this up to the
   *          page size.
   *
   * @return The new number of usedBytes.
   */
  long release(long count) {
    return usedBytesCount.release(count);
  }

  /**
   * Release some bytes that we're using rounded down to the page size.
   *
   * @param count
   *          The number of bytes to release. We will round this down to the
   *          page size.
   *
   * @return The new number of usedBytes.
   */
  long releaseRoundDown(long count) {
    return usedBytesCount.releaseRoundDown(count);
  }

  /**
   * Get the OS page size.
   *
   * @return the OS page size.
   */
  long getPageSize() {
    return usedBytesCount.rounder.osPageSize;
  }

  /**
   * Round up to the OS page size.
   */
  long roundUpPageSize(long count) {
    return usedBytesCount.rounder.roundUp(count);
  }
}