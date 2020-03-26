/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE;

/**
 * Pool for byte[]
 */
public class AbfsByteBufferPool {

  /**
   * Queue holding the free buffers.
   */
  private ArrayBlockingQueue<byte[]> freeBuffers;
  /**
   * Count to track the buffers issued and yet to be returned.
   */
  private int numBuffersInUse;
  /**
   * Maximum number of buffers that can be in use.
   */
  private int maxBuffersInUse;
  private int bufferSize;

  /**
   * @param bufferSize                 Size of the byte[] to be returned.
   * @param maxFreeBuffers             Maximum number of buffers that cab
   *                                   reside in the pool.
   * @param maxWriteMemUsagePercentage Maximum percentage of memory that can
   *                                   be used by the pool from the max
   *                                   available memory.
   */
  public AbfsByteBufferPool(final int bufferSize, final int maxFreeBuffers,
      final int maxWriteMemUsagePercentage) {
    Preconditions.checkArgument(maxWriteMemUsagePercentage
            >= MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE
            && maxWriteMemUsagePercentage
            <= MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
        "maxWriteMemUsagePercentage should be in range (%s - %s)",
        MIN_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE,
        MAX_VALUE_MAX_AZURE_WRITE_MEM_USAGE_PERCENTAGE);
    Preconditions
        .checkArgument(maxFreeBuffers > 0, "maxFreeBuffers cannot be < 1");
    this.bufferSize = bufferSize;
    this.numBuffersInUse = 0;
    freeBuffers = new ArrayBlockingQueue<>(maxFreeBuffers);

    double maxMemoryAllowedForPool =
        Runtime.getRuntime().maxMemory() * maxWriteMemUsagePercentage / 100;
    double bufferCountByMemory = maxMemoryAllowedForPool / bufferSize;
    double bufferCountByMaxFreeBuffers =
        maxFreeBuffers + Runtime.getRuntime().availableProcessors();

    maxBuffersInUse = (int) Math
        .ceil(Math.min(bufferCountByMemory, bufferCountByMaxFreeBuffers));
    if (maxBuffersInUse < 2) {
      maxBuffersInUse = 2;
    }
  }

  /**
   * @return byte[] from the pool if available otherwise new byte[] is returned.
   * Waits if pool is empty and already maximum number of buffers are in use.
   */
  public byte[] get() {
    byte[] byteArray = null;
    synchronized (this) {
      byteArray = freeBuffers.poll();
      if (byteArray == null && numBuffersInUse < maxBuffersInUse) {
        byteArray = new byte[bufferSize];
      }
      if (byteArray != null) {
        numBuffersInUse++;
        return byteArray;
      }
    }
    try {
      byteArray = freeBuffers.take();
      synchronized (this) {
        numBuffersInUse++;
        return byteArray;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * @param byteArray The buffer to be offered back to the pool.
   */
  public synchronized void release(byte[] byteArray) {
    Preconditions.checkArgument(byteArray.length == bufferSize,
        "Buffer size has" + " to be %s (Received buffer length: %s)",
        bufferSize, byteArray.length);
    numBuffersInUse--;
    if (numBuffersInUse < 0) {
      numBuffersInUse = 0;
    }
    freeBuffers.offer(byteArray);
  }

  @VisibleForTesting
  public synchronized int getMaxBuffersInUse() {
    return this.maxBuffersInUse;
  }

  @VisibleForTesting
  public synchronized int getBuffersInUse() {
    return this.numBuffersInUse;
  }

  @VisibleForTesting
  public synchronized ArrayBlockingQueue<byte[]> getFreeBuffers() {
    return freeBuffers;
  }
}
