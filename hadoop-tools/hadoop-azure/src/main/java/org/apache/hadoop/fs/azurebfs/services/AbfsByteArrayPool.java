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

import org.checkerframework.checker.units.qual.min;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Pool for byte[]
 */
public class AbfsByteArrayPool {

  private Queue<byte[]> queue = new LinkedList<>();
  private int buffersToBeReturned;

  private int bufferSize;
  private int maxBuffersInMemory;
  private int minBuffersRequiredInPool;

  public AbfsByteArrayPool(final int bufferSize,
      final int maxConcurrentThreadCount,
      final int maxWriteMemUsagePercentage) {
    this.bufferSize = bufferSize;
    this.buffersToBeReturned = 0;
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    this.minBuffersRequiredInPool = maxConcurrentThreadCount + 1;

    double maxAvailableMemory = Runtime.getRuntime().maxMemory();
    double maxMemoryAllowedForPool =
        maxAvailableMemory * maxWriteMemUsagePercentage / 100;
    double bufferCountByMemory = maxMemoryAllowedForPool / bufferSize;
    double bufferCountByConcurrency =
        maxConcurrentThreadCount + availableProcessors + 1;
    maxBuffersInMemory = (int) Math
        .ceil(Math.min(bufferCountByMemory, bufferCountByConcurrency));
  }

  /**
   * @return byte[] from the pool if available otherwise new byte[] is returned.
   */
  public synchronized byte[] get() {
    while (this.size() < 1 && buffersToBeReturned >= maxBuffersInMemory) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    buffersToBeReturned++;
    if (queue.isEmpty()) {
      return new byte[bufferSize];
    }
    return queue.remove();
  }

  /**
   * @param byteArray The byte[] to be returned back to the pool. byteArray
   *                  is returned to the pool only if the pool contains less
   *                  than the minimum required byte[] objects.
   * @return true if success
   */
  public void release(byte[] byteArray) {
    if (byteArray != null && byteArray.length != bufferSize
        || this.size() >= minBuffersRequiredInPool) {
      return;
    }
    queue.add(byteArray);
    synchronized (this) {
      buffersToBeReturned--;
      notifyAll();
    }
  }

  private int size() {
    return queue.size();
  }

  public void resetBufferSize(int bufferSize) {
    if (this.bufferSize == bufferSize) {
      return;
    }
    queue = new LinkedList<>();
    this.bufferSize = bufferSize;
  }
}
