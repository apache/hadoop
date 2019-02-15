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

package org.apache.hadoop.fs.s3a.multipart;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A blocking queue that only returns data whose offsets are in order.
 */
public final class OrderingQueue {

  private final PriorityQueue<Pair<Long, byte[]>> pendingWrites =
          new PriorityQueue<>();

  private final long totalSize;
  private final long bufferSize;
  private final long startingOffset;

  private final Lock lock = new ReentrantLock();
  private final Condition writeAhead = lock.newCondition();
  private final Condition availableNotEmpty = lock.newCondition();

  private long nextOffset;
  private RuntimeException exception;

  public OrderingQueue(long startingOffset, long totalSize, long bufferSize) {
    this.nextOffset = startingOffset;
    this.startingOffset = startingOffset;
    this.totalSize = totalSize;
    this.bufferSize = bufferSize;
  }

  public void push(long offset, byte[] data) throws InterruptedException {
    Preconditions.checkArgument(offset >= 0);
    Preconditions.checkArgument(data.length <= bufferSize);
    lock.lock();
    try {
      maybeThrow();
      while (offset + data.length > nextOffset + bufferSize) {
        writeAhead.await();
        maybeThrow();
      }

      pendingWrites.add(Pair.of(offset, data));
      availableNotEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Nullable
  public byte[] popInOrder() throws InterruptedException {
    lock.lock();
    try {
      maybeThrow();

      if ((nextOffset - startingOffset) == totalSize) {
        return null;
      }

      while (pendingWrites.size() == 0 ||
              pendingWrites.peek().getLeft().longValue() != nextOffset) {
        availableNotEmpty.await();
        maybeThrow();
      }

      Pair<Long, byte[]> availableWrite = pendingWrites.poll();
      byte[] bytes = availableWrite.getRight();
      nextOffset = availableWrite.getLeft() + bytes.length;
      writeAhead.signalAll();
      return bytes;
    } finally {
      lock.unlock();
    }
  }

  private void maybeThrow() {
    if (exception != null) {
      throw exception;
    }
  }

  public void closeWithException(RuntimeException e) {
    lock.lock();
    try {
      if (this.exception == null) {
        this.exception = e;
        availableNotEmpty.signalAll();
        writeAhead.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }
}
