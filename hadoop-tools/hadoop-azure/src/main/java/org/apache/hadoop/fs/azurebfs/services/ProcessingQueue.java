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

import java.util.LinkedList;
import java.util.Queue;

public class ProcessingQueue<T> {

  private final Queue<T> internalQueue = new LinkedList<>();
  private int processorCount = 0;

  ProcessingQueue() {
  }

  public synchronized void add(T item) {
    if (item == null) {
      throw new IllegalArgumentException("Cannot put null into queue");
    } else {
      this.internalQueue.add(item);
      this.notifyAll();
    }
  }

  public synchronized T poll() {
    while (true) {
      try {
        if (this.isQueueEmpty() && !this.done()) {
          this.wait();
          continue;
        }
        if (!this.isQueueEmpty()) {
          ++this.processorCount;
          return this.internalQueue.poll();
        }
        return null;
      } catch (InterruptedException var2) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  public synchronized void unregister() {
    --this.processorCount;
    if (this.processorCount < 0) {
      throw new IllegalStateException(
          "too many unregister()'s. processorCount is now "
              + this.processorCount);
    } else {
      if (this.done()) {
        this.notifyAll();
      }
    }
  }

  private boolean done() {
    return this.processorCount == 0 && this.isQueueEmpty();
  }

  private boolean isQueueEmpty() {
    return this.internalQueue.peek() == null;
  }
}
