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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Pauses one chosen thread in updateLeafQueueState while it holds the
 * policy's write lock, right before the policy calls back into the parent
 * queue. Used to reproduce the parent queue / policy lock-order inversion.
 */
public class GatedGuaranteedOrZeroCapacityOverTimePolicy
    extends GuaranteedOrZeroCapacityOverTimePolicy {

  private final CountDownLatch gateReached = new CountDownLatch(1);
  private final CountDownLatch released = new CountDownLatch(1);
  private volatile Thread gatedThread;

  public void gateThread(Thread thread) {
    gatedThread = thread;
  }

  public boolean awaitGate(long timeout, TimeUnit unit)
      throws InterruptedException {
    return gateReached.await(timeout, unit);
  }

  public void release() {
    released.countDown();
  }

  @Override
  void updateLeafQueueState() {
    if (Thread.currentThread() != gatedThread) {
      super.updateLeafQueueState();
      return;
    }
    // The gated thread acquires the policy's write lock and waits until
    // the gate is released before moving on.
    ReentrantReadWriteLock.WriteLock writeLock = getWriteLock();
    writeLock.lock();
    try {
      gateReached.countDown();
      if (!released.await(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Policy gate was not released");
      }
      super.updateLeafQueueState();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      writeLock.unlock();
    }
  }
}
