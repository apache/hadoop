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
package org.apache.hadoop.fs.cosn;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The context of the copy task, including concurrency control,
 * asynchronous acquisition of copy results and etc.
 */
public class CosNCopyFileContext {

  private final ReentrantLock lock = new ReentrantLock();
  private Condition readyCondition = lock.newCondition();

  private AtomicBoolean copySuccess = new AtomicBoolean(true);
  private AtomicInteger copiesFinish = new AtomicInteger(0);

  public void lock() {
    this.lock.lock();
  }

  public void unlock() {
    this.lock.unlock();
  }

  public void awaitAllFinish(int waitCopiesFinish) throws InterruptedException {
    while (this.copiesFinish.get() != waitCopiesFinish) {
      this.readyCondition.await();
    }
  }

  public void signalAll() {
    this.readyCondition.signalAll();
  }

  public boolean isCopySuccess() {
    return this.copySuccess.get();
  }

  public void setCopySuccess(boolean copySuccess) {
    this.copySuccess.set(copySuccess);
  }

  public void incCopiesFinish() {
    this.copiesFinish.addAndGet(1);
  }
}
