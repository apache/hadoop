/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Used by {@link AliyunOSSFileSystem} and {@link AliyunOSSCopyFileTask}
 * as copy context. It contains some variables used in copy process.
 */
public class AliyunOSSCopyFileContext {
  private final ReentrantLock lock = new ReentrantLock();

  private Condition readyCondition = lock.newCondition();

  private boolean copyFailure;
  private int copiesFinish;

  public AliyunOSSCopyFileContext() {
    copyFailure = false;
    copiesFinish = 0;
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  public void awaitAllFinish(int copiesToFinish) throws InterruptedException {
    while (this.copiesFinish != copiesToFinish) {
      readyCondition.await();
    }
  }

  public void signalAll() {
    readyCondition.signalAll();
  }

  public boolean isCopyFailure() {
    return copyFailure;
  }

  public void setCopyFailure(boolean copyFailure) {
    this.copyFailure = copyFailure;
  }

  public void incCopiesFinish() {
    ++copiesFinish;
  }
}
