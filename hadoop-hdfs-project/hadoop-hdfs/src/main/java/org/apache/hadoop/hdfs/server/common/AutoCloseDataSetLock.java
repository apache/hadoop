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

package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.StringUtils;

import java.util.concurrent.locks.Lock;

import static org.apache.hadoop.hdfs.server.datanode.DataSetLockManager.LOG;

/**
 * Extending AutoCloseableLock such that the users can
 * use a try-with-resource syntax.
 */
public class AutoCloseDataSetLock extends AutoCloseableLock {
  private Lock lock;
  private AutoCloseDataSetLock parentLock;
  private DataNodeLockManager<AutoCloseDataSetLock> dataNodeLockManager;

  public AutoCloseDataSetLock(Lock lock) {
    this.lock = lock;
  }

  @Override
  public void close() {
    if (lock != null) {
      lock.unlock();
      if (dataNodeLockManager != null) {
        dataNodeLockManager.hook();
      }
    } else {
      LOG.error("Try to unlock null lock" +
          StringUtils.getStackTrace(Thread.currentThread()));
    }
    if (parentLock != null) {
      parentLock.close();
    }
  }

  /**
   * Actually acquire the lock.
   */
  public void lock() {
    if (lock != null) {
      lock.lock();
      return;
    }
    LOG.error("Try to lock null lock" +
        StringUtils.getStackTrace(Thread.currentThread()));
  }

  public void setParentLock(AutoCloseDataSetLock parent) {
    if (parentLock == null) {
      this.parentLock = parent;
    }
  }

  public void setDataNodeLockManager(DataNodeLockManager<AutoCloseDataSetLock>
      dataNodeLockManager) {
    this.dataNodeLockManager = dataNodeLockManager;
  }
}
