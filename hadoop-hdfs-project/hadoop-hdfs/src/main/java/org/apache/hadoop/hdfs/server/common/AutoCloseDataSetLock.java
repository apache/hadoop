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

import org.apache.hadoop.hdfs.server.datanode.DataSetLockHeldInfo;
import org.apache.hadoop.hdfs.server.datanode.LockHeldInfo;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import java.util.concurrent.locks.Lock;

import static org.apache.hadoop.hdfs.server.datanode.DataSetLockManager.LOG;

/**
 * Extending AutoCloseableLock such that the users can
 * use a try-with-resource syntax.
 */
public class AutoCloseDataSetLock extends AutoCloseableLock {

  private final Lock lock;

  private final DataNodeLockManager<AutoCloseDataSetLock> dataNodeLockManager;

  private final boolean isReadLock;

  private static ThreadLocal<DataSetLockHeldInfo> heldInfoThreadLocal = new
      ThreadLocal<DataSetLockHeldInfo>() {
        protected DataSetLockHeldInfo initialValue() {
          return new DataSetLockHeldInfo();
        }
  };

  private AutoCloseDataSetLock parentLock;

  private final long timeThreshold;

  private final boolean enableHoldInfo;

  public AutoCloseDataSetLock(Lock lock, DataNodeLockManager manager,
      boolean isReadLock, boolean enableLockHoldInfo, long timeThreshold) {
    this.dataNodeLockManager = manager;
    this.isReadLock = isReadLock;
    this.lock = lock;
    this.timeThreshold = timeThreshold;
    this.enableHoldInfo = enableLockHoldInfo;
  }

  @Override
  public void close() {
    if (lock != null) {
      endLockInfo();
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
    } else if (enableHoldInfo) {
      DataSetLockHeldInfo dataSetLockHeldInfo = heldInfoThreadLocal.get();
      dataSetLockHeldInfo.printHeldInfo(timeThreshold);
    }
  }

  /**
   * Actually acquire the lock.
   */
  public void lock(String opName, String lockName, boolean isBpLevel) {
    if (lock != null) {
      lock.lock();
      startLockInfo(opName, lockName, isBpLevel);
      return;
    }
    LOG.error("Try to lock null lock" +
        StringUtils.getStackTrace(Thread.currentThread()));
  }

  private void startLockInfo(String opName, String lockName, boolean isBpLevel) {
    if (!enableHoldInfo) {
      return;
    }
    DataSetLockHeldInfo dataSetLockHeldInfo = heldInfoThreadLocal.get();
    LockHeldInfo lockHeldInfo = dataSetLockHeldInfo.addNewLockInfo(isBpLevel, isReadLock);
    if (isBpLevel) {
      lockHeldInfo.setOpName(opName);
      lockHeldInfo.setStartTimeMs(Time.monotonicNow());
    } else {
      lockHeldInfo.setStartChildLockTimeMs(Time.monotonicNow());
      lockHeldInfo.setChildLockName(lockName);
    }
  }

  private void endLockInfo() {
    if (!enableHoldInfo) {
      return;
    }
    DataSetLockHeldInfo dataSetLockHeldInfo = heldInfoThreadLocal.get();
    LockHeldInfo lastLockInfo = dataSetLockHeldInfo.getLastLockInfo();
    if (!lastLockInfo.isBpLevel()) {
      lastLockInfo.setEndChildLockTimeMs(Time.monotonicNow());
    } else {
      lastLockInfo.setEndTimeMs(Time.monotonicNow());
    }
  }

  public void setParentLock(AutoCloseDataSetLock parent) {
    if (parentLock == null) {
      this.parentLock = parent;
    }
  }

  public boolean isReadLock() {
    return this.isReadLock;
  }

  public DataSetLockHeldInfo getLockHeldInfo() {
    return heldInfoThreadLocal.get();
  }
}
