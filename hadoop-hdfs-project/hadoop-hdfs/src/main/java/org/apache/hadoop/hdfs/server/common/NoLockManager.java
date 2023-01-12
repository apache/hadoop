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

import java.util.concurrent.locks.Lock;

/**
 * Some ut or temp replicaMap not need to lock with DataSetLockManager.
 */
public class NoLockManager implements DataNodeLockManager<AutoCloseDataSetLock> {
  private final NoDataSetLock lock = new NoDataSetLock(null);

  private static final class NoDataSetLock extends AutoCloseDataSetLock {

    private NoDataSetLock(Lock lock) {
      super(lock);
    }

    @Override
    public void lock() {
    }

    @Override
    public void close() {
    }
  }

  public NoLockManager() {
  }

  @Override
  public AutoCloseDataSetLock readLock(LockLevel level, String... resources) {
    return lock;
  }

  @Override
  public AutoCloseDataSetLock writeLock(LockLevel level, String... resources) {
    return lock;
  }

  @Override
  public void addLock(LockLevel level, String... resources) {
  }

  @Override
  public void removeLock(LockLevel level, String... resources) {
  }

  @Override
  public void hook() {
  }
}
