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

/**
 * Use for manage a set of lock for datanode.
 */
public interface DataNodeLockManager<T extends AutoCloseDataSetLock> {

  /**
   * Acquire block pool level first if you want to Acquire volume lock.
   * Or only acquire block pool level lock.
   */
  enum LockLevel {
    BLOCK_POOl,
    VOLUME
  }

  /**
   * Acquire readLock and then lock.
   */
  T readLock(LockLevel level, String... resources);

  /**
   * Acquire writeLock and then lock.
   */
  T writeLock(LockLevel level, String... resources);

  /**
   * Add a lock to LockManager.
   */
  void addLock(LockLevel level, String... resources);

  /**
   * Remove a lock from LockManager.
   */
  void removeLock(LockLevel level, String... resources);

  /**
   * LockManager may need to back hook.
   */
  void hook();
}
