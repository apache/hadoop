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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.server.namenode.Namesystem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class BlockManagerLock implements ReadWriteLock {
  private final ReentrantReadWriteLock coarseLock;

  BlockManagerLock(Namesystem ns) {
    this.coarseLock = ns.getLockImplementation();
  }

  @Override
  public Lock readLock() {
    return coarseLock.readLock();
  }

  @Override
  public Lock writeLock() {
    return coarseLock.writeLock();
  }

  boolean hasReadLock() {
    return hasWriteLock() || coarseLock.getReadHoldCount() > 0;
  }

  boolean hasWriteLock() {
    return coarseLock.isWriteLockedByCurrentThread();
  }
}
