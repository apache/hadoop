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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Container class with a {@link ReentrantReadWriteLock}, reference count {@link AtomicInteger},
 * lock key {@link K}.
 * @param <K> key for the lock
 */
public class LockResource<K> {

  /** Underlying lock for this object */
  final ReentrantReadWriteLock rwLock;
  /** Reference count, used for eviction */
  final AtomicInteger ref;
  /** Lock key used to identify the lock */
  final K lockKey;

  public LockResource(K key) {
    this.rwLock = new ReentrantReadWriteLock();
    this.ref = new AtomicInteger(1);
    this.lockKey = key;
  }

  public K getLockKey() {
    return this.lockKey;
  }

  public AtomicInteger getRef() {
    return ref;
  }

  public ReentrantReadWriteLock getRwLock() {
    return rwLock;
  }

  @Override
  public String toString() {
    return lockKey.toString() + ", ref=" + ref.get();
  }
}
