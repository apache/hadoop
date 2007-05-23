/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * HLocking is a set of lock primitives that does not rely on a
 * particular thread holding the monitor for an object. This is
 * especially important when a lock must persist over multiple RPC's
 * since there is no guarantee that the same Server thread will handle
 * all the RPC's until the lock is released.  Not requiring that the locker
 * thread is same as unlocking thread is the key distinction between this
 * class and {@link java.util.concurrent.locks.ReentrantReadWriteLock}. 
 *
 * <p>For each independent entity that needs locking, create a new HLocking
 * instance.
 */
public class HLocking {
  private Integer mutex;
  
  // If lockers == 0, the lock is unlocked
  // If lockers > 0, locked for read
  // If lockers == -1 locked for write
  
  private AtomicInteger lockers;
  
  /** Constructor */
  public HLocking() {
    this.mutex = new Integer(0);
    this.lockers = new AtomicInteger(0);
  }

  /**
   * Caller needs the nonexclusive read-lock
   */
  public void obtainReadLock() {
    synchronized(mutex) {
      while(lockers.get() < 0) {
        try {
          mutex.wait();
        } catch(InterruptedException ie) {
        }
      }
      lockers.incrementAndGet();
      mutex.notifyAll();
    }
  }

  /**
   * Caller is finished with the nonexclusive read-lock
   */
  public void releaseReadLock() {
    synchronized(mutex) {
      if(lockers.decrementAndGet() < 0) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      mutex.notifyAll();
    }
  }

  /**
   * Caller needs the exclusive write-lock
   */
  public void obtainWriteLock() {
    synchronized(mutex) {
      while(!lockers.compareAndSet(0, -1)) {
        try {
          mutex.wait();
        } catch (InterruptedException ie) {
        }
      }
      mutex.notifyAll();
    }
  }

  /**
   * Caller is finished with the write lock
   */
  public void releaseWriteLock() {
    synchronized(mutex) {
      if(!lockers.compareAndSet(-1, 0)) {
        throw new IllegalStateException("lockers: " + lockers);
      }
      mutex.notifyAll();
    }
  }
}
