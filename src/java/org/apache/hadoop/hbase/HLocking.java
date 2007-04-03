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

/*******************************************************************************
 * HLocking is a set of lock primitives that are pretty helpful in a few places
 * around the HBase code.  For each independent entity that needs locking, create
 * a new HLocking instance.
 ******************************************************************************/
public class HLocking {
  Integer readerLock = new Integer(0);
  Integer writerLock = new Integer(0);
  int numReaders = 0;
  int numWriters = 0;

  public HLocking() {
  }

  /** Caller needs the nonexclusive read-lock */
  public void obtainReadLock() {
    synchronized(readerLock) {
      synchronized(writerLock) {
        while(numWriters > 0) {
          try {
            writerLock.wait();
          } catch (InterruptedException ie) {
          }
        }
        numReaders++;
        readerLock.notifyAll();
      }
    }
  }

  /** Caller is finished with the nonexclusive read-lock */
  public void releaseReadLock() {
    synchronized(readerLock) {
      synchronized(writerLock) {
        numReaders--;
        readerLock.notifyAll();
      }
    }
  }

  /** Caller needs the exclusive write-lock */
  public void obtainWriteLock() {
    synchronized(readerLock) {
      synchronized(writerLock) {
        while(numReaders > 0) {
          try {
            readerLock.wait();
          } catch (InterruptedException ie) {
          }
        }
        while(numWriters > 0) {
          try {
            writerLock.wait();
          } catch (InterruptedException ie) {
          }
        }
        numWriters++;
        writerLock.notifyAll();
      }
    }
  }

  /** Caller is finished with the write lock */
  public void releaseWriteLock() {
    synchronized(readerLock) {
      synchronized(writerLock) {
        numWriters--;
        writerLock.notifyAll();
      }
    }
  }
}

