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
package org.apache.hadoop.util;

/**
 * LatchLock controls two hierarchical Read/Write locks:
 * the topLock and the childLock.
 * Typically an operation starts with the topLock already acquired.
 * To acquire child lock LatchLock will
 * first acquire the childLock, and then release the topLock.
 */
public abstract class LatchLock<C> {
  // Interfaces methods to be defined for subclasses
  /** @return true topLock is locked for read by any thread */
  protected abstract boolean isReadTopLocked();
  /** @return true topLock is locked for write by any thread */
  protected abstract boolean isWriteTopLocked();
  protected abstract void readTopUnlock();
  protected abstract void writeTopUnlock();

  protected abstract boolean hasReadChildLock();
  protected abstract void readChildLock();
  protected abstract void readChildUnlock();

  protected abstract boolean hasWriteChildLock();
  protected abstract void writeChildLock();
  protected abstract void writeChildUnlock();

  protected abstract LatchLock<C> clone();

  // Public APIs to use with the class
  public void readLock() {
    readChildLock();
    readTopUnlock();
  }

  public void readUnlock() {
    readChildUnlock();
  }

  public void writeLock() {
    writeChildLock();
    writeTopUnlock();
  }

  public void writeUnlock() {
    writeChildUnlock();
  }
}
