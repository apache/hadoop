/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.client;

/**
 * Holds row name and lock id.
 */
public class RowLock {
  private byte [] row = null;
  private long lockId = -1L;

  /**
   * Creates a RowLock from a row and lock id
   * @param row
   * @param lockId
   */
  public RowLock(final byte [] row, final long lockId) {
    this.row = row;
    this.lockId = lockId;
  }
  
  /**
   * Creates a RowLock with only a lock id
   * @param lockId
   */
  public RowLock(final long lockId) {
    this.lockId = lockId;
  }

  /**
   * Get the row for this RowLock
   * @return the row
   */
  public byte [] getRow() {
    return row;
  }

  /**
   * Get the lock id from this RowLock
   * @return the lock id
   */
  public long getLockId() {
    return lockId;
  }
}
