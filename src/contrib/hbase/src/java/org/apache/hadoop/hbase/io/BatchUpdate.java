/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A Writable object that contains a series of BatchOperations
 * 
 * There is one BatchUpdate object per server, so a series of batch operations
 * can result in multiple BatchUpdate objects if the batch contains rows that
 * are served by multiple region servers.
 */
public class BatchUpdate implements Writable, Iterable<BatchOperation> {
  
  // the row being updated
  private Text row;
  
  // the lockid - not used on server side
  private transient long lockid;
  
  // the batched operations
  private ArrayList<BatchOperation> operations;
  
  /** Default constructor - used by Writable. */
  public BatchUpdate() {
    this.row = new Text();
    this.lockid = -1L;
    this.operations = new ArrayList<BatchOperation>();
  }
  
  /**
   * Client side constructor. Clients need to provide the lockid by some means
   * such as Random.nextLong()
   * 
   * @param lockid
   */
  public BatchUpdate(long lockid) {
    this.row = new Text();
    this.lockid = Math.abs(lockid);
    this.operations = new ArrayList<BatchOperation>();
  }

  /** @return the lock id */
  public long getLockid() {
    return lockid;
  }
  
  /** @return the row */
  public Text getRow() {
    return row;
  }
  
  /** 
   * Start a batch row insertion/update.
   * 
   * No changes are committed until the client commits the batch operation via
   * HClient.batchCommit().
   * 
   * The entire batch update can be abandoned by calling HClient.batchAbort();
   *
   * Callers to this method are given a handle that corresponds to the row being
   * changed. The handle must be supplied on subsequent put or delete calls.
   * 
   * @param row Name of row to start update against.
   * @return Row lockid.
   */
  public synchronized long startUpdate(final Text row) {
    this.row = row;
    return this.lockid;
  }
  
  /** 
   * Change a value for the specified column
   *
   * @param lid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null (can be empty).
   */
  public synchronized void put(final long lid, final Text column,
      final byte val[]) {
    if(this.lockid != lid) {
      throw new IllegalArgumentException("invalid lockid " + lid);
    }
    if (val == null) {
      // If null, the PUT becomes a DELETE operation.
      throw new IllegalArgumentException("Passed value cannot be null");
    }
    operations.add(new BatchOperation(column, val));
  }
  
  /** 
   * Delete the value for a column
   * Deletes the cell whose row/column/commit-timestamp match those of the
   * delete.
   * @param lid lock id returned from startUpdate
   * @param column name of column whose value is to be deleted
   */
  public synchronized void delete(final long lid, final Text column) {
    if(this.lockid != lid) {
      throw new IllegalArgumentException("invalid lockid " + lid);
    }
    operations.add(new BatchOperation(column));
  }

  //
  // Iterable
  //
  
  /**
   * @return Iterator<BatchOperation>
   */
  public Iterator<BatchOperation> iterator() {
    return operations.iterator();
  }
  
  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    row.readFields(in);
    int nOps = in.readInt();
    for (int i = 0; i < nOps; i++) {
      BatchOperation op = new BatchOperation();
      op.readFields(in);
      operations.add(op);
    }
  }

  public void write(final DataOutput out) throws IOException {
    row.write(out);
    out.writeInt(operations.size());
    for (BatchOperation op: operations) {
      op.write(out);
    }
  }
}