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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A Writable object that contains a series of BatchOperations
 * 
 * There is one BatchUpdate object per server, so a series of batch operations
 * can result in multiple BatchUpdate objects if the batch contains rows that
 * are served by multiple region servers.
 */
public class BatchUpdate implements Writable,
Iterable<Map.Entry<Text, ArrayList<BatchOperation>>> {
  
  // used to generate lock ids
  private Random rand;
  
  // used on client side to map lockid to a set of row updates
  private HashMap<Long, ArrayList<BatchOperation>> lockToRowOps;
  
  // the operations for each row
  private HashMap<Text, ArrayList<BatchOperation>> operations;
  
  /** constructor */
  public BatchUpdate() {
    this.rand = new Random();
    this.lockToRowOps = new HashMap<Long, ArrayList<BatchOperation>>();
    this.operations = new HashMap<Text, ArrayList<BatchOperation>>();
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
   * changed. The handle must be supplied on subsequent put or delete calls so
   * that the row can be identified.
   * 
   * @param row Name of row to start update against.
   * @return Row lockid.
   */
  public synchronized long startUpdate(Text row) {
    Long lockid = Long.valueOf(Math.abs(rand.nextLong()));
    ArrayList<BatchOperation> ops = operations.get(row);
    if(ops == null) {
      ops = new ArrayList<BatchOperation>();
      operations.put(row, ops);
    }
    lockToRowOps.put(lockid, ops);
    return lockid.longValue();
  }
  
  /** 
   * Change a value for the specified column
   *
   * @param lockid              - lock id returned from startUpdate
   * @param column              - column whose value is being set
   * @param val                 - new value for column
   */
  public synchronized void put(long lockid, Text column, byte val[]) {
    ArrayList<BatchOperation> ops = lockToRowOps.get(lockid);
    if(ops == null) {
      throw new IllegalArgumentException("no row for lockid " + lockid);
    }
    ops.add(new BatchOperation(column, val));
  }
  
  /** 
   * Delete the value for a column
   *
   * @param lockid              - lock id returned from startUpdate
   * @param column              - name of column whose value is to be deleted
   */
  public synchronized void delete(long lockid, Text column) {
    ArrayList<BatchOperation> ops = lockToRowOps.get(lockid);
    if(ops == null) {
      throw new IllegalArgumentException("no row for lockid " + lockid);
    }
    ops.add(new BatchOperation(column));
  }

  //
  // Iterable
  //
  
  /**
   * @return Iterator<Map.Entry<Text, ArrayList<BatchOperation>>>
   *         Text row -> ArrayList<BatchOperation> changes
   */
  public Iterator<Map.Entry<Text, ArrayList<BatchOperation>>> iterator() {
    return operations.entrySet().iterator();
  }
  
  //
  // Writable
  //

  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    int nOps = in.readInt();
    for (int i = 0; i < nOps; i++) {
      Text row = new Text();
      row.readFields(in);
      
      int nRowOps = in.readInt();
      ArrayList<BatchOperation> rowOps = new ArrayList<BatchOperation>();
      for(int j = 0; j < nRowOps; j++) {
        BatchOperation op = new BatchOperation();
        op.readFields(in);
        rowOps.add(op);
      }
      
      operations.put(row, rowOps);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(operations.size());
    for (Map.Entry<Text, ArrayList<BatchOperation>> e: operations.entrySet()) {
      e.getKey().write(out);
      
      ArrayList<BatchOperation> ops = e.getValue();
      out.writeInt(ops.size());
      
      for(BatchOperation op: ops) {
        op.write(out);
      }
    }
  }
}
