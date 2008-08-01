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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
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
  private byte [] row = null;
    
  // the batched operations
  private ArrayList<BatchOperation> operations =
    new ArrayList<BatchOperation>();
  
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  
  /**
   * Default constructor used serializing.  Do not use directly.
   */
  public BatchUpdate() {
    this ((byte [])null);
  }

  /**
   * Initialize a BatchUpdate operation on a row. Timestamp is assumed to be
   * now.
   * 
   * @param row
   */
  public BatchUpdate(final Text row) {
    this(row, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Initialize a BatchUpdate operation on a row. Timestamp is assumed to be
   * now.
   * 
   * @param row
   */
  public BatchUpdate(final String row) {
    this(Bytes.toBytes(row), HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Initialize a BatchUpdate operation on a row. Timestamp is assumed to be
   * now.
   * 
   * @param row
   */
  public BatchUpdate(final byte [] row) {
    this(row, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Initialize a BatchUpdate operation on a row with a specific timestamp.
   * 
   * @param row
   */
  public BatchUpdate(final String row, long timestamp){
    this(Bytes.toBytes(row), timestamp);
  }

  /**
   * Initialize a BatchUpdate operation on a row with a specific timestamp.
   * 
   * @param row
   */
  public BatchUpdate(final Text row, long timestamp){
    this(row.getBytes(), timestamp);
  }

  /**
   * Initialize a BatchUpdate operation on a row with a specific timestamp.
   * 
   * @param row
   */
  public BatchUpdate(final byte [] row, long timestamp){
    this.row = row;
    this.timestamp = timestamp;
    this.operations = new ArrayList<BatchOperation>();
  }

  /** @return the row */
  public byte [] getRow() {
    return row;
  }

  /**
   * Return the timestamp this BatchUpdate will be committed with.
   */
  public long getTimestamp() {
    return timestamp;
  }
  
  /**
   * Set this BatchUpdate's timestamp.
   */  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /** 
   * Change a value for the specified column
   *
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null (can be empty).
   */
  public synchronized void put(final Text column, final byte val[]) {
    put(column.getBytes(), val);
  }
  
  /** 
   * Change a value for the specified column
   *
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null (can be empty).
   */
  public synchronized void put(final String column, final byte val[]) {
    put(Bytes.toBytes(column), val);
  }

  /** 
   * Change a value for the specified column
   *
   * @param column column whose value is being set
   * @param val new value for column.  Cannot be null (can be empty).
   */
  public synchronized void put(final byte [] column, final byte val[]) {
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
   * @param column name of column whose value is to be deleted
   */
  public void delete(final Text column) {
    delete(column.getBytes());
  }
 
  /** 
   * Delete the value for a column
   * Deletes the cell whose row/column/commit-timestamp match those of the
   * delete.
   * @param column name of column whose value is to be deleted
   */
  public void delete(final String column) {
    delete(Bytes.toBytes(column));
  }

  /** 
   * Delete the value for a column
   * Deletes the cell whose row/column/commit-timestamp match those of the
   * delete.
   * @param column name of column whose value is to be deleted
   */
  public synchronized void delete(final byte [] column) {
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
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row => ");
    sb.append(row == null? "": Bytes.toString(row));
    sb.append(", {");
    boolean morethanone = false;
    for (BatchOperation bo: this.operations) {
      if (morethanone) {
        sb.append(", ");
      }
      morethanone = true;
      sb.append(bo.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  //
  // Writable
  //

  public void readFields(final DataInput in) throws IOException {
    // Clear any existing operations; may be hangovers from previous use of
    // this instance.
    if (this.operations.size() != 0) {
      this.operations.clear();
    }
    this.row = Bytes.readByteArray(in);
    timestamp = in.readLong();
    int nOps = in.readInt();
    for (int i = 0; i < nOps; i++) {
      BatchOperation op = new BatchOperation();
      op.readFields(in);
      this.operations.add(op);
    }
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(timestamp);
    out.writeInt(operations.size());
    for (BatchOperation op: operations) {
      op.write(out);
    }
  }
}