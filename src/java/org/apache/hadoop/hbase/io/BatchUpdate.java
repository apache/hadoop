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
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

/**
 * A Writable object that contains a series of BatchOperations
 * 
 * There is one BatchUpdate object per server, so a series of batch operations
 * can result in multiple BatchUpdate objects if the batch contains rows that
 * are served by multiple region servers.
 */
public class BatchUpdate
implements WritableComparable<BatchUpdate>, Iterable<BatchOperation>, HeapSize {
  private static final Log LOG = LogFactory.getLog(BatchUpdate.class);
  
  /**
   * Estimated 'shallow size' of this object not counting payload.
   */
  // Shallow size is 56.  Add 32 for the arraylist below.
  public static final int ESTIMATED_HEAP_TAX = 56 + 32;
  
  // the row being updated
  private byte [] row = null;
  private long size = 0;
    
  // the batched operations
  private ArrayList<BatchOperation> operations =
    new ArrayList<BatchOperation>();
  
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  
  private long rowLock = -1l;
  
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
   * @param timestamp
   */
  public BatchUpdate(final String row, long timestamp){
    this(Bytes.toBytes(row), timestamp);
  }
  
  /**
   * Recopy constructor
   * @param buToCopy BatchUpdate to copy
   */
  public BatchUpdate(BatchUpdate buToCopy) {
    this(buToCopy.getRow(), buToCopy.getTimestamp());
    for(BatchOperation bo : buToCopy) {
      byte [] val = bo.getValue();
      if (val == null) {
        // Presume a delete is intended.
        this.delete(bo.getColumn());
      } else {
        this.put(bo.getColumn(), val);
      }
    }
  }

  /**
   * Initialize a BatchUpdate operation on a row with a specific timestamp.
   * 
   * @param row
   * @param timestamp
   */
  public BatchUpdate(final byte [] row, long timestamp){
    this.row = row;
    this.timestamp = timestamp;
    this.operations = new ArrayList<BatchOperation>();
    this.size = (row == null)? 0: row.length;
  }
  /**
   * Get the row lock associated with this update
   * @return the row lock
   */
  public long getRowLock() {
    return rowLock;
  }

  /**
   * Set the lock to be used for this update
   * @param rowLock the row lock
   */
  public void setRowLock(long rowLock) {
    this.rowLock = rowLock;
  }


  /** @return the row */
  public byte [] getRow() {
    return row;
  }

  /**
   * @return the timestamp this BatchUpdate will be committed with.
   */
  public long getTimestamp() {
    return timestamp;
  }
  
  /**
   * Set this BatchUpdate's timestamp.
   * 
   * @param timestamp
   */  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  /**
   * Get the current value of the specified column
   * 
   * @param column column name
   * @return byte[] the cell value, returns null if the column does not exist.
   */
  public synchronized byte[] get(final String column) {
    return get(Bytes.toBytes(column));
  }
  
  /**
   * Get the current value of the specified column 
   * 
   * @param column column name
   * @return byte[] the cell value, returns null if the column does not exist.
   */
  public synchronized byte[] get(final byte[] column) {
    for (BatchOperation operation: operations) {
      if (Arrays.equals(column, operation.getColumn())) {
        return operation.getValue();
      }
    }
    return null;
  }

  /**
   * Get the current columns
   * 
   * @return byte[][] an array of byte[] columns
   */
  public synchronized byte[][] getColumns() {
    byte[][] columns = new byte[operations.size()][];
    for (int i = 0; i < operations.size(); i++) {
      columns[i] = operations.get(i).getColumn();
    }
    return columns;
  }

  /**
   * Check if the specified column is currently assigned a value
   * 
   * @param column column to check for
   * @return boolean true if the given column exists
   */
  public synchronized boolean hasColumn(String column) {
    return hasColumn(Bytes.toBytes(column));
  }
  
  /**
   * Check if the specified column is currently assigned a value
   * 
   * @param column column to check for
   * @return boolean true if the given column exists
   */
  public synchronized boolean hasColumn(byte[] column) {
    byte[] getColumn = get(column);
    if (getColumn == null) {
      return false;
    }
    return true;
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
    BatchOperation bo = new BatchOperation(column, val);
    this.size += bo.heapSize();
    operations.add(bo);
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
    this.size = in.readLong();
    int nOps = in.readInt();
    for (int i = 0; i < nOps; i++) {
      BatchOperation op = new BatchOperation();
      op.readFields(in);
      this.operations.add(op);
    }
    this.rowLock = in.readLong();
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(timestamp);
    out.writeLong(this.size);
    out.writeInt(operations.size());
    for (BatchOperation op: operations) {
      op.write(out);
    }
    out.writeLong(this.rowLock);
  }

  public int compareTo(BatchUpdate o) {
    return Bytes.compareTo(this.row, o.getRow());
  }

  public long heapSize() {
    return this.row.length + Bytes.ESTIMATED_HEAP_TAX + this.size +
      ESTIMATED_HEAP_TAX;
  }
  
  /**
   * Code to test sizes of BatchUpdate arrays.
   * @param args
   * @throws InterruptedException
   */
  public static void main(String[] args) throws InterruptedException {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor="
        + runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    final int count = 10000;
    BatchUpdate[] batch1 = new BatchUpdate[count];
    // TODO: x32 vs x64
    long size = 0;
    for (int i = 0; i < count; i++) {
      BatchUpdate bu = new BatchUpdate(HConstants.EMPTY_BYTE_ARRAY);
      bu.put(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);
      batch1[i] = bu;
      size += bu.heapSize();
    }
    LOG.info("batch1 estimated size=" + size);
    // Make a variably sized memcache.
    size = 0;
    BatchUpdate[] batch2 = new BatchUpdate[count];
    for (int i = 0; i < count; i++) {
      BatchUpdate bu = new BatchUpdate(Bytes.toBytes(i));
      bu.put(Bytes.toBytes(i), new byte[i]);
      batch2[i] = bu;
      size += bu.heapSize();
    }
    LOG.info("batch2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    for (int i = 0; i < seconds; i++) {
      Thread.sleep(1000);
    }
    LOG.info("Exiting.");
  }
}