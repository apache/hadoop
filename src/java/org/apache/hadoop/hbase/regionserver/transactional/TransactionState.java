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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Holds the state of a transaction.
 */
class TransactionState {

  private static final Log LOG = LogFactory.getLog(TransactionState.class);

  /** Current status. */
  public enum Status {
    /** Initial status, still performing operations. */
    PENDING,
    /**
     * Checked if we can commit, and said yes. Still need to determine the
     * global decision.
     */
    COMMIT_PENDING,
    /** Committed. */
    COMMITED,
    /** Aborted. */
    ABORTED
  }

  private final long hLogStartSequenceId;
  private final long transactionId;
  private Status status;
  private SortedSet<byte[]> readSet = new TreeSet<byte[]>(
      Bytes.BYTES_COMPARATOR);
  private List<BatchUpdate> writeSet = new LinkedList<BatchUpdate>();
  private Set<TransactionState> transactionsToCheck = new HashSet<TransactionState>();
  private int startSequenceNumber;
  private Integer sequenceNumber;
  boolean hasScan = false;

  //TODO: Why don't these methods and the class itself use default access?
  //      They are only referenced from within this package.
  
  public TransactionState(final long transactionId,
      final long rLogStartSequenceId) {
    this.transactionId = transactionId;
    this.hLogStartSequenceId = rLogStartSequenceId;
    this.status = Status.PENDING;
  }

  public void addRead(final byte[] rowKey) {
    readSet.add(rowKey);
  }

  public Set<byte[]> getReadSet() {
    return readSet;
  }

  public void addWrite(final BatchUpdate write) {
    writeSet.add(write);
  }

  public List<BatchUpdate> getWriteSet() {
    return writeSet;
  }

  /**
   * GetFull from the writeSet.
   * 
   * @param row
   * @param columns
   * @param timestamp
   * @return
   */
  public Map<byte[], Cell> localGetFull(final byte[] row,
      final Set<byte[]> columns, final long timestamp) {
    Map<byte[], Cell> results = new TreeMap<byte[], Cell>(
        Bytes.BYTES_COMPARATOR); // Must use the Bytes Conparator because
    for (BatchUpdate b : writeSet) {
      if (!Bytes.equals(row, b.getRow())) {
        continue;
      }
      if (b.getTimestamp() > timestamp) {
        continue;
      }
      for (BatchOperation op : b) {
        if (!op.isPut()
            || (columns != null && !columns.contains(op.getColumn()))) {
          continue;
        }
        results.put(op.getColumn(), new Cell(op.getValue(), b.getTimestamp()));
      }
    }
    return results.size() == 0 ? null : results;
  }

  /**
   * Get from the writeSet.
   * 
   * @param row
   * @param column
   * @param timestamp
   * @return
   */
  public Cell[] localGet(final byte[] row, final byte[] column,
      final long timestamp) {
    ArrayList<Cell> results = new ArrayList<Cell>();

    // Go in reverse order to put newest updates first in list
    for (int i = writeSet.size() - 1; i >= 0; i--) {
      BatchUpdate b = writeSet.get(i);

      if (!Bytes.equals(row, b.getRow())) {
        continue;
      }
      if (b.getTimestamp() > timestamp) {
        continue;
      }
      for (BatchOperation op : b) {
        if (!op.isPut() || !Bytes.equals(column, op.getColumn())) {
          continue;
        }
        results.add(new Cell(op.getValue(), b.getTimestamp()));
      }
    }
    return results.size() == 0 ? null : results
        .toArray(new Cell[results.size()]);
  }

  public void addTransactionToCheck(final TransactionState transaction) {
    transactionsToCheck.add(transaction);
  }

  public boolean hasConflict() {
    for (TransactionState transactionState : transactionsToCheck) {
      if (hasConflict(transactionState)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasConflict(final TransactionState checkAgainst) {
    if (checkAgainst.getStatus().equals(TransactionState.Status.ABORTED)) {
      return false; // Cannot conflict with aborted transactions
    }

    for (BatchUpdate otherUpdate : checkAgainst.getWriteSet()) {
      if (this.hasScan) {
        LOG.info("Transaction" + this.toString()
            + " has a scan read. Meanwile a write occured. "
            + "Conservitivly reporting conflict");
        return true;
      }

      if (this.getReadSet().contains(otherUpdate.getRow())) {
        LOG.trace("Transaction " + this.toString() + " conflicts with "
            + checkAgainst.toString());
        return true;
      }
    }
    return false;
  }

  /**
   * Get the status.
   * 
   * @return Return the status.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Set the status.
   * 
   * @param status The status to set.
   */
  public void setStatus(final Status status) {
    this.status = status;
  }

  /**
   * Get the startSequenceNumber.
   * 
   * @return Return the startSequenceNumber.
   */
  public int getStartSequenceNumber() {
    return startSequenceNumber;
  }

  /**
   * Set the startSequenceNumber.
   * 
   * @param startSequenceNumber.
   */
  public void setStartSequenceNumber(final int startSequenceNumber) {
    this.startSequenceNumber = startSequenceNumber;
  }

  /**
   * Get the sequenceNumber.
   * 
   * @return Return the sequenceNumber.
   */
  public Integer getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Set the sequenceNumber.
   * 
   * @param sequenceNumber The sequenceNumber to set.
   */
  public void setSequenceNumber(final Integer sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("[transactionId: ");
    result.append(transactionId);
    result.append(" status: ");
    result.append(status.name());
    result.append(" read Size: ");
    result.append(readSet.size());
    result.append(" write Size: ");
    result.append(writeSet.size());
    result.append(" startSQ: ");
    result.append(startSequenceNumber);
    if (sequenceNumber != null) {
      result.append(" commitedSQ:");
      result.append(sequenceNumber);
    }
    result.append("]");

    return result.toString();
  }

  /**
   * Get the transactionId.
   * 
   * @return Return the transactionId.
   */
  public long getTransactionId() {
    return transactionId;
  }

  /**
   * Get the startSequenceId.
   * 
   * @return Return the startSequenceId.
   */
  public long getHLogStartSequenceId() {
    return hLogStartSequenceId;
  }

  /**
   * Set the hasScan.
   * 
   * @param hasScan The hasScan to set.
   */
  public void setHasScan(final boolean hasScan) {
    this.hasScan = hasScan;
  }

}
