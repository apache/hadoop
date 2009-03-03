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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
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

  /**
   * Simple container of the range of the scanners we've opened. Used to check
   * for conflicting writes.
   */
  private class ScanRange {
    private byte[] startRow;
    private byte[] endRow;

    public ScanRange(byte[] startRow, byte[] endRow) {
      this.startRow = startRow;
      this.endRow = endRow;
    }

    /**
     * Check if this scan range contains the given key.
     * 
     * @param rowKey
     * @return
     */
    public boolean contains(byte[] rowKey) {
      if (startRow != null && Bytes.compareTo(rowKey, startRow) < 0) {
        return false;
      }
      if (endRow != null && Bytes.compareTo(endRow, rowKey) < 0) {
        return false;
      }
      return true;
    }
  }

  private final HRegionInfo regionInfo;
  private final long hLogStartSequenceId;
  private final long transactionId;
  private Status status;
  private SortedSet<byte[]> readSet = new TreeSet<byte[]>(
      Bytes.BYTES_COMPARATOR);
  private List<BatchUpdate> writeSet = new LinkedList<BatchUpdate>();
  private List<ScanRange> scanSet = new LinkedList<ScanRange>();
  private Set<TransactionState> transactionsToCheck = new HashSet<TransactionState>();
  private int startSequenceNumber;
  private Integer sequenceNumber;

  TransactionState(final long transactionId, final long rLogStartSequenceId,
      HRegionInfo regionInfo) {
    this.transactionId = transactionId;
    this.hLogStartSequenceId = rLogStartSequenceId;
    this.regionInfo = regionInfo;
    this.status = Status.PENDING;
  }

  void addRead(final byte[] rowKey) {
    readSet.add(rowKey);
  }

  Set<byte[]> getReadSet() {
    return readSet;
  }

  void addWrite(final BatchUpdate write) {
    writeSet.add(write);
  }

  List<BatchUpdate> getWriteSet() {
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
  Map<byte[], Cell> localGetFull(final byte[] row, final Set<byte[]> columns,
      final long timestamp) {
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
  Cell[] localGet(final byte[] row, final byte[] column, final long timestamp) {
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

  void addTransactionToCheck(final TransactionState transaction) {
    transactionsToCheck.add(transaction);
  }

  boolean hasConflict() {
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
      if (this.getReadSet().contains(otherUpdate.getRow())) {
        LOG.debug("Transaction [" + this.toString()
            + "] has read which conflicts with [" + checkAgainst.toString()
            + "]: region [" + regionInfo.getRegionNameAsString() + "], row["
            + Bytes.toString(otherUpdate.getRow()) + "]");
        return true;
      }
      for (ScanRange scanRange : this.scanSet) {
        if (scanRange.contains(otherUpdate.getRow())) {
          LOG.debug("Transaction [" + this.toString()
              + "] has scan which conflicts with [" + checkAgainst.toString()
              + "]: region [" + regionInfo.getRegionNameAsString() + "], row["
              + Bytes.toString(otherUpdate.getRow()) + "]");
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the status.
   * 
   * @return Return the status.
   */
  Status getStatus() {
    return status;
  }

  /**
   * Set the status.
   * 
   * @param status The status to set.
   */
  void setStatus(final Status status) {
    this.status = status;
  }

  /**
   * Get the startSequenceNumber.
   * 
   * @return Return the startSequenceNumber.
   */
  int getStartSequenceNumber() {
    return startSequenceNumber;
  }

  /**
   * Set the startSequenceNumber.
   * 
   * @param startSequenceNumber.
   */
  void setStartSequenceNumber(final int startSequenceNumber) {
    this.startSequenceNumber = startSequenceNumber;
  }

  /**
   * Get the sequenceNumber.
   * 
   * @return Return the sequenceNumber.
   */
  Integer getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Set the sequenceNumber.
   * 
   * @param sequenceNumber The sequenceNumber to set.
   */
  void setSequenceNumber(final Integer sequenceNumber) {
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
    result.append(" scan Size: ");
    result.append(scanSet.size());
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
  long getTransactionId() {
    return transactionId;
  }

  /**
   * Get the startSequenceId.
   * 
   * @return Return the startSequenceId.
   */
  long getHLogStartSequenceId() {
    return hLogStartSequenceId;
  }

  void addScan(byte[] firstRow, RowFilterInterface filter) {
    ScanRange scanRange = new ScanRange(firstRow, getEndRow(filter));
    LOG.trace(String.format(
        "Adding scan for transcaction [%s], from [%s] to [%s]", transactionId,
        scanRange.startRow == null ? "null" : Bytes
            .toString(scanRange.startRow), scanRange.endRow == null ? "null"
            : Bytes.toString(scanRange.endRow)));
    scanSet.add(scanRange);
  }

  private byte[] getEndRow(RowFilterInterface filter) {
    if (filter instanceof WhileMatchRowFilter) {
      WhileMatchRowFilter wmrFilter = (WhileMatchRowFilter) filter;
      if (wmrFilter.getInternalFilter() instanceof StopRowFilter) {
        StopRowFilter stopFilter = (StopRowFilter) wmrFilter
            .getInternalFilter();
        return stopFilter.getStopRowKey();
      }
    } else if (filter instanceof RowFilterSet) {
      RowFilterSet rowFilterSet = (RowFilterSet) filter;
      if (rowFilterSet.getOperator()
          .equals(RowFilterSet.Operator.MUST_PASS_ALL)) {
        for (RowFilterInterface subFilter : rowFilterSet.getFilters()) {
          byte[] endRow = getEndRow(subFilter);
          if (endRow != null) {
            return endRow;
          }
        }
      }
    }
    return null;
  }

}
