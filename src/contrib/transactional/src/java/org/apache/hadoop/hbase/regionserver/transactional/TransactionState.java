/**
 * Copyright 2009 The Apache Software Foundation
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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
  private static class ScanRange {
    protected byte[] startRow;
    protected byte[] endRow;

    public ScanRange(byte[] startRow, byte[] endRow) {
      this.startRow = startRow;
      this.endRow = endRow;
    }

    /**
     * Check if this scan range contains the given key.
     * 
     * @param rowKey
     * @return boolean
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
    
    @Override
    public String toString() {
      return "startRow: "
          + (startRow == null ? "null" : Bytes.toString(startRow))
          + ", endRow: " + (endRow == null ? "null" : Bytes.toString(endRow));
    }
  }

  private final HRegionInfo regionInfo;
  private final long hLogStartSequenceId;
  private final long transactionId;
  private Status status;
  private SortedSet<byte[]> readSet = new TreeSet<byte[]>(
      Bytes.BYTES_COMPARATOR);
  private List<Put> writeSet = new LinkedList<Put>();
  private List<ScanRange> scanSet = new LinkedList<ScanRange>();
  private Set<TransactionState> transactionsToCheck = new HashSet<TransactionState>();
  private int startSequenceNumber;
  private Integer sequenceNumber;
  private int commitPendingWaits = 0;

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

  void addWrite(final Put write) {
    writeSet.add(write);
  }

  List<Put> getWriteSet() {
    return writeSet;
  }
  
  void addDelete(final Delete delete) {
    throw new UnsupportedOperationException("NYI");
  }

  /**
   * GetFull from the writeSet.
   * 
   * @param row
   * @param columns
   * @param timestamp
   * @return
   */
  Result localGet(Get get) {
    
    List<KeyValue> localKVs = new LinkedList<KeyValue>();
    
    for (Put put : writeSet) {
      if (!Bytes.equals(get.getRow(), put.getRow())) {
        continue;
      }
      if (put.getTimeStamp() > get.getTimeRange().getMax()) {
        continue;
      }
      if (put.getTimeStamp() < get.getTimeRange().getMin()) {
        continue;
      }
      
      for (Entry<byte [], NavigableSet<byte []>> getFamilyEntry : get.getFamilyMap().entrySet()) {
        List<KeyValue> familyPuts = put.getFamilyMap().get(getFamilyEntry.getKey());
        if (familyPuts == null) {
          continue;
        }
        if (getFamilyEntry.getValue() == null){
          localKVs.addAll(familyPuts);
        } else {
          for (KeyValue kv : familyPuts) {
            if (getFamilyEntry.getValue().contains(kv.getQualifier())) {
              localKVs.add(kv);
            }
          }
        }
      }
    }
      
   if (localKVs.isEmpty()) {
     return null;
   }
   return new Result(localKVs);
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

    for (Put otherUpdate : checkAgainst.getWriteSet()) {
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
              + "]: region [" + regionInfo.getRegionNameAsString() + "], scanRange[" +
              scanRange.toString()+"] ,row["
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
   * @param startSequenceNumber
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

  void addScan(Scan scan) {
    ScanRange scanRange = new ScanRange(scan.getStartRow(), scan.getStopRow());
    LOG.trace(String.format(
        "Adding scan for transcaction [%s], from [%s] to [%s]", transactionId,
        scanRange.startRow == null ? "null" : Bytes
            .toString(scanRange.startRow), scanRange.endRow == null ? "null"
            : Bytes.toString(scanRange.endRow)));
    scanSet.add(scanRange);
  }
  
  int getCommitPendingWaits() {
    return commitPendingWaits;
  }
  
  void incrementCommitPendingWaits() {
    this.commitPendingWaits++;
  }

}
