/*
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

package org.apache.hadoop.hbase.regionserver;

import java.util.NavigableSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is the primary class used to process KeyValues during a Get or Scan
 * operation.
 * <p>
 * It encapsulates the handling of the column and version input parameters to 
 * the query through a {@link ColumnTracker}.
 * <p>
 * Deletes are handled using the {@link DeleteTracker}.
 * <p>
 * All other query parameters are accessed from the client-specified Get.
 * <p>
 * The primary method used is {@link #match} with the current KeyValue.  It will
 * return a {@link QueryMatcher.MatchCode} 
 * 
 * , deletes,
 * versions, 
 */
public class QueryMatcher {
  /**
   * {@link #match} return codes.  These instruct the scanner moving through
   * memstores and StoreFiles what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to
   * move on to the next File (memstore or Storefile), or to return immediately.
   */
  static enum MatchCode {
    /**
     * Include KeyValue in the returned result
     */
    INCLUDE,
    
    /**
     * Do not include KeyValue in the returned result
     */
    SKIP,
    
    /**
     * Do not include, jump to next StoreFile or memstore (in time order)
     */
    NEXT,
    
    /**
     * Do not include, return current result
     */
    DONE,

    /**
     * These codes are used by the ScanQueryMatcher
     */

    /**
     * Done with the row, seek there.
     */
    SEEK_NEXT_ROW,
    /**
     * Done with column, seek to next.
     */
    SEEK_NEXT_COL,

    /**
     * Done with scan, thanks to the row filter.
     */
    DONE_SCAN,
  }
  
  /** Keeps track of deletes */
  protected DeleteTracker deletes;
  
  /** Keeps track of columns and versions */
  protected ColumnTracker columns;
  
  /** Key to seek to in memstore and StoreFiles */
  protected KeyValue startKey;
  
  /** Row comparator for the region this query is for */
  KeyComparator rowComparator;
  
  /** Row the query is on */
  protected byte [] row;
  
  /** TimeRange the query is for */
  protected TimeRange tr;
  
  /** Oldest allowed version stamp for TTL enforcement */
  protected long oldestStamp;

  protected Filter filter;

  /**
   * Constructs a QueryMatcher for a Get.
   * @param get
   * @param family
   * @param columns
   * @param ttl
   * @param rowComparator
   */
  public QueryMatcher(Get get, byte [] family, 
      NavigableSet<byte[]> columns, long ttl, KeyComparator rowComparator,
      int maxVersions) {
    this.row = get.getRow();
    this.filter = get.getFilter();
    this.tr = get.getTimeRange();
    this.oldestStamp = System.currentTimeMillis() - ttl;
    this.rowComparator = rowComparator;
    this.deletes =  new GetDeleteTracker();
    this.startKey = KeyValue.createFirstOnRow(row);
    // Single branch to deal with two types of Gets (columns vs all in family)
    if (columns == null || columns.size() == 0) {
      this.columns = new WildcardColumnTracker(maxVersions);
    } else {
      this.columns = new ExplicitColumnTracker(columns, maxVersions);
    }
  }

  // For the subclasses.
  protected QueryMatcher() {
    super();
  }

  /**
   * Constructs a copy of an existing QueryMatcher with a new row.
   * @param matcher
   * @param row
   */
  public QueryMatcher(QueryMatcher matcher, byte [] row) {
    this.row = row;
    this.filter = matcher.filter;
    this.tr = matcher.getTimeRange();
    this.oldestStamp = matcher.getOldestStamp();
    this.rowComparator = matcher.getRowComparator();
    this.columns = matcher.getColumnTracker();
    this.deletes = matcher.getDeleteTracker();
    this.startKey = matcher.getStartKey();
    reset();
  }
  
  /**
   * Main method for ColumnMatcher.
   * <p>
   * Determines whether the specified KeyValue should be included in the
   * result or not.
   * <p>
   * Contains additional language to early-out of the current file or to
   * return immediately.
   * <p>
   * Things to be checked:<ul>
   * <li>Row
   * <li>TTL
   * <li>Type
   * <li>TimeRange
   * <li>Deletes
   * <li>Column
   * <li>Versions
   * @param kv KeyValue to check
   * @return MatchCode: include, skip, next, done
   */
  public MatchCode match(KeyValue kv) {
    if (this.columns.done()) {
      return MatchCode.DONE;  // done_row
    }
    if (this.filter != null && this.filter.filterAllRemaining()) {
      return MatchCode.DONE;
    }
    // Directly act on KV buffer
    byte [] bytes = kv.getBuffer();
    int offset = kv.getOffset();
    
    int keyLength = Bytes.toInt(bytes, offset);
    offset += KeyValue.ROW_OFFSET;
    
    short rowLength = Bytes.toShort(bytes, offset);
    offset += Bytes.SIZEOF_SHORT;

    // scanners are relying on us to check the row first, and return
    // "NEXT" when we are there.
    /* Check ROW
     * If past query's row, go to next StoreFile
     * If not reached query's row, go to next KeyValue
     */ 
    int ret = this.rowComparator.compareRows(row, 0, row.length,
        bytes, offset, rowLength);
    if (ret <= -1) {
      // Have reached the next row
      return MatchCode.NEXT;  // got_to_next_row (end)
    } else if (ret >= 1) {
      // At a previous row
      return MatchCode.SKIP;  // skip_to_cur_row
    }
    offset += rowLength;
    byte familyLength = bytes[offset];
    offset += Bytes.SIZEOF_BYTE + familyLength;
    
    int columnLength = keyLength + KeyValue.ROW_OFFSET -
      (offset - kv.getOffset()) - KeyValue.TIMESTAMP_TYPE_SIZE;
    int columnOffset = offset;
    offset += columnLength;

    /* Check TTL
     * If expired, go to next KeyValue
     */
    long timestamp = Bytes.toLong(bytes, offset);
    if(isExpired(timestamp)) {
      // reached the expired part, for scans, this indicates we're done.
      return MatchCode.NEXT;  // done_row
    }
    offset += Bytes.SIZEOF_LONG;

    /* Check TYPE
     * If a delete within (or after) time range, add to deletes
     * Move to next KeyValue
     */
    byte type = bytes[offset];
    // if delete type == delete family, return done_row
    
    if (isDelete(type)) {
      if (tr.withinOrAfterTimeRange(timestamp)) {
        this.deletes.add(bytes, columnOffset, columnLength, timestamp, type);
      }
      return MatchCode.SKIP;  // skip the delete cell.
    }
    
    /* Check TimeRange
     * If outside of range, move to next KeyValue
     */
    if (!tr.withinTimeRange(timestamp)) {
      return MatchCode.SKIP;  // optimization chances here.
    }

    /* Check Deletes
     * If deleted, move to next KeyValue
     */
    if (!deletes.isEmpty() && deletes.isDeleted(bytes, columnOffset,
        columnLength, timestamp)) {
      // 2 types of deletes:
      // affects 1 cell or 1 column, so just skip the keyvalues.
      // - delete family, so just skip to the next row.
      return MatchCode.SKIP;
    }

    /* Check Column and Versions
     * Returns a MatchCode directly, identical language
     * If matched column without enough versions, include
     * If enough versions of this column or does not match, skip
     * If have moved past 
     * If enough versions of everything, 
     * TODO: No mapping from Filter.ReturnCode to MatchCode.
     */
    MatchCode mc = columns.checkColumn(bytes, columnOffset, columnLength);
    if (mc == MatchCode.INCLUDE && this.filter != null) {
      switch(this.filter.filterKeyValue(kv)) {
        case INCLUDE: return MatchCode.INCLUDE;
        case SKIP: return MatchCode.SKIP;
        default: return MatchCode.DONE;
      }
    }
    return mc;
  }

  // should be in KeyValue.
  protected boolean isDelete(byte type) {
    return (type != KeyValue.Type.Put.getCode());
  }
  
  protected boolean isExpired(long timestamp) {
    return (timestamp < oldestStamp);
  }

  /**
   * If matcher returns SEEK_NEXT_COL you may be able
   * to get a hint of the next column to seek to - call this.
   * If it returns null, there is no hint.
   *
   * @return immediately after match returns SEEK_NEXT_COL - null if no hint,
   *  else the next column we want
   */
  public ColumnCount getSeekColumn() {
    return this.columns.getColumnHint();
  }
  
  /**
   * Called after reading each section (memstore, snapshot, storefiles).
   * <p>
   * This method will update the internal structures to be accurate for
   * the next section. 
   */
  public void update() {
    this.deletes.update();
    this.columns.update();
  }
  
  /**
   * Resets the current columns and deletes
   */
  public void reset() {
    this.deletes.reset();
    this.columns.reset();
    if (this.filter != null) this.filter.reset();
  }

  /**
   * Set current row
   * @param row
   */
  public void setRow(byte [] row) {
    this.row = row;
  }
  
  /**
   * 
   * @return the start key
   */
  public KeyValue getStartKey() {
    return this.startKey;
  }
  
  /**
   * @return the TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  }
  
  /**
   * @return the oldest stamp
   */
  public long getOldestStamp() {
    return this.oldestStamp;
  }
  
  /**
   * @return current KeyComparator
   */
  public KeyComparator getRowComparator() {
    return this.rowComparator;
  }
  
  /**
   * @return ColumnTracker
   */
  public ColumnTracker getColumnTracker() {
    return this.columns;
  }
  
  /**
   * @return DeleteTracker
   */
  public DeleteTracker getDeleteTracker() {
    return this.deletes;
  }
  
  /**
   * 
   * @return <code>true</code> when done.
   */
  public boolean isDone() {
    return this.columns.done();
  }
}
