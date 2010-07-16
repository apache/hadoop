/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableSet;

/**
 * A query matcher that is specifically designed for the scan case.
 */
public class ScanQueryMatcher {
  // Optimization so we can skip lots of compares when we decide to skip
  // to the next row.
  private boolean stickyNextRow;
  private byte[] stopRow;

  protected TimeRange tr;

  protected Filter filter;

  /** Keeps track of deletes */
  protected DeleteTracker deletes;

  /** Keeps track of columns and versions */
  protected ColumnTracker columns;

  /** Key to seek to in memstore and StoreFiles */
  protected KeyValue startKey;

  /** Oldest allowed version stamp for TTL enforcement */
  protected long oldestStamp;

  /** Row comparator for the region this query is for */
  KeyValue.KeyComparator rowComparator;

  /** Row the query is on */
  protected byte [] row;

  /**
   * Constructs a ScanQueryMatcher for a Scan.
   * @param scan
   * @param family
   * @param columns
   * @param ttl
   * @param rowComparator
   */
  public ScanQueryMatcher(Scan scan, byte [] family,
      NavigableSet<byte[]> columns, long ttl,
      KeyValue.KeyComparator rowComparator, int maxVersions) {
    this.tr = scan.getTimeRange();
    this.oldestStamp = System.currentTimeMillis() - ttl;
    this.rowComparator = rowComparator;
    this.deletes =  new ScanDeleteTracker();
    this.stopRow = scan.getStopRow();
    this.startKey = KeyValue.createFirstOnRow(scan.getStartRow());
    this.filter = scan.getFilter();

    // Single branch to deal with two types of reads (columns vs all in family)
    if (columns == null || columns.size() == 0) {
      // use a specialized scan for wildcard column tracker.
      this.columns = new ScanWildcardColumnTracker(maxVersions);
    } else {
      // We can share the ExplicitColumnTracker, diff is we reset
      // between rows, not between storefiles.
      this.columns = new ExplicitColumnTracker(columns,maxVersions);
    }
  }

  /**
   * Determines if the caller should do one of several things:
   * - seek/skip to the next row (MatchCode.SEEK_NEXT_ROW)
   * - seek/skip to the next column (MatchCode.SEEK_NEXT_COL)
   * - include the current KeyValue (MatchCode.INCLUDE)
   * - ignore the current KeyValue (MatchCode.SKIP)
   * - got to the next row (MatchCode.DONE)
   *
   * @param kv KeyValue to check
   * @return The match code instance.
   */
  public MatchCode match(KeyValue kv) {
    if (filter != null && filter.filterAllRemaining()) {
      return MatchCode.DONE_SCAN;
    }

    byte [] bytes = kv.getBuffer();
    int offset = kv.getOffset();
    int initialOffset = offset;

    int keyLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
    offset += KeyValue.ROW_OFFSET;

    short rowLength = Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT);
    offset += Bytes.SIZEOF_SHORT;

    int ret = this.rowComparator.compareRows(row, 0, row.length,
        bytes, offset, rowLength);
    if (ret <= -1) {
      return MatchCode.DONE;
    } else if (ret >= 1) {
      // could optimize this, if necessary?
      // Could also be called SEEK_TO_CURRENT_ROW, but this
      // should be rare/never happens.
      return MatchCode.SKIP;
    }

    // optimize case.
    if (this.stickyNextRow)
        return MatchCode.SEEK_NEXT_ROW;

    if (this.columns.done()) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    //Passing rowLength
    offset += rowLength;

    //Skipping family
    byte familyLength = bytes [offset];
    offset += familyLength + 1;

    int qualLength = keyLength + KeyValue.ROW_OFFSET -
      (offset - initialOffset) - KeyValue.TIMESTAMP_TYPE_SIZE;

    long timestamp = kv.getTimestamp();
    if (isExpired(timestamp)) {
      // done, the rest of this column will also be expired as well.
      return MatchCode.SEEK_NEXT_COL;
    }

    byte type = kv.getType();
    if (isDelete(type)) {
      if (tr.withinOrAfterTimeRange(timestamp)) {
        this.deletes.add(bytes, offset, qualLength, timestamp, type);
        // Can't early out now, because DelFam come before any other keys
      }
      // May be able to optimize the SKIP here, if we matched
      // due to a DelFam, we can skip to next row
      // due to a DelCol, we can skip to next col
      // But it requires more info out of isDelete().
      // needful -> million column challenge.
      return MatchCode.SKIP;
    }

    if (!this.deletes.isEmpty() &&
        deletes.isDeleted(bytes, offset, qualLength, timestamp)) {
      return MatchCode.SKIP;
    }

    int timestampComparison = tr.compare(timestamp);
    if (timestampComparison >= 1) {
      return MatchCode.SKIP;
    } else if (timestampComparison <= -1) {
      return getNextRowOrNextColumn(bytes, offset, qualLength);
    }

    /**
     * Filters should be checked before checking column trackers. If we do
     * otherwise, as was previously being done, ColumnTracker may increment its
     * counter for even that KV which may be discarded later on by Filter. This
     * would lead to incorrect results in certain cases.
     */
    if (filter != null) {
      ReturnCode filterResponse = filter.filterKeyValue(kv);
      if (filterResponse == ReturnCode.SKIP) {
        return MatchCode.SKIP;
      } else if (filterResponse == ReturnCode.NEXT_COL) {
        return getNextRowOrNextColumn(bytes, offset, qualLength);
      } else if (filterResponse == ReturnCode.NEXT_ROW) {
        stickyNextRow = true;
        return MatchCode.SEEK_NEXT_ROW;
      }
    }

    MatchCode colChecker = columns.checkColumn(bytes, offset, qualLength);
    // if SKIP -> SEEK_NEXT_COL
    // if (NEXT,DONE) -> SEEK_NEXT_ROW
    // if (INCLUDE) -> INCLUDE
    if (colChecker == MatchCode.SKIP) {
      return MatchCode.SEEK_NEXT_COL;
    } else if (colChecker == MatchCode.NEXT || colChecker == MatchCode.DONE) {
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
    }

    return MatchCode.INCLUDE;

  }

  public MatchCode getNextRowOrNextColumn(byte[] bytes, int offset,
      int qualLength) {
    if (columns instanceof ExplicitColumnTracker) {
      //We only come here when we know that columns is an instance of
      //ExplicitColumnTracker so we should never have a cast exception
      ((ExplicitColumnTracker)columns).doneWithColumn(bytes, offset,
          qualLength);
      if (columns.getColumnHint() == null) {
        return MatchCode.SEEK_NEXT_ROW;
      } else {
        return MatchCode.SEEK_NEXT_COL;
      }
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  public boolean moreRowsMayExistAfter(KeyValue kv) {
    if (!Bytes.equals(stopRow , HConstants.EMPTY_END_ROW) &&
        rowComparator.compareRows(kv.getBuffer(),kv.getRowOffset(),
            kv.getRowLength(), stopRow, 0, stopRow.length) >= 0) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Set current row
   * @param row
   */
  public void setRow(byte [] row) {
    this.row = row;
    reset();
  }

  public void reset() {
    this.deletes.reset();
    this.columns.reset();

    stickyNextRow = false;
  }

  // should be in KeyValue.
  protected boolean isDelete(byte type) {
    return (type != KeyValue.Type.Put.getCode());
  }

  protected boolean isExpired(long timestamp) {
    return (timestamp < oldestStamp);
  }

  /**
   *
   * @return the start key
   */
  public KeyValue getStartKey() {
    return this.startKey;
  }

  /**
   * {@link #match} return codes.  These instruct the scanner moving through
   * memstores and StoreFiles what to do with the current KeyValue.
   * <p>
   * Additionally, this contains "early-out" language to tell the scanner to
   * move on to the next File (memstore or Storefile), or to return immediately.
   */
  public static enum MatchCode {
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
}