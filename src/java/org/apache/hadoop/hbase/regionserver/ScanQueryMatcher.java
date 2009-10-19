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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A query matcher that is specifically designed for the scan case.
 */
public class ScanQueryMatcher extends QueryMatcher {
  // Optimization so we can skip lots of compares when we decide to skip
  // to the next row.
  private boolean stickyNextRow;

  /**
   * Constructs a QueryMatcher for a Scan.
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
      // done, the rest wil also be expired as well.
      stickyNextRow = true;
      return MatchCode.SEEK_NEXT_ROW;
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

    if (!tr.withinTimeRange(timestamp)) {
      return MatchCode.SKIP;
    }

    if (!this.deletes.isEmpty() &&
        deletes.isDeleted(bytes, offset, qualLength, timestamp)) {
      return MatchCode.SKIP;
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

    // else INCLUDE
    // if (colChecker == MatchCode.INCLUDE)
    // give the filter a chance to run.
    if (filter == null)
      return MatchCode.INCLUDE;

    ReturnCode filterResponse = filter.filterKeyValue(kv);
    if (filterResponse == ReturnCode.INCLUDE)
      return MatchCode.INCLUDE;

    if (filterResponse == ReturnCode.SKIP)
      return MatchCode.SKIP;

    // else if (filterResponse == ReturnCode.NEXT_ROW)
    stickyNextRow = true;
    return MatchCode.SEEK_NEXT_ROW;
  }

  /**
   * Set current row
   * @param row
   */
  @Override
  public void setRow(byte [] row) {
    this.row = row;
    reset();
  }

  @Override
  public void reset() {
    super.reset();
    stickyNextRow = false;
  }
}