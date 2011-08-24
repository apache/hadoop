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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Keeps track of the columns for a scan if they are not explicitly specified
 */
public class ScanWildcardColumnTracker implements ColumnTracker {
  private static final Log LOG =
    LogFactory.getLog(ScanWildcardColumnTracker.class);
  private byte [] columnBuffer = null;
  private int columnOffset = 0;
  private int columnLength = 0;
  private int currentCount = 0;
  private int maxVersions;
  private int minVersions;
  /* Keeps track of the latest timestamp included for current column.
   * Used to eliminate duplicates. */
  private long latestTSOfCurrentColumn;
  private long oldestStamp;

  /**
   * Return maxVersions of every row.
   * @param minVersion Minimum number of versions to keep
   * @param maxVersion Maximum number of versions to return
   * @param ttl TimeToLive to enforce
   */
  public ScanWildcardColumnTracker(int minVersion, int maxVersion, long ttl) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.oldestStamp = System.currentTimeMillis() - ttl;
  }

  /**
   * Can only return INCLUDE or SKIP, since returning "NEXT" or
   * "DONE" would imply we have finished with this row, when
   * this class can't figure that out.
   *
   * @param bytes
   * @param offset
   * @param length
   * @param timestamp
   * @return The match code instance.
   */
  @Override
  public MatchCode checkColumn(byte[] bytes, int offset, int length,
      long timestamp) {
    if (columnBuffer == null) {
      // first iteration.
      resetBuffer(bytes, offset, length);
      return checkVersion(++currentCount, timestamp);
    }
    int cmp = Bytes.compareTo(bytes, offset, length,
        columnBuffer, columnOffset, columnLength);
    if (cmp == 0) {
      //If column matches, check if it is a duplicate timestamp
      if (sameAsPreviousTS(timestamp)) {
        return ScanQueryMatcher.MatchCode.SKIP;
      }
      return checkVersion(++currentCount, timestamp);
    }

    resetTS();

    // new col > old col
    if (cmp > 0) {
      // switched columns, lets do something.x
      resetBuffer(bytes, offset, length);
      return checkVersion(++currentCount, timestamp);
    }

    // new col < oldcol
    // if (cmp < 0) {
    // WARNING: This means that very likely an edit for some other family
    // was incorrectly stored into the store for this one. Continue, but
    // complain.
    LOG.error("ScanWildcardColumnTracker.checkColumn ran " +
        "into a column actually smaller than the previous column: " +
      Bytes.toStringBinary(bytes, offset, length));
    // switched columns
    resetBuffer(bytes, offset, length);
    return checkVersion(++currentCount, timestamp);
  }

  private void resetBuffer(byte[] bytes, int offset, int length) {
    columnBuffer = bytes;
    columnOffset = offset;
    columnLength = length;
    currentCount = 0;
  }

  private MatchCode checkVersion(int version, long timestamp) {
    if (version > maxVersions) {
      return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL; // skip to next col
    }
    // keep the KV if required by minversions or it is not expired, yet
    if (version <= minVersions || !isExpired(timestamp)) {
      setTS(timestamp);
      return ScanQueryMatcher.MatchCode.INCLUDE;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }

  }

  @Override
  public void update() {
    // no-op, shouldn't even be called
    throw new UnsupportedOperationException(
        "ScanWildcardColumnTracker.update should never be called!");
  }

  @Override
  public void reset() {
    columnBuffer = null;
    resetTS();
  }

  private void resetTS() {
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
  }

  private void setTS(long timestamp) {
    latestTSOfCurrentColumn = timestamp;
  }

  private boolean sameAsPreviousTS(long timestamp) {
    return timestamp == latestTSOfCurrentColumn;
  }

  private boolean isExpired(long timestamp) {
    return timestamp < oldestStamp;
  }

  /**
   * Used by matcher and scan/get to get a hint of the next column
   * to seek to after checkColumn() returns SKIP.  Returns the next interesting
   * column we want, or NULL there is none (wildcard scanner).
   *
   * @return The column count.
   */
  public ColumnCount getColumnHint() {
    return null;
  }


  /**
   * We can never know a-priori if we are done, so always return false.
   * @return false
   */
  @Override
  public boolean done() {
    return false;
  }

  public MatchCode getNextRowOrNextColumn(byte[] bytes, int offset,
      int qualLength) {
    return MatchCode.SEEK_NEXT_COL;
  }

  public boolean isDone(long timestamp) {
    return minVersions <=0 && isExpired(timestamp);
  }

}
