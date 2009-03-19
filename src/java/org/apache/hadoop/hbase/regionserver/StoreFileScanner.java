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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A scanner that iterates through HStore files
 */
class StoreFileScanner extends HAbstractScanner
implements ChangedReadersObserver {
    // Keys retrieved from the sources
  private volatile HStoreKey keys[];
  // Values that correspond to those keys
  private ByteBuffer [] vals;
  
  // Readers we go against.
  private volatile HFileScanner [] scanners;
  
  // Store this scanner came out of.
  private final Store store;
  
  // Used around replacement of Readers if they change while we're scanning.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  /**
   * @param store
   * @param timestamp
   * @param targetCols
   * @param firstRow
   * @throws IOException
   */
  public StoreFileScanner(final Store store, final long timestamp,
    final byte [][] targetCols, final byte [] firstRow)
  throws IOException {
    super(timestamp, targetCols);
    this.store = store;
    this.store.addChangedReaderObserver(this);
    try {
      openScanner(firstRow);
    } catch (Exception ex) {
      close();
      IOException e = new IOException("HStoreScanner failed construction");
      e.initCause(ex);
      throw e;
    }
  }
  
  /*
   * Go open new scanners and cue them at <code>firstRow</code>.
   * Closes existing Readers if any.
   * @param firstRow
   * @throws IOException
   */
  private void openScanner(final byte [] firstRow) throws IOException {
    List<HFileScanner> s =
      new ArrayList<HFileScanner>(this.store.getStorefiles().size());
    Map<Long, StoreFile> map = this.store.getStorefiles().descendingMap();
    for (StoreFile f: map.values()) {
       s.add(f.getReader().getScanner());
    }
    this.scanners = s.toArray(new HFileScanner [] {});
    this.keys = new HStoreKey[this.scanners.length];
    this.vals = new ByteBuffer[this.scanners.length];
    // Advance the readers to the first pos.
    for (int i = 0; i < this.scanners.length; i++) {
      if (firstRow != null && firstRow.length != 0) {
        if (findFirstRow(i, firstRow)) {
          continue;
        }
      }
      while (getNext(i)) {
        if (columnMatch(i)) {
          break;
        }
      }
    }
  }

  /**
   * For a particular column i, find all the matchers defined for the column.
   * Compare the column family and column key using the matchers. The first one
   * that matches returns true. If no matchers are successful, return false.
   * 
   * @param i index into the keys array
   * @return true if any of the matchers for the column match the column family
   * and the column key.
   * @throws IOException
   */
  boolean columnMatch(int i) throws IOException {
    return columnMatch(keys[i].getColumn());
  }

  /**
   * Get the next set of values for this scanner.
   * 
   * @param key The key that matched
   * @param results All the results for <code>key</code>
   * @return true if a match was found
   * @throws IOException
   * 
   * @see org.apache.hadoop.hbase.regionserver.InternalScanner#next(org.apache.hadoop.hbase.HStoreKey, java.util.SortedMap)
   */
  @Override
  public boolean next(HStoreKey key, SortedMap<byte [], Cell> results)
  throws IOException {
    if (this.scannerClosed) {
      return false;
    }
    this.lock.readLock().lock();
    try {
      // Find the next viable row label (and timestamp).
      ViableRow viableRow = getNextViableRow();

      // Grab all the values that match this row/timestamp
      boolean insertedItem = false;
      if (viableRow.getRow() != null) {
        key.setRow(viableRow.getRow());
        key.setVersion(viableRow.getTimestamp());
        for (int i = 0; i < keys.length; i++) {
          // Fetch the data
          while ((keys[i] != null) &&
            (this.store.rawcomparator.compareRows(this.keys[i].getRow(),
                viableRow.getRow()) == 0)) {
            // If we are doing a wild card match or there are multiple matchers
            // per column, we need to scan all the older versions of this row
            // to pick up the rest of the family members
            if(!isWildcardScanner()
                && !isMultipleMatchScanner()
                && (keys[i].getTimestamp() != viableRow.getTimestamp())) {
              break;
            }
            if(columnMatch(i)) {
              // We only want the first result for any specific family member
              if(!results.containsKey(keys[i].getColumn())) {
                results.put(keys[i].getColumn(), 
                    new Cell(vals[i], keys[i].getTimestamp()));
                insertedItem = true;
              }
            }

            if (!getNext(i)) {
              closeSubScanner(i);
            }
          }
          // Advance the current scanner beyond the chosen row, to
          // a valid timestamp, so we're ready next time.
          while ((keys[i] != null) &&
            ((this.store.rawcomparator.compareRows(this.keys[i].getRow(),
                viableRow.getRow()) <= 0) ||
              (keys[i].getTimestamp() > this.timestamp) ||
              (! columnMatch(i)))) {
            getNext(i);
          }
        }
      }
      return insertedItem;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  // Data stucture to hold next, viable row (and timestamp).
  static class ViableRow {
    private final byte [] row;
    private final long ts;

    ViableRow(final byte [] r, final long t) {
      this.row = r;
      this.ts = t;
    }

    byte [] getRow() {
      return this.row;
    }

    long getTimestamp() {
      return this.ts;
    }
  }

  /*
   * @return An instance of <code>ViableRow</code>
   * @throws IOException
   */
  private ViableRow getNextViableRow() throws IOException {
    // Find the next viable row label (and timestamp).
    byte [] viableRow = null;
    long viableTimestamp = -1;
    long now = System.currentTimeMillis();
    long ttl = store.ttl;
    for (int i = 0; i < keys.length; i++) {
      // The first key that we find that matches may have a timestamp greater
      // than the one we're looking for. We have to advance to see if there
      // is an older version present, since timestamps are sorted descending
      while (keys[i] != null &&
          keys[i].getTimestamp() > this.timestamp &&
          columnMatch(i) &&
          getNext(i)) {
        if (columnMatch(i)) {
          break;
        }
      }
      if((keys[i] != null)
          // If we get here and keys[i] is not null, we already know that the
          // column matches and the timestamp of the row is less than or equal
          // to this.timestamp, so we do not need to test that here
          && ((viableRow == null) ||
            (this.store.rawcomparator.compareRows(this.keys[i].getRow(),
              viableRow) < 0) ||
            ((this.store.rawcomparator.compareRows(this.keys[i].getRow(),
                viableRow) == 0) &&
              (keys[i].getTimestamp() > viableTimestamp)))) {
        if (ttl == HConstants.FOREVER || now < keys[i].getTimestamp() + ttl) {
          viableRow = keys[i].getRow();
          viableTimestamp = keys[i].getTimestamp();
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("getNextViableRow :" + keys[i] + ": expired, skipped");
          }
        }
      }
    }
    return new ViableRow(viableRow, viableTimestamp);
  }

  /*
   * The user didn't want to start scanning at the first row. This method
   * seeks to the requested row.
   *
   * @param i which iterator to advance
   * @param firstRow seek to this row
   * @return true if this is the first row or if the row was not found
   */
  private boolean findFirstRow(int i, final byte [] firstRow) throws IOException {
    if (firstRow == null || firstRow.length <= 0) {
      if (!this.scanners[i].seekTo()) {
        closeSubScanner(i);
        return true;
      }
    } else {
      if (!Store.getClosest(this.scanners[i], HStoreKey.getBytes(firstRow))) {
        closeSubScanner(i);
        return true;
      }
    }
    this.keys[i] = HStoreKey.create(this.scanners[i].getKey());
    this.vals[i] = this.scanners[i].getValue();
    long now = System.currentTimeMillis();
    long ttl = store.ttl;
    if (ttl != HConstants.FOREVER && now >= this.keys[i].getTimestamp() + ttl) {
      // Didn't find it. Close the scanner and return TRUE
      closeSubScanner(i);
      return true;
    }
    return columnMatch(i);
  }

  /**
   * Get the next value from the specified reader.
   * 
   * @param i which reader to fetch next value from
   * @return true if there is more data available
   */
  private boolean getNext(int i) throws IOException {
    boolean result = false;
    long now = System.currentTimeMillis();
    long ttl = store.ttl;
    while (true) {
      if ((this.scanners[i].isSeeked() && !this.scanners[i].next()) ||
          (!this.scanners[i].isSeeked() && !this.scanners[i].seekTo())) {
        closeSubScanner(i);
        break;
      }
      this.keys[i] = HStoreKey.create(this.scanners[i].getKey());
      if (keys[i].getTimestamp() <= this.timestamp) {
        if (ttl == HConstants.FOREVER || now < keys[i].getTimestamp() + ttl) {
          vals[i] = this.scanners[i].getValue();
          result = true;
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("getNext: " + keys[i] + ": expired, skipped");
        }
      }
    }
    return result;
  }

  /** Close down the indicated reader. */
  private void closeSubScanner(int i) {
    this.scanners[i] = null;
    this.keys[i] = null;
    this.vals[i] = null;
  }

  /** Shut it down! */
  public void close() {
    if (!this.scannerClosed) {
      this.store.deleteChangedReaderObserver(this);
      try {
        for(int i = 0; i < this.scanners.length; i++) {
          closeSubScanner(i);
        }
      } finally {
        this.scannerClosed = true;
      }
    }
  }

  // Implementation of ChangedReadersObserver
  
  public void updateReaders() throws IOException {
    this.lock.writeLock().lock();
    try {
      // The keys are currently lined up at the next row to fetch.  Pass in
      // the current row as 'first' row and readers will be opened and cue'd
      // up so future call to next will start here.
      ViableRow viableRow = getNextViableRow();
      openScanner(viableRow.getRow());
      LOG.debug("Replaced Scanner Readers at row " +
        (viableRow == null || viableRow.getRow() == null? "null":
          Bytes.toString(viableRow.getRow())));
    } finally {
      this.lock.writeLock().unlock();
    }
  }
}