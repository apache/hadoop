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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

/**
 * A scanner that iterates through HStore files
 */
class StoreFileScanner extends HAbstractScanner
implements ChangedReadersObserver {
    // Keys retrieved from the sources
  private volatile KeyValue keys[];
  
  // Readers we go against.
  private volatile HFileScanner [] scanners;
  
  // Store this scanner came out of.
  private final Store store;
  
  // Used around replacement of Readers if they change while we're scanning.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final long now = System.currentTimeMillis();

  /**
   * @param store
   * @param timestamp
   * @param columns
   * @param firstRow
   * @param deletes Set of running deletes
   * @throws IOException
   */
  public StoreFileScanner(final Store store, final long timestamp,
    final NavigableSet<byte []> columns, final byte [] firstRow)
  throws IOException {
    super(timestamp, columns);
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
    this.keys = new KeyValue[this.scanners.length];
    // Advance the readers to the first pos.
    KeyValue firstKey = (firstRow != null && firstRow.length > 0)?
      new KeyValue(firstRow, HConstants.LATEST_TIMESTAMP): null;
    for (int i = 0; i < this.scanners.length; i++) {
      if (firstKey != null) {
        if (seekTo(i, firstKey)) {
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
    return columnMatch(keys[i]);
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
  public boolean next(List<KeyValue> results)
  throws IOException {
    if (this.scannerClosed) {
      return false;
    }
    this.lock.readLock().lock();
    try {
      // Find the next viable row label (and timestamp).
      KeyValue viable = getNextViableRow();
      if (viable == null) {
        return false;
      }

      // Grab all the values that match this row/timestamp
      boolean addedItem = false;
      for (int i = 0; i < keys.length; i++) {
        // Fetch the data
        while ((keys[i] != null) &&
            (this.store.comparator.compareRows(this.keys[i], viable) == 0)) {
          // If we are doing a wild card match or there are multiple matchers
          // per column, we need to scan all the older versions of this row
          // to pick up the rest of the family members
          if(!isWildcardScanner()
              && !isMultipleMatchScanner()
              && (keys[i].getTimestamp() != viable.getTimestamp())) {
            break;
          }
          if (columnMatch(i)) {
            // We only want the first result for any specific family member
            // TODO: Do we have to keep a running list of column entries in
            // the results across all of the StoreScanner?  Like we do
            // doing getFull?
            if (!results.contains(keys[i])) {
              results.add(keys[i]);
              addedItem = true;
            }
          }

          if (!getNext(i)) {
            closeSubScanner(i);
          }
        }
        // Advance the current scanner beyond the chosen row, to
        // a valid timestamp, so we're ready next time.
        while ((keys[i] != null) &&
            ((this.store.comparator.compareRows(this.keys[i], viable) <= 0) ||
                (keys[i].getTimestamp() > this.timestamp) ||
                !columnMatch(i))) {
          getNext(i);
        }
      }
      return addedItem;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * @return An instance of <code>ViableRow</code>
   * @throws IOException
   */
  private KeyValue getNextViableRow() throws IOException {
    // Find the next viable row label (and timestamp).
    KeyValue viable = null;
    long viableTimestamp = -1;
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
          && ((viable == null) ||
            (this.store.comparator.compareRows(this.keys[i], viable) < 0) ||
            ((this.store.comparator.compareRows(this.keys[i], viable) == 0) &&
              (keys[i].getTimestamp() > viableTimestamp)))) {
        if (ttl == HConstants.FOREVER || now < keys[i].getTimestamp() + ttl) {
          viable = keys[i];
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("getNextViableRow :" + keys[i] + ": expired, skipped");
          }
        }
      }
    }
    return viable;
  }

  /*
   * The user didn't want to start scanning at the first row. This method
   * seeks to the requested row.
   *
   * @param i which iterator to advance
   * @param firstRow seek to this row
   * @return true if we found the first row and so the scanner is properly
   * primed or true if the row was not found and this scanner is exhausted.
   */
  private boolean seekTo(int i, final KeyValue firstKey)
  throws IOException {
    if (firstKey == null) {
      if (!this.scanners[i].seekTo()) {
        closeSubScanner(i);
        return true;
      }
    } else {
      // TODO: sort columns and pass in column as part of key so we get closer.
      if (!Store.getClosest(this.scanners[i], firstKey)) {
        closeSubScanner(i);
        return true;
      }
    }
    this.keys[i] = this.scanners[i].getKeyValue();
    return isGoodKey(this.keys[i]);
  }

  /**
   * Get the next value from the specified reader.
   * 
   * @param i which reader to fetch next value from
   * @return true if there is more data available
   */
  private boolean getNext(int i) throws IOException {
    boolean result = false;
    while (true) {
      if ((this.scanners[i].isSeeked() && !this.scanners[i].next()) ||
          (!this.scanners[i].isSeeked() && !this.scanners[i].seekTo())) {
        closeSubScanner(i);
        break;
      }
      this.keys[i] = this.scanners[i].getKeyValue();
      if (isGoodKey(this.keys[i])) {
          result = true;
          break;
      }
    }
    return result;
  }

  /*
   * @param kv
   * @return True if good key candidate.
   */
  private boolean isGoodKey(final KeyValue kv) {
    return !Store.isExpired(kv, this.store.ttl, this.now);
  }

  /** Close down the indicated reader. */
  private void closeSubScanner(int i) {
    this.scanners[i] = null;
    this.keys[i] = null;
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
      KeyValue viable = getNextViableRow();
      openScanner(viable.getRow());
      LOG.debug("Replaced Scanner Readers at row " +
        viable.getRow().toString());
    } finally {
      this.lock.writeLock().unlock();
    }
  }
}