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
import java.util.SortedMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.MapFile;

/**
 * A scanner that iterates through HStore files
 */
class StoreFileScanner extends HAbstractScanner
implements ChangedReadersObserver {
    // Keys retrieved from the sources
  private volatile HStoreKey keys[];
  // Values that correspond to those keys
  private volatile byte [][] vals;
  
  // Readers we go against.
  private volatile MapFile.Reader[] readers;
  
  // Store this scanner came out of.
  private final HStore store;
  
  // Used around replacement of Readers if they change while we're scanning.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  /**
   * @param store
   * @param timestamp
   * @param targetCols
   * @param firstRow
   * @throws IOException
   */
  public StoreFileScanner(final HStore store, final long timestamp,
    final byte [][] targetCols, final byte [] firstRow)
  throws IOException {
    super(timestamp, targetCols);
    this.store = store;
    this.store.addChangedReaderObserver(this);
    try {
      openReaders(firstRow);
    } catch (Exception ex) {
      close();
      IOException e = new IOException("HStoreScanner failed construction");
      e.initCause(ex);
      throw e;
    }
  }
  
  /*
   * Go open new Reader iterators and cue them at <code>firstRow</code>.
   * Closes existing Readers if any.
   * @param firstRow
   * @throws IOException
   */
  private void openReaders(final byte [] firstRow) throws IOException {
    if (this.readers != null) {
      for (int i = 0; i < this.readers.length; i++) {
        if (this.readers[i] != null) {
          this.readers[i].close();
        }
      }
    }
    // Open our own copies of the Readers here inside in the scanner.
    this.readers = new MapFile.Reader[this.store.getStorefiles().size()];
    
    // Most recent map file should be first
    int i = readers.length - 1;
    for(HStoreFile curHSF: store.getStorefiles().values()) {
      readers[i--] = curHSF.getReader(store.fs, false, false);
    }
    
    this.keys = new HStoreKey[readers.length];
    this.vals = new byte[readers.length][];
    
    // Advance the readers to the first pos.
    for (i = 0; i < readers.length; i++) {
      keys[i] = new HStoreKey(HConstants.EMPTY_BYTE_ARRAY, this.store.getHRegionInfo());
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
          while ((keys[i] != null)
              && (HStoreKey.compareTwoRowKeys(store.getHRegionInfo(), 
                  keys[i].getRow(), viableRow.getRow()) == 0)) {

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
          while ((keys[i] != null)
              && ((HStoreKey.compareTwoRowKeys(store.getHRegionInfo(), 
                keys[i].getRow(), viableRow.getRow()) <= 0)
                  || (keys[i].getTimestamp() > this.timestamp)
                  || (! columnMatch(i)))) {
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
  class ViableRow {
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
    for(int i = 0; i < keys.length; i++) {
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
          && ((viableRow == null)
              || (HStoreKey.compareTwoRowKeys(store.getHRegionInfo(), 
                  keys[i].getRow(), viableRow) < 0)
              || ((HStoreKey.compareTwoRowKeys(store.getHRegionInfo(),
                  keys[i].getRow(), viableRow) == 0)
                  && (keys[i].getTimestamp() > viableTimestamp)))) {
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

  /**
   * The user didn't want to start scanning at the first row. This method
   * seeks to the requested row.
   *
   * @param i which iterator to advance
   * @param firstRow seek to this row
   * @return true if this is the first row or if the row was not found
   */
  private boolean findFirstRow(int i, final byte [] firstRow) throws IOException {
    ImmutableBytesWritable ibw = new ImmutableBytesWritable();
    HStoreKey firstKey
      = (HStoreKey)readers[i].getClosest(new HStoreKey(firstRow, this.store.getHRegionInfo()), ibw);
    if (firstKey == null) {
      // Didn't find it. Close the scanner and return TRUE
      closeSubScanner(i);
      return true;
    }
    long now = System.currentTimeMillis();
    long ttl = store.ttl;
    if (ttl != HConstants.FOREVER && now >= firstKey.getTimestamp() + ttl) {
      // Didn't find it. Close the scanner and return TRUE
      closeSubScanner(i);
      return true;
    }
    this.vals[i] = ibw.get();
    keys[i].setRow(firstKey.getRow());
    keys[i].setColumn(firstKey.getColumn());
    keys[i].setVersion(firstKey.getTimestamp());
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
    ImmutableBytesWritable ibw = new ImmutableBytesWritable();
    long now = System.currentTimeMillis();
    long ttl = store.ttl;
    while (true) {
      if (!readers[i].next(keys[i], ibw)) {
        closeSubScanner(i);
        break;
      }
      if (keys[i].getTimestamp() <= this.timestamp) {
        if (ttl == HConstants.FOREVER || now < keys[i].getTimestamp() + ttl) {
          vals[i] = ibw.get();
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
    try {
      if(readers[i] != null) {
        try {
          readers[i].close();
        } catch(IOException e) {
          LOG.error(store.storeName + " closing sub-scanner", e);
        }
      }
      
    } finally {
      readers[i] = null;
      keys[i] = null;
      vals[i] = null;
    }
  }

  /** Shut it down! */
  public void close() {
    if (!this.scannerClosed) {
      this.store.deleteChangedReaderObserver(this);
      try {
        for(int i = 0; i < readers.length; i++) {
          if(readers[i] != null) {
            try {
              readers[i].close();
            } catch(IOException e) {
              LOG.error(store.storeName + " closing scanner", e);
            }
          }
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
      openReaders(viableRow.getRow());
      LOG.debug("Replaced Scanner Readers at row " +
        (viableRow == null || viableRow.getRow() == null? "null":
          Bytes.toString(viableRow.getRow())));
    } finally {
      this.lock.writeLock().unlock();
    }
  }
}
