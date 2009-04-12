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
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Scanner scans both the memcache and the HStore
 */
class StoreScanner implements InternalScanner,  ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);

  private InternalScanner [] scanners;
  private List<KeyValue> [] resultSets;
  private boolean wildcardMatch = false;
  private boolean multipleMatchers = false;
  private RowFilterInterface dataFilter;
  private Store store;
  private final long timestamp;
  private final NavigableSet<byte []> columns;
  
  // Indices for memcache scanner and hstorefile scanner.
  private static final int MEMS_INDEX = 0;
  private static final int HSFS_INDEX = MEMS_INDEX + 1;
  
  // Used around transition from no storefile to the first.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private final AtomicBoolean closing = new AtomicBoolean(false);

  /** Create an Scanner with a handle on the memcache and HStore files. */
  @SuppressWarnings("unchecked")
  StoreScanner(Store store, final NavigableSet<byte []> targetCols,
    byte [] firstRow, long timestamp, RowFilterInterface filter) 
  throws IOException {
    this.store = store;
    this.dataFilter = filter;
    if (null != dataFilter) {
      dataFilter.reset();
    }
    this.scanners = new InternalScanner[2];
    this.resultSets = new List[scanners.length];
    // Save these args in case we need them later handling change in readers
    // See updateReaders below.
    this.timestamp = timestamp;
    this.columns = targetCols;
    try {
      scanners[MEMS_INDEX] =
        store.memcache.getScanner(timestamp, targetCols, firstRow);
      scanners[HSFS_INDEX] =
        new StoreFileScanner(store, timestamp, targetCols, firstRow);
      for (int i = MEMS_INDEX; i < scanners.length; i++) {
        checkScannerFlags(i);
      }
    } catch (IOException e) {
      doClose();
      throw e;
    }
    
    // Advance to the first key in each scanner.
    // All results will match the required column-set and scanTime.
    for (int i = MEMS_INDEX; i < scanners.length; i++) {
      setupScanner(i);
    }
    this.store.addChangedReaderObserver(this);
  }
  
  /*
   * @param i Index.
   */
  private void checkScannerFlags(final int i) {
    if (this.scanners[i].isWildcardScanner()) {
      this.wildcardMatch = true;
    }
    if (this.scanners[i].isMultipleMatchScanner()) {
      this.multipleMatchers = true;
    }
  }
  
  /*
   * Do scanner setup.
   * @param i
   * @throws IOException
   */
  private void setupScanner(final int i) throws IOException {
    this.resultSets[i] = new ArrayList<KeyValue>();
    if (this.scanners[i] != null && !this.scanners[i].next(this.resultSets[i])) {
      closeScanner(i);
    }
  }

  /** @return true if the scanner is a wild card scanner */
  public boolean isWildcardScanner() {
    return this.wildcardMatch;
  }

  /** @return true if the scanner is a multiple match scanner */
  public boolean isMultipleMatchScanner() {
    return this.multipleMatchers;
  }

  public boolean next(List<KeyValue> results)
  throws IOException {
    this.lock.readLock().lock();
    try {
    // Filtered flag is set by filters.  If a cell has been 'filtered out'
    // -- i.e. it is not to be returned to the caller -- the flag is 'true'.
    boolean filtered = true;
    boolean moreToFollow = true;
    while (filtered && moreToFollow) {
      // Find the lowest-possible key.
      KeyValue chosen = null;
      long chosenTimestamp = -1;
      for (int i = 0; i < this.scanners.length; i++) {
        KeyValue kv = this.resultSets[i] == null || this.resultSets[i].isEmpty()?
          null: this.resultSets[i].get(0);
        if (kv == null) {
          continue;
        }
        if (scanners[i] != null &&
            (chosen == null ||
              (this.store.comparator.compareRows(kv, chosen) < 0) ||
              ((this.store.comparator.compareRows(kv, chosen) == 0) &&
              (kv.getTimestamp() > chosenTimestamp)))) {
          chosen = kv;
          chosenTimestamp = chosen.getTimestamp();
        }
      }

      // Filter whole row by row key?
      filtered = dataFilter == null || chosen == null? false:
        dataFilter.filterRowKey(chosen.getBuffer(), chosen.getRowOffset(),
          chosen.getRowLength());

      // Store results for each sub-scanner.
      if (chosenTimestamp >= 0 && !filtered) {
        NavigableSet<KeyValue> deletes =
          new TreeSet<KeyValue>(this.store.comparatorIgnoringType);
        for (int i = 0; i < scanners.length && !filtered; i++) {
          if ((scanners[i] != null && !filtered && moreToFollow &&
              this.resultSets[i] != null && !this.resultSets[i].isEmpty())) {
            // Test this resultset is for the 'chosen' row.
            KeyValue firstkv = resultSets[i].get(0);
            if (!this.store.comparator.matchingRows(firstkv, chosen)) {
              continue;
            }
            // Its for the 'chosen' row, work it.
            for (KeyValue kv: resultSets[i]) {
              if (kv.isDeleteType()) {
                deletes.add(kv);
              } else if ((deletes.isEmpty() || !deletes.contains(kv)) &&
                  !filtered && moreToFollow && !results.contains(kv)) {
                if (this.dataFilter != null) {
                  // Filter whole row by column data?
                  int rowlength = kv.getRowLength();
                  int columnoffset = kv.getColumnOffset(rowlength);
                  filtered = dataFilter.filterColumn(kv.getBuffer(),
                      kv.getRowOffset(), rowlength,
                    kv.getBuffer(), columnoffset, kv.getColumnLength(columnoffset),
                    kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
                  if (filtered) {
                    results.clear();
                    break;
                  }
                }
                results.add(kv);
                /* REMOVING BECAUSE COULD BE BUNCH OF DELETES IN RESULTS
                   AND WE WANT TO INCLUDE THEM -- below short-circuit is
                   probably not wanted.
                // If we are doing a wild card match or there are multiple
                // matchers per column, we need to scan all the older versions of 
                // this row to pick up the rest of the family members
                if (!wildcardMatch && !multipleMatchers &&
                    (kv.getTimestamp() != chosenTimestamp)) {
                  break;
                }
                */
              }
            }
            // Move on to next row.
            resultSets[i].clear();
            if (!scanners[i].next(resultSets[i])) {
              closeScanner(i);
            }
          }
        }
      }

      moreToFollow = chosenTimestamp >= 0;
      if (dataFilter != null) {
        if (dataFilter.filterAllRemaining()) {
          moreToFollow = false;
        }
      }

      if (results.isEmpty() && !filtered) {
        // There were no results found for this row.  Marked it as 
        // 'filtered'-out otherwise we will not move on to the next row.
        filtered = true;
      }
    }
    
    // If we got no results, then there is no more to follow.
    if (results == null || results.isEmpty()) {
      moreToFollow = false;
    }
    
    // Make sure scanners closed if no more results
    if (!moreToFollow) {
      for (int i = 0; i < scanners.length; i++) {
        if (null != scanners[i]) {
          closeScanner(i);
        }
      }
    }
    
    return moreToFollow;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /** Shut down a single scanner */
  void closeScanner(int i) {
    try {
      try {
        scanners[i].close();
      } catch (IOException e) {
        LOG.warn(Bytes.toString(store.storeName) + " failed closing scanner " +
          i, e);
      }
    } finally {
      scanners[i] = null;
      resultSets[i] = null;
    }
  }

  public void close() {
    this.closing.set(true);
    this.store.deleteChangedReaderObserver(this);
    doClose();
  }
  
  private void doClose() {
    for (int i = MEMS_INDEX; i < scanners.length; i++) {
      if (scanners[i] != null) {
        closeScanner(i);
      }
    }
  }
  
  // Implementation of ChangedReadersObserver
  
  public void updateReaders() throws IOException {
    if (this.closing.get()) {
      return;
    }
    this.lock.writeLock().lock();
    try {
      Map<Long, StoreFile> map = this.store.getStorefiles();
      if (this.scanners[HSFS_INDEX] == null && map != null && map.size() > 0) {
        // Presume that we went from no readers to at least one -- need to put
        // a HStoreScanner in place.
        try {
          // I think its safe getting key from mem at this stage -- it shouldn't have
          // been flushed yet
          // TODO: MAKE SURE WE UPDATE FROM TRUNNK.
          this.scanners[HSFS_INDEX] = new StoreFileScanner(this.store,
              this.timestamp, this. columns, this.resultSets[MEMS_INDEX].get(0).getRow());
          checkScannerFlags(HSFS_INDEX);
          setupScanner(HSFS_INDEX);
          LOG.debug("Added a StoreFileScanner to outstanding HStoreScanner");
        } catch (IOException e) {
          doClose();
          throw e;
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }
}