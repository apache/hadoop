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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.MapFile;

/**
 * Scanner scans both the memcache and the HStore
 */
class HStoreScanner implements InternalScanner,  ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(HStoreScanner.class);

  private InternalScanner[] scanners;
  private TreeMap<byte [], Cell>[] resultSets;
  private HStoreKey[] keys;
  private boolean wildcardMatch = false;
  private boolean multipleMatchers = false;
  private RowFilterInterface dataFilter;
  private HStore store;
  private final long timestamp;
  private final byte [][] targetCols;
  
  // Indices for memcache scanner and hstorefile scanner.
  private static final int MEMS_INDEX = 0;
  private static final int HSFS_INDEX = MEMS_INDEX + 1;
  
  // Used around transition from no storefile to the first.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  /** Create an Scanner with a handle on the memcache and HStore files. */
  @SuppressWarnings("unchecked")
  HStoreScanner(HStore store, byte [][] targetCols, byte [] firstRow,
    long timestamp, RowFilterInterface filter) 
  throws IOException {
    this.store = store;
    this.dataFilter = filter;
    if (null != dataFilter) {
      dataFilter.reset();
    }
    this.scanners = new InternalScanner[2];
    this.resultSets = new TreeMap[scanners.length];
    this.keys = new HStoreKey[scanners.length];
    // Save these args in case we need them later handling change in readers
    // See updateReaders below.
    this.timestamp = timestamp;
    this.targetCols = targetCols;

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
    this.keys[i] = new HStoreKey();
    this.resultSets[i] = new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
    if (this.scanners[i] != null && !this.scanners[i].next(this.keys[i], this.resultSets[i])) {
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

  public boolean next(HStoreKey key, SortedMap<byte [], Cell> results)
  throws IOException {
    this.lock.readLock().lock();
    try {
    // Filtered flag is set by filters.  If a cell has been 'filtered out'
    // -- i.e. it is not to be returned to the caller -- the flag is 'true'.
    boolean filtered = true;
    boolean moreToFollow = true;
    while (filtered && moreToFollow) {
      // Find the lowest-possible key.
      byte [] chosenRow = null;
      long chosenTimestamp = -1;
      for (int i = 0; i < this.keys.length; i++) {
        if (scanners[i] != null &&
            (chosenRow == null ||
            (HStoreKey.compareTwoRowKeys(store.getHRegionInfo(),
              keys[i].getRow(), chosenRow) < 0) ||
            ((HStoreKey.compareTwoRowKeys(store.getHRegionInfo(),
              keys[i].getRow(), chosenRow) == 0) &&
            (keys[i].getTimestamp() > chosenTimestamp)))) {
          chosenRow = keys[i].getRow();
          chosenTimestamp = keys[i].getTimestamp();
        }
      }
      
      // Filter whole row by row key?
      filtered = dataFilter != null? dataFilter.filterRowKey(chosenRow) : false;

      // Store the key and results for each sub-scanner. Merge them as
      // appropriate.
      if (chosenTimestamp >= 0 && !filtered) {
        // Here we are setting the passed in key with current row+timestamp
        key.setRow(chosenRow);
        key.setVersion(chosenTimestamp);
        key.setColumn(HConstants.EMPTY_BYTE_ARRAY);
        // Keep list of deleted cell keys within this row.  We need this
        // because as we go through scanners, the delete record may be in an
        // early scanner and then the same record with a non-delete, non-null
        // value in a later. Without history of what we've seen, we'll return
        // deleted values. This List should not ever grow too large since we
        // are only keeping rows and columns that match those set on the
        // scanner and which have delete values.  If memory usage becomes a
        // problem, could redo as bloom filter.
        Set<HStoreKey> deletes = new HashSet<HStoreKey>();
        for (int i = 0; i < scanners.length && !filtered; i++) {
          while ((scanners[i] != null
              && !filtered
              && moreToFollow)
              && (HStoreKey.compareTwoRowKeys(store.getHRegionInfo(),
                keys[i].getRow(), chosenRow) == 0)) {
            // If we are doing a wild card match or there are multiple
            // matchers per column, we need to scan all the older versions of 
            // this row to pick up the rest of the family members
            if (!wildcardMatch
                && !multipleMatchers
                && (keys[i].getTimestamp() != chosenTimestamp)) {
              break;
            }

            // NOTE: We used to do results.putAll(resultSets[i]);
            // but this had the effect of overwriting newer
            // values with older ones. So now we only insert
            // a result if the map does not contain the key.
            HStoreKey hsk = new HStoreKey(key.getRow(),
              HConstants.EMPTY_BYTE_ARRAY,
              key.getTimestamp(), this.store.getHRegionInfo());
            for (Map.Entry<byte [], Cell> e : resultSets[i].entrySet()) {
              hsk.setColumn(e.getKey());
              if (HLogEdit.isDeleted(e.getValue().getValue())) {
                // Only first key encountered is added; deletes is a Set.
                deletes.add(new HStoreKey(hsk));
              } else if (!deletes.contains(hsk) &&
                  !filtered &&
                  moreToFollow &&
                  !results.containsKey(e.getKey())) {
                if (dataFilter != null) {
                  // Filter whole row by column data?
                  filtered = dataFilter.filterColumn(chosenRow, e.getKey(),
                      e.getValue().getValue());
                  if (filtered) {
                    results.clear();
                    break;
                  }
                }
                results.put(e.getKey(), e.getValue());
              }
            }
            resultSets[i].clear();
            if (!scanners[i].next(keys[i], resultSets[i])) {
              closeScanner(i);
            }
          }
        }          
      }
      
      for (int i = 0; i < scanners.length; i++) {
        // If the current scanner is non-null AND has a lower-or-equal
        // row label, then its timestamp is bad. We need to advance it.
        while ((scanners[i] != null) &&
            (HStoreKey.compareTwoRowKeys(store.getHRegionInfo(), 
              keys[i].getRow(), chosenRow) <= 0)) {
          resultSets[i].clear();
          if (!scanners[i].next(keys[i], resultSets[i])) {
            closeScanner(i);
          }
        }
      }

      moreToFollow = chosenTimestamp >= 0;
      
      if (dataFilter != null) {
        if (dataFilter.filterAllRemaining()) {
          moreToFollow = false;
        }
      }
      
      if (results.size() <= 0 && !filtered) {
        // There were no results found for this row.  Marked it as 
        // 'filtered'-out otherwise we will not move on to the next row.
        filtered = true;
      }
    }
    
    // If we got no results, then there is no more to follow.
    if (results == null || results.size() <= 0) {
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
        LOG.warn(store.storeName + " failed closing scanner " + i, e);
      }
    } finally {
      scanners[i] = null;
      keys[i] = null;
      resultSets[i] = null;
    }
  }

  public void close() {
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
    this.lock.writeLock().lock();
    try {
      MapFile.Reader [] readers = this.store.getReaders();
      if (this.scanners[HSFS_INDEX] == null && readers != null &&
          readers.length > 0) {
        // Presume that we went from no readers to at least one -- need to put
        // a HStoreScanner in place.
        try {
          // I think its safe getting key from mem at this stage -- it shouldn't have
          // been flushed yet
          this.scanners[HSFS_INDEX] = new StoreFileScanner(this.store,
              this.timestamp, this. targetCols, this.keys[MEMS_INDEX].getRow());
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