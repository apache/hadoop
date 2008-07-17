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

package org.apache.hadoop.hbase.util.migration.v5;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.regionserver.HAbstractScanner;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * The Memcache holds in-memory modifications to the HRegion.
 * Keeps a current map.  When asked to flush the map, current map is moved
 * to snapshot and is cleared.  We continue to serve edits out of new map
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 */
class Memcache {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  
  private long ttl;

  // Note that since these structures are always accessed with a lock held,
  // so no additional synchronization is required.
  
  // The currently active sorted map of edits.
  private volatile SortedMap<HStoreKey, byte[]> memcache =
    createSynchronizedSortedMap();

  // Snapshot of memcache.  Made for flusher.
  private volatile SortedMap<HStoreKey, byte[]> snapshot =
    createSynchronizedSortedMap();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Default constructor. Used for tests.
   */
  public Memcache()
  {
    ttl = HConstants.FOREVER;
  }

  /**
   * Constructor.
   * @param ttl The TTL for cache entries, in milliseconds.
   */
  public Memcache(long ttl) {
    this.ttl = ttl;
  }

  /*
   * Utility method.
   * @return sycnhronized sorted map of HStoreKey to byte arrays.
   */
  private static SortedMap<HStoreKey, byte[]> createSynchronizedSortedMap() {
    return Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
  }

  /**
   * Creates a snapshot of the current Memcache.
   * Snapshot must be cleared by call to {@link #clearSnapshot(SortedMap)}
   * To get the snapshot made by this method, use
   * {@link #getSnapshot}.
   */
  void snapshot() {
    this.lock.writeLock().lock();
    try {
      // If snapshot currently has entries, then flusher failed or didn't call
      // cleanup.  Log a warning.
      if (this.snapshot.size() > 0) {
        LOG.debug("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
      } else {
        // We used to synchronize on the memcache here but we're inside a
        // write lock so removed it. Comment is left in case removal was a
        // mistake. St.Ack
        if (this.memcache.size() != 0) {
          this.snapshot = this.memcache;
          this.memcache = createSynchronizedSortedMap();
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  /**
   * Return the current snapshot.
   * Called by flusher to get current snapshot made by a previous
   * call to {@link snapshot}.
   * @return Return snapshot.
   * @see {@link #snapshot()}
   * @see {@link #clearSnapshot(SortedMap)}
   */
  SortedMap<HStoreKey, byte[]> getSnapshot() {
    return this.snapshot;
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param ss The snapshot to clean out.
   * @throws UnexpectedException
   * @see {@link #snapshot()}
   */
  void clearSnapshot(final SortedMap<HStoreKey, byte []> ss)
  throws UnexpectedException {
    this.lock.writeLock().lock();
    try {
      if (this.snapshot != ss) {
        throw new UnexpectedException("Current snapshot is " +
          this.snapshot + ", was passed " + ss);
      }
      // OK. Passed in snapshot is same as current snapshot.  If not-empty,
      // create a new snapshot and let the old one go.
      if (ss.size() != 0) {
        this.snapshot = createSynchronizedSortedMap();
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Write an update
   * @param key
   * @param value
   * @return memcache size delta
   */
  long add(final HStoreKey key, final byte[] value) {
    this.lock.readLock().lock();
    try {
      byte[] oldValue = this.memcache.remove(key);
      this.memcache.put(key, value);
      return key.getSize() + (value == null ? 0 : value.length) -
          (oldValue == null ? 0 : oldValue.length);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Look back through all the backlog TreeMaps to find the target.
   * @param key
   * @param numVersions
   * @return An array of byte arrays ordered by timestamp.
   */
  List<Cell> get(final HStoreKey key, final int numVersions) {
    this.lock.readLock().lock();
    try {
      List<Cell> results;
      // The synchronizations here are because internalGet iterates
      synchronized (this.memcache) {
        results = internalGet(this.memcache, key, numVersions);
      }
      synchronized (this.snapshot) {
        results.addAll(results.size(),
          internalGet(this.snapshot, key, numVersions - results.size()));
      }
      return results;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  @SuppressWarnings("unchecked")
  private byte [] getLowest(final byte [] a,
      final byte [] b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return Bytes.compareTo(a, b) <= 0? a: b;
  }

  /**
   * @param row Find the row that comes after this one.
   * @return Next row or null if none found
   */
  byte [] getNextRow(final byte [] row) {
    this.lock.readLock().lock();
    try {
      return getLowest(getNextRow(row, this.memcache),
        getNextRow(row, this.snapshot));
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /*
   * @param row Find row that follows this one.
   * @param map Map to look in for a row beyond <code>row</code>.
   * This method synchronizes on passed map while iterating it.
   * @return Next row or null if none found.
   */
  private byte [] getNextRow(final byte [] row,
      final SortedMap<HStoreKey, byte []> map) {
    byte [] result = null;
    // Synchronize on the map to make the tailMap making 'safe'.
    synchronized (map) {
      // Make an HSK with maximum timestamp so we get past most of the current
      // rows cell entries.
      HStoreKey hsk = new HStoreKey(row, HConstants.LATEST_TIMESTAMP);
      SortedMap<HStoreKey, byte []> tailMap = map.tailMap(hsk);
      // Iterate until we fall into the next row; i.e. move off current row
      for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
        HStoreKey itKey = es.getKey();
        if (Bytes.compareTo(itKey.getRow(), row) <= 0) {
          continue;
        }
        // Note: Not suppressing deletes or expired cells.
        result = itKey.getRow();
        break;
      }
    }
    return result;
  }

  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   * @param key
   * @param columns Pass null for all columns else the wanted subset.
   * @param deletes Map to accumulate deletes found.
   * @param results Where to stick row results found.
   */
  void getFull(HStoreKey key, Set<byte []> columns, Map<byte [], Long> deletes, 
    Map<byte [], Cell> results) {
    this.lock.readLock().lock();
    try {
      // The synchronizations here are because internalGet iterates
      synchronized (this.memcache) {
        internalGetFull(this.memcache, key, columns, deletes, results);
      }
      synchronized (this.snapshot) {
        internalGetFull(this.snapshot, key, columns, deletes, results);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void internalGetFull(SortedMap<HStoreKey, byte[]> map, HStoreKey key, 
      Set<byte []> columns, Map<byte [], Long> deletes,
      Map<byte [], Cell> results) {
    if (map.isEmpty() || key == null) {
      return;
    }
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte[]> tailMap = map.tailMap(key);
    long now = System.currentTimeMillis();
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      byte [] itCol = itKey.getColumn();
      if (results.get(itCol) == null && key.matchesWithoutColumn(itKey)) {
        if (columns == null || columns.contains(itKey.getColumn())) {
          byte [] val = tailMap.get(itKey);
          if (HLogEdit.isDeleted(val)) {
            if (!deletes.containsKey(itCol) 
              || deletes.get(itCol).longValue() < itKey.getTimestamp()) {
              deletes.put(itCol, Long.valueOf(itKey.getTimestamp()));
            }
          } else if (!(deletes.containsKey(itCol) 
              && deletes.get(itCol).longValue() >= itKey.getTimestamp())) {
            // Skip expired cells
            if (ttl == HConstants.FOREVER ||
                  now < itKey.getTimestamp() + ttl) {
              results.put(itCol, new Cell(val, itKey.getTimestamp()));
            } else {
              victims.add(itKey);
              if (LOG.isDebugEnabled()) {
                LOG.debug("internalGetFull: " + itKey + ": expired, skipped");
              }
            }
          }
        }
      } else if (Bytes.compareTo(key.getRow(), itKey.getRow()) < 0) {
        break;
      }
    }
    // Remove expired victims from the map.
    for (HStoreKey v: victims)
      map.remove(v);
  }

  /**
   * @param row Row to look for.
   * @param candidateKeys Map of candidate keys (Accumulation over lots of
   * lookup over stores and memcaches)
   */
  void getRowKeyAtOrBefore(final byte [] row, 
    SortedMap<HStoreKey, Long> candidateKeys) {
    this.lock.readLock().lock();
    try {
      synchronized (memcache) {
        internalGetRowKeyAtOrBefore(memcache, row, candidateKeys);
      }
      synchronized (snapshot) {
        internalGetRowKeyAtOrBefore(snapshot, row, candidateKeys);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void internalGetRowKeyAtOrBefore(SortedMap<HStoreKey, byte []> map,
    byte [] key, SortedMap<HStoreKey, Long> candidateKeys) {
    HStoreKey strippedKey = null;
    
    // we want the earliest possible to start searching from
    HStoreKey search_key = candidateKeys.isEmpty() ? 
      new HStoreKey(key) : new HStoreKey(candidateKeys.firstKey().getRow());
    Iterator<HStoreKey> key_iterator = null;
    HStoreKey found_key = null;
    ArrayList<HStoreKey> victims = new ArrayList<HStoreKey>();
    long now = System.currentTimeMillis();
    // get all the entries that come equal or after our search key
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(search_key);
    
    // if there are items in the tail map, there's either a direct match to
    // the search key, or a range of values between the first candidate key
    // and the ultimate search key (or the end of the cache)
    if (!tailMap.isEmpty() &&
        Bytes.compareTo(tailMap.firstKey().getRow(), key) <= 0) {
      key_iterator = tailMap.keySet().iterator();

      // keep looking at cells as long as they are no greater than the 
      // ultimate search key and there's still records left in the map.
      do {
        found_key = key_iterator.next();
        if (Bytes.compareTo(found_key.getRow(), key) <= 0) {
          strippedKey = stripTimestamp(found_key);
          if (HLogEdit.isDeleted(tailMap.get(found_key))) {
            if (candidateKeys.containsKey(strippedKey)) {
              long bestCandidateTs = 
                candidateKeys.get(strippedKey).longValue();
              if (bestCandidateTs <= found_key.getTimestamp()) {
                candidateKeys.remove(strippedKey);
              }
            }
          } else {
            if (ttl == HConstants.FOREVER ||
                  now < found_key.getTimestamp() + ttl) {
              candidateKeys.put(strippedKey, 
                new Long(found_key.getTimestamp()));
            } else {
              victims.add(found_key);
              if (LOG.isDebugEnabled()) {
                LOG.debug(":" + found_key + ": expired, skipped");
              }
            }
          }
        }
      } while (Bytes.compareTo(found_key.getRow(), key) <= 0 
        && key_iterator.hasNext());
    } else {
      // the tail didn't contain any keys that matched our criteria, or was 
      // empty. examine all the keys that preceed our splitting point.
      SortedMap<HStoreKey, byte []> headMap = map.headMap(search_key);
      
      // if we tried to create a headMap and got an empty map, then there are
      // no keys at or before the search key, so we're done.
      if (headMap.isEmpty()) {
        return;
      }        
      
      // if there aren't any candidate keys at this point, we need to search
      // backwards until we find at least one candidate or run out of headMap.
      if (candidateKeys.isEmpty()) {
        HStoreKey[] cells = 
          headMap.keySet().toArray(new HStoreKey[headMap.keySet().size()]);
           
        byte [] lastRowFound = null;
        for(int i = cells.length - 1; i >= 0; i--) {
          HStoreKey thisKey = cells[i];
          
          // if the last row we found a candidate key for is different than
          // the row of the current candidate, we can stop looking.
          if (lastRowFound != null &&
              !Bytes.equals(lastRowFound, thisKey.getRow())) {
            break;
          }
          
          // if this isn't a delete, record it as a candidate key. also 
          // take note of the row of this candidate so that we'll know when
          // we cross the row boundary into the previous row.
          if (!HLogEdit.isDeleted(headMap.get(thisKey))) {
            if (ttl == HConstants.FOREVER) {
              lastRowFound = thisKey.getRow();
              candidateKeys.put(stripTimestamp(thisKey), 
                new Long(thisKey.getTimestamp()));
            } else {
              victims.add(found_key);
              if (LOG.isDebugEnabled()) {
                LOG.debug("internalGetRowKeyAtOrBefore: " + found_key +
                  ": expired, skipped");
              }
            }
          }
        }
      } else {
        // if there are already some candidate keys, we only need to consider
        // the very last row's worth of keys in the headMap, because any 
        // smaller acceptable candidate keys would have caused us to start
        // our search earlier in the list, and we wouldn't be searching here.
        SortedMap<HStoreKey, byte[]> thisRowTailMap = 
          headMap.tailMap(new HStoreKey(headMap.lastKey().getRow()));
          
        key_iterator = thisRowTailMap.keySet().iterator();
  
        do {
          found_key = key_iterator.next();
          
          if (HLogEdit.isDeleted(thisRowTailMap.get(found_key))) {
            strippedKey = stripTimestamp(found_key);              
            if (candidateKeys.containsKey(strippedKey)) {
              long bestCandidateTs = 
                candidateKeys.get(strippedKey).longValue();
              if (bestCandidateTs <= found_key.getTimestamp()) {
                candidateKeys.remove(strippedKey);
              }
            }
          } else {
            if (ttl == HConstants.FOREVER ||
                    now < found_key.getTimestamp() + ttl) {
              candidateKeys.put(stripTimestamp(found_key), 
                Long.valueOf(found_key.getTimestamp()));
            } else {
              victims.add(found_key);
              if (LOG.isDebugEnabled()) {
                LOG.debug("internalGetRowKeyAtOrBefore: " + found_key +
                  ": expired, skipped");
              }
            }
          }
        } while (key_iterator.hasNext());
      }
    }
    // Remove expired victims from the map.
    for (HStoreKey victim: victims)
      map.remove(victim);
  }
  
  static HStoreKey stripTimestamp(HStoreKey key) {
    return new HStoreKey(key.getRow(), key.getColumn());
  }
  
  /**
   * Examine a single map for the desired key.
   *
   * TODO - This is kinda slow.  We need a data structure that allows for 
   * proximity-searches, not just precise-matches.
   * 
   * @param map
   * @param key
   * @param numVersions
   * @return Ordered list of items found in passed <code>map</code>.  If no
   * matching values, returns an empty list (does not return null).
   */
  private ArrayList<Cell> internalGet(
      final SortedMap<HStoreKey, byte []> map, final HStoreKey key,
      final int numVersions) {

    ArrayList<Cell> result = new ArrayList<Cell>();
    
    // TODO: If get is of a particular version -- numVersions == 1 -- we
    // should be able to avoid all of the tailmap creations and iterations
    // below.
    long now = System.currentTimeMillis();
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(key);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      if (itKey.matchesRowCol(key)) {
        if (!HLogEdit.isDeleted(es.getValue())) { 
          // Filter out expired results
          if (ttl == HConstants.FOREVER ||
                now < itKey.getTimestamp() + ttl) {
            result.add(new Cell(tailMap.get(itKey), itKey.getTimestamp()));
            if (numVersions > 0 && result.size() >= numVersions) {
              break;
            }
          } else {
            victims.add(itKey);
            if (LOG.isDebugEnabled()) {
              LOG.debug("internalGet: " + itKey + ": expired, skipped");
            }
          }
        }
      } else {
        // By L.N. HBASE-684, map is sorted, so we can't find match any more.
        break;
      }
    }
    // Remove expired victims from the map.
    for (HStoreKey v: victims) {
      map.remove(v);
    }
    return result;
  }

  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all.
   * @return Ordered list of <code>versions</code> keys going from newest back.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin, final int versions) {
    this.lock.readLock().lock();
    try {
      List<HStoreKey> results;
      synchronized (memcache) {
        results = internalGetKeys(this.memcache, origin, versions);
      }
      synchronized (snapshot) {
        results.addAll(results.size(), internalGetKeys(snapshot, origin,
            versions == HConstants.ALL_VERSIONS ? versions :
              (versions - results.size())));
      }
      return results;
      
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all.
   * @return List of all keys that are of the same row and column and of
   * equal or older timestamp.  If no keys, returns an empty List. Does not
   * return null.
   */
  private List<HStoreKey> internalGetKeys(
      final SortedMap<HStoreKey, byte []> map, final HStoreKey origin,
      final int versions) {

    long now = System.currentTimeMillis();
    List<HStoreKey> result = new ArrayList<HStoreKey>();
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(origin);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey key = es.getKey();
  
      // if there's no column name, then compare rows and timestamps
      if (origin.getColumn() != null && origin.getColumn().length == 0) {
        // if the current and origin row don't match, then we can jump
        // out of the loop entirely.
        if (!Bytes.equals(key.getRow(), origin.getRow())) {
          break;
        }
        // if the rows match but the timestamp is newer, skip it so we can
        // get to the ones we actually want.
        if (key.getTimestamp() > origin.getTimestamp()) {
          continue;
        }
      }
      else{ // compare rows and columns
        // if the key doesn't match the row and column, then we're done, since 
        // all the cells are ordered.
        if (!key.matchesRowCol(origin)) {
          break;
        }
      }
      if (!HLogEdit.isDeleted(es.getValue())) {
        if (ttl == HConstants.FOREVER || now < key.getTimestamp() + ttl) {
          result.add(key);
        } else {
          victims.add(key);
          if (LOG.isDebugEnabled()) {
            LOG.debug("internalGetKeys: " + key + ": expired, skipped");
          }
        }
        if (result.size() >= versions) {
          // We have enough results.  Return.
          break;
        }
      }
    }

    // Clean expired victims from the map.
    for (HStoreKey v: victims)
       map.remove(v);

    return result;
  }


  /**
   * @param key
   * @return True if an entry and its content is {@link HGlobals.deleteBytes}.
   * Use checking values in store. On occasion the memcache has the fact that
   * the cell has been deleted.
   */
  boolean isDeleted(final HStoreKey key) {
    return HLogEdit.isDeleted(this.memcache.get(key));
  }

  /**
   * @return a scanner over the keys in the Memcache
   */
  InternalScanner getScanner(long timestamp,
    final byte [][] targetCols, final byte [] firstRow)
  throws IOException {
    this.lock.readLock().lock();
    try {
      return new MemcacheScanner(timestamp, targetCols, firstRow);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // MemcacheScanner implements the InternalScanner.
  // It lets the caller scan the contents of the Memcache.
  //////////////////////////////////////////////////////////////////////////////

  private class MemcacheScanner extends HAbstractScanner {
    private byte [] currentRow;
    private Set<byte []> columns = null;
    
    MemcacheScanner(final long timestamp, final byte [] targetCols[],
      final byte [] firstRow)
    throws IOException {
      // Call to super will create ColumnMatchers and whether this is a regex
      // scanner or not.  Will also save away timestamp.  Also sorts rows.
      super(timestamp, targetCols);
      this.currentRow = firstRow;
      // If we're being asked to scan explicit columns rather than all in 
      // a family or columns that match regexes, cache the sorted array of
      // columns.
      this.columns = null;
      if (!isWildcardScanner()) {
        this.columns = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < targetCols.length; i++) {
          this.columns.add(targetCols[i]);
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean next(HStoreKey key, SortedMap<byte [], Cell> results)
    throws IOException {
      if (this.scannerClosed) {
        return false;
      }
      // This is a treemap rather than a Hashmap because then I can have a
      // byte array as key -- because I can independently specify a comparator.
      Map<byte [], Long> deletes =
        new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);
      // Catch all row results in here.  These results are ten filtered to
      // ensure they match column name regexes, or if none, added to results.
      Map<byte [], Cell> rowResults =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      if (results.size() > 0) {
        results.clear();
      }
      long latestTimestamp = -1;
      while (results.size() <= 0 && this.currentRow != null) {
        if (deletes.size() > 0) {
          deletes.clear();
        }
        if (rowResults.size() > 0) {
          rowResults.clear();
        }
        key.setRow(this.currentRow);
        key.setVersion(this.timestamp);
        getFull(key, isWildcardScanner() ? null : this.columns, deletes,
            rowResults);
        for (Map.Entry<byte [], Long> e: deletes.entrySet()) {
          rowResults.put(e.getKey(),
            new Cell(HLogEdit.deleteBytes.get(), e.getValue().longValue()));
        }
        for (Map.Entry<byte [], Cell> e: rowResults.entrySet()) {
          byte [] column = e.getKey();
          Cell c = e.getValue();
          if (isWildcardScanner()) {
            // Check the results match.  We only check columns, not timestamps.
            // We presume that timestamps have been handled properly when we
            // called getFull.
            if (!columnMatch(column)) {
              continue;
            }
          }
          // We should never return HConstants.LATEST_TIMESTAMP as the time for
          // the row. As a compromise, we return the largest timestamp for the
          // entries that we find that match.
          if (c.getTimestamp() != HConstants.LATEST_TIMESTAMP &&
              c.getTimestamp() > latestTimestamp) {
            latestTimestamp = c.getTimestamp();
          }
          results.put(column, c);
        }
        this.currentRow = getNextRow(this.currentRow);

      }
      // Set the timestamp to the largest one for the row if we would otherwise
      // return HConstants.LATEST_TIMESTAMP
      if (key.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
        key.setVersion(latestTimestamp);
      }
      return results.size() > 0;
    }

    /** {@inheritDoc} */
    public void close() {
      if (!scannerClosed) {
        scannerClosed = true;
      }
    }
  }
}
