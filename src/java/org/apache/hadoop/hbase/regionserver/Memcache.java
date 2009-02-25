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
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * The Memcache holds in-memory modifications to the HRegion.
 * Keeps a current map.  When asked to flush the map, current map is moved
 * to snapshot and is cleared.  We continue to serve edits out of new map
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 */
class Memcache {
  private static final Log LOG = LogFactory.getLog(Memcache.class);
  
  private final long ttl;

  // Note that since these structures are always accessed with a lock held,
  // so no additional synchronization is required.
  
  // The currently active sorted map of edits.
  private volatile SortedMap<HStoreKey, byte[]> memcache;

  // Snapshot of memcache.  Made for flusher.
  private volatile SortedMap<HStoreKey, byte[]> snapshot;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Default constructor. Used for tests.
   */
  public Memcache() {
    this.ttl = HConstants.FOREVER;
    this.memcache = createSynchronizedSortedMap();
    this.snapshot = createSynchronizedSortedMap();
  }

  /**
   * Constructor.
   * @param ttl The TTL for cache entries, in milliseconds.
   * @param regionInfo The HRI for this cache 
   */
  public Memcache(final long ttl) {
    this.ttl = ttl;
    this.memcache = createSynchronizedSortedMap();
    this.snapshot = createSynchronizedSortedMap();
  }

  /*
   * Utility method using HSKWritableComparator
   * @return synchronized sorted map of HStoreKey to byte arrays.
   */
  @SuppressWarnings("unchecked")
  private SortedMap<HStoreKey, byte[]> createSynchronizedSortedMap() {
    return Collections.synchronizedSortedMap(
      new TreeMap<HStoreKey, byte []>(
        new HStoreKey.HStoreKeyWritableComparator()));
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
   * @return memcache Approximate size of the passed key and value.  Includes
   * cost of hosting HSK and byte arrays as well as the Map.Entry this addition
   * costs when we insert into the backing TreeMap.
   */
  long add(final HStoreKey key, final byte[] value) {
    long size = -1;
    this.lock.readLock().lock();
    try {
      byte [] oldValue = this.memcache.remove(key);
      this.memcache.put(key, value);
      size = heapSize(key, value, oldValue);
    } finally {
      this.lock.readLock().unlock();
    }
    return size;
  }
  
  /*
   * Calcuate how the memcache size has changed, approximately.
   * Add in tax of TreeMap.Entry.
   * @param key
   * @param value
   * @param oldValue
   * @return
   */
  long heapSize(final HStoreKey key, final byte [] value,
      final byte [] oldValue) {
    // First add value length.
    long keySize = key.heapSize();
    // Add value.
    long size = value == null? 0: value.length;
    if (oldValue == null) {
      size += keySize;
      // Add overhead for value byte array and for Map.Entry -- 57 bytes
      // on x64 according to jprofiler.
      size += Bytes.ESTIMATED_HEAP_TAX + 57;
    } else {
      // If old value, don't add overhead again nor key size. Just add
      // difference in  value sizes.
      size -= oldValue.length;
    }
    return size;
  }

  /**
   * Look back through all the backlog TreeMaps to find the target.
   * @param key
   * @param numVersions
   * @return An array of byte arrays ordered by timestamp.
   */
  List<Cell> get(final HStoreKey key, final int numVersions) {
    return get(key, numVersions, null, System.currentTimeMillis());
  }
  
  /**
   * Look back through all the backlog TreeMaps to find the target.
   * @param key
   * @param numVersions
   * @param deletes
   * @param now
   * @return An array of byte arrays ordered by timestamp.
   */
  List<Cell> get(final HStoreKey key, final int numVersions,
      final Set<HStoreKey> deletes, final long now) {
    this.lock.readLock().lock();
    try {
      List<Cell> results;
      // The synchronizations here are because the below get iterates
      synchronized (this.memcache) {
        results = get(this.memcache, key, numVersions, deletes, now);
      }
      synchronized (this.snapshot) {
        results.addAll(results.size(),
          get(this.snapshot, key, numVersions - results.size(), deletes, now));
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
  private byte [] getLowest(final byte [] a,
      final byte [] b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return HStoreKey.compareTwoRowKeys(a, b) <= 0? a: b;
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
        if (HStoreKey.compareTwoRowKeys(itKey.getRow(), row) <= 0)
          continue;
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
   * @param numVersions number of versions to retrieve
   * @param deletes Map to accumulate deletes found.
   * @param results Where to stick row results found.
   */
  void getFull(HStoreKey key, Set<byte []> columns, int numVersions,
    Map<byte [], Long> deletes, Map<byte [], Cell> results) {
    this.lock.readLock().lock();
    try {
      // The synchronizations here are because internalGet iterates
      synchronized (this.memcache) {
        internalGetFull(this.memcache, key, columns, numVersions, deletes, results);
      }
      synchronized (this.snapshot) {
        internalGetFull(this.snapshot, key, columns, numVersions, deletes, results);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void internalGetFull(SortedMap<HStoreKey, byte[]> map, HStoreKey key, 
      Set<byte []> columns, int numVersions, Map<byte [], Long> deletes,
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
      Cell cell = results.get(itCol);
      if ((cell == null || cell.getNumValues() < numVersions) && key.matchesWithoutColumn(itKey)) {
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
              if (cell == null) {
                results.put(itCol, new Cell(val, itKey.getTimestamp()));
              } else {
                cell.add(val, itKey.getTimestamp());
              }
            } else {
              addVictim(victims, itKey);
            }
          }
        }
      } else if (HStoreKey.compareTwoRowKeys(key.getRow(), itKey.getRow()) < 0) {
        break;
      }
    }
    // Remove expired victims from the map.
    for (HStoreKey v: victims) {
      map.remove(v);
    }
  }

  /**
   * @param row Row to look for.
   * @param candidateKeys Map of candidate keys (Accumulation over lots of
   * lookup over stores and memcaches)
   * @param deletes Deletes collected so far.
   */
  void getRowKeyAtOrBefore(final byte [] row,
      final SortedMap<HStoreKey, Long> candidateKeys) {
    getRowKeyAtOrBefore(row, candidateKeys, new HashSet<HStoreKey>());
  }
  
  /**
   * @param row Row to look for.
   * @param candidateKeys Map of candidate keys (Accumulation over lots of
   * lookup over stores and memcaches)
   * @param deletes Deletes collected so far.
   */
  void getRowKeyAtOrBefore(final byte [] row,
      final SortedMap<HStoreKey, Long> candidateKeys, 
      final Set<HStoreKey> deletes) {
    this.lock.readLock().lock();
    try {
      synchronized (memcache) {
        getRowKeyAtOrBefore(memcache, row, candidateKeys, deletes);
      }
      synchronized (snapshot) {
        getRowKeyAtOrBefore(snapshot, row, candidateKeys, deletes);
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void getRowKeyAtOrBefore(final SortedMap<HStoreKey, byte []> map,
      final byte [] row, final SortedMap<HStoreKey, Long> candidateKeys,
      final Set<HStoreKey> deletes) {
    // We want the earliest possible to start searching from.  Start before
    // the candidate key in case it turns out a delete came in later.
    HStoreKey search_key = candidateKeys.isEmpty()?
     new HStoreKey(row):
     new HStoreKey(candidateKeys.firstKey().getRow());
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    long now = System.currentTimeMillis();

    // Get all the entries that come equal or after our search key
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(search_key);

    // if there are items in the tail map, there's either a direct match to
    // the search key, or a range of values between the first candidate key
    // and the ultimate search key (or the end of the cache)
    if (!tailMap.isEmpty() &&
        HStoreKey.compareTwoRowKeys(tailMap.firstKey().getRow(),
          search_key.getRow()) <= 0) {
      Iterator<HStoreKey> key_iterator = tailMap.keySet().iterator();

      // Keep looking at cells as long as they are no greater than the 
      // ultimate search key and there's still records left in the map.
      HStoreKey deletedOrExpiredRow = null;
      for (HStoreKey found_key = null; key_iterator.hasNext() &&
          (found_key == null ||
            HStoreKey.compareTwoRowKeys(found_key.getRow(), row) <= 0);) {
        found_key = key_iterator.next();
        if (HStoreKey.compareTwoRowKeys(found_key.getRow(), row) <= 0) {
          if (HLogEdit.isDeleted(tailMap.get(found_key))) {
            Store.handleDeleted(found_key, candidateKeys, deletes);
            if (deletedOrExpiredRow == null) {
              deletedOrExpiredRow = found_key;
            }
          } else {
            if (Store.notExpiredAndNotInDeletes(this.ttl, 
                found_key, now, deletes)) {
              candidateKeys.put(stripTimestamp(found_key),
                new Long(found_key.getTimestamp()));
            } else {
              if (deletedOrExpiredRow == null) {
                deletedOrExpiredRow = new HStoreKey(found_key);
              }
              addVictim(victims, found_key);
            }
          }
        }
      }
      if (candidateKeys.isEmpty() && deletedOrExpiredRow != null) {
        getRowKeyBefore(map, deletedOrExpiredRow, candidateKeys, victims,
          deletes, now);
      }
    } else {
      // The tail didn't contain any keys that matched our criteria, or was 
      // empty. Examine all the keys that proceed our splitting point.
      getRowKeyBefore(map, search_key, candidateKeys, victims, deletes, now);
    }
    // Remove expired victims from the map.
    for (HStoreKey victim: victims) {
      map.remove(victim);
    }
  }
  
  /*
   * Get row key that comes before passed <code>search_key</code>
   * Use when we know search_key is not in the map and we need to search
   * earlier in the cache.
   * @param map
   * @param search_key
   * @param candidateKeys
   * @param victims
   */
  private void getRowKeyBefore(SortedMap<HStoreKey, byte []> map,
      HStoreKey search_key, SortedMap<HStoreKey, Long> candidateKeys,
      final List<HStoreKey> expires, final Set<HStoreKey> deletes,
      final long now) {
    SortedMap<HStoreKey, byte []> headMap = map.headMap(search_key);
    // If we tried to create a headMap and got an empty map, then there are
    // no keys at or before the search key, so we're done.
    if (headMap.isEmpty()) {
      return;
    }

    // If there aren't any candidate keys at this point, we need to search
    // backwards until we find at least one candidate or run out of headMap.
    if (candidateKeys.isEmpty()) {
      Set<HStoreKey> keys = headMap.keySet();
      HStoreKey [] cells = keys.toArray(new HStoreKey[keys.size()]);
      byte [] lastRowFound = null;
      for (int i = cells.length - 1; i >= 0; i--) {
        HStoreKey found_key = cells[i];
        // if the last row we found a candidate key for is different than
        // the row of the current candidate, we can stop looking -- if its
        // not a delete record.
        boolean deleted = HLogEdit.isDeleted(headMap.get(found_key));
        if (lastRowFound != null &&
            !HStoreKey.equalsTwoRowKeys(lastRowFound, found_key.getRow()) &&
            !deleted) {
          break;
        }
        // If this isn't a delete, record it as a candidate key. Also 
        // take note of the row of this candidate so that we'll know when
        // we cross the row boundary into the previous row.
        if (!deleted) {
          if (Store.notExpiredAndNotInDeletes(this.ttl, found_key, now, deletes)) {
            lastRowFound = found_key.getRow();
            candidateKeys.put(stripTimestamp(found_key), 
              new Long(found_key.getTimestamp()));
          } else {
            expires.add(found_key);
            if (LOG.isDebugEnabled()) {
              LOG.debug("getRowKeyBefore: " + found_key + ": expired, skipped");
            }
          }
        } else {
          deletes.add(found_key);
        }
      }
    } else {
      // If there are already some candidate keys, we only need to consider
      // the very last row's worth of keys in the headMap, because any 
      // smaller acceptable candidate keys would have caused us to start
      // our search earlier in the list, and we wouldn't be searching here.
      SortedMap<HStoreKey, byte[]> thisRowTailMap = 
        headMap.tailMap(new HStoreKey(headMap.lastKey().getRow()));
      Iterator<HStoreKey> key_iterator = thisRowTailMap.keySet().iterator();
      do {
        HStoreKey found_key = key_iterator.next();
        if (HLogEdit.isDeleted(thisRowTailMap.get(found_key))) {
          Store.handleDeleted(found_key, candidateKeys, deletes);
        } else {
          if (ttl == HConstants.FOREVER ||
              now < found_key.getTimestamp() + ttl ||
              !deletes.contains(found_key)) {
            candidateKeys.put(stripTimestamp(found_key), 
              Long.valueOf(found_key.getTimestamp()));
          } else {
            expires.add(found_key);
            if (LOG.isDebugEnabled()) {
              LOG.debug("internalGetRowKeyAtOrBefore: " + found_key +
                ": expired, skipped");
            }
          }
        }
      } while (key_iterator.hasNext());
    }
  }
  
  static HStoreKey stripTimestamp(HStoreKey key) {
    return new HStoreKey(key.getRow(), key.getColumn());
  }
  
  /*
   * Examine a single map for the desired key.
   *
   * TODO - This is kinda slow.  We need a data structure that allows for 
   * proximity-searches, not just precise-matches.
   * 
   * @param map
   * @param key
   * @param numVersions
   * @param deletes
   * @return Ordered list of items found in passed <code>map</code>.  If no
   * matching values, returns an empty list (does not return null).
   */
  private ArrayList<Cell> get(final SortedMap<HStoreKey, byte []> map,
      final HStoreKey key, final int numVersions, final Set<HStoreKey> deletes,
      final long now) {
    ArrayList<Cell> result = new ArrayList<Cell>();
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte[]> tailMap = map.tailMap(key);
    for (Map.Entry<HStoreKey, byte[]> es : tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      if (itKey.matchesRowCol(key)) {
        if (!isDeleted(es.getValue())) {
          // Filter out expired results
          if (Store.notExpiredAndNotInDeletes(ttl, itKey, now, deletes)) {
            result.add(new Cell(tailMap.get(itKey), itKey.getTimestamp()));
            if (numVersions > 0 && result.size() >= numVersions) {
              break;
            }
          } else {
            addVictim(victims, itKey);
          }
        } else {
          // Cell holds a delete value.
          deletes.add(itKey);
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

  /*
   * Add <code>key</code> to the list of 'victims'.
   * @param victims
   * @param key
   */
  private void addVictim(final List<HStoreKey> victims, final HStoreKey key) {
    victims.add(key);
    if (LOG.isDebugEnabled()) {
      LOG.debug(key + ": expired or in deletes, skipped");
    }
  }

  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all.
   * @param now
   * @param deletes Accumulating list of deletes
   * @param columnPattern regex pattern for column matching. if columnPattern
   * is not null, we use column pattern to match columns. And the columnPattern
   * only works when origin's column is null or its length is zero.
   * @return Ordered list of <code>versions</code> keys going from newest back.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin, final int versions,
      final Set<HStoreKey> deletes, final long now, 
      final Pattern columnPattern) {
    this.lock.readLock().lock();
    try {
      List<HStoreKey> results;
      synchronized (memcache) {
        results = 
          getKeys(this.memcache, origin, versions, deletes, now, columnPattern);
      }
      synchronized (snapshot) {
        results.addAll(results.size(), getKeys(snapshot, origin,
            versions == HConstants.ALL_VERSIONS ? versions :
              (versions - results.size()), deletes, now, columnPattern));
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
   * @param now
   * @param deletes
   * @param columnPattern regex pattern for column matching. if columnPattern
   * is not null, we use column pattern to match columns. And the columnPattern
   * only works when origin's column is null or its length is zero.
   * @return List of all keys that are of the same row and column and of
   * equal or older timestamp.  If no keys, returns an empty List. Does not
   * return null.
   */
  private List<HStoreKey> getKeys(final SortedMap<HStoreKey,
      byte []> map, final HStoreKey origin, final int versions,
      final Set<HStoreKey> deletes, final long now, 
      final Pattern columnPattern) {
    List<HStoreKey> result = new ArrayList<HStoreKey>();
    List<HStoreKey> victims = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(origin);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey key = es.getKey();
      // if there's no column name, then compare rows and timestamps
      if (origin.getColumn() != null && origin.getColumn().length == 0) {
        // if the current and origin row don't match, then we can jump
        // out of the loop entirely.
        if (!HStoreKey.equalsTwoRowKeys( key.getRow(), origin.getRow())) {
          break;
        }
        // if the column pattern is not null, we use it for column matching.
        // we will skip the keys whose column doesn't match the pattern.
        if (columnPattern != null) {
          if (!(columnPattern.matcher(Bytes.toString(key.getColumn())).matches())) {
            continue;
          }
        }
        // if the rows match but the timestamp is newer, skip it so we can
        // get to the ones we actually want.
        if (key.getTimestamp() > origin.getTimestamp()) {
          continue;
        }
      } else { // compare rows and columns
        // if the key doesn't match the row and column, then we're done, since 
        // all the cells are ordered.
        if (!key.matchesRowCol(origin)) {
          break;
        }
      }
      if (!isDeleted(es.getValue())) {
        if (Store.notExpiredAndNotInDeletes(this.ttl, key, now, deletes)) {
          result.add(key);
          if (versions > 0 && result.size() >= versions) {
            break;
          }
        } else {
          addVictim(victims, key);
        }
      } else {
        // Delete
        deletes.add(key);
      }
    }
    // Clean expired victims from the map.
    for (HStoreKey v: victims) {
       map.remove(v);
    }
    return result;
  }

  /**
   * @param key
   * @return True if an entry and its content is {@link HGlobals.deleteBytes}.
   * Use checking values in store. On occasion the memcache has the fact that
   * the cell has been deleted.
   */
  boolean isDeleted(final HStoreKey key) {
    return isDeleted(this.memcache.get(key)) ||
      (this.snapshot != null && isDeleted(this.snapshot.get(key)));
  }
  
  /*
   * @param b Cell value.
   * @return True if this is a delete value.
   */
  private boolean isDeleted(final byte [] b) {
    return HLogEdit.isDeleted(b);
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
        getFull(key, isWildcardScanner() ? null : this.columns, 1, deletes,
            rowResults);
        for (Map.Entry<byte [], Long> e: deletes.entrySet()) {
          rowResults.put(e.getKey(),
            new Cell(HLogEdit.DELETED_BYTES, e.getValue().longValue()));
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

    public void close() {
      if (!scannerClosed) {
        scannerClosed = true;
      }
    }
  }

  /**
   * Code to help figure if our approximation of object heap sizes is close
   * enough.  See hbase-900.  Fills memcaches then waits so user can heap
   * dump and bring up resultant hprof in something like jprofiler which
   * allows you get 'deep size' on objects.
   * @param args
   * @throws InterruptedException
   */
  public static void main(String [] args) throws InterruptedException {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
      runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    Memcache memcache1 = new Memcache();
    // TODO: x32 vs x64
    long size = 0;
    final int count = 10000;
    for (int i = 0; i < count; i++) {
      size += memcache1.add(new HStoreKey(Bytes.toBytes(i)),
        HConstants.EMPTY_BYTE_ARRAY);
    }
    LOG.info("memcache1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      size += memcache1.add(new HStoreKey(Bytes.toBytes(i)),
        HConstants.EMPTY_BYTE_ARRAY);
    }
    LOG.info("memcache1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memcache.
    Memcache memcache2 = new Memcache();
    for (int i = 0; i < count; i++) {
      byte [] b = Bytes.toBytes(i);
      size += memcache2.add(new HStoreKey(b, b),
        new byte [i]);
    }
    LOG.info("memcache2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    for (int i = 0; i < seconds; i++) {
      Thread.sleep(1000);
    }
    LOG.info("Exiting.");
  }
}