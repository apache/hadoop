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
import java.util.TreeMap;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HConstants;


/**
 * The Memcache holds in-memory modifications to the HRegion.  This is really a
 * wrapper around a TreeMap that helps us when staging the Memcache out to disk.
 */
class Memcache {

  // Note that since these structures are always accessed with a lock held,
  // no additional synchronization is required.

  @SuppressWarnings("hiding")
  private final SortedMap<HStoreKey, byte[]> memcache =
    Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
    
  volatile SortedMap<HStoreKey, byte[]> snapshot;
    
  @SuppressWarnings("hiding")
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructor
   */
  public Memcache() {
    snapshot = 
      Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
  }

  /**
   * Creates a snapshot of the current Memcache
   */
  void snapshot() {
    this.lock.writeLock().lock();
    try {
      synchronized (memcache) {
        if (memcache.size() != 0) {
          snapshot.putAll(memcache);
          memcache.clear();
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  /**
   * @return memcache snapshot
   */
  SortedMap<HStoreKey, byte[]> getSnapshot() {
    this.lock.writeLock().lock();
    try {
      SortedMap<HStoreKey, byte[]> currentSnapshot = snapshot;
      snapshot = 
        Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
      
      return currentSnapshot;

    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  /**
   * Store a value.  
   * @param key
   * @param value
   */
  void add(final HStoreKey key, final byte[] value) {
    this.lock.readLock().lock();
    try {
      memcache.put(key, value);
      
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
      synchronized (memcache) {
        results = internalGet(memcache, key, numVersions);
      }
      synchronized (snapshot) {
        results.addAll(results.size(),
          internalGet(snapshot, key, numVersions - results.size()));
      }
      return results;
      
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to byte arrays (byte[]).
   * @param key
   * @param results
   */
  void getFull(HStoreKey key, Set<Text> columns, Map<Text, Long> deletes, 
    SortedMap<Text, Cell> results) {
    this.lock.readLock().lock();
    try {
      synchronized (memcache) {
        internalGetFull(memcache, key, columns, deletes, results);
      }
      synchronized (snapshot) {
        internalGetFull(snapshot, key, columns, deletes, results);
      }

    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void internalGetFull(SortedMap<HStoreKey, byte[]> map, HStoreKey key, 
    Set<Text> columns, Map<Text, Long> deletes, 
    SortedMap<Text, Cell> results) {

    if (map.isEmpty() || key == null) {
      return;
    }

    SortedMap<HStoreKey, byte[]> tailMap = map.tailMap(key);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      Text itCol = itKey.getColumn();
      if (results.get(itCol) == null && key.matchesWithoutColumn(itKey)) {
        byte [] val = tailMap.get(itKey);

        if (columns == null || columns.contains(itKey.getColumn())) {
          if (HLogEdit.isDeleted(val)) {
            if (!deletes.containsKey(itCol) 
              || deletes.get(itCol).longValue() < itKey.getTimestamp()) {
              deletes.put(new Text(itCol), itKey.getTimestamp());
            }
          } else if (!(deletes.containsKey(itCol) 
            && deletes.get(itCol).longValue() >= itKey.getTimestamp())) {
            results.put(new Text(itCol), new Cell(val, itKey.getTimestamp()));
          }
        }
      } else if (key.getRow().compareTo(itKey.getRow()) < 0) {
        break;
      }
    }
  }

  /**
   * @param row
   * @param timestamp
   * @return the key that matches <i>row</i> exactly, or the one that
   * immediately preceeds it.
   */
  public Text getRowKeyAtOrBefore(final Text row, long timestamp) {
    this.lock.readLock().lock();
    
    Text key_memcache = null;
    Text key_snapshot = null;
    
    try {
      synchronized (memcache) {
        key_memcache = internalGetRowKeyAtOrBefore(memcache, row, timestamp);
      }
      synchronized (snapshot) {
        key_snapshot = internalGetRowKeyAtOrBefore(snapshot, row, timestamp);
      }

      if (key_memcache == null && key_snapshot == null) {
        // didn't find any candidates, return null
        return null;
      } else if (key_memcache == null && key_snapshot != null) {
        return key_snapshot;
      } else if (key_memcache != null && key_snapshot == null) {
        return key_memcache;
      } else if ( (key_memcache != null && key_memcache.equals(row)) 
          || (key_snapshot != null && key_snapshot.equals(row)) ) {
        // if either is a precise match, return the original row.
        return row;
      } else if (key_memcache != null) {
        // no precise matches, so return the one that is closer to the search
        // key (greatest)
        return key_memcache.compareTo(key_snapshot) > 0 ? 
            key_memcache : key_snapshot;
      }
      return null;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private Text internalGetRowKeyAtOrBefore(SortedMap<HStoreKey, byte []> map, 
    Text key, long timestamp) {
    // TODO: account for deleted cells

    HStoreKey search_key = new HStoreKey(key, timestamp);

    // get all the entries that come equal or after our search key
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(search_key);

    // if the first item in the tail has a matching row, then we have an 
    // exact match, and we should return that item
    if (!tailMap.isEmpty() && tailMap.firstKey().getRow().equals(key)) {
      // seek forward past any cells that don't fulfill the timestamp
      // argument
      Iterator<HStoreKey> key_iterator = tailMap.keySet().iterator();
      HStoreKey found_key = key_iterator.next();
      
      // keep seeking so long as we're in the same row, and the timstamp
      // isn't as small as we'd like, and there are more cells to check
      while (found_key.getRow().equals(key)
        && found_key.getTimestamp() > timestamp && key_iterator.hasNext()) {
        found_key = key_iterator.next();
      }
      
      // if this check fails, then we've iterated through all the keys that 
      // match by row, but none match by timestamp, so we fall through to
      // the headMap case.
      if (found_key.getTimestamp() <= timestamp) {
        // we didn't find a key that matched by timestamp, so we have to 
        // return null;
/*          LOG.debug("Went searching for " + key + ", found " + found_key.getRow());*/
        return found_key.getRow();
      }
    }
    
    // the tail didn't contain the key we're searching for, so we should
    // use the last key in the headmap as the closest before
    SortedMap<HStoreKey, byte []> headMap = map.headMap(search_key);
    return headMap.isEmpty()? null: headMap.lastKey().getRow();
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
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(key);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      if (itKey.matchesRowCol(key)) {
        if (!HLogEdit.isDeleted(es.getValue())) { 
          result.add(new Cell(tailMap.get(itKey), itKey.getTimestamp()));
        }
      }
      if (numVersions > 0 && result.size() >= numVersions) {
        break;
      }
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
  private List<HStoreKey> internalGetKeys(final SortedMap<HStoreKey, byte []> map,
      final HStoreKey origin, final int versions) {

    List<HStoreKey> result = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(origin);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey key = es.getKey();
  
      // if there's no column name, then compare rows and timestamps
      if (origin.getColumn().toString().equals("")) {
        // if the current and origin row don't match, then we can jump
        // out of the loop entirely.
        if (!key.getRow().equals(origin.getRow())) {
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
        result.add(key);
        if (versions != HConstants.ALL_VERSIONS && result.size() >= versions) {
          // We have enough results.  Return.
          break;
        }
      }
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
    return HLogEdit.isDeleted(this.memcache.get(key));
  }

  /**
   * @return a scanner over the keys in the Memcache
   */
  HInternalScannerInterface getScanner(long timestamp,
      Text targetCols[], Text firstRow) throws IOException {

    // Here we rely on ReentrantReadWriteLock's ability to acquire multiple
    // locks by the same thread and to be able to downgrade a write lock to
    // a read lock. We need to hold a lock throughout this method, but only
    // need the write lock while creating the memcache snapshot
    
    this.lock.writeLock().lock(); // hold write lock during memcache snapshot
    snapshot();                       // snapshot memcache
    this.lock.readLock().lock();      // acquire read lock
    this.lock.writeLock().unlock();   // downgrade to read lock
    try {
      // Prevent a cache flush while we are constructing the scanner

      return new MemcacheScanner(timestamp, targetCols, firstRow);
    
    } finally {
      this.lock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // MemcacheScanner implements the HScannerInterface.
  // It lets the caller scan the contents of the Memcache.
  //////////////////////////////////////////////////////////////////////////////

  class MemcacheScanner extends HAbstractScanner {
    SortedMap<HStoreKey, byte []> backingMap;
    Iterator<HStoreKey> keyIterator;

    @SuppressWarnings("unchecked")
    MemcacheScanner(final long timestamp, final Text targetCols[],
        final Text firstRow) throws IOException {

      super(timestamp, targetCols);
      try {
        this.backingMap = new TreeMap<HStoreKey, byte[]>();
        this.backingMap.putAll(snapshot);
        this.keys = new HStoreKey[1];
        this.vals = new byte[1][];

        // Generate list of iterators

        HStoreKey firstKey = new HStoreKey(firstRow);
          if (firstRow != null && firstRow.getLength() != 0) {
            keyIterator =
              backingMap.tailMap(firstKey).keySet().iterator();

          } else {
            keyIterator = backingMap.keySet().iterator();
          }

          while (getNext(0)) {
            if (!findFirstRow(0, firstRow)) {
              continue;
            }
            if (columnMatch(0)) {
              break;
            }
          }
      } catch (RuntimeException ex) {
        LOG.error("error initializing Memcache scanner: ", ex);
        close();
        IOException e = new IOException("error initializing Memcache scanner");
        e.initCause(ex);
        throw e;

      } catch(IOException ex) {
        LOG.error("error initializing Memcache scanner: ", ex);
        close();
        throw ex;
      }
    }

    /**
     * The user didn't want to start scanning at the first row. This method
     * seeks to the requested row.
     *
     * @param i which iterator to advance
     * @param firstRow seek to this row
     * @return true if this is the first row
     */
    @Override
    boolean findFirstRow(int i, Text firstRow) {
      return firstRow.getLength() == 0 ||
      keys[i].getRow().compareTo(firstRow) >= 0;
    }

    /**
     * Get the next value from the specified iterator.
     * 
     * @param i Which iterator to fetch next value from
     * @return true if there is more data available
     */
    @Override
    boolean getNext(int i) {
      boolean result = false;
      while (true) {
        if (!keyIterator.hasNext()) {
          closeSubScanner(i);
          break;
        }
        // Check key is < than passed timestamp for this scanner.
        HStoreKey hsk = keyIterator.next();
        if (hsk == null) {
          throw new NullPointerException("Unexpected null key");
        }
        if (hsk.getTimestamp() <= this.timestamp) {
          this.keys[i] = hsk;
          this.vals[i] = backingMap.get(keys[i]);
          result = true;
          break;
        }
      }
      return result;
    }

    /** Shut down an individual map iterator. */
    @Override
    void closeSubScanner(int i) {
      keyIterator = null;
      keys[i] = null;
      vals[i] = null;
      backingMap = null;
    }

    /** Shut down map iterators */
    public void close() {
      if (!scannerClosed) {
        if(keyIterator != null) {
          closeSubScanner(0);
        }
        scannerClosed = true;
      }
    }
  }
}
