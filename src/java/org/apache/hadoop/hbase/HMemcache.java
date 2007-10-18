/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;

/**
 * The HMemcache holds in-memory modifications to the HRegion.  This is really a
 * wrapper around a TreeMap that helps us when staging the Memcache out to disk.
 */
public class HMemcache {
  static final Log LOG = LogFactory.getLog(HMemcache.class);
  
  // Note that since these structures are always accessed with a lock held,
  // no additional synchronization is required.
  
  volatile SortedMap<HStoreKey, byte []> memcache;
  List<SortedMap<HStoreKey, byte []>> history =
    Collections.synchronizedList(new ArrayList<SortedMap<HStoreKey, byte []>>());
  volatile SortedMap<HStoreKey, byte []> snapshot = null;

  final HLocking lock = new HLocking();
  
  /*
   * Approximate size in bytes of the payload carried by this memcache.
   * Does not consider deletes nor adding again on same key.
   */
  private AtomicLong size = new AtomicLong(0);


  /**
   * Constructor
   */
  public HMemcache() {
    super();
    memcache  =
      Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
  }

  /** represents the state of the memcache at a specified point in time */
  static class Snapshot {
    final SortedMap<HStoreKey, byte []> memcacheSnapshot;
    final long sequenceId;
    
    Snapshot(final SortedMap<HStoreKey, byte[]> memcache, final Long i) {
      super();
      this.memcacheSnapshot = memcache;
      this.sequenceId = i.longValue();
    }
  }
  
  /**
   * Returns a snapshot of the current HMemcache with a known HLog 
   * sequence number at the same time.
   *
   * We need to prevent any writing to the cache during this time,
   * so we obtain a write lock for the duration of the operation.
   * 
   * <p>If this method returns non-null, client must call
   * {@link #deleteSnapshot()} to clear 'snapshot-in-progress'
   * state when finished with the returned {@link Snapshot}.
   * 
   * @return frozen HMemcache TreeMap and HLog sequence number.
   */
  Snapshot snapshotMemcacheForLog(HLog log) throws IOException {
    this.lock.obtainWriteLock();
    try {
      if(snapshot != null) {
        throw new IOException("Snapshot in progress!");
      }
      // If no entries in memcache.
      if(memcache.size() == 0) {
        return null;
      }
      Snapshot retval =
        new Snapshot(memcache, Long.valueOf(log.startCacheFlush()));
      // From here on, any failure is catastrophic requiring replay of hlog
      this.snapshot = memcache;
      synchronized (history) {
        history.add(memcache);
      }
      memcache =
        Collections.synchronizedSortedMap(new TreeMap<HStoreKey, byte []>());
      // Reset size of this memcache.
      this.size.set(0);
      return retval;
    } finally {
      this.lock.releaseWriteLock();
    }
  }

  /**
   * Delete the snapshot, remove from history.
   *
   * Modifying the structure means we need to obtain a writelock.
   * @throws IOException
   */
  public void deleteSnapshot() throws IOException {
    this.lock.obtainWriteLock();

    try {
      if(snapshot == null) {
        throw new IOException("Snapshot not present!");
      }
      synchronized (history) {
        history.remove(snapshot);
      }
      this.snapshot = null;
    } finally {
      this.lock.releaseWriteLock();
    }
  }

  /**
   * Store a value.  
   * Operation uses a write lock.
   * @param row
   * @param columns
   * @param timestamp
   */
  public void add(final Text row, final TreeMap<Text, byte []> columns,
      final long timestamp) {
    this.lock.obtainWriteLock();
    try {
      for (Map.Entry<Text, byte []> es: columns.entrySet()) {
        HStoreKey key = new HStoreKey(row, es.getKey(), timestamp);
        byte [] value = es.getValue();
        this.size.addAndGet(key.getSize());
        this.size.addAndGet(((value == null)? 0: value.length));
        memcache.put(key, value);
      }
    } finally {
      this.lock.releaseWriteLock();
    }
  }
  
  /**
   * @return Approximate size in bytes of payload carried by this memcache.
   * Does not take into consideration deletes nor adding again on same key.
   */
  public long getSize() {
    return this.size.get();
  }

  /**
   * Look back through all the backlog TreeMaps to find the target.
   * @param key
   * @param numVersions
   * @return An array of byte arrays ordered by timestamp.
   */
  public byte [][] get(final HStoreKey key, final int numVersions) {
    this.lock.obtainReadLock();
    try {
      ArrayList<byte []> results = get(memcache, key, numVersions);
      synchronized (history) {
        for (int i = history.size() - 1; i >= 0; i--) {
          if (numVersions > 0 && results.size() >= numVersions) {
            break;
          }
          results.addAll(results.size(),
              get(history.get(i), key, numVersions - results.size()));
        }
      }
      return (results.size() == 0) ? null :
        ImmutableBytesWritable.toArray(results);
      
    } finally {
      this.lock.releaseReadLock();
    }
  }
  
  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to byte arrays (byte[]).
   * @param key
   * @return All columns for given key.
   */
  public TreeMap<Text, byte []> getFull(HStoreKey key) {
    TreeMap<Text, byte []> results = new TreeMap<Text, byte []>();
    this.lock.obtainReadLock();
    try {
      internalGetFull(memcache, key, results);
      synchronized (history) {
        for (int i = history.size() - 1; i >= 0; i--) {
          SortedMap<HStoreKey, byte []> cur = history.get(i);
          internalGetFull(cur, key, results);
        }
      }
      return results;
      
    } finally {
      this.lock.releaseReadLock();
    }
  }
  
  void internalGetFull(SortedMap<HStoreKey, byte []> map, HStoreKey key, 
      TreeMap<Text, byte []> results) {
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(key);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      Text itCol = itKey.getColumn();
      if (results.get(itCol) == null
          && key.matchesWithoutColumn(itKey)) {
        byte [] val = tailMap.get(itKey);
        results.put(itCol, val);
      } else if (key.getRow().compareTo(itKey.getRow()) > 0) {
        break;
      }
    }
  }

  /**
   * Examine a single map for the desired key.
   *
   * We assume that all locking is done at a higher-level. No locking within 
   * this method.
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
  ArrayList<byte []> get(final SortedMap<HStoreKey, byte []> map,
      final HStoreKey key, final int numVersions) {
    ArrayList<byte []> result = new ArrayList<byte []>();
    // TODO: If get is of a particular version -- numVersions == 1 -- we
    // should be able to avoid all of the tailmap creations and iterations
    // below.
    HStoreKey curKey = new HStoreKey(key);
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(curKey);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey itKey = es.getKey();
      if (itKey.matchesRowCol(curKey)) {
        if (!HLogEdit.isDeleted(es.getValue())) {
          result.add(tailMap.get(itKey));
          curKey.setVersion(itKey.getTimestamp() - 1);
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
    this.lock.obtainReadLock();
    try {
      List<HStoreKey> results = getKeys(this.memcache, origin, versions);
      synchronized (history) {
        for (int i = history.size() - 1; i >= 0; i--) {
          results.addAll(results.size(), getKeys(history.get(i), origin,
              versions == HConstants.ALL_VERSIONS ? versions :
                (versions - results.size())));
        }
      }
      return results;
    } finally {
      this.lock.releaseReadLock();
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
  private List<HStoreKey> getKeys(final SortedMap<HStoreKey, byte []> map,
      final HStoreKey origin, final int versions) {
    List<HStoreKey> result = new ArrayList<HStoreKey>();
    SortedMap<HStoreKey, byte []> tailMap = map.tailMap(origin);
    for (Map.Entry<HStoreKey, byte []> es: tailMap.entrySet()) {
      HStoreKey key = es.getKey();
      if (!key.matchesRowCol(origin)) {
        break;
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
   * Return a scanner over the keys in the HMemcache
   */
  HInternalScannerInterface getScanner(long timestamp,
      Text targetCols[], Text firstRow) throws IOException {  
      return new HMemcacheScanner(timestamp, targetCols, firstRow);
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMemcacheScanner implements the HScannerInterface.
  // It lets the caller scan the contents of the Memcache.
  //////////////////////////////////////////////////////////////////////////////

  class HMemcacheScanner extends HAbstractScanner {
    SortedMap<HStoreKey, byte []> backingMaps[];
    final Iterator<HStoreKey> keyIterators[];

    @SuppressWarnings("unchecked")
    HMemcacheScanner(final long timestamp, final Text targetCols[],
        final Text firstRow) throws IOException {

      super(timestamp, targetCols);
      lock.obtainReadLock();
      try {
        synchronized (history) {
          this.backingMaps = new SortedMap[history.size() + 1];

          // Note that since we iterate through the backing maps from 0 to n, we
          // need to put the memcache first, the newest history second, ..., etc.

          backingMaps[0] = memcache;
          for (int i = history.size() - 1; i >= 0; i--) {
            backingMaps[i + 1] = history.get(i);
          }
        }

        this.keyIterators = new Iterator[backingMaps.length];
        this.keys = new HStoreKey[backingMaps.length];
        this.vals = new byte[backingMaps.length][];

        // Generate list of iterators
        
        HStoreKey firstKey = new HStoreKey(firstRow);
        for (int i = 0; i < backingMaps.length; i++) {
          if (firstRow != null && firstRow.getLength() != 0) {
            keyIterators[i] =
              backingMaps[i].tailMap(firstKey).keySet().iterator();
            
          } else {
            keyIterators[i] = backingMaps[i].keySet().iterator();
          }

          while (getNext(i)) {
            if (!findFirstRow(i, firstRow)) {
              continue;
            }
            if (columnMatch(i)) {
              break;
            }
          }
        }
      } catch (RuntimeException ex) {
        LOG.error("error initializing HMemcache scanner: ", ex);
        close();
        IOException e = new IOException("error initializing HMemcache scanner");
        e.initCause(ex);
        throw e;
        
      } catch(IOException ex) {
        LOG.error("error initializing HMemcache scanner: ", ex);
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
        if (!keyIterators[i].hasNext()) {
          closeSubScanner(i);
          break;
        }
        // Check key is < than passed timestamp for this scanner.
        HStoreKey hsk = keyIterators[i].next();
        if (hsk == null) {
          throw new NullPointerException("Unexpected null key");
        }
        if (hsk.getTimestamp() <= this.timestamp) {
          this.keys[i] = hsk;
          this.vals[i] = backingMaps[i].get(keys[i]);
          result = true;
          break;
        }
      }
      return result;
    }

    /** Shut down an individual map iterator. */
    @Override
    void closeSubScanner(int i) {
      keyIterators[i] = null;
      keys[i] = null;
      vals[i] = null;
      backingMaps[i] = null;
    }

    /** Shut down map iterators, and release the lock */
    public void close() {
      if (!scannerClosed) {
        try {
          for (int i = 0; i < keys.length; i++) {
            if(keyIterators[i] != null) {
              closeSubScanner(i);
            }
          }
        } finally {
          lock.releaseReadLock();
          scannerClosed = true;
        }
      }
    }
  }
}
