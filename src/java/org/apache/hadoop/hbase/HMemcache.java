/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;

/*******************************************************************************
 * The HMemcache holds in-memory modifications to the HRegion.  This is really a
 * wrapper around a TreeMap that helps us when staging the Memcache out to disk.
 ******************************************************************************/
public class HMemcache {
  private static final Log LOG = LogFactory.getLog(HMemcache.class);
  
  TreeMap<HStoreKey, BytesWritable> memcache 
    = new TreeMap<HStoreKey, BytesWritable>();
  
  Vector<TreeMap<HStoreKey, BytesWritable>> history 
    = new Vector<TreeMap<HStoreKey, BytesWritable>>();
  
  TreeMap<HStoreKey, BytesWritable> snapshot = null;

  HLocking locking = new HLocking();

  public HMemcache() {
  }

  public static class Snapshot {
    public TreeMap<HStoreKey, BytesWritable> memcacheSnapshot = null;
    public long sequenceId = 0;
    
    public Snapshot() {
    }
  }
  
  /**
   * We want to return a snapshot of the current HMemcache with a known HLog 
   * sequence number at the same time.
   * 
   * Return both the frozen HMemcache TreeMap, as well as the HLog seq number.
   *
   * We need to prevent any writing to the cache during this time, so we obtain 
   * a write lock for the duration of the operation.
   */
  public Snapshot snapshotMemcacheForLog(HLog log) throws IOException {
    Snapshot retval = new Snapshot();

    locking.obtainWriteLock();
    try {
      if(snapshot != null) {
        throw new IOException("Snapshot in progress!");
      }
      if(memcache.size() == 0) {
        LOG.debug("memcache empty. Skipping snapshot");
        return retval;
      }

      LOG.debug("starting memcache snapshot");
      
      retval.memcacheSnapshot = memcache;
      this.snapshot = memcache;
      history.add(memcache);
      memcache = new TreeMap<HStoreKey, BytesWritable>();
      retval.sequenceId = log.startCacheFlush();
      
      LOG.debug("memcache snapshot complete");
      
      return retval;
      
    } finally {
      locking.releaseWriteLock();
    }
  }

  /**
   * Delete the snapshot, remove from history.
   *
   * Modifying the structure means we need to obtain a writelock.
   */
  public void deleteSnapshot() throws IOException {
    locking.obtainWriteLock();

    try {
      if(snapshot == null) {
        throw new IOException("Snapshot not present!");
      }
      LOG.debug("deleting snapshot");
      
      for(Iterator<TreeMap<HStoreKey, BytesWritable>> it = history.iterator(); 
          it.hasNext(); ) {
        
        TreeMap<HStoreKey, BytesWritable> cur = it.next();
        if(snapshot == cur) {
          it.remove();
          break;
        }
      }
      this.snapshot = null;
      
      LOG.debug("snapshot deleted");
      
    } finally {
      locking.releaseWriteLock();
    }
  }

  /**
   * Store a value.  
   *
   * Operation uses a write lock.
   */
  public void add(Text row, TreeMap<Text, byte[]> columns, long timestamp) {
    locking.obtainWriteLock();
    try {
      for(Iterator<Text> it = columns.keySet().iterator(); it.hasNext(); ) {
        Text column = it.next();
        byte[] val = columns.get(column);

        HStoreKey key = new HStoreKey(row, column, timestamp);
        memcache.put(key, new BytesWritable(val));
      }
      
    } finally {
      locking.releaseWriteLock();
    }
  }

  /**
   * Look back through all the backlog TreeMaps to find the target.
   *
   * We only need a readlock here.
   */
  public byte[][] get(HStoreKey key, int numVersions) {
    Vector<byte[]> results = new Vector<byte[]>();
    locking.obtainReadLock();
    try {
      Vector<byte[]> result = get(memcache, key, numVersions-results.size());
      results.addAll(0, result);

      for(int i = history.size()-1; i >= 0; i--) {
        if(numVersions > 0 && results.size() >= numVersions) {
          break;
        }
        
        result = get(history.elementAt(i), key, numVersions-results.size());
        results.addAll(results.size(), result);
      }
      
      if(results.size() == 0) {
        return null;
        
      } else {
        return (byte[][]) results.toArray(new byte[results.size()][]);
      }
      
    } finally {
      locking.releaseReadLock();
    }
  }

  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to byte arrays (byte[]).
   */
  public TreeMap<Text, byte[]> getFull(HStoreKey key) throws IOException {
    TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    locking.obtainReadLock();
    try {
      internalGetFull(memcache, key, results);
      for(int i = history.size()-1; i >= 0; i--) {
        TreeMap<HStoreKey, BytesWritable> cur = history.elementAt(i);
        internalGetFull(cur, key, results);
      }
      return results;
      
    } finally {
      locking.releaseReadLock();
    }
  }
  
  void internalGetFull(TreeMap<HStoreKey, BytesWritable> map, HStoreKey key, 
                       TreeMap<Text, byte[]> results) {
    
    SortedMap<HStoreKey, BytesWritable> tailMap = map.tailMap(key);
    
    for(Iterator<HStoreKey> it = tailMap.keySet().iterator(); it.hasNext(); ) {
      HStoreKey itKey = it.next();
      Text itCol = itKey.getColumn();

      if(results.get(itCol) == null
         && key.matchesWithoutColumn(itKey)) {
        BytesWritable val = tailMap.get(itKey);
        results.put(itCol, val.get());
        
      } else if(key.getRow().compareTo(itKey.getRow()) > 0) {
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
   */    
  Vector<byte[]> get(TreeMap<HStoreKey, BytesWritable> map, HStoreKey key, int numVersions) {
    Vector<byte[]> result = new Vector<byte[]>();
    HStoreKey curKey = new HStoreKey(key.getRow(), key.getColumn(), key.getTimestamp());
    SortedMap<HStoreKey, BytesWritable> tailMap = map.tailMap(curKey);

    for(Iterator<HStoreKey> it = tailMap.keySet().iterator(); it.hasNext(); ) {
      HStoreKey itKey = it.next();
      
      if(itKey.matchesRowCol(curKey)) {
        result.add(tailMap.get(itKey).get());
        curKey.setVersion(itKey.getTimestamp() - 1);
      }
      
      if(numVersions > 0 && result.size() >= numVersions) {
        break;
      }
    }
    return result;
  }

  /**
   * Return a scanner over the keys in the HMemcache
   */
  public HScannerInterface getScanner(long timestamp, Text targetCols[], Text firstRow)
    throws IOException {
    
    return new HMemcacheScanner(timestamp, targetCols, firstRow);
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMemcacheScanner implements the HScannerInterface.
  // It lets the caller scan the contents of the Memcache.
  //////////////////////////////////////////////////////////////////////////////

  class HMemcacheScanner extends HAbstractScanner {
    TreeMap<HStoreKey, BytesWritable> backingMaps[];
    Iterator<HStoreKey> keyIterators[];

    @SuppressWarnings("unchecked")
      public HMemcacheScanner(long timestamp, Text targetCols[], Text firstRow)
      throws IOException {
      
      super(timestamp, targetCols);
      
      locking.obtainReadLock();
      try {
        this.backingMaps = new TreeMap[history.size() + 1];
        int i = 0;
        for(Iterator<TreeMap<HStoreKey, BytesWritable>> it = history.iterator();
            it.hasNext(); ) {
          
          backingMaps[i++] = it.next();
        }
        backingMaps[backingMaps.length - 1] = memcache;

        this.keyIterators = new Iterator[backingMaps.length];
        this.keys = new HStoreKey[backingMaps.length];
        this.vals = new BytesWritable[backingMaps.length];

        // Generate list of iterators

        HStoreKey firstKey = new HStoreKey(firstRow);
        for(i = 0; i < backingMaps.length; i++) {
          if(firstRow.getLength() != 0) {
            keyIterators[i] = backingMaps[i].tailMap(firstKey).keySet().iterator();
            
          } else {
            keyIterators[i] = backingMaps[i].keySet().iterator();
          }
          
          while(getNext(i)) {
            if(! findFirstRow(i, firstRow)) {
              continue;
            }
            if(columnMatch(i)) {
              break;
            }
          }
        }
        
      } catch(Exception ex) {
        close();
      }
    }

    /**
     * The user didn't want to start scanning at the first row. This method
     * seeks to the requested row.
     *
     * @param i         - which iterator to advance
     * @param firstRow  - seek to this row
     * @return          - true if this is the first row
     */
    boolean findFirstRow(int i, Text firstRow) {
      return ((firstRow.getLength() == 0) || (keys[i].getRow().equals(firstRow)));
    }
    
    /**
     * Get the next value from the specified iterater.
     * 
     * @param i - which iterator to fetch next value from
     * @return - true if there is more data available
     */
    boolean getNext(int i) {
      if(! keyIterators[i].hasNext()) {
        closeSubScanner(i);
        return false;
      }
      this.keys[i] = keyIterators[i].next();
      this.vals[i] = backingMaps[i].get(keys[i]);
      return true;
    }

    /** Shut down an individual map iterator. */
    void closeSubScanner(int i) {
      keyIterators[i] = null;
      keys[i] = null;
      vals[i] = null;
      backingMaps[i] = null;
    }

    /** Shut down map iterators, and release the lock */
    public void close() throws IOException {
      if(! scannerClosed) {
        try {
          for(int i = 0; i < keys.length; i++) {
            if(keyIterators[i] != null) {
              closeSubScanner(i);
            }
          }
          
        } finally {
          locking.releaseReadLock();
          scannerClosed = true;
        }
      }
    }
  }
}
