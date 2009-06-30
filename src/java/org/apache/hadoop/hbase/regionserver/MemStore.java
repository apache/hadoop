/**
 * Copyright 2009 The Apache Software Foundation
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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.DeleteCompare.DeleteCode;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The MemStore holds in-memory modifications to the Store.  Modifications
 * are {@link KeyValue}s.  When asked to flush, current memstore is moved
 * to snapshot and is cleared.  We continue to serve edits out of new memstore
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 * TODO: Adjust size of the memstore when we remove items because they have
 * been deleted.
 * TODO: With new KVSLS, need to make sure we update HeapSize with difference
 * in KV size.
 */
class MemStore {
  private static final Log LOG = LogFactory.getLog(MemStore.class);

  private final long ttl;

  // MemStore.  Use a SkipListMap rather than SkipListSet because of the
  // better semantics.  The Map will overwrite if passed a key it already had
  // whereas the Set will not add new KV if key is same though value might be
  // different.  Value is not important -- just make sure always same
  // reference passed.
  volatile KeyValueSkipListSet kvset;

  // Snapshot of memstore.  Made for flusher.
  volatile KeyValueSkipListSet snapshot;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final KeyValue.KVComparator comparator;

  // Used comparing versions -- same r/c and ts but different type.
  final KeyValue.KVComparator comparatorIgnoreType;

  // Used comparing versions -- same r/c and type but different timestamp.
  final KeyValue.KVComparator comparatorIgnoreTimestamp;

  // TODO: Fix this guess by studying jprofiler
  private final static int ESTIMATED_KV_HEAP_TAX = 60;

  /**
   * Default constructor. Used for tests.
   */
  public MemStore() {
    this(HConstants.FOREVER, KeyValue.COMPARATOR);
  }

  /**
   * Constructor.
   * @param ttl The TTL for cache entries, in milliseconds.
   * @param c
   */
  public MemStore(final long ttl, final KeyValue.KVComparator c) {
    this.ttl = ttl;
    this.comparator = c;
    this.comparatorIgnoreTimestamp =
      this.comparator.getComparatorIgnoringTimestamps();
    this.comparatorIgnoreType = this.comparator.getComparatorIgnoringType();
    this.kvset = new KeyValueSkipListSet(c);
    this.snapshot = new KeyValueSkipListSet(c);
  }

  void dump() {
    for (KeyValue kv: this.kvset) {
      LOG.info(kv);
    }
    for (KeyValue kv: this.snapshot) {
      LOG.info(kv);
    }
  }

  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot(java.util.Map)}
   * To get the snapshot made by this method, use {@link #getSnapshot()}
   */
  void snapshot() {
    this.lock.writeLock().lock();
    try {
      // If snapshot currently has entries, then flusher failed or didn't call
      // cleanup.  Log a warning.
      if (!this.snapshot.isEmpty()) {
        LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
      } else {
        // We used to synchronize on the memstore here but we're inside a
        // write lock so removed it. Comment is left in case removal was a
        // mistake. St.Ack
        if (!this.kvset.isEmpty()) {
          this.snapshot = this.kvset;
          this.kvset = new KeyValueSkipListSet(this.comparator);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Return the current snapshot.
   * Called by flusher to get current snapshot made by a previous
   * call to {@link #snapshot()}
   * @return Return snapshot.
   * @see {@link #snapshot()}
   * @see {@link #clearSnapshot(java.util.Map)}
   */
  KeyValueSkipListSet getSnapshot() {
    return this.snapshot;
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param ss The snapshot to clean out.
   * @throws UnexpectedException
   * @see {@link #snapshot()}
   */
  void clearSnapshot(final KeyValueSkipListSet ss)
  throws UnexpectedException {
    this.lock.writeLock().lock();
    try {
      if (this.snapshot != ss) {
        throw new UnexpectedException("Current snapshot is " +
          this.snapshot + ", was passed " + ss);
      }
      // OK. Passed in snapshot is same as current snapshot.  If not-empty,
      // create a new snapshot and let the old one go.
      if (!ss.isEmpty()) {
        this.snapshot = new KeyValueSkipListSet(this.comparator);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * Write an update
   * @param kv
   * @return approximate size of the passed key and value.
   */
  long add(final KeyValue kv) {
    long size = -1;
    this.lock.readLock().lock();
    try {
      size = heapSize(kv, this.kvset.add(kv));
    } finally {
      this.lock.readLock().unlock();
    }
    return size;
  }

  /** 
   * Write a delete
   * @param delete
   * @return approximate size of the passed key and value.
   */
  long delete(final KeyValue delete) {
    long size = 0;
    this.lock.readLock().lock();
    //Have to find out what we want to do here, to find the fastest way of
    //removing things that are under a delete.
    //Actions that will take place here are:
    //1. Insert a delete and remove all the affected entries already in memstore
    //2. In the case of a Delete and the matching put is found then don't insert
    //   the delete
    //TODO Would be nice with if we had an iterator for this, so we could remove
    //things that needs to be removed while iterating and don't have to go
    //back and do it afterwards
    
    try {
      boolean notpresent = false;
      List<KeyValue> deletes = new ArrayList<KeyValue>();
      SortedSet<KeyValue> tail = this.kvset.tailSet(delete);

      //Parse the delete, so that it is only done once
      byte [] deleteBuffer = delete.getBuffer();
      int deleteOffset = delete.getOffset();
  
      int deleteKeyLen = Bytes.toInt(deleteBuffer, deleteOffset);
      deleteOffset += Bytes.SIZEOF_INT + Bytes.SIZEOF_INT;
  
      short deleteRowLen = Bytes.toShort(deleteBuffer, deleteOffset);
      deleteOffset += Bytes.SIZEOF_SHORT;
      int deleteRowOffset = deleteOffset;
  
      deleteOffset += deleteRowLen;
  
      byte deleteFamLen = deleteBuffer[deleteOffset];
      deleteOffset += Bytes.SIZEOF_BYTE + deleteFamLen;
  
      int deleteQualifierOffset = deleteOffset;
      int deleteQualifierLen = deleteKeyLen - deleteRowLen - deleteFamLen -
        Bytes.SIZEOF_SHORT - Bytes.SIZEOF_BYTE - Bytes.SIZEOF_LONG - 
        Bytes.SIZEOF_BYTE;
      
      deleteOffset += deleteQualifierLen;
  
      int deleteTimestampOffset = deleteOffset;
      deleteOffset += Bytes.SIZEOF_LONG;
      byte deleteType = deleteBuffer[deleteOffset];
      
      //Comparing with tail from memstore
      for (KeyValue kv : tail) {
        DeleteCode res = DeleteCompare.deleteCompare(kv, deleteBuffer, 
            deleteRowOffset, deleteRowLen, deleteQualifierOffset, 
            deleteQualifierLen, deleteTimestampOffset, deleteType,
            comparator.getRawComparator());
        if (res == DeleteCode.DONE) {
          break;
        } else if (res == DeleteCode.DELETE) {
          deletes.add(kv);
        } // SKIP
      }

      //Delete all the entries effected by the last added delete
      for (KeyValue kv : deletes) {
        notpresent = this.kvset.remove(kv);
        size -= heapSize(kv, notpresent);
      }
      
      // Adding the delete to memstore. Add any value, as long as
      // same instance each time.
      size += heapSize(delete, this.kvset.add(delete));
    } finally {
      this.lock.readLock().unlock();
    }
    return size;
  }
  
  /*
   * Calculate how the memstore size has changed, approximately.  Be careful.
   * If class changes, be sure to change the size calculation.
   * Add in tax of Map.Entry.
   * @param kv
   * @param notpresent True if the kv was NOT present in the set.
   * @return Size
   */
  long heapSize(final KeyValue kv, final boolean notpresent) {
    return notpresent?
      // Add overhead for value byte array and for Map.Entry -- 57 bytes
      // on x64 according to jprofiler.
      ESTIMATED_KV_HEAP_TAX + 57 + kv.getLength(): 0; // Guess no change in size.
  }

  /**
   * @param kv Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  KeyValue getNextRow(final KeyValue kv) {
    this.lock.readLock().lock();
    try {
      return getLowest(getNextRow(kv, this.kvset), getNextRow(kv, this.snapshot));
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  private KeyValue getLowest(final KeyValue a, final KeyValue b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0? a: b;
  }

  /*
   * @param key Find row that follows this one.  If null, return first.
   * @param map Set to look in for a row beyond <code>row</code>.
   * @return Next row or null if none found.  If one found, will be a new
   * KeyValue -- can be destroyed by subsequent calls to this method.
   */
  private KeyValue getNextRow(final KeyValue key,
      final NavigableSet<KeyValue> set) {
    KeyValue result = null;
    SortedSet<KeyValue> tail = key == null? set: set.tailSet(key);
    // Iterate until we fall into the next row; i.e. move off current row
    for (KeyValue kv: tail) {
      if (comparator.compareRows(kv, key) <= 0)
        continue;
      // Note: Not suppressing deletes or expired cells.  Needs to be handled
      // by higher up functions.
      result = kv;
      break;
    }
    return result;
  }


  /**
   * @param row Row to look for.
   * @param candidateKeys Map of candidate keys (Accumulation over lots of
   * lookup over stores and memstores)
   */
  void getRowKeyAtOrBefore(final KeyValue row,
      final NavigableSet<KeyValue> candidateKeys) {
    getRowKeyAtOrBefore(row, candidateKeys,
      new TreeSet<KeyValue>(this.comparator), System.currentTimeMillis());
  }

  /**
   * @param kv Row to look for.
   * @param candidates Map of candidate keys (Accumulation over lots of
   * lookup over stores and memstores).  Pass a Set with a Comparator that
   * ignores key Type so we can do Set.remove using a delete, i.e. a KeyValue
   * with a different Type to the candidate key.
   * @param deletes Pass a Set that has a Comparator that ignores key type.
   * @param now
   */
  void getRowKeyAtOrBefore(final KeyValue kv,
      final NavigableSet<KeyValue> candidates, 
      final NavigableSet<KeyValue> deletes, final long now) {
    this.lock.readLock().lock();
    try {
      getRowKeyAtOrBefore(kvset, kv, candidates, deletes, now);
      getRowKeyAtOrBefore(snapshot, kv, candidates, deletes, now);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  private void getRowKeyAtOrBefore(final NavigableSet<KeyValue> set,
      final KeyValue kv, final NavigableSet<KeyValue> candidates,
      final NavigableSet<KeyValue> deletes, final long now) {
    if (set.isEmpty()) {
      return;
    }
    // We want the earliest possible to start searching from.  Start before
    // the candidate key in case it turns out a delete came in later.
    KeyValue search = candidates.isEmpty()? kv: candidates.first();

    // Get all the entries that come equal or after our search key
    SortedSet<KeyValue> tail = set.tailSet(search);

    // if there are items in the tail map, there's either a direct match to
    // the search key, or a range of values between the first candidate key
    // and the ultimate search key (or the end of the cache)
    if (!tail.isEmpty() &&
        this.comparator.compareRows(tail.first(), search) <= 0) {
      // Keep looking at cells as long as they are no greater than the 
      // ultimate search key and there's still records left in the map.
      KeyValue deleted = null;
      KeyValue found = null;
      for (Iterator<KeyValue> iterator = tail.iterator();
        iterator.hasNext() && (found == null ||
          this.comparator.compareRows(found, kv) <= 0);) {
        found = iterator.next();
        if (this.comparator.compareRows(found, kv) <= 0) {
          if (found.isDeleteType()) {
            Store.handleDeletes(found, candidates, deletes);
            if (deleted == null) {
              deleted = found;
            }
          } else {
            if (Store.notExpiredAndNotInDeletes(this.ttl, found, now, deletes)) {
              candidates.add(found);
            } else {
              if (deleted == null) {
                deleted = found;
              }
              // TODO: Check this removes the right key.
              // Its expired.  Remove it.
              iterator.remove();
            }
          }
        }
      }
      if (candidates.isEmpty() && deleted != null) {
        getRowKeyBefore(set, deleted, candidates, deletes, now);
      }
    } else {
      // The tail didn't contain any keys that matched our criteria, or was 
      // empty. Examine all the keys that proceed our splitting point.
      getRowKeyBefore(set, search, candidates, deletes, now);
    }
  }

  /*
   * Get row key that comes before passed <code>search_key</code>
   * Use when we know search_key is not in the map and we need to search
   * earlier in the cache.
   * @param set
   * @param search
   * @param candidates
   * @param deletes Pass a Set that has a Comparator that ignores key type.
   * @param now
   */
  private void getRowKeyBefore(NavigableSet<KeyValue> set,
      KeyValue search, NavigableSet<KeyValue> candidates,
      final NavigableSet<KeyValue> deletes, final long now) {
    NavigableSet<KeyValue> head = set.headSet(search, false);
    // If we tried to create a headMap and got an empty map, then there are
    // no keys at or before the search key, so we're done.
    if (head.isEmpty()) {
      return;
    }

    // If there aren't any candidate keys at this point, we need to search
    // backwards until we find at least one candidate or run out of headMap.
    if (candidates.isEmpty()) {
      KeyValue lastFound = null;
      // TODO: Confirm we're iterating in the right order
      for (Iterator<KeyValue> i = head.descendingIterator();
          i.hasNext();) {
        KeyValue found = i.next();
        // if the last row we found a candidate key for is different than
        // the row of the current candidate, we can stop looking -- if its
        // not a delete record.
        boolean deleted = found.isDeleteType();
        if (lastFound != null &&
            this.comparator.matchingRows(lastFound, found) && !deleted) {
          break;
        }
        // If this isn't a delete, record it as a candidate key. Also 
        // take note of this candidate so that we'll know when
        // we cross the row boundary into the previous row.
        if (!deleted) {
          if (Store.notExpiredAndNotInDeletes(this.ttl, found, now, deletes)) {
            lastFound = found;
            candidates.add(found);
          } else {
            // Its expired.
            Store.expiredOrDeleted(set, found);
          }
        } else {
          // We are encountering items in reverse.  We may have just added
          // an item to candidates that this later item deletes.  Check.  If we
          // found something in candidates, remove it from the set.
          if (Store.handleDeletes(found, candidates, deletes)) {
            remove(set, found);
          }
        }
      }
    } else {
      // If there are already some candidate keys, we only need to consider
      // the very last row's worth of keys in the headMap, because any 
      // smaller acceptable candidate keys would have caused us to start
      // our search earlier in the list, and we wouldn't be searching here.
      SortedSet<KeyValue> rowTail = 
        head.tailSet(head.last().cloneRow(HConstants.LATEST_TIMESTAMP));
      Iterator<KeyValue> i = rowTail.iterator();
      do {
        KeyValue found = i.next();
        if (found.isDeleteType()) {
          Store.handleDeletes(found, candidates, deletes);
        } else {
          if (ttl == HConstants.FOREVER ||
              now < found.getTimestamp() + ttl ||
              !deletes.contains(found)) {
            candidates.add(found);
          } else {
            Store.expiredOrDeleted(set, found);
          }
        }
      } while (i.hasNext());
    }
  }


  /*
   * @param set
   * @param delete This is a delete record.  Remove anything behind this of same
   * r/c/ts.
   * @return True if we removed anything.
   */
  private boolean remove(final NavigableSet<KeyValue> set,
      final KeyValue delete) {
    SortedSet<KeyValue> s = set.tailSet(delete);
    if (s.isEmpty()) {
      return false;
    }
    boolean removed = false;
    for (KeyValue kv: s) {
      if (this.comparatorIgnoreType.compare(kv, delete) == 0) {
        // Same r/c/ts.  Remove it.
        s.remove(kv);
        removed = true;
        continue;
      }
      break;
    }
    return removed;
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  KeyValueScanner [] getScanners() {
    this.lock.readLock().lock();
    try {
      KeyValueScanner [] scanners = new KeyValueScanner[2];
      scanners[0] = new MemStoreScanner(this.kvset);
      scanners[1] = new MemStoreScanner(this.snapshot);
      return scanners;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  //
  // HBASE-880/1249/1304
  //

  /**
   * Perform a single-row Get on the  and snapshot, placing results
   * into the specified KV list.
   * <p>
   * This will return true if it is determined that the query is complete
   * and it is not necessary to check any storefiles after this.
   * <p>
   * Otherwise, it will return false and you should continue on.
   * @param matcher Column matcher
   * @param result List to add results to
   * @return true if done with store (early-out), false if not
   * @throws IOException
   */
  public boolean get(QueryMatcher matcher, List<KeyValue> result)
  throws IOException {
    this.lock.readLock().lock();
    try {
      if(internalGet(this.kvset, matcher, result) || matcher.isDone()) {
        return true;
      }
      matcher.update();
      return internalGet(this.snapshot, matcher, result) || matcher.isDone();
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   *
   * @param set memstore or snapshot
   * @param matcher query matcher
   * @param result list to add results to
   * @return true if done with store (early-out), false if not
   * @throws IOException
   */
  private boolean internalGet(final NavigableSet<KeyValue> set,
      final QueryMatcher matcher, final List<KeyValue> result)
  throws IOException {
    if(set.isEmpty()) return false;
    // Seek to startKey
    SortedSet<KeyValue> tail = set.tailSet(matcher.getStartKey());
    for (KeyValue kv : tail) {
      QueryMatcher.MatchCode res = matcher.match(kv);
      switch(res) {
        case INCLUDE:
          result.add(kv);
          break;
        case SKIP:
          break;
        case NEXT:
          return false;
        case DONE:
          return true;
        default:
          throw new RuntimeException("Unexpected " + res);
      }
    }
    return false;
  }
  

  /*
   * MemStoreScanner implements the KeyValueScanner.
   * It lets the caller scan the contents of a memstore.
   * This behaves as if it were a real scanner but does not maintain position
   * in the passed memstore tree.
   */
  protected class MemStoreScanner implements KeyValueScanner {
    private final NavigableSet<KeyValue> kvs;
    private KeyValue current = null;
    private List<KeyValue> result = new ArrayList<KeyValue>();
    private int idx = 0;

    MemStoreScanner(final NavigableSet<KeyValue> s) {
      this.kvs = s;
    }

    public boolean seek(KeyValue key) {
      try {
        if (key == null) {
          close();
          return false;
        }
        this.current = key;
        return cacheNextRow();
      } catch(Exception e) {
        close();
        return false;
      }
    }

    public KeyValue peek() {
      if (idx >= this.result.size()) {
        if (!cacheNextRow()) {
          return null;
        }
        return peek();
      }
      return result.get(idx);
    }

    public KeyValue next() {
      if (idx >= result.size()) {
        if (!cacheNextRow()) {
          return null;
        }
        return next();
      }
      return this.result.get(idx++);
    }

    /**
     * @return True if we successfully cached a NavigableSet aligned on
     * next row.
     */
    boolean cacheNextRow() {
      SortedSet<KeyValue> keys;
      try {
        keys = this.kvs.tailSet(this.current);
      } catch (Exception e) {
        close();
        return false;
      }
      if (keys == null || keys.isEmpty()) {
        close();
        return false;
      }
      this.current = null;
      byte [] row = keys.first().getRow();
      for (KeyValue kv: keys) {
        if (comparator.compareRows(kv, row) != 0) {
          this.current = kv;
          break;
        }
        result.add(kv);
      }
      return true;
    }

    public void close() {
      current = null;
      idx = 0;
      if (!result.isEmpty()) {
        result.clear();
      }
    }
  }

  /**
   * Code to help figure if our approximation of object heap sizes is close
   * enough.  See hbase-900.  Fills memstores then waits so user can heap
   * dump and bring up resultant hprof in something like jprofiler which
   * allows you get 'deep size' on objects.
   * @param args
   */
  public static void main(String [] args) {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
      runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    MemStore memstore1 = new MemStore();
    // TODO: x32 vs x64
    long size = 0;
    final int count = 10000;
    byte [] column = Bytes.toBytes("col:umn");
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), column, i));
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), column, i));
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    MemStore memstore2 = new MemStore();
    for (int i = 0; i < count; i++) {
      size += memstore2.add(new KeyValue(Bytes.toBytes(i), column, i,
        new byte[i]));
    }
    LOG.info("memstore2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    for (int i = 0; i < seconds; i++) {
      // Thread.sleep(1000);
    }
    LOG.info("Exiting.");
  }
}
