/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.DeleteCompare.DeleteCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
public class MemStore implements HeapSize {
  private static final Log LOG = LogFactory.getLog(MemStore.class);

  // MemStore.  Use a KeyValueSkipListSet rather than SkipListSet because of the
  // better semantics.  The Map will overwrite if passed a key it already had
  // whereas the Set will not add new KV if key is same though value might be
  // different.  Value is not important -- just make sure always same
  // reference passed.
  volatile KeyValueSkipListSet kvset;

  // Snapshot of memstore.  Made for flusher.
  volatile KeyValueSkipListSet snapshot;

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final KeyValue.KVComparator comparator;

  // Used comparing versions -- same r/c and ts but different type.
  final KeyValue.KVComparator comparatorIgnoreType;

  // Used comparing versions -- same r/c and type but different timestamp.
  final KeyValue.KVComparator comparatorIgnoreTimestamp;

  // Used to track own heapSize
  final AtomicLong size;

  // All access must be synchronized.
  final CopyOnWriteArraySet<ChangedMemStoreObserver> changedMemStoreObservers =
    new CopyOnWriteArraySet<ChangedMemStoreObserver>();

  /**
   * Default constructor. Used for tests.
   */
  public MemStore() {
    this(KeyValue.COMPARATOR);
  }

  /**
   * Constructor.
   * @param c Comparator
   */
  public MemStore(final KeyValue.KVComparator c) {
    this.comparator = c;
    this.comparatorIgnoreTimestamp =
      this.comparator.getComparatorIgnoringTimestamps();
    this.comparatorIgnoreType = this.comparator.getComparatorIgnoringType();
    this.kvset = new KeyValueSkipListSet(c);
    this.snapshot = new KeyValueSkipListSet(c);
    this.size = new AtomicLong(DEEP_OVERHEAD);
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
        if (!this.kvset.isEmpty()) {
          this.snapshot = this.kvset;
          this.kvset = new KeyValueSkipListSet(this.comparator);
          tellChangedMemStoreObservers();
          // Reset heap to not include any keys
          this.size.set(DEEP_OVERHEAD);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /*
   * Tell outstanding scanners that memstore has changed.
   */
  private void tellChangedMemStoreObservers() {
    for (ChangedMemStoreObserver o: this.changedMemStoreObservers) {
      o.changedMemStore();
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
        tellChangedMemStoreObservers();
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
    long s = -1;
    this.lock.readLock().lock();
    try {
      s = heapSizeChange(kv, this.kvset.add(kv));
      this.size.addAndGet(s);
    } finally {
      this.lock.readLock().unlock();
    }
    return s;
  }

  /**
   * Write a delete
   * @param delete
   * @return approximate size of the passed key and value.
   */
  long delete(final KeyValue delete) {
    long s = 0;
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
        s -= heapSizeChange(kv, notpresent);
      }

      // Adding the delete to memstore. Add any value, as long as
      // same instance each time.
      s += heapSizeChange(delete, this.kvset.add(delete));
    } finally {
      this.lock.readLock().unlock();
    }
    this.size.addAndGet(s);
    return s;
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
   * @param state
   */
  void getRowKeyAtOrBefore(final GetClosestRowBeforeTracker state) {
    this.lock.readLock().lock();
    try {
      getRowKeyAtOrBefore(kvset, state);
      getRowKeyAtOrBefore(snapshot, state);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /*
   * @param set
   * @param state Accumulates deletes and candidates.
   */
  private void getRowKeyAtOrBefore(final NavigableSet<KeyValue> set,
      final GetClosestRowBeforeTracker state) {
    if (set.isEmpty()) {
      return;
    }
    if (!walkForwardInSingleRow(set, state.getTargetKey(), state)) {
      // Found nothing in row.  Try backing up.
      getRowKeyBefore(set, state);
    }
  }

  /*
   * Walk forward in a row from <code>firstOnRow</code>.  Presumption is that
   * we have been passed the first possible key on a row.  As we walk forward
   * we accumulate deletes until we hit a candidate on the row at which point
   * we return.
   * @param set
   * @param firstOnRow First possible key on this row.
   * @param state
   * @return True if we found a candidate walking this row.
   */
  private boolean walkForwardInSingleRow(final SortedSet<KeyValue> set,
      final KeyValue firstOnRow, final GetClosestRowBeforeTracker state) {
    boolean foundCandidate = false;
    SortedSet<KeyValue> tail = set.tailSet(firstOnRow);
    if (tail.isEmpty()) return foundCandidate;
    for (Iterator<KeyValue> i = tail.iterator(); i.hasNext();) {
      KeyValue kv = i.next();
      // Did we go beyond the target row? If so break.
      if (state.isTooFar(kv, firstOnRow)) break;
      if (state.isExpired(kv)) {
        i.remove();
        continue;
      }
      // If we added something, this row is a contender. break.
      if (state.handle(kv)) {
        foundCandidate = true;
        break;
      }
    }
    return foundCandidate;
  }

  /*
   * Walk backwards through the passed set a row at a time until we run out of
   * set or until we get a candidate.
   * @param set
   * @param state
   */
  private void getRowKeyBefore(NavigableSet<KeyValue> set,
      final GetClosestRowBeforeTracker state) {
    KeyValue firstOnRow = state.getTargetKey();
    for (Member p = memberOfPreviousRow(set, state, firstOnRow);
        p != null; p = memberOfPreviousRow(p.set, state, firstOnRow)) {
      // Make sure we don't fall out of our table.
      if (!state.isTargetTable(p.kv)) break;
      // Stop looking if we've exited the better candidate range.
      if (!state.isBetterCandidate(p.kv)) break;
      // Make into firstOnRow
      firstOnRow = new KeyValue(p.kv.getRow(), HConstants.LATEST_TIMESTAMP);
      // If we find something, break;
      if (walkForwardInSingleRow(p.set, firstOnRow, state)) break;
    }
  }

  /*
   * Immutable data structure to hold member found in set and the set it was
   * found in.  Include set because it is carrying context.
   */
  private static class Member {
    final KeyValue kv;
    final NavigableSet<KeyValue> set;
    Member(final NavigableSet<KeyValue> s, final KeyValue kv) {
      this.kv = kv;
      this.set = s;
    }
  }

  /*
   * @param set Set to walk back in.  Pass a first in row or we'll return
   * same row (loop).
   * @param state Utility and context.
   * @param firstOnRow First item on the row after the one we want to find a
   * member in.
   * @return Null or member of row previous to <code>firstOnRow</code>
   */
  private Member memberOfPreviousRow(NavigableSet<KeyValue> set,
      final GetClosestRowBeforeTracker state, final KeyValue firstOnRow) {
    NavigableSet<KeyValue> head = set.headSet(firstOnRow, false);
    if (head.isEmpty()) return null;
    for (Iterator<KeyValue> i = head.descendingIterator(); i.hasNext();) {
      KeyValue found = i.next();
      if (state.isExpired(found)) {
        i.remove();
        continue;
      }
      return new Member(head, found);
    }
    return null;
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  KeyValueScanner [] getScanners() {
    this.lock.readLock().lock();
    try {
      KeyValueScanner [] scanners = new KeyValueScanner[1];
      scanners[0] = new MemStoreScanner(this.changedMemStoreObservers);
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
   * Gets from either the memstore or the snapshop, and returns a code
   * to let you know which is which.
   *
   * @param matcher
   * @param result
   * @return 1 == memstore, 2 == snapshot, 0 == none
   */
  int getWithCode(QueryMatcher matcher, List<KeyValue> result) throws IOException {
    this.lock.readLock().lock();
    try {
      boolean fromMemstore = internalGet(this.kvset, matcher, result);
      if (fromMemstore || matcher.isDone())
        return 1;

      matcher.update();
      boolean fromSnapshot = internalGet(this.snapshot, matcher, result);
      if (fromSnapshot || matcher.isDone())
        return 2;

      return 0;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Small utility functions for use by Store.incrementColumnValue
   * _only_ under the threat of pain and everlasting race conditions.
   */
  void readLockLock() {
    this.lock.readLock().lock();
  }
  void readLockUnlock() {
    this.lock.readLock().unlock();
  }

  /**
   *
   * @param set memstore or snapshot
   * @param matcher query matcher
   * @param result list to add results to
   * @return true if done with store (early-out), false if not
   * @throws IOException
   */
  boolean internalGet(final NavigableSet<KeyValue> set,
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
   * It lets the caller scan the contents of a memstore -- both current
   * map and snapshot.
   * This behaves as if it were a real scanner but does not maintain position.
   */
  protected class MemStoreScanner implements KeyValueScanner, ChangedMemStoreObserver {
    private List<KeyValue> result = new ArrayList<KeyValue>();
    private int idx = 0;
    // Make access atomic.
    private FirstOnRow firstOnNextRow = new FirstOnRow();
    // Keep reference to Set so can remove myself when closed.
    private final Set<ChangedMemStoreObserver> observers;

    MemStoreScanner(final Set<ChangedMemStoreObserver> observers) {
      super();
      this.observers = observers;
      this.observers.add(this);
    }

    public boolean seek(KeyValue key) {
      try {
        if (key == null) {
          close();
          return false;
        }
        this.firstOnNextRow.set(key);
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
     * @return True if successfully cached a next row.
     */
    boolean cacheNextRow() {
      // Prevent snapshot being cleared while caching a row.
      lock.readLock().lock();
      try {
        this.result.clear();
        this.idx = 0;
        // Look at each set, kvset and snapshot.
        // Both look for matching entries for this.current row returning what
        // they
        // have as next row after this.current (or null if nothing in set or if
        // nothing follows.
        KeyValue kvsetNextRow = cacheNextRow(kvset);
        KeyValue snapshotNextRow = cacheNextRow(snapshot);
        if (kvsetNextRow == null && snapshotNextRow == null) {
          // Nothing more in memstore but we might have gotten current row
          // results
          // Indicate at end of store by setting next row to null.
          this.firstOnNextRow.set(null);
          return !this.result.isEmpty();
        } else if (kvsetNextRow != null && snapshotNextRow != null) {
          // Set current at the lowest of the two values.
          int compare = comparator.compare(kvsetNextRow, snapshotNextRow);
          this.firstOnNextRow.set(compare <= 0? kvsetNextRow: snapshotNextRow);
        } else {
          this.firstOnNextRow.set(kvsetNextRow != null? kvsetNextRow: snapshotNextRow);
        }
        return true;
      } finally {
        lock.readLock().unlock();
      }
    }

    /*
     * See if set has entries for the <code>this.current</code> row.  If so,
     * add them to <code>this.result</code>.
     * @param set Set to examine
     * @return Next row in passed <code>set</code> or null if nothing in this
     * passed <code>set</code>
     */
    private KeyValue cacheNextRow(final NavigableSet<KeyValue> set) {
      if (this.firstOnNextRow.get() == null || set.isEmpty()) return null;
      SortedSet<KeyValue> tail = set.tailSet(this.firstOnNextRow.get());
      if (tail == null || tail.isEmpty()) return null;
      KeyValue first = tail.first();
      KeyValue nextRow = null;
      for (KeyValue kv: tail) {
        if (comparator.compareRows(first, kv) != 0) {
          nextRow = kv;
          break;
        }
        this.result.add(kv);
      }
      return nextRow;
    }

    public void close() {
      this.firstOnNextRow.set(null);
      idx = 0;
      if (!result.isEmpty()) {
        result.clear();
      }
      this.observers.remove(this);
    }

    public void changedMemStore() {
      this.firstOnNextRow.reset();
    }
  }

  /*
   * Private class that holds firstOnRow and utility.
   * Usually firstOnRow is the first KeyValue we find on next row rather than
   * the absolute minimal first key (empty column, Type.Maximum, maximum ts).
   * Usually its ok being sloppy with firstOnRow letting it be the first thing
   * found on next row -- this works -- but if the memstore changes on us, reset
   * firstOnRow to be the ultimate firstOnRow.  We play sloppy with firstOnRow
   * usually so we don't have to  allocate a new KeyValue each time firstOnRow
   * is updated.
   */
  private static class FirstOnRow {
    private KeyValue firstOnRow = null;

    FirstOnRow() {
      super();
    }

    synchronized void set(final KeyValue kv) {
      this.firstOnRow = kv;
    }

    /* Reset firstOnRow to a 'clean', absolute firstOnRow.
     */
    synchronized void reset() {
      if (this.firstOnRow == null) return;
      this.firstOnRow =
         new KeyValue(this.firstOnRow.getRow(), HConstants.LATEST_TIMESTAMP);
    }

    synchronized KeyValue get() {
      return this.firstOnRow;
    }
  }

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (8 * ClassSize.REFERENCE));

  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.REENTRANT_LOCK + ClassSize.ATOMIC_LONG +
      ClassSize.COPYONWRITE_ARRAYSET + ClassSize.COPYONWRITE_ARRAYLIST +
      (2 * ClassSize.CONCURRENT_SKIPLISTMAP));

  /*
   * Calculate how the MemStore size has changed.  Includes overhead of the
   * backing Map.
   * @param kv
   * @param notpresent True if the kv was NOT present in the set.
   * @return Size
   */
  long heapSizeChange(final KeyValue kv, final boolean notpresent) {
    return notpresent ?
        ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + kv.heapSize()):
        0;
  }

  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
  @Override
  public long heapSize() {
    return size.get();
  }

  /**
   * Get the heap usage of KVs in this MemStore.
   */
  public long keySize() {
    return heapSize() - DEEP_OVERHEAD;
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
    byte [] fam = Bytes.toBytes("col");
    byte [] qf = Bytes.toBytes("umn");
    byte [] empty = new byte[0];
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      size += memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    MemStore memstore2 = new MemStore();
    for (int i = 0; i < count; i++) {
      size += memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i,
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

  /**
   * Observers want to know about MemStore changes.
   * Called when snapshot is cleared and when we make one.
   */
  interface ChangedMemStoreObserver {
    /**
     * Notify observers.
     * @throws IOException
     */
    void changedMemStore();
  }
}
