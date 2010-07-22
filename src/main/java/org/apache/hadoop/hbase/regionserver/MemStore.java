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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.rmi.UnexpectedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

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

  TimeRangeTracker timeRangeTracker;
  TimeRangeTracker snapshotTimeRangeTracker;

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
    timeRangeTracker = new TimeRangeTracker();
    snapshotTimeRangeTracker = new TimeRangeTracker();
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
   * Snapshot must be cleared by call to {@link #clearSnapshot(SortedSet<KeyValue>)}
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
          this.snapshotTimeRangeTracker = this.timeRangeTracker;
          this.timeRangeTracker = new TimeRangeTracker();
          // Reset heap to not include any keys
          this.size.set(DEEP_OVERHEAD);
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
   * @see {@link #clearSnapshot(SortedSet<KeyValue>)}
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
  void clearSnapshot(final SortedSet<KeyValue> ss)
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
        this.snapshotTimeRangeTracker = new TimeRangeTracker();
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
      timeRangeTracker.includeTimestamp(kv);
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
    try {
      s += heapSizeChange(delete, this.kvset.add(delete));
      timeRangeTracker.includeTimestamp(delete);
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
   * @param state column/delete tracking state
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

  /**
   * Given the specs of a column, update it, first by inserting a new record,
   * then removing the old one.  Since there is only 1 KeyValue involved, the memstoreTS
   * will be set to 0, thus ensuring that they instantly appear to anyone. The underlying
   * store will ensure that the insert/delete each are atomic. A scanner/reader will either
   * get the new value, or the old value and all readers will eventually only see the new
   * value after the old was removed.
   *
   * @param row
   * @param family
   * @param qualifier
   * @param newValue
   * @param now
   * @return
   */
  public long updateColumnValue(byte[] row,
                                byte[] family,
                                byte[] qualifier,
                                long newValue,
                                long now) {
   this.lock.readLock().lock();
    try {
      KeyValue firstKv = KeyValue.createFirstOnRow(
          row, family, qualifier);
      // create a new KeyValue with 'now' and a 0 memstoreTS == immediately visible
      KeyValue newKv;
      // Is there a KeyValue in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
      SortedSet<KeyValue> snSs = snapshot.tailSet(firstKv);
      if (!snSs.isEmpty()) {
        KeyValue snKv = snSs.first();
        // is there a matching KV in the snapshot?
        if (snKv.matchingRow(firstKv) && snKv.matchingQualifier(firstKv)) {
          if (snKv.getTimestamp() == now) {
            // poop,
            now += 1;
          }
        }
      }

      // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
      // But the timestamp should also be max(now, mostRecentTsInMemstore)

      // so we cant add the new KV w/o knowing what's there already, but we also
      // want to take this chance to delete some kvs. So two loops (sad)

      SortedSet<KeyValue> ss = kvset.tailSet(firstKv);
      Iterator<KeyValue> it = ss.iterator();
      while ( it.hasNext() ) {
        KeyValue kv = it.next();

        // if this isnt the row we are interested in, then bail:
        if (!firstKv.matchingRow(kv)) {
          break; // rows dont match, bail.
        }

        // if the qualifier matches and it's a put, just RM it out of the kvset.
        if (firstKv.matchingQualifier(kv)) {
          // to be extra safe we only remove Puts that have a memstoreTS==0
          if (kv.getType() == KeyValue.Type.Put.getCode()) {
            now = Math.max(now, kv.getTimestamp());
          }
        }
      }


      // add the new value now. this might have the same TS as an existing KV, thus confusing
      // readers slightly for a MOMENT until we erase the old one (and thus old value).
      newKv = new KeyValue(row, family, qualifier,
          now,
          Bytes.toBytes(newValue));
      long addedSize = add(newKv);

      // remove extra versions.
      ss = kvset.tailSet(firstKv);
      it = ss.iterator();
      while ( it.hasNext() ) {
        KeyValue kv = it.next();

        if (kv == newKv) {
          // ignore the one i just put in (heh)
          continue;
        }

        // if this isnt the row we are interested in, then bail:
        if (!firstKv.matchingRow(kv)) {
          break; // rows dont match, bail.
        }

        // if the qualifier matches and it's a put, just RM it out of the kvset.
        if (firstKv.matchingQualifier(kv)) {
          // to be extra safe we only remove Puts that have a memstoreTS==0
          if (kv.getType() == KeyValue.Type.Put.getCode()) {
            // false means there was a change, so give us the size.
            addedSize -= heapSizeChange(kv, false);

            it.remove();
          }
        }
      }

      return addedSize;
    } finally {
      this.lock.readLock().unlock();
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
  List<KeyValueScanner> getScanners() {
    this.lock.readLock().lock();
    try {
      return Collections.<KeyValueScanner>singletonList(
          new MemStoreScanner());
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Check if this memstore may contain the required keys
   * @param scan
   * @return False if the key definitely does not exist in this Memstore
   */
  public boolean shouldSeek(Scan scan) {
    return timeRangeTracker.includesTimeRange(scan.getTimeRange()) ||
        snapshotTimeRangeTracker.includesTimeRange(scan.getTimeRange());
  }

  public TimeRangeTracker getSnapshotTimeRangeTracker() {
    return this.snapshotTimeRangeTracker;
  }

  /*
   * MemStoreScanner implements the KeyValueScanner.
   * It lets the caller scan the contents of a memstore -- both current
   * map and snapshot.
   * This behaves as if it were a real scanner but does not maintain position.
   */
  protected class MemStoreScanner implements KeyValueScanner {
    // Next row information for either kvset or snapshot
    private KeyValue kvsetNextRow = null;
    private KeyValue snapshotNextRow = null;

    // iterator based scanning.
    Iterator<KeyValue> kvsetIt;
    Iterator<KeyValue> snapshotIt;

    /*
    Some notes...

     So memstorescanner is fixed at creation time. this includes pointers/iterators into
    existing kvset/snapshot.  during a snapshot creation, the kvset is null, and the
    snapshot is moved.  since kvset is null there is no point on reseeking on both,
      we can save us the trouble. During the snapshot->hfile transition, the memstore
      scanner is re-created by StoreScanner#updateReaders().  StoreScanner should
      potentially do something smarter by adjusting the existing memstore scanner.

      But there is a greater problem here, that being once a scanner has progressed
      during a snapshot scenario, we currently iterate past the kvset then 'finish' up.
      if a scan lasts a little while, there is a chance for new entries in kvset to
      become available but we will never see them.  This needs to be handled at the
      StoreScanner level with coordination with MemStoreScanner.

    */

    MemStoreScanner() {
      super();

      //DebugPrint.println(" MS new@" + hashCode());
    }

    protected KeyValue getNext(Iterator<KeyValue> it) {
      KeyValue ret = null;
      long readPoint = ReadWriteConsistencyControl.getThreadReadPoint();
      //DebugPrint.println( " MS@" + hashCode() + ": threadpoint = " + readPoint);

      while (ret == null && it.hasNext()) {
        KeyValue v = it.next();
        if (v.getMemstoreTS() <= readPoint) {
          // keep it.
          ret = v;
        }
      }
      return ret;
    }

    public synchronized boolean seek(KeyValue key) {
      if (key == null) {
        close();
        return false;
      }

      // kvset and snapshot will never be empty.
      // if tailSet cant find anything, SS is empty (not null).
      SortedSet<KeyValue> kvTail = kvset.tailSet(key);
      SortedSet<KeyValue> snapshotTail = snapshot.tailSet(key);

      kvsetIt = kvTail.iterator();
      snapshotIt = snapshotTail.iterator();

      kvsetNextRow = getNext(kvsetIt);
      snapshotNextRow = getNext(snapshotIt);


      //long readPoint = ReadWriteConsistencyControl.getThreadReadPoint();
      //DebugPrint.println( " MS@" + hashCode() + " kvset seek: " + kvsetNextRow + " with size = " +
      //    kvset.size() + " threadread = " + readPoint);
      //DebugPrint.println( " MS@" + hashCode() + " snapshot seek: " + snapshotNextRow + " with size = " +
      //    snapshot.size() + " threadread = " + readPoint);


      KeyValue lowest = getLowest();

      // has data := (lowest != null)
      return lowest != null;
    }

    public synchronized KeyValue peek() {
      //DebugPrint.println(" MS@" + hashCode() + " peek = " + getLowest());
      return getLowest();
    }


    public synchronized KeyValue next() {
      KeyValue theNext = getLowest();

      if (theNext == null) {
          return null;
      }

      // Advance one of the iterators
      if (theNext == kvsetNextRow) {
        kvsetNextRow = getNext(kvsetIt);
      } else {
        snapshotNextRow = getNext(snapshotIt);
      }

      //long readpoint = ReadWriteConsistencyControl.getThreadReadPoint();
      //DebugPrint.println(" MS@" + hashCode() + " next: " + theNext + " next_next: " +
      //    getLowest() + " threadpoint=" + readpoint);
      return theNext;
    }

    protected KeyValue getLowest() {
      return getLower(kvsetNextRow,
          snapshotNextRow);
    }

    /*
     * Returns the lower of the two key values, or null if they are both null.
     * This uses comparator.compare() to compare the KeyValue using the memstore
     * comparator.
     */
    protected KeyValue getLower(KeyValue first, KeyValue second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare <= 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    public synchronized void close() {
      this.kvsetNextRow = null;
      this.snapshotNextRow = null;

      this.kvsetIt = null;
      this.snapshotIt = null;
    }
  }

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (9 * ClassSize.REFERENCE));

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
   * @param args main args
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
}
