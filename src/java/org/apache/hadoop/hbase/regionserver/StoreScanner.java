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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

/**
 * Scanner scans both the memcache and the HStore. Coaleace KeyValue stream
 * into List<KeyValue> for a single row.
 */
class StoreScanner implements KeyValueScanner, InternalScanner,
ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);

  private Store store;

  private ScanQueryMatcher matcher;

  private KeyValueHeap heap;

  // Used around transition from no storefile to the first.
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * Opens a scanner across memcache, snapshot, and all StoreFiles.
   */
  StoreScanner(Store store, Scan scan, final NavigableSet<byte[]> columns) {
    this.store = store;
    matcher = new ScanQueryMatcher(scan, store.getFamily().getName(),
        columns, store.ttl, store.comparator.getRawComparator(),
        store.versionsToReturn(scan.getMaxVersions()));

    List<KeyValueScanner> scanners = getStoreFileScanners();
    scanners.add(store.memcache.getScanner());

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(
        scanners.toArray(new KeyValueScanner[scanners.size()]), store.comparator);

    this.store.addChangedReaderObserver(this);
  }

  // Constructor for testing.
  StoreScanner(Scan scan, byte [] colFamily,
      long ttl, KeyValue.KVComparator comparator,
      final NavigableSet<byte[]> columns,
      KeyValueScanner [] scanners) {
    this.store = null;
    this.matcher = new ScanQueryMatcher(scan, colFamily, columns, ttl, 
        comparator.getRawComparator(), scan.getMaxVersions());

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    heap = new KeyValueHeap(
        scanners, comparator);
  }

  public KeyValue peek() {
    return this.heap.peek();
  }

  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  public void close() {
    this.closing.set(true);
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    this.heap.close();
  }

  public boolean seek(KeyValue key) {

    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param result
   * @return true if there are more rows, false if scanner is done
   */
  public boolean next(List<KeyValue> result) throws IOException {
    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return false;
    }
    matcher.setRow(peeked.getRow());
    KeyValue kv;
    while((kv = this.heap.peek()) != null) {
      QueryMatcher.MatchCode mc = matcher.match(kv);
      switch(mc) {
        case INCLUDE:
          KeyValue next = this.heap.next();
          result.add(next);
          continue;
        case DONE:
          // what happens if we have 0 results?
          if (result.isEmpty()) {
            // try the next one.
            matcher.setRow(this.heap.peek().getRow());
            continue;
          }
          if (matcher.filterEntireRow()) {
            // wow, well, um, reset the result and continue.
            result.clear();
            matcher.setRow(heap.peek().getRow());
            continue;
          }

          return true;

        case DONE_SCAN:
          close();
          return false;

        case SEEK_NEXT_ROW:
          // TODO see comments in SEEK_NEXT_COL
          /*
          KeyValue rowToSeek =
              new KeyValue(kv.getRow(),
                  0,
                  KeyValue.Type.Minimum);
          heap.seek(rowToSeek);
           */
          heap.next();
          break;

        case SEEK_NEXT_COL:
          // TODO hfile needs 'hinted' seeking to prevent it from
          // reseeking from the start of the block on every dang seek.
          // We need that API and expose it the scanner chain.
          /*
          ColumnCount hint = matcher.getSeekColumn();
          KeyValue colToSeek;
          if (hint == null) {
            // seek to the 'last' key on this column, this is defined
            // as the key with the same row, fam, qualifier,
            // smallest timestamp, largest type.
            colToSeek =
                new KeyValue(kv.getRow(),
                    kv.getFamily(),
                    kv.getColumn(),
                    Long.MIN_VALUE,
                    KeyValue.Type.Minimum);
          } else {
            // This is ugmo.  Move into KeyValue convience method.
            // First key on a column is:
            // same row, cf, qualifier, max_timestamp, max_type, no value.
            colToSeek =
                new KeyValue(kv.getRow(),
                    0,
                    kv.getRow().length,

                    kv.getFamily(),
                    0,
                    kv.getFamily().length,

                    hint.getBuffer(),
                    hint.getOffset(),
                    hint.getLength(),

                    Long.MAX_VALUE,
                    KeyValue.Type.Maximum,
                    null,
                    0,
                    0);
          }
          heap.seek(colToSeek);
           */

          heap.next();
          break;

        case SKIP:
          this.heap.next();
          break;
      }
    }
    if(result.size() > 0) {
      return true;
    }
    // No more keys
    close();
    return false;
  }

  private List<KeyValueScanner> getStoreFileScanners() {
    List<HFileScanner> s =
      new ArrayList<HFileScanner>(this.store.getStorefilesCount());
    Map<Long, StoreFile> map = this.store.getStorefiles().descendingMap();
    for(StoreFile sf : map.values()) {
      s.add(sf.getReader().getScanner());
    }
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(s.size()+1);
    for(HFileScanner hfs : s) {
      scanners.add(new StoreFileScanner(hfs));
    }
    return scanners;
  }

  // Implementation of ChangedReadersObserver
  public void updateReaders() throws IOException {
    if (this.closing.get()) {
      return;
    }
    this.lock.writeLock().lock();
    try {
      // Could do this pretty nicely with KeyValueHeap, but the existing
      // implementation of this method only updated if no existing storefiles?
      // Lets discuss.
      return;
    } finally {
      this.lock.writeLock().unlock();
    }
  }
}