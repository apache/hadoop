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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

/**
 * Scanner scans both the memstore and the HStore. Coaleace KeyValue stream
 * into List<KeyValue> for a single row.
 */
class StoreScanner implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  private Store store;
  private ScanQueryMatcher matcher;
  private KeyValueHeap heap;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private final AtomicBoolean closing = new AtomicBoolean(false);

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles.
   */
  StoreScanner(Store store, Scan scan, final NavigableSet<byte[]> columns) {
    this.store = store;
    matcher = new ScanQueryMatcher(scan, store.getFamily().getName(),
        columns, store.ttl, store.comparator.getRawComparator(),
        store.versionsToReturn(scan.getMaxVersions()));

    List<KeyValueScanner> scanners = getScanners();

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(
      scanners.toArray(new KeyValueScanner[scanners.size()]), store.comparator);

    this.store.addChangedReaderObserver(this);
  }

  /**
   * Used for major compactions.<p>
   * 
   * Opens a scanner across specified StoreFiles.
   */
  StoreScanner(Store store, Scan scan, KeyValueScanner [] scanners) {
    this.store = store;
    matcher = new ScanQueryMatcher(scan, store.getFamily().getName(),
        null, store.ttl, store.comparator.getRawComparator(),
        store.versionsToReturn(scan.getMaxVersions()));

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);
  }

  // Constructor for testing.
  StoreScanner(final Scan scan, final byte [] colFamily, final long ttl,
      final KeyValue.KVComparator comparator,
      final NavigableSet<byte[]> columns,
      final KeyValueScanner [] scanners) {
    this.store = null;
    this.matcher = new ScanQueryMatcher(scan, colFamily, columns, ttl, 
        comparator.getRawComparator(), scan.getMaxVersions());

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }
    heap = new KeyValueHeap(scanners, comparator);
  }

  /*
   * @return List of scanners ordered properly.
   */
  private List<KeyValueScanner> getScanners() {
    List<KeyValueScanner> scanners = getStoreFileScanners();
    KeyValueScanner [] memstorescanners = this.store.memstore.getScanners();
    for (int i = memstorescanners.length - 1; i >= 0; i--) {
      scanners.add(memstorescanners[i]);
    }
    return scanners;
  }

  public synchronized KeyValue peek() {
    return this.heap.peek();
  }

  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  public synchronized void close() {
    this.closing.set(true);
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    this.heap.close();
  }

  public synchronized boolean seek(KeyValue key) {
    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param result
   * @return true if there are more rows, false if scanner is done
   */
  public synchronized boolean next(List<KeyValue> outResult) throws IOException {
    List<KeyValue> results = new ArrayList<KeyValue>();
    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return false;
    }
    matcher.setRow(peeked.getRow());
    KeyValue kv;
    while((kv = this.heap.peek()) != null) {
      QueryMatcher.MatchCode qcode = matcher.match(kv);
      switch(qcode) {
        case INCLUDE:
          KeyValue next = this.heap.next();
          results.add(next);
          continue;
          
        case DONE:
          if (matcher.filterEntireRow()) {
            // nuke all results, and then return.
            results.clear();
          }

          // copy jazz
          outResult.addAll(results);
          return true;

        case DONE_SCAN:
          if (matcher.filterEntireRow()) {
            // nuke all results, and then return.
            results.clear();
          }
          close();

          // copy jazz
          outResult.addAll(results);

          return false;

        case SEEK_NEXT_ROW:
          heap.next();
          break;

        case SEEK_NEXT_COL:
          // TODO hfile needs 'hinted' seeking to prevent it from
          // reseeking from the start of the block on every dang seek.
          // We need that API and expose it the scanner chain.
          heap.next();
          break;

        case SKIP:
          this.heap.next();
          break;
          
        default:
          throw new RuntimeException("UNEXPECTED");
      }
    }
 
    if (matcher.filterEntireRow()) {
      // nuke all results, and then return.
      results.clear();
    }
    
    if (!results.isEmpty()) {
      // copy jazz
      outResult.addAll(results);

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
      HFile.Reader r = sf.getReader();
      if (r == null) {
        LOG.warn("StoreFile " + sf + " has null Reader");
        continue;
      }
      s.add(r.getScanner());
    }
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(s.size()+1);
    for(HFileScanner hfs : s) {
      scanners.add(new StoreFileScanner(hfs));
    }
    return scanners;
  }

  // Implementation of ChangedReadersObserver
  public synchronized void updateReaders() throws IOException {
    if (this.closing.get()) return;
    KeyValue topKey = this.peek();
    if (topKey == null) return;
    List<KeyValueScanner> scanners = getScanners();

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(topKey);
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(
        scanners.toArray(new KeyValueScanner[scanners.size()]), store.comparator);

    // Reset the state of the Query Matcher and set to top row
    matcher.reset();
    matcher.setRow(heap.peek().getRow());
  }
}