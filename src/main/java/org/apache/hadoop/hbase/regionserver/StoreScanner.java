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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

/**
 * Scanner scans both the memstore and the HStore. Coaleace KeyValue stream
 * into List<KeyValue> for a single row.
 */
class StoreScanner implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  private Store store;
  private ScanQueryMatcher matcher;
  private KeyValueHeap heap;
  private boolean cacheBlocks;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private boolean closing = false;
  private final boolean isGet;

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles.
   *
   * @param store who we scan
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException 
   */
  StoreScanner(Store store, Scan scan, final NavigableSet<byte[]> columns) throws IOException {
    this.store = store;
    this.cacheBlocks = scan.getCacheBlocks();
    matcher = new ScanQueryMatcher(scan, store.getFamily().getName(),
        columns, store.ttl, store.comparator.getRawComparator(),
        store.versionsToReturn(scan.getMaxVersions()));

    this.isGet = scan.isGetScan();
    // pass columns = try to filter out unnecessary ScanFiles
    List<KeyValueScanner> scanners = getScanners(scan, columns);

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);

    this.store.addChangedReaderObserver(this);
  }

  /**
   * Used for major compactions.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancilliary scanners
   */
  StoreScanner(Store store, Scan scan, List<? extends KeyValueScanner> scanners)
      throws IOException {
    this.store = store;
    this.cacheBlocks = false;
    this.isGet = false;
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
      final List<KeyValueScanner> scanners)
        throws IOException {
    this.store = null;
    this.isGet = false;
    this.cacheBlocks = scan.getCacheBlocks();
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
  private List<KeyValueScanner> getScanners() throws IOException {
    // First the store file scanners
    
    // TODO this used to get the store files in descending order,
    // but now we get them in ascending order, which I think is
    // actually more correct, since memstore get put at the end.
    List<StoreFileScanner> sfScanners = StoreFileScanner
      .getScannersForStoreFiles(store.getStorefiles(), cacheBlocks, isGet);
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(sfScanners.size()+1);
    scanners.addAll(sfScanners);
    // Then the memstore scanners
    scanners.addAll(this.store.memstore.getScanners());
    return scanners;
  }

  /*
   * @return List of scanners to seek, possibly filtered by StoreFile.
   */
  private List<KeyValueScanner> getScanners(Scan scan, 
      final NavigableSet<byte[]> columns) throws IOException {
    // First the store file scanners
    List<StoreFileScanner> sfScanners = StoreFileScanner
      .getScannersForStoreFiles(store.getStorefiles(), cacheBlocks, isGet);
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(sfScanners.size()+1);

    // exclude scan files that have failed file filters
    for(StoreFileScanner sfs : sfScanners) {
      if (isGet && 
          !sfs.getHFileScanner().shouldSeek(scan.getStartRow(), columns)) {
        continue; // exclude this hfs
      }
      scanners.add(sfs);
    }

    // Then the memstore scanners
    scanners.addAll(this.store.memstore.getScanners());
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
    if (this.closing) return;
    this.closing = true;
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    this.heap.close();
  }

  public synchronized boolean seek(KeyValue key) throws IOException {
    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @return true if there are more rows, false if scanner is done
   */
  public synchronized boolean next(List<KeyValue> outResult, int limit) throws IOException {
    //DebugPrint.println("SS.next");
    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return false;
    }
    matcher.setRow(peeked.getRow());
    KeyValue kv;
    List<KeyValue> results = new ArrayList<KeyValue>();
    LOOP: while((kv = this.heap.peek()) != null) {
      QueryMatcher.MatchCode qcode = matcher.match(kv);
      //DebugPrint.println("SS peek kv = " + kv + " with qcode = " + qcode);
      switch(qcode) {
        case INCLUDE:
          KeyValue next = this.heap.next();
          results.add(next);
          if (limit > 0 && (results.size() == limit)) {
            break LOOP;
          }
          continue;

        case DONE:
          // copy jazz
          outResult.addAll(results);
          return true;

        case DONE_SCAN:
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

    if (!results.isEmpty()) {
      // copy jazz
      outResult.addAll(results);
      return true;
    }

    // No more keys
    close();
    return false;
  }

  public synchronized boolean next(List<KeyValue> outResult) throws IOException {
    return next(outResult, -1);
  }

  // Implementation of ChangedReadersObserver
  public synchronized void updateReaders() throws IOException {
    if (this.closing) return;
    KeyValue topKey = this.peek();
    if (topKey == null) return;

    List<KeyValueScanner> scanners = getScanners();

    // close the previous scanners:
    this.heap.close(); // bubble thru and close all scanners.
    this.heap = null; // the re-seeks could be slow (access HDFS) free up memory ASAP

    for(KeyValueScanner scanner : scanners) {
      scanner.seek(topKey);
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);

    // Reset the state of the Query Matcher and set to top row
    matcher.reset();
    KeyValue kv = heap.peek();
    matcher.setRow((kv == null ? topKey : kv).getRow());
  }
}
