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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;

import java.io.IOException;
import java.util.List;

/**
 * A scanner that does a minor compaction at the same time.  Doesn't need to
 * implement ChangedReadersObserver, since it doesn't scan memstore, only store files
 * and optionally the memstore-snapshot.
 */
public class MinorCompactingStoreScanner implements KeyValueScanner, InternalScanner {
  private KeyValueHeap heap;
  private KeyValue.KVComparator comparator;

  MinorCompactingStoreScanner(Store store, List<? extends KeyValueScanner> scanners)
      throws IOException {
    comparator = store.comparator;
    KeyValue firstKv = KeyValue.createFirstOnRow(HConstants.EMPTY_START_ROW);
    for (KeyValueScanner scanner : scanners ) {
      scanner.seek(firstKv);
    }
    heap = new KeyValueHeap(scanners, store.comparator);
  }

  MinorCompactingStoreScanner(String cfName, KeyValue.KVComparator comparator,
                              List<? extends KeyValueScanner> scanners)
      throws IOException {
    this.comparator = comparator;

    KeyValue firstKv = KeyValue.createFirstOnRow(HConstants.EMPTY_START_ROW);
    for (KeyValueScanner scanner : scanners ) {
      scanner.seek(firstKv);
    }

    heap = new KeyValueHeap(scanners, comparator);
  }

  public KeyValue peek() {
    return heap.peek();
  }

  public KeyValue next() throws IOException {
    return heap.next();
  }

  @Override
  public boolean seek(KeyValue key) {
    // cant seek.
    throw new UnsupportedOperationException("Can't seek a MinorCompactingStoreScanner");
  }

  /**
   * High performance merge scan.
   * @param writer
   * @return True if more.
   * @throws IOException
   */
  public boolean next(HFile.Writer writer) throws IOException {
    KeyValue row = heap.peek();
    if (row == null) {
      close();
      return false;
    }
    KeyValue kv;
    while ((kv = heap.peek()) != null) {
      // check to see if this is a different row
      if (comparator.compareRows(row, kv) != 0) {
        // reached next row
        return true;
      }
      writer.append(heap.next());
    }
    close();
    return false;
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    KeyValue row = heap.peek();
    if (row == null) {
      close();
      return false;
    }
    KeyValue kv;
    while ((kv = heap.peek()) != null) {
      // check to see if this is a different row
      if (comparator.compareRows(row, kv) != 0) {
        // reached next row
        return true;
      }
      results.add(heap.next());
    }
    close();
    return false;
  }

  @Override
  public boolean next(List<KeyValue> results, int limit) throws IOException {
    // should not use limits with minor compacting store scanner
    return next(results);
  }

  public void close() {
    heap.close();
  }
}
