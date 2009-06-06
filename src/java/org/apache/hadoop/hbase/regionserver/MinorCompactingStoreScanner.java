/*
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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.io.IOException;

/**
 * A scanner that does a minor compaction at the same time.  Doesn't need to
 * implement ChangedReadersObserver, since it doesn't scan memcache, only store files
 * and optionally the memcache-snapshot.
 */
public class MinorCompactingStoreScanner implements KeyValueScanner, InternalScanner {
  private QueryMatcher matcher;

  private KeyValueHeap heap;


  MinorCompactingStoreScanner(Store store,
                              KeyValueScanner [] scanners) {
    Scan scan = new Scan();

    // No max version, no ttl matching, start at first row, all columns.
    matcher = new ScanQueryMatcher(scan, store.getFamily().getName(),
        null, Long.MAX_VALUE, store.comparator.getRawComparator(),
        store.versionsToReturn(Integer.MAX_VALUE));

    for (KeyValueScanner scanner : scanners ) {
      scanner.seek(matcher.getStartKey());
    }

    heap = new KeyValueHeap(scanners, store.comparator);
  }

  MinorCompactingStoreScanner(String cfName, KeyValue.KVComparator comparator,
                              KeyValueScanner [] scanners) {
    Scan scan = new Scan();
    matcher = new ScanQueryMatcher(scan, Bytes.toBytes(cfName),
        null, Long.MAX_VALUE, comparator.getRawComparator(),
        Integer.MAX_VALUE);

    for (KeyValueScanner scanner : scanners ) {
      scanner.seek(matcher.getStartKey());
    }

    heap = new KeyValueHeap(scanners, comparator);
  }

  public KeyValue peek() {
    return heap.peek();
  }

  public KeyValue next() {
    return heap.next();
  }

  @Override
  public boolean seek(KeyValue key) {
    // cant seek.
    throw new UnsupportedOperationException("Can't seek a MinorCompactingStoreScanner");
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    KeyValue peeked = heap.peek();
    if (peeked == null) {
      close();
      return false;
    }
    matcher.setRow(peeked.getRow());
    KeyValue kv;
    while ((kv = heap.peek()) != null) {
      // if delete type, output no matter what:
      if (kv.getType() != KeyValue.Type.Put.getCode())
        results.add(kv);

      switch (matcher.match(kv)) {
        case INCLUDE:
          results.add(heap.next());
          continue;
        case DONE:
          if (results.isEmpty()) {
            matcher.setRow(heap.peek().getRow());
            continue;
          }
          return true;
      }
      heap.next();
    }
    close();
    return false;
  }

  public void close() {
    heap.close();
  }
}
