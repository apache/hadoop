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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Use to execute a get by scanning all the store files in order.
 */
public class StoreFileGetScan {

  private List<HFileScanner> scanners;
  private QueryMatcher matcher;

  private KeyValue startKey;

  /**
   * Constructor
   * @param scanners
   * @param matcher
   */
  public StoreFileGetScan(List<HFileScanner> scanners, QueryMatcher matcher) {
    this.scanners = scanners;
    this.matcher = matcher;
    this.startKey = matcher.getStartKey();
  }

  /**
   * Performs a GET operation across multiple StoreFiles.
   * <p>
   * This style of StoreFile scanning goes through each
   * StoreFile in its entirety, most recent first, before
   * proceeding to the next StoreFile.
   * <p>
   * This strategy allows for optimal, stateless (no persisted Scanners)
   * early-out scenarios.
   * @param result List to add results to
   * @throws IOException
   */
  public void get(List<KeyValue> result) throws IOException {
    for(HFileScanner scanner : this.scanners) {
      this.matcher.update();
      if (getStoreFile(scanner, result) || matcher.isDone()) {
        return;
      }
    }
  }

  /**
   * Performs a GET operation on a single StoreFile.
   * @param scanner
   * @param result
   * @return true if done with this store, false if must continue to next
   * @throws IOException
   */
  public boolean getStoreFile(HFileScanner scanner, List<KeyValue> result)
  throws IOException {
    if (scanner.seekTo(startKey.getBuffer(), startKey.getKeyOffset(),
        startKey.getKeyLength()) == -1) {
      // No keys in StoreFile at or after specified startKey
      // First row may be = our row, so we have to check anyways.
      byte [] firstKey = scanner.getReader().getFirstKey();
      // Key may be null if storefile is empty.
      if (firstKey == null) return false;
      short rowLen = Bytes.toShort(firstKey, 0, Bytes.SIZEOF_SHORT);
      int rowOffset = Bytes.SIZEOF_SHORT;
      if (this.matcher.rowComparator.compareRows(firstKey, rowOffset, rowLen,
          startKey.getBuffer(), startKey.getRowOffset(), startKey.getRowLength())
          != 0)
        return false;
      scanner.seekTo();
    }
    do {
      KeyValue kv = scanner.getKeyValue();
      switch(matcher.match(kv)) {
        case INCLUDE:
          result.add(kv);
          break;
        case SKIP:
          break;
        case NEXT:
          return false;
        case DONE:
          return true;
      }
    } while(scanner.next());
    return false;
  }

}
