/**
 * Copyright 2008 The Apache Software Foundation
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

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * A scanner that iterates through the HStore files
 */
class StoreFileScanner extends HAbstractScanner {
  @SuppressWarnings("hiding")
  private MapFile.Reader[] readers;
  private HStore store;
  
  public StoreFileScanner(HStore store, long timestamp, Text[] targetCols, Text firstRow)
  throws IOException {
    super(timestamp, targetCols);
    this.store = store;
    try {
      this.readers = new MapFile.Reader[store.storefiles.size()];
      
      // Most recent map file should be first
      int i = readers.length - 1;
      for(HStoreFile curHSF: store.storefiles.values()) {
        readers[i--] = curHSF.getReader(store.fs, store.bloomFilter);
      }
      
      this.keys = new HStoreKey[readers.length];
      this.vals = new byte[readers.length][];
      
      // Advance the readers to the first pos.
      for(i = 0; i < readers.length; i++) {
        keys[i] = new HStoreKey();
        
        if(firstRow.getLength() != 0) {
          if(findFirstRow(i, firstRow)) {
            continue;
          }
        }
        
        while(getNext(i)) {
          if(columnMatch(i)) {
            break;
          }
        }
      }
      
    } catch (Exception ex) {
      close();
      IOException e = new IOException("HStoreScanner failed construction");
      e.initCause(ex);
      throw e;
    }
  }

  /**
   * The user didn't want to start scanning at the first row. This method
   * seeks to the requested row.
   *
   * @param i         - which iterator to advance
   * @param firstRow  - seek to this row
   * @return          - true if this is the first row or if the row was not found
   */
  @Override
  boolean findFirstRow(int i, Text firstRow) throws IOException {
    ImmutableBytesWritable ibw = new ImmutableBytesWritable();
    HStoreKey firstKey
      = (HStoreKey)readers[i].getClosest(new HStoreKey(firstRow), ibw);
    if (firstKey == null) {
      // Didn't find it. Close the scanner and return TRUE
      closeSubScanner(i);
      return true;
    }
    this.vals[i] = ibw.get();
    keys[i].setRow(firstKey.getRow());
    keys[i].setColumn(firstKey.getColumn());
    keys[i].setVersion(firstKey.getTimestamp());
    return columnMatch(i);
  }
  
  /**
   * Get the next value from the specified reader.
   * 
   * @param i - which reader to fetch next value from
   * @return - true if there is more data available
   */
  @Override
  boolean getNext(int i) throws IOException {
    boolean result = false;
    ImmutableBytesWritable ibw = new ImmutableBytesWritable();
    while (true) {
      if (!readers[i].next(keys[i], ibw)) {
        closeSubScanner(i);
        break;
      }
      if (keys[i].getTimestamp() <= this.timestamp) {
        vals[i] = ibw.get();
        result = true;
        break;
      }
    }
    return result;
  }
  
  /** Close down the indicated reader. */
  @Override
  void closeSubScanner(int i) {
    try {
      if(readers[i] != null) {
        try {
          readers[i].close();
        } catch(IOException e) {
          LOG.error(store.storeName + " closing sub-scanner", e);
        }
      }
      
    } finally {
      readers[i] = null;
      keys[i] = null;
      vals[i] = null;
    }
  }

  /** Shut it down! */
  public void close() {
    if(! scannerClosed) {
      try {
        for(int i = 0; i < readers.length; i++) {
          if(readers[i] != null) {
            try {
              readers[i].close();
            } catch(IOException e) {
              LOG.error(store.storeName + " closing scanner", e);
            }
          }
        }
        
      } finally {
        scannerClosed = true;
      }
    }
  }
}
