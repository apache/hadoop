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
package org.apache.hadoop.hbase.master;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/*
 * Data structure used to return results out of the toRowMap method.
 */
class RowMap {
  static final Log LOG = LogFactory.getLog(RowMap.class.getName());
  
  final Text row;
  final SortedMap<Text, byte[]> map;
  
  RowMap(final Text r, final SortedMap<Text, byte[]> m) {
    this.row = r;
    this.map = m;
  }

  Text getRow() {
    return this.row;
  }

  SortedMap<Text, byte[]> getMap() {
    return this.map;
  }
  
  /*
   * Convert an HbaseMapWritable to a Map keyed by column.
   * Utility method used scanning meta regions
   * @param mw The MapWritable to convert.  Cannot be null.
   * @return Returns a SortedMap currently.  TODO: This looks like it could
   * be a plain Map.
   */
  public static RowMap fromHbaseMapWritable(HbaseMapWritable mw) {
    if (mw == null) {
      throw new IllegalArgumentException("Passed MapWritable cannot be null");
    }
    SortedMap<Text, byte[]> m = new TreeMap<Text, byte[]>();
    Text row = null;
    for (Map.Entry<Writable, Writable> e: mw.entrySet()) {
      HStoreKey key = (HStoreKey) e.getKey();
      Text thisRow = key.getRow();
      if (row == null) {
        row = thisRow;
      } else {
        if (!row.equals(thisRow)) {
          LOG.error("Multiple rows in same scanner result set. firstRow=" +
            row + ", currentRow=" + thisRow);
        }
      }
      m.put(key.getColumn(), ((ImmutableBytesWritable) e.getValue()).get());
    }
    return new RowMap(row, m);
  }
}