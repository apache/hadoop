/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of RowFilterInterface that filters out rows greater than or 
 * equal to a specified rowKey.
 *
 * @deprecated Use filters that are rooted on @{link Filter} instead
 */
public class StopRowFilter implements RowFilterInterface {
  private byte [] stopRowKey;
  
  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public StopRowFilter() {
    super();
  }

  /**
   * Constructor that takes a stopRowKey on which to filter
   * 
   * @param stopRowKey rowKey to filter on.
   */
  public StopRowFilter(final byte [] stopRowKey) {
    this.stopRowKey = stopRowKey;
  }
  
  /**
   * An accessor for the stopRowKey
   * 
   * @return the filter's stopRowKey
   */
  public byte [] getStopRowKey() {
    return this.stopRowKey;
  }

  public void validate(final byte [][] columns) {
    // Doesn't filter columns
  }

  public void reset() {
    // Nothing to reset
  }

  public void rowProcessed(boolean filtered, byte [] rowKey) {
    // Doesn't care
  }

  public void rowProcessed(boolean filtered, byte[] key, int offset, int length) {
    // Doesn't care
  }

  public boolean processAlways() {
    return false;
  }
  
  public boolean filterAllRemaining() {
    return false;
  }

  public boolean filterRowKey(final byte [] rowKey) {
    return filterRowKey(rowKey, 0, rowKey.length);
  }

  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    if (rowKey == null) {
      if (this.stopRowKey == null) {
        return true;
      }
      return false;
    }
    return Bytes.compareTo(stopRowKey, 0, stopRowKey.length, rowKey, offset,
      length) <= 0;
  }

  /**
   * Because StopRowFilter does not examine column information, this method 
   * defaults to calling the rowKey-only version of filter.
   * @param rowKey 
   * @param colKey 
   * @param data 
   * @return boolean
   */
  public boolean filterColumn(final byte [] rowKey, final byte [] colKey,
      final byte[] data) {
    return filterRowKey(rowKey);
  }

  public boolean filterColumn(byte[] rowKey, int roffset, int rlength,
      byte[] colunmName, int coffset, int clength, byte[] columnValue,
      int voffset, int vlength) {
    return filterRowKey(rowKey, roffset, rlength);
  }

  /**
   * Because StopRowFilter does not examine column information, this method 
   * defaults to calling filterAllRemaining().
   * @param columns 
   * @return boolean
   */
  public boolean filterRow(final SortedMap<byte [], Cell> columns) {
    return filterAllRemaining();
  }

  public boolean filterRow(List<KeyValue> results) {
    return filterAllRemaining();
  }

  public void readFields(DataInput in) throws IOException {
    this.stopRowKey = Bytes.readByteArray(in);
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.stopRowKey);
  }
}