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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedMap;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * RowFilterInterface that filters everything that does not match a prefix
 */
public class PrefixRowFilter implements RowFilterInterface {
  protected byte[] prefix;
  
  /**
   * Constructor that takes a row prefix to filter on
   */
  public PrefixRowFilter(byte[] prefix) {
    this.prefix = prefix;
  }
  
  /**
   * Default Constructor, filters nothing. Required for RPC
   * deserialization
   */
  @SuppressWarnings("unused")
  public PrefixRowFilter() { }
  
  @SuppressWarnings("unused")
  public void reset() {
    // Nothing to reset
  }
  
  @SuppressWarnings("unused")
  public void rowProcessed(boolean filtered, byte [] key) {
    // does not care
  }
  
  public boolean processAlways() {
    return false;
  }
  
  public boolean filterAllRemaining() {
    return false;
  }
  
  public boolean filterRowKey(final byte [] rowKey) {
    if (rowKey == null)
      return true;
    if (rowKey.length < prefix.length)
      return true;
    for(int i = 0;i < prefix.length;i++)
      if (prefix[i] != rowKey[i])
        return true;
    return false;
  }

  @SuppressWarnings("unused")
  public boolean filterColumn(final byte [] rowKey, final byte [] colunmName,
      final byte[] columnValue) {
    return false;
  }

  @SuppressWarnings("unused")
  public boolean filterRow(final SortedMap<byte [], Cell> columns) {
    return false;
  }

  @SuppressWarnings("unused")
  public void validate(final byte [][] columns) {
    // does not do this
  }
  
  public void readFields(final DataInput in) throws IOException {
    prefix = Bytes.readByteArray(in);
  }
  
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, prefix);
  }
}
