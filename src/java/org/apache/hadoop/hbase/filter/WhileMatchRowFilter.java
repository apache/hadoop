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
import java.util.SortedMap;

import org.apache.hadoop.hbase.io.Cell;

/**
 * WhileMatchRowFilter is a wrapper filter that filters everything after the 
 * first filtered row.  Once the nested filter returns true for either of it's 
 * filter(..) methods or filterNotNull(SortedMap<Text, byte[]>), this wrapper's 
 * filterAllRemaining() will return true.  All filtering methods will 
 * thereafter defer to the result of filterAllRemaining().
 */
public class WhileMatchRowFilter implements RowFilterInterface {
  
  private boolean filterAllRemaining = false;
  private RowFilterInterface filter;

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public WhileMatchRowFilter() {
    super();
  }
  
  /**
   * Constructor
   * @param filter
   */
  public WhileMatchRowFilter(RowFilterInterface filter) {
    this.filter = filter;
  }
  
  /**
   * Returns the internal filter being wrapped
   * 
   * @return the internal filter
   */
  public RowFilterInterface getInternalFilter() {
    return this.filter;
  }
  
  /** {@inheritDoc} */
  public void reset() {
    this.filterAllRemaining = false;
    this.filter.reset();
  }

  /** {@inheritDoc} */
  public boolean processAlways() {
    return true;
  }
  
  /**
   * Returns true once the nested filter has filtered out a row (returned true 
   * on a call to one of it's filtering methods).  Until then it returns false.
   * 
   * @return true/false whether the nested filter has returned true on a filter 
   * call.
   */
  public boolean filterAllRemaining() {
    return this.filterAllRemaining || this.filter.filterAllRemaining();
  }
  
  /** {@inheritDoc} */
  public boolean filterRowKey(final byte [] rowKey) {
    changeFAR(this.filter.filterRowKey(rowKey));
    return filterAllRemaining();
  }
  
  /** {@inheritDoc} */
  public boolean filterColumn(final byte [] rowKey, final byte [] colKey,
    final byte[] data) {
    changeFAR(this.filter.filterColumn(rowKey, colKey, data));
    return filterAllRemaining();
  }
  
  /** {@inheritDoc} */
  public boolean filterRow(final SortedMap<byte [], Cell> columns) {
    changeFAR(this.filter.filterRow(columns));
    return filterAllRemaining();
  }
  
  /**
   * Change filterAllRemaining from false to true if value is true, otherwise 
   * leave as is.
   * 
   * @param value
   */
  private void changeFAR(boolean value) {
    this.filterAllRemaining = this.filterAllRemaining || value;
  }

  /** {@inheritDoc} */
  public void rowProcessed(boolean filtered, byte [] rowKey) {
    this.filter.rowProcessed(filtered, rowKey);
  }
  
  /** {@inheritDoc} */
  public void validate(final byte [][] columns) {
    this.filter.validate(columns);
  }
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    String className = in.readUTF();
    
    try {
      this.filter = (RowFilterInterface)(Class.forName(className).
        newInstance());
      this.filter.readFields(in);
    } catch (InstantiationException e) {
      throw new RuntimeException("Failed to deserialize WhileMatchRowFilter.",
          e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to deserialize WhileMatchRowFilter.",
          e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize WhileMatchRowFilter.",
          e);
    }
  }
  
  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.filter.getClass().getName());
    this.filter.write(out);
  }
}
