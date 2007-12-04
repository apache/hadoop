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
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * Implementation of RowFilterInterface that filters out rows greater than or 
 * equal to a specified rowKey.
 */
public class StopRowFilter implements RowFilterInterface {

  protected Text stopRowKey;
  
  static final Log LOG = LogFactory.getLog(StopRowFilter.class);
  
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
  public StopRowFilter(final Text stopRowKey) {
    this.stopRowKey = stopRowKey;
  }
  
  /**
   * An accessor for the stopRowKey
   * 
   * @return the filter's stopRowKey
   */
  public Text getStopRowKey() {
    return this.stopRowKey;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void validate(@SuppressWarnings("unused") final Text[] columns) {
    // Doesn't filter columns
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void reset() {
    // Nothing to reset
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unused")
  public void rowProcessed(boolean filtered, Text rowKey) {
    // Doesn't care
  }

  /** {@inheritDoc} */
  public boolean processAlways() {
    return false;
  }
  
  /** {@inheritDoc} */
  public boolean filterAllRemaining() {
    return false;
  }

  /** {@inheritDoc} */
  public boolean filter(final Text rowKey) {
    if (rowKey == null) {
      if (this.stopRowKey == null) {
        return true;
      }
      return false;
    }
    boolean result = this.stopRowKey.compareTo(rowKey) <= 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter result for rowKey: " + rowKey + ".  Result: " + 
        result);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * Because StopRowFilter does not examine column information, this method 
   * defaults to calling the rowKey-only version of filter.
   */
  public boolean filter(@SuppressWarnings("unused") final Text rowKey,
    @SuppressWarnings("unused") final Text colKey,
    @SuppressWarnings("unused") final byte[] data) {
    return filter(rowKey);
  }

  /** {@inheritDoc}
   *
   * Because StopRowFilter does not examine column information, this method 
   * defaults to calling filterAllRemaining().
   * 
   * @param columns
   */
  public boolean filterNotNull(@SuppressWarnings("unused")
      final TreeMap<Text, byte[]> columns) {
    return filterAllRemaining();
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    stopRowKey = new Text(in.readUTF());
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(stopRowKey.toString());
  }
}
