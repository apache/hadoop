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
 * Implementation of RowFilterInterface that limits results to a specific page
 * size. It terminates scanning once the number of filter-passed results is >=
 * the given page size.
 * 
 * <p>
 * Note that this filter cannot guarantee that the number of results returned
 * to a client are <= page size. This is because the filter is applied
 * separately on different region servers. It does however optimize the scan of
 * individual HRegions by making sure that the page size is never exceeded
 * locally.
 * </p>
 */
public class PageRowFilter implements RowFilterInterface {

  private long pageSize = Long.MAX_VALUE;
  private int rowsAccepted = 0;

  static final Log LOG = LogFactory.getLog(PageRowFilter.class);
  
  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public PageRowFilter() {
    super();
  }

  /**
   * Constructor that takes a maximum page size.
   * 
   * @param pageSize Maximum result size.
   */
  public PageRowFilter(final long pageSize) {
    this.pageSize = pageSize;
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
    rowsAccepted = 0;
  }

  /** {@inheritDoc} */
  public void rowProcessed(boolean filtered,
      @SuppressWarnings("unused") Text rowKey) {
    if (!filtered) {
      this.rowsAccepted++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("rowProcessed incremented rowsAccepted to " + 
          this.rowsAccepted);
      }
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean processAlways() {
    return false;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterAllRemaining() {
    boolean result = this.rowsAccepted > this.pageSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug("filtering decision is " + result + " with rowsAccepted: " + 
        this.rowsAccepted);
    }
    return result;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(@SuppressWarnings("unused") final Text rowKey) {
    return filterAllRemaining();
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(@SuppressWarnings("unused") final Text rowKey,
    @SuppressWarnings("unused") final Text colKey,
    @SuppressWarnings("unused") final byte[] data) {
    return filterAllRemaining();
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterNotNull(@SuppressWarnings("unused")
      final TreeMap<Text, byte[]> columns) {
    return filterAllRemaining();
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void readFields(final DataInput in) throws IOException {
    this.pageSize = in.readLong();
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void write(final DataOutput out) throws IOException {
    out.writeLong(pageSize);
  }
}