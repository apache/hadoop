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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Subclass of StopRowFilter that filters rows > the stop row,
 * making it include up to the last row but no further.
 *
 * @deprecated Use filters that are rooted on @{link Filter} instead
 */
public class InclusiveStopRowFilter extends StopRowFilter {
  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public InclusiveStopRowFilter() {super();}

  /**
   * Constructor that takes a stopRowKey on which to filter
   * 
   * @param stopRowKey rowKey to filter on.
   */
  public InclusiveStopRowFilter(final byte [] stopRowKey) {
    super(stopRowKey);
  }

  /**
   * @see org.apache.hadoop.hbase.filter.StopRowFilter#filterRowKey(byte[])
   */
  @Override
  public boolean filterRowKey(final byte [] rowKey) {
    return filterRowKey(rowKey, 0, rowKey.length);
  }

  public boolean filterRowKey(byte []rowKey, int offset, int length) {
    if (rowKey == null) {
      if (getStopRowKey() == null) {
        return true;
      }
      return false;
    }
    return Bytes.compareTo(getStopRowKey(), 0, getStopRowKey().length,
        rowKey, offset, length) < 0;
  }
}
