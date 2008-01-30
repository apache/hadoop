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


/*
 * Subclass of StopRowFilter that filters rows > the stop row,
 * making it include up to the last row but no further.
 */
public class InclusiveStopRowFilter extends StopRowFilter{
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
  public InclusiveStopRowFilter(final Text stopRowKey) {
    super(stopRowKey);
  }
  
  public boolean filter(final Text rowKey) {
    if (rowKey == null) {
      if (this.stopRowKey == null) {
        return true;
      }
      return false;
    }    
    boolean result = this.stopRowKey.compareTo(rowKey) < 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter result for rowKey: " + rowKey + ".  Result: " + 
        result);
    }
    return result;
  }
  
}
