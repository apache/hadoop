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
package org.apache.hadoop.hbase.rest.filter;

import org.apache.hadoop.hbase.filter.InclusiveStopRowFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * FilterFactory that construct a InclusiveStopRowFilter
 * from a JSON argument String.
 * 
 * It expects that the whole input string consists of only
 * the rowKey that you wish to stop on.
 */
public class InclusiveStopRowFilterFactory implements FilterFactory {
  public RowFilterInterface getFilterFromJSON(String args) {
    return new InclusiveStopRowFilter(Bytes.toBytes(args));
  }
}
