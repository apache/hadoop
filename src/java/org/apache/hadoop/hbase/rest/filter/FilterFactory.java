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

import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * Constructs Filters from JSON. Filters are defined
 * as JSON Objects of the form:
 * {
 *  "type" : "FILTER_CLASS_NAME",
 * "args" : "FILTER_ARGUMENTS"
 * }
 * 
 * For Filters like WhileMatchRowFilter,
 * nested Filters are supported. Just serialize a different
 * filter in the form (for instance if you wanted to use WhileMatchRowFilter
 * with a StopRowFilter:
 * 
 * {
 *  "type" : "WhileMatchRowFilter",
 * "args" : {
 *              "type" : "StopRowFilter",
 *              "args" : "ROW_KEY_TO_STOP_ON"
 *            }
 * }
 * 
 * For filters like RowSetFilter, nested Filters AND Filter arrays
 * are supported. So for instance If one wanted to do a RegExp
 * RowFilter UNIONed with a WhileMatchRowFilter(StopRowFilter),
 * you would look like this:
 * 
 * {
 *   "type" : "RowFilterSet",
 *   "args" : [
 *                {
 *                  "type" : "RegExpRowFilter",
 *                  "args" : "MY_REGULAR_EXPRESSION"
 *                },
 *                {
 *                  "type" : "WhileMatchRowFilter"
 *                  "args" : {
 *                                "type" : "StopRowFilter"
 *                                "args" : "MY_STOP_ROW_EXPRESSION"
 *                             }
 *                }
 *              ]
 * }
 */
public interface FilterFactory extends FilterFactoryConstants {
  public RowFilterInterface getFilterFromJSON(String args)
      throws HBaseRestException;
}
