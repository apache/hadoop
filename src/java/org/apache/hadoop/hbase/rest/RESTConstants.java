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
package org.apache.hadoop.hbase.rest;

import org.apache.hadoop.hbase.rest.filter.RowFilterSetFactory;
import org.apache.hadoop.hbase.rest.filter.StopRowFilterFactory;
import org.apache.hadoop.hbase.rest.filter.WhileMatchRowFilterFactory;
import org.apache.hadoop.hbase.rest.filter.PageRowFilterFactory;
import org.apache.hadoop.hbase.rest.filter.ColumnValueFilterFactory;
import org.apache.hadoop.hbase.rest.filter.RegExpRowFilterFactory;
import org.apache.hadoop.hbase.rest.filter.InclusiveStopRowFilterFactory;
import java.util.HashMap;
import org.apache.hadoop.hbase.rest.filter.FilterFactory;

public interface RESTConstants {
  final static String TRUE = "true";
  final static String FALSE = "false";
  // Used for getting all data from a column specified in that order.
  final static String COLUMNS = "columns";
  final static String COLUMN = "column";
  // Used with TableExists
  final static String EXISTS = "exists";
  // Maps to Transaction ID
  final static String TRANSACTION = "transaction";
  // Transaction Operation Key.
  final static String TRANSACTION_OPERATION = "transaction_op";
  // Transaction Operation Values
  final static String TRANSACTION_OPERATION_COMMIT = "commit";
  final static String TRANSACTION_OPERATION_CREATE = "create";
  final static String TRANSACTION_OPERATION_ABORT = "abort";
  // Filter Key
  final static String FILTER = "filter";
  final static String FILTER_TYPE = "type";
  final static String FILTER_VALUE = "value";
  final static String FILTER_RANK = "rank";
  // Scanner Key
  final static String SCANNER = "scanner";
  // The amount of rows to return at one time.
  final static String SCANNER_RESULT_SIZE = "result_size";
  final static String SCANNER_START_ROW = "start_row";
  final static String SCANNER_STOP_ROW = "stop_row";
  final static String SCANNER_FILTER = "filter";
  final static String SCANNER_TIMESTAMP = "timestamp";
  final static String NUM_VERSIONS = "num_versions";
  final static String SCANNER_COLUMN = "column";
  // static items used on the path
  static final String DISABLE = "disable";
  static final String ENABLE = "enable";
  static final String REGIONS = "regions";
  static final String ROW = "row";
  static final String TIME_STAMPS = "timestamps";
  static final String METADATA = "metadata";

  static final String NAME = "name";
  static final String VALUE = "value";
  static final String ROWS = "rows";

  static final FactoryMap filterFactories = FactoryMap.getFactoryMap();
  static final String LIMIT = "limit";

  static class FactoryMap {

    static boolean created = false;
    protected HashMap<String, FilterFactory> map = new HashMap<String, FilterFactory>();

    protected FactoryMap() {
    }

    public static FactoryMap getFactoryMap() {
      if (!created) {
        created = true;
        FactoryMap f = new FactoryMap();
        f.initialize();
        return f;
      }
      return null;
    }

    public FilterFactory get(String c) {
      return map.get(c);
    }

    protected void initialize() {
      map.put("ColumnValueFilter", new ColumnValueFilterFactory());
      map.put("InclusiveStopRowFilter", new InclusiveStopRowFilterFactory());
      map.put("PageRowFilter", new PageRowFilterFactory());
      map.put("RegExpRowFilter", new RegExpRowFilterFactory());
      map.put("RowFilterSet", new RowFilterSetFactory());
      map.put("StopRowFilter", new StopRowFilterFactory());
      map.put("WhileMatchRowFilter", new WhileMatchRowFilterFactory());
    }
  }
}
