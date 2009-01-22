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

import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * FilterFactory that constructs a ColumnValueFilter from a JSON arg String.
 * Expects a Stringified JSON argument with the following form:
 * 
 * { "column_name" : "MY_COLUMN_NAME", "compare_op" : "INSERT_COMPARE_OP_HERE",
 * "value" : "MY_COMPARE_VALUE" }
 * 
 * The current valid compare ops are: equal, greater, greater_or_equal, less,
 * less_or_equal, not_equal
 */
public class ColumnValueFilterFactory implements FilterFactory {

  public RowFilterInterface getFilterFromJSON(String args)
      throws HBaseRestException {
    JSONObject innerJSON;
    String columnName;
    String compareOp;
    String value;

    try {
      innerJSON = new JSONObject(args);
    } catch (JSONException e) {
      throw new HBaseRestException(e);
    }

    if ((columnName = innerJSON.optString(COLUMN_NAME)) == null) {
      throw new MalformedFilterException();
    }
    if ((compareOp = innerJSON.optString(COMPARE_OP)) == null) {
      throw new MalformedFilterException();
    }
    if ((value = innerJSON.optString(VALUE)) == null) {
      throw new MalformedFilterException();
    }

    return new ColumnValueFilter(columnName.getBytes(),
        ColumnValueFilter.CompareOp.valueOf(compareOp), value.getBytes());
  }
}
