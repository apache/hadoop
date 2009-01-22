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
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.rest.RESTConstants;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Factory to produce WhileMatchRowFilters from JSON
 * Expects as an arguement a valid JSON Object in 
 * String form of another RowFilterInterface.
 */
public class WhileMatchRowFilterFactory implements FilterFactory {
  public RowFilterInterface getFilterFromJSON(String args)
      throws HBaseRestException {
    JSONObject innerFilterJSON;
    FilterFactory f;
    String innerFilterType;
    String innerFilterArgs;

    try {
      innerFilterJSON = new JSONObject(args);
    } catch (JSONException e) {
      throw new HBaseRestException(e);
    }

    // Check if filter is correct
    if ((innerFilterType = innerFilterJSON.optString(TYPE)) == null)
      throw new MalformedFilterException();
    if ((innerFilterArgs = innerFilterJSON.optString(ARGUMENTS)) == null)
      throw new MalformedFilterException();

    if ((f = RESTConstants.filterFactories.get(innerFilterType)) == null)
      throw new MalformedFilterException();

    RowFilterInterface innerFilter = f.getFilterFromJSON(innerFilterArgs);

    return new WhileMatchRowFilter(innerFilter);
  }
}
