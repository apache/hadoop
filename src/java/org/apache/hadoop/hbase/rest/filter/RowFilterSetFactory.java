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

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.rest.RESTConstants;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Constructs a RowFilterSet from a JSON argument String.
 * 
 * Assumes that the input is a JSONArray consisting of JSON Object version of
 * the filters that you wish to mash together in an AND statement.
 * 
 * The Syntax for the individual inner filters are defined by their respective
 * FilterFactory. If a filter factory for said Factory does not exist, a
 * MalformedFilterJSONException will be thrown.
 * 
 * Currently OR Statements are not supported even though at a later iteration
 * they could be supported easily.
 */
public class RowFilterSetFactory implements FilterFactory {

  public RowFilterInterface getFilterFromJSON(String args)
      throws HBaseRestException {
    JSONArray filterArray;
    Set<RowFilterInterface> set;
    JSONObject filter;

    try {
      filterArray = new JSONArray(args);
    } catch (JSONException e) {
      throw new HBaseRestException(e);
    }

    // If only 1 Row, just return the row.
    if (filterArray.length() == 1) {
      return getRowFilter(filterArray.optJSONObject(0));
    }

    // Otherwise continue
    set = new HashSet<RowFilterInterface>();

    for (int i = 0; i < filterArray.length(); i++) {

      // Get FIlter Object
      if ((filter = filterArray.optJSONObject(i)) == null) {
        throw new MalformedFilterException();
      }

      // Add newly constructed filter to the filter set;
      set.add(getRowFilter(filter));
    }

    // Put set into a RowFilterSet and return.
    return new RowFilterSet(set);
  }

  /**
   * A refactored method that encapsulates the creation of a RowFilter given a
   * JSONObject with a correct form of: { "type" : "MY_TYPE", "args" : MY_ARGS,
   * }
   * 
   * @param filter
   * @return RowFilter
   * @throws org.apache.hadoop.hbase.rest.exception.HBaseRestException
   */
  protected RowFilterInterface getRowFilter(JSONObject filter)
      throws HBaseRestException {
    FilterFactory f;
    String filterType;
    String filterArgs;

    // Get Filter's Type
    if ((filterType = filter.optString(FilterFactoryConstants.TYPE)) == null) {
      throw new MalformedFilterException();
    }

    // Get Filter Args
    if ((filterArgs = filter.optString(FilterFactoryConstants.ARGUMENTS)) == null) {
      throw new MalformedFilterException();
    }

    // Get Filter Factory for given Filter Type
    if ((f = RESTConstants.filterFactories.get(filterType)) == null) {
      throw new MalformedFilterException();
    }

    return f.getFilterFromJSON(filterArgs);
  }
}
