/**
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

package org.apache.hadoop.mapred.lib.aggregate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class implements a value aggregator that dedupes a sequence of objects.
 * 
 */
public class UniqValueCount implements ValueAggregator {

  TreeMap<Object, Object> uniqItems = null;

  /**
   * the default constructor
   * 
   */
  public UniqValueCount() {
    uniqItems = new TreeMap<Object, Object>();
  }

  /**
   * add a value to the aggregator
   * 
   * @param val
   *          an object.
   * 
   */
  public void addNextValue(Object val) {
    uniqItems.put(val, "1");

  }

  /**
   * @return return the number of unique objects aggregated
   */
  public String getReport() {
    return "" + uniqItems.size();
  }

  /**
   * 
   * @return the set of the unique objects
   */
  public Set getUniqueItems() {
    return uniqItems.keySet();
  }

  /**
   * reset the aggregator
   */
  public void reset() {
    uniqItems = new TreeMap<Object, Object>();
  }

  /**
   * @return return an array of the unique objects. The return value is
   *         expected to be used by the a combiner.
   */
  public ArrayList getCombinerOutput() {
    Object key = null;
    Iterator iter = uniqItems.keySet().iterator();
    ArrayList<Object> retv = new ArrayList<Object>();

    while (iter.hasNext()) {
      key = iter.next();
      retv.add(key);
    }
    return retv;
  }
}