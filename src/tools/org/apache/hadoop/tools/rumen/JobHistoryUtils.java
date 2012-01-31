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
/**
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.util.HashMap;
import java.util.Map;

public class JobHistoryUtils {

  /**
   * Extract/Add counters into the Map from the given JhCounters object.
   * @param counters the counters to be extracted from
   * @return the map of counters
   */
  static Map<String, Long> extractCounters(JhCounters counters) {
    Map<String, Long> countersMap = new HashMap<String, Long>();
    if (counters != null) {
      for (JhCounterGroup group : counters.groups) {
        for (JhCounter counter : group.counts) {
          countersMap.put(counter.name.toString(), counter.value);
        }
      }
    }
    return countersMap;
  }
}
