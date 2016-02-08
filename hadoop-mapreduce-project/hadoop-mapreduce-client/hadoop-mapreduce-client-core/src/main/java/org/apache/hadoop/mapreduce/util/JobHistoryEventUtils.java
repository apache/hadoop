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
package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Class containing utility methods to be used by JobHistoryEventHandler.
 */
public final class JobHistoryEventUtils {
  private JobHistoryEventUtils() {
  }

  public static JsonNode countersToJSON(Counters counters) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode nodes = mapper.createArrayNode();
    if (counters != null) {
      for (CounterGroup counterGroup : counters) {
        ObjectNode groupNode = nodes.addObject();
        groupNode.put("NAME", counterGroup.getName());
        groupNode.put("DISPLAY_NAME", counterGroup.getDisplayName());
        ArrayNode countersNode = groupNode.putArray("COUNTERS");
        for (Counter counter : counterGroup) {
          ObjectNode counterNode = countersNode.addObject();
          counterNode.put("NAME", counter.getName());
          counterNode.put("DISPLAY_NAME", counter.getDisplayName());
          counterNode.put("VALUE", counter.getValue());
        }
      }
    }
    return nodes;
  }

}
