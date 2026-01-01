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
package org.apache.hadoop.hdfs.net;

import java.util.HashMap;
import java.util.Map;

public class StaticDataNodeWeightMapping extends AbstractDataNodeWeightMapping {

  private static final Map<String, Integer> WEIGHT_MAP = new HashMap<>();

  @Override
  public int resolve(String ipAddress, String hostName) {
    // this class is used for unit test, we should use hostName to get the weight
    return WEIGHT_MAP.getOrDefault(hostName, DEFAULT_WEIGHT);
  }

  public static void setNodeWeight(String name, int weight) {
    synchronized (WEIGHT_MAP) {
      WEIGHT_MAP.put(name, weight);
    }
  }

  public static void resetMap() {
    synchronized (WEIGHT_MAP) {
      WEIGHT_MAP.clear();
    }
  }

  @Override
  public void reload() {
  }

}
