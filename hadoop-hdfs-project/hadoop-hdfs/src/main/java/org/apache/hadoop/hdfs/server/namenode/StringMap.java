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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Map;

/**
 * Atomically maps string to id
 */
class StringMap {
  private final Map<String, Integer> map = Maps.newHashMap();
  private final ArrayList<String> revMap;
  StringMap() {
    this.revMap = Lists.newArrayList();
    this.revMap.add(0, null);
  }

  int getId(String value) {
    if (value == null) {
      return 0;
    }
    synchronized (this) {
      Integer v = map.get(value);
      if (v == null) {
        int nv = map.size() + 1;
        map.put(value, nv);
        revMap.add(value);
        return nv;
      }
      return v;
    }
  }

  public String get(int v) {
    return revMap.get(v);
  }
}
