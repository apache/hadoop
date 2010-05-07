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
package org.apache.hadoop.hbase.util;

import java.util.SortedMap;
import java.util.TreeMap;

public class SoftValueSortedMapTest {
  private static void testMap(SortedMap<Integer, Integer> map) {
    System.out.println("Testing " + map.getClass());
    for(int i = 0; i < 1000000; i++) {
      map.put(i, i);
    }
    System.out.println(map.size());
    @SuppressWarnings("unused")
    byte[] block = new byte[849*1024*1024]; // FindBugs DLS_DEAD_LOCAL_STORE
    System.out.println(map.size());
  }

  public static void main(String[] args) {
    testMap(new SoftValueSortedMap<Integer, Integer>());
    testMap(new TreeMap<Integer, Integer>());
  }
}