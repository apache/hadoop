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
package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

public class TestTimelineServiceHelper {

  @Test
  public void testMapCastToHashMap() {

    // Test null map be casted to null
    Map<String, String> nullMap = null;
    Assert.assertNull(TimelineServiceHelper.mapCastToHashMap(nullMap));

    // Test empty hashmap be casted to a empty hashmap
    Map<String, String> emptyHashMap = new HashMap<String, String>();
    Assert.assertEquals(
        TimelineServiceHelper.mapCastToHashMap(emptyHashMap).size(), 0);

    // Test empty non-hashmap be casted to a empty hashmap
    Map<String, String> emptyTreeMap = new TreeMap<String, String>();
    Assert.assertEquals(
        TimelineServiceHelper.mapCastToHashMap(emptyTreeMap).size(), 0);

    // Test non-empty hashmap be casted to hashmap correctly
    Map<String, String> firstHashMap = new HashMap<String, String>();
    String key = "KEY";
    String value = "VALUE";
    firstHashMap.put(key, value);
    Assert.assertEquals(
        TimelineServiceHelper.mapCastToHashMap(firstHashMap), firstHashMap);

    // Test non-empty non-hashmap is casted correctly.
    Map<String, String> firstTreeMap = new TreeMap<String, String>();
    firstTreeMap.put(key, value);
    HashMap<String, String> alternateHashMap =
        TimelineServiceHelper.mapCastToHashMap(firstTreeMap);
    Assert.assertEquals(firstTreeMap.size(), alternateHashMap.size());
    Assert.assertEquals(alternateHashMap.get(key), value);

    // Test complicated hashmap be casted correctly
    Map<String, Set<String>> complicatedHashMap =
        new HashMap<String, Set<String>>();
    Set<String> hashSet = new HashSet<String>();
    hashSet.add(value);
    complicatedHashMap.put(key, hashSet);
    Assert.assertEquals(
        TimelineServiceHelper.mapCastToHashMap(complicatedHashMap),
        complicatedHashMap);

    // Test complicated non-hashmap get casted correctly
    Map<String, Set<String>> complicatedTreeMap =
        new TreeMap<String, Set<String>>();
    complicatedTreeMap.put(key, hashSet);
    Assert.assertEquals(
        TimelineServiceHelper.mapCastToHashMap(complicatedTreeMap).get(key),
        hashSet);
  }

}
