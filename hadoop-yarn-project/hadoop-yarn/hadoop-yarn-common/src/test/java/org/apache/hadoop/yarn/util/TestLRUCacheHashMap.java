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

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to validate the correctness of the {@code LRUCacheHashMap}.
 *
 */
public class TestLRUCacheHashMap {

  /**
   * Test if the different entries are generated, and LRU cache is working as
   * expected.
   */
  @Test
  public void testLRUCache()
      throws YarnException, IOException, InterruptedException {

    int mapSize = 5;

    LRUCacheHashMap<String, Integer> map =
        new LRUCacheHashMap<String, Integer>(mapSize, true);

    map.put("1", 1);
    map.put("2", 2);
    map.put("3", 3);
    map.put("4", 4);
    map.put("5", 5);

    Assert.assertEquals(mapSize, map.size());

    // Check if all the elements in the map are from 1 to 5
    for (int i = 1; i < mapSize; i++) {
      Assert.assertTrue(map.containsKey(Integer.toString(i)));
    }

    map.put("6", 6);
    map.put("3", 3);
    map.put("7", 7);
    map.put("8", 8);

    Assert.assertEquals(mapSize, map.size());

    // Check if all the elements in the map are from 5 to 8 and the 3
    for (int i = 5; i < mapSize; i++) {
      Assert.assertTrue(map.containsKey(Integer.toString(i)));
    }

    Assert.assertTrue(map.containsKey("3"));

  }

}
