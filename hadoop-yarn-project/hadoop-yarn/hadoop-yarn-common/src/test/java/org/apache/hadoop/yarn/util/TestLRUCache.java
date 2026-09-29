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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestLRUCache {
  public static final int CACHE_EXPIRE_TIME = 200;
  @Test
  public void testLRUCache() throws InterruptedException {
    LRUCache<String, Integer> lruCache = new LRUCache<>(3, CACHE_EXPIRE_TIME);
    lruCache.put("1", 1);
    lruCache.put("2", 1);
    lruCache.put("3", 3);
    lruCache.put("4", 4);
    assertEquals(lruCache.size(), 3);
    assertNull(lruCache.get("1"));
    assertNotNull(lruCache.get("2"));
    assertNotNull(lruCache.get("3"));
    assertNotNull(lruCache.get("3"));
    lruCache.clear();

    lruCache.put("1", 1);
    Thread.sleep(201);
    assertEquals(lruCache.size(), 1);
    lruCache.get("1");
    assertEquals(lruCache.size(), 0);
    lruCache.put("2", 2);
    assertEquals(lruCache.size(), 1);
    lruCache.put("3", 3);
    assertEquals(lruCache.size(), 2);
  }
}