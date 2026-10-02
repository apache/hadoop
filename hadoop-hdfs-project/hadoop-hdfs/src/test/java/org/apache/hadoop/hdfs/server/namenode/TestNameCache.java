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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link NameCache} class
 */
public class TestNameCache {
  @Test
  public void testDictionary() throws Exception {
    // Create dictionary with useThreshold 2
    NameCache<String> cache = 
      new NameCache<String>(2);
    String[] matching = {"part1", "part10000000", "fileabc", "abc", "filepart"};
    String[] notMatching = {"spart1", "apart", "abcd", "def"};

    for (String s : matching) {
      // Add useThreshold times so the names are promoted to dictionary
      cache.put(s);
      assertTrue(s == cache.put(s));
    }
    for (String s : notMatching) {
      // Add < useThreshold times so the names are not promoted to dictionary
      cache.put(s);
    }
    
    // Mark dictionary as initialized
    cache.initialized();
    
    for (String s : matching) {
      verifyNameReuse(cache, s, true);
    }
    // Check dictionary size
    assertEquals(matching.length, cache.size());
    
    for (String s : notMatching) {
      verifyNameReuse(cache, s, false);
    }
    
    cache.reset();
    cache.initialized();
    
    for (String s : matching) {
      verifyNameReuse(cache, s, false);
    }
    
    for (String s : notMatching) {
      verifyNameReuse(cache, s, false);
    }
  }

  @Test
  public void testPromotionPreservesObjectIdentity() throws Exception {
    // Use a threshold of 2 so a name is promoted after two puts
    NameCache<String> cache = new NameCache<>(2);

    // Construct a String whose identity we can track — use new String() so it
    // is never interned and is a distinct heap object.
    String original = new String("testfile");

    // First put: enters the transient map; useCount.value = original
    String returned1 = cache.put(original);
    // Not yet promoted; returns null on first insertion
    assertNull(returned1);

    // Second put with an equal-but-distinct String — this crosses the threshold
    // and triggers promotion.  The returned value must be the original object.
    String duplicate = new String("testfile");
    String returned2 = cache.put(duplicate);
    // put() returns useCount.value, which must be the original object
    assertSame(original, returned2,
        "put() after promotion must return the original cached object, not the caller's argument");

    // After promotion the name is in the main cache.  A subsequent put()
    // (even after initialized()) must also hand back the same reference.
    cache.initialized();
    String returned3 = cache.put(new String("testfile"));
    assertSame(original, returned3,
        "cache.get() after initialized() must return the original cached object");
  }

  private void verifyNameReuse(NameCache<String> cache, String s, boolean reused) {
    cache.put(s);
    int lookupCount = cache.getLookupCount();
    if (reused) {
      // Dictionary returns non null internal value
      assertNotNull(cache.put(s));
      // Successful lookup increments lookup count
      assertEquals(lookupCount + 1, cache.getLookupCount());
    } else {
      // Dictionary returns null - since name is not in the dictionary
      assertNull(cache.put(s));
      // Lookup count remains the same
      assertEquals(lookupCount, cache.getLookupCount());
    }
  }
}
