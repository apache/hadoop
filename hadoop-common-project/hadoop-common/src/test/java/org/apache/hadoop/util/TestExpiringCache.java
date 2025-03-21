/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.util;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicInteger;

// Unit test class for ExpiringCache
public class TestExpiringCache {

  // Test that the cache loads the value on the first call, then retrieves it from the cache,
  // and that removing the entry forces a reload.
  @Test
  public void testCacheLoadAndRetrieval() throws Exception {
    ControlledClock testClock = new ControlledClock(SystemClock.getInstance());
    AtomicInteger loadCounter = new AtomicInteger(0);

    // Define a loader that increments the counter each time it is invoked and returns a specific string.
    ExpiringCache.Loader<String, String> loader = key -> {
      loadCounter.incrementAndGet();
      return "value_" + key;
    };

    // Use a long expiration interval to avoid expiration during this test.
    ExpiringCache<String, String> cache = new ExpiringCache<>("TestExpiringCache", testClock, 60, loader);

    // First call should invoke the loader.
    String value1 = cache.get("key1");
    assertEquals("value_key1", value1);
    assertEquals(1, loadCounter.get());

    // Second call should fetch the value from the cache without invoking the loader.
    String value2 = cache.get("key1");
    assertEquals("value_key1", value2);
    assertEquals(1, loadCounter.get());

    // Removing the cache entry, then a new call should trigger the loader again.
    cache.remove("key1");
    String value3 = cache.get("key1");
    assertEquals("value_key1", value3);
    assertEquals(2, loadCounter.get());

    // Stop the cleanup timer to avoid interference with other tests.
    cache.stopCleanup();
  }

  // Test that expired cache entries are removed by the scheduled cleanup task,
  // which forces a reload upon the next access.
  @Test
  public void testCacheExpiry() throws Exception {
    // Set expiration interval to 1 second.
    ControlledClock testClock = new ControlledClock(SystemClock.getInstance());
    AtomicInteger loadCounter = new AtomicInteger(0);
    ExpiringCache.Loader<String, String> loader = key -> {
      loadCounter.incrementAndGet();
      return "value_" + key;
    };

    ExpiringCache<String, String> cache = new ExpiringCache<>("TestCacheExpiry", testClock, 1, loader);

    // Load the value and store it in the cache with the current timestamp.
    String value1 = cache.get("key1");
    assertEquals("value_key1", value1);
    assertEquals(1, loadCounter.get());

    // Simulate time advancement beyond the expiration interval.
    testClock.setTime(testClock.getTime() + 1100);
    // Wait a short time to allow the cleanup task to execute.
    Thread.sleep(500);

    // The cache entry should have been cleaned up; hence, a new call should trigger a reload.
    String value2 = cache.get("key1");
    assertEquals("value_key1", value2);
    assertEquals(2, loadCounter.get());

    // Stop the cleanup timer.
    cache.stopCleanup();
  }

  // Test that when the loader throws an exception, the get method propagates the exception.
  @Test
  public void testLoaderException() {
    ControlledClock testClock = new ControlledClock(SystemClock.getInstance());

    // Define a loader that always throws an exception.
    ExpiringCache.Loader<String, String> loader = key -> {
      throw new Exception("Loading error for key: " + key);
    };

    ExpiringCache<String, String> cache = new ExpiringCache<>("TestCacheException", testClock, 60, loader);

    Exception thrown = assertThrows(Exception.class, () -> {
      cache.get("key_exception");
    });
    assertTrue(thrown.getMessage().contains("Loading error"));

    // Stop the cleanup timer.
    cache.stopCleanup();
  }
}