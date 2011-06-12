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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * TestCounters checks the sanity and recoverability of {@code Counters}
 */
public class TestCounters {

  /**
   * Verify counter value works
   */
  @Test
  public void testCounterValue() {
    final int NUMBER_TESTS = 100;
    final int NUMBER_INC = 10;
    final Random rand = new Random();
    for (int i = 0; i < NUMBER_TESTS; i++) {
      long initValue = rand.nextInt();
      long expectedValue = initValue;
      Counter counter = new Counter("foo", "bar", expectedValue);
      assertEquals("Counter value is not initialized correctly",
          expectedValue, counter.getValue());
      for (int j = 0; j < NUMBER_INC; j++) {
        int incValue = rand.nextInt();
        counter.increment(incValue);
        expectedValue += incValue;
        assertEquals("Counter value is not incremented correctly",
            expectedValue, counter.getValue());
      }
      expectedValue = rand.nextInt();
      counter.setValue(expectedValue);
      assertEquals("Counter value is not set correctly",
          expectedValue, counter.getValue());
    }
  }

}
