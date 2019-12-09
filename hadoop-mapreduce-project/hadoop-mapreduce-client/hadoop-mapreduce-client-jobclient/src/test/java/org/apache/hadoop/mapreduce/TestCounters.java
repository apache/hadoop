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

import java.util.Random;

import org.apache.hadoop.mapreduce.counters.LimitExceededException;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import static org.junit.Assert.*;
/**
 * TestCounters checks the sanity and recoverability of {@code Counters}
 */
public class TestCounters {

  static final Logger LOG = LoggerFactory.getLogger(TestCounters.class);

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
      Counter counter = new Counters().findCounter("test", "foo");
      counter.setValue(initValue);
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

  @Test public void testLimits() {
    for (int i = 0; i < 3; ++i) {
      // make sure limits apply to separate containers
      testMaxCounters(new Counters());
      testMaxGroups(new Counters());
    }
  }
  
  @Test
  public void testCountersIncrement() {
    Counters fCounters = new Counters();
    Counter fCounter = fCounters.findCounter(FRAMEWORK_COUNTER);
    fCounter.setValue(100);
    Counter gCounter = fCounters.findCounter("test", "foo");
    gCounter.setValue(200);

    Counters counters = new Counters();
    counters.incrAllCounters(fCounters);
    Counter counter;
    for (CounterGroup cg : fCounters) {
      CounterGroup group = counters.getGroup(cg.getName());
      if (group.getName().equals("test")) {
        counter = counters.findCounter("test", "foo");
        assertEquals(200, counter.getValue());
      } else {
        counter = counters.findCounter(FRAMEWORK_COUNTER);
        assertEquals(100, counter.getValue());
      }
    }
  }

  static final Enum<?> FRAMEWORK_COUNTER = TaskCounter.CPU_MILLISECONDS;
  static final long FRAMEWORK_COUNTER_VALUE = 8;
  static final String FS_SCHEME = "HDFS";
  static final FileSystemCounter FS_COUNTER = FileSystemCounter.BYTES_READ;
  static final long FS_COUNTER_VALUE = 10;

  private void testMaxCounters(final Counters counters) {
    LOG.info("counters max="+ Limits.getCountersMax());
    for (int i = 0; i < Limits.getCountersMax(); ++i) {
      counters.findCounter("test", "test"+ i);
    }
    setExpected(counters);
    shouldThrow(LimitExceededException.class, new Runnable() {
      public void run() {
        counters.findCounter("test", "bad");
      }
    });
    checkExpected(counters);
  }

  private void testMaxGroups(final Counters counters) {
    LOG.info("counter groups max="+ Limits.getGroupsMax());
    for (int i = 0; i < Limits.getGroupsMax(); ++i) {
      // assuming COUNTERS_MAX > GROUPS_MAX
      counters.findCounter("test"+ i, "test");
    }
    setExpected(counters);
    shouldThrow(LimitExceededException.class, new Runnable() {
      public void run() {
        counters.findCounter("bad", "test");
      }
    });
    checkExpected(counters);
  }

  private void setExpected(Counters counters) {
    counters.findCounter(FRAMEWORK_COUNTER).setValue(FRAMEWORK_COUNTER_VALUE);
    counters.findCounter(FS_SCHEME, FS_COUNTER).setValue(FS_COUNTER_VALUE);
  }

  private void checkExpected(Counters counters) {
    assertEquals(FRAMEWORK_COUNTER_VALUE,
                 counters.findCounter(FRAMEWORK_COUNTER).getValue());
    assertEquals(FS_COUNTER_VALUE,
                 counters.findCounter(FS_SCHEME, FS_COUNTER).getValue());
  }

  private void shouldThrow(Class<? extends Exception> ecls, Runnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      assertSame(ecls, e.getClass());
      LOG.info("got expected: "+ e);
      return;
    }
    assertTrue("Should've thrown "+ ecls.getSimpleName(), false);
  }
}
