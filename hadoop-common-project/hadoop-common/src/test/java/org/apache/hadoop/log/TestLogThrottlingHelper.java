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
package org.apache.hadoop.log;

import org.apache.hadoop.log.LogThrottlingHelper.LogAction;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LogThrottlingHelper}.
 */
public class TestLogThrottlingHelper {

  private static final int LOG_PERIOD = 100;

  private LogThrottlingHelper helper;
  private FakeTimer timer;

  @Before
  public void setup() {
    timer = new FakeTimer();
    helper = new LogThrottlingHelper(LOG_PERIOD, null, timer);
  }

  @Test
  public void testBasicLogging() {
    assertTrue(helper.record().shouldLog());

    for (int i = 0; i < 5; i++) {
      timer.advance(LOG_PERIOD / 10);
      assertFalse(helper.record().shouldLog());
    }
    timer.advance(LOG_PERIOD);
    assertTrue(helper.record().shouldLog());
  }

  @Test
  public void testLoggingWithValue() {
    assertTrue(helper.record(1).shouldLog());

    for (int i = 0; i < 4; i++) {
      timer.advance(LOG_PERIOD / 5);
      assertFalse(helper.record(i % 2 == 0 ? 0 : 1).shouldLog());
    }

    timer.advance(LOG_PERIOD);
    LogAction action = helper.record(0.5);
    assertTrue(action.shouldLog());
    assertEquals(5, action.getCount());
    assertEquals(0.5, action.getStats(0).getMean(), 0.01);
    assertEquals(1.0, action.getStats(0).getMax(), 0.01);
    assertEquals(0.0, action.getStats(0).getMin(), 0.01);
  }

  @Test
  public void testLoggingWithMultipleValues() {
    assertTrue(helper.record(1).shouldLog());

    for (int i = 0; i < 4; i++) {
      timer.advance(LOG_PERIOD / 5);
      int base = i % 2 == 0 ? 0 : 1;
      assertFalse(helper.record(base, base * 2).shouldLog());
    }

    timer.advance(LOG_PERIOD);
    LogAction action = helper.record(0.5, 1.0);
    assertTrue(action.shouldLog());
    assertEquals(5, action.getCount());
    for (int i = 1; i <= 2; i++) {
      assertEquals(0.5 * i, action.getStats(i - 1).getMean(), 0.01);
      assertEquals(1.0 * i, action.getStats(i - 1).getMax(), 0.01);
      assertEquals(0.0, action.getStats(i - 1).getMin(), 0.01);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLoggingWithInconsistentValues() {
    assertTrue(helper.record(1, 2).shouldLog());
    helper.record(1, 2);
    helper.record(1, 2, 3);
  }

  @Test
  public void testNamedLoggersWithoutSpecifiedPrimary() {
    assertTrue(helper.record("foo", 0).shouldLog());
    assertTrue(helper.record("bar", 0).shouldLog());

    assertFalse(helper.record("foo", LOG_PERIOD / 2).shouldLog());
    assertFalse(helper.record("bar", LOG_PERIOD / 2).shouldLog());

    assertTrue(helper.record("foo", LOG_PERIOD).shouldLog());
    assertTrue(helper.record("bar", LOG_PERIOD).shouldLog());

    assertFalse(helper.record("foo", (LOG_PERIOD * 3) / 2).shouldLog());
    assertFalse(helper.record("bar", (LOG_PERIOD * 3) / 2).shouldLog());

    assertFalse(helper.record("bar", LOG_PERIOD * 2).shouldLog());
    assertTrue(helper.record("foo", LOG_PERIOD * 2).shouldLog());
    assertTrue(helper.record("bar", LOG_PERIOD * 2).shouldLog());
  }

  @Test
  public void testPrimaryAndDependentLoggers() {
    helper = new LogThrottlingHelper(LOG_PERIOD, "foo", timer);

    assertTrue(helper.record("foo", 0).shouldLog());
    assertTrue(helper.record("bar", 0).shouldLog());
    assertFalse(helper.record("bar", 0).shouldLog());
    assertFalse(helper.record("foo", 0).shouldLog());

    assertFalse(helper.record("foo", LOG_PERIOD / 2).shouldLog());
    assertFalse(helper.record("bar", LOG_PERIOD / 2).shouldLog());

    // Both should log once the period has elapsed
    assertTrue(helper.record("foo", LOG_PERIOD).shouldLog());
    assertTrue(helper.record("bar", LOG_PERIOD).shouldLog());

    // "bar" should not log yet because "foo" hasn't been triggered
    assertFalse(helper.record("bar", LOG_PERIOD * 2).shouldLog());
    assertTrue(helper.record("foo", LOG_PERIOD * 2).shouldLog());
    // The timing of "bar" shouldn't matter as it is dependent on "foo"
    assertTrue(helper.record("bar", 0).shouldLog());
  }

  @Test
  public void testMultipleLoggersWithValues() {
    helper = new LogThrottlingHelper(LOG_PERIOD, "foo", timer);

    assertTrue(helper.record("foo", 0).shouldLog());
    assertTrue(helper.record("bar", 0, 2).shouldLog());
    assertTrue(helper.record("baz", 0, 3, 3).shouldLog());

    // "bar"/"baz" should not log yet because "foo" hasn't been triggered
    assertFalse(helper.record("bar", LOG_PERIOD, 2).shouldLog());
    assertFalse(helper.record("baz", LOG_PERIOD, 3, 3).shouldLog());

    // All should log once the period has elapsed
    LogAction foo = helper.record("foo", LOG_PERIOD);
    LogAction bar = helper.record("bar", LOG_PERIOD, 2);
    LogAction baz = helper.record("baz", LOG_PERIOD, 3, 3);
    assertTrue(foo.shouldLog());
    assertTrue(bar.shouldLog());
    assertTrue(baz.shouldLog());
    assertEquals(1, foo.getCount());
    assertEquals(2, bar.getCount());
    assertEquals(2, baz.getCount());
    assertEquals(2.0, bar.getStats(0).getMean(), 0.01);
    assertEquals(3.0, baz.getStats(0).getMean(), 0.01);
    assertEquals(3.0, baz.getStats(1).getMean(), 0.01);

    assertEquals(2.0, helper.getCurrentStats("bar", 0).getMax(), 0);
    assertEquals(3.0, helper.getCurrentStats("baz", 0).getMax(), 0);
  }

}
