/*
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

package org.apache.hadoop.metrics2.sink;

import java.time.Instant;
import java.time.LocalDate;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;

import org.junit.jupiter.api.Test;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test that the init() method picks up all the configuration settings
 * correctly.
 */
public class TestRollingFileSystemSink {
  @Test
  public void testInit() {
    ConfigBuilder builder = new ConfigBuilder();
    SubsetConfiguration conf =
        builder.add("sink.roll-interval", "10m")
            .add("sink.roll-offset-interval-millis", "1")
            .add("sink.basepath", "path")
            .add("sink.ignore-error", "true")
            .add("sink.allow-append", "true")
            .add("sink.source", "src")
            .subset("sink");

    RollingFileSystemSink sink = new RollingFileSystemSink();

    sink.init(conf);

    assertEquals(sink.rollIntervalMillis, 600000,
        "The roll interval was not set correctly");
    assertEquals(sink.rollOffsetIntervalMillis, 1,
        "The roll offset interval was not set correctly");
    assertEquals(sink.basePath, new Path("path"),
        "The base path was not set correctly");
    assertEquals(sink.ignoreError, true, "ignore-error was not set correctly");
    assertEquals(sink.allowAppend, true, "allow-append was not set correctly");
    assertEquals(sink.source, "src", "The source was not set correctly");
  }

  /**
   * Test whether the initial roll interval is set correctly.
   */
  @Test
  public void testSetInitialFlushTime() {
    RollingFileSystemSink rfsSink = new RollingFileSystemSink(1000, 0);
    Instant instant = LocalDate.of(2016, 1, 1).atStartOfDay(UTC).toInstant();

    assertNull(
        rfsSink.nextFlush, "Last flush time should have been null prior to calling init()");

    rfsSink.setInitialFlushTime(instant);

    long diff =
        rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertEquals(0L, diff, "The initial flush time was calculated incorrectly");

    instant = instant.plusMillis(10);
    rfsSink.setInitialFlushTime(instant);
    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertEquals(
        -10L, diff, "The initial flush time was calculated incorrectly");

    instant = instant.plusSeconds(1);
    rfsSink.setInitialFlushTime(instant);
    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertEquals(
        -10L, diff, "The initial flush time was calculated incorrectly");

    // Try again with a random offset
    rfsSink = new RollingFileSystemSink(1000, 100);

    assertNull(
        rfsSink.nextFlush, "Last flush time should have been null prior to calling init()");

    instant = instant.truncatedTo(HOURS);
    rfsSink.setInitialFlushTime(instant);

    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertTrue((diff == 0L) || ((diff > -1000L) && (diff < -900L)),
        "The initial flush time was calculated incorrectly: " + diff);

    instant = instant.plusMillis(10);
    rfsSink.setInitialFlushTime(instant);
    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertTrue((diff >= -10L) && (diff <= 0L) || ((diff > -1000L) && (diff < -910L)),
        "The initial flush time was calculated incorrectly: " + diff);

    instant = instant.plusSeconds(1);
    rfsSink.setInitialFlushTime(instant);
    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertTrue((diff >= -10L) && (diff <= 0L) || ((diff > -1000L) && (diff < -910L)),
        "The initial flush time was calculated incorrectly: " + diff);

    // Now try pathological settings
    rfsSink = new RollingFileSystemSink(1000, 1000000);

    assertNull(rfsSink.nextFlush,
        "Last flush time should have been null prior to calling init()");

    instant = instant.truncatedTo(HOURS).plusMillis(1);
    rfsSink.setInitialFlushTime(instant);

    diff = rfsSink.nextFlush.toEpochMilli() - instant.toEpochMilli();

    assertTrue((diff > -1000L) && (diff <= 0L),
        "The initial flush time was calculated incorrectly: " + diff);
  }

  /**
   * Test that the roll time updates correctly.
   */
  @Test
  public void testUpdateRollTime() {
    RollingFileSystemSink rfsSink = new RollingFileSystemSink(1000, 0);
    Instant instant = LocalDate.of(2016, 1, 1).atStartOfDay(UTC).toInstant();

    rfsSink.nextFlush = instant;
    rfsSink.updateFlushTime(instant);

    assertEquals(instant.toEpochMilli() + 1000,
        rfsSink.nextFlush.toEpochMilli(),
        "The next roll time should have been 1 second in the future");

    rfsSink.nextFlush = instant;
    instant = instant.plusMillis(10);
    rfsSink.updateFlushTime(instant);

    assertEquals(instant.toEpochMilli() + 990,
        rfsSink.nextFlush.toEpochMilli(),
        "The next roll time should have been 990 ms in the future");

    rfsSink.nextFlush = instant;
    instant = instant.plusSeconds(1).plusMillis(10);
    rfsSink.updateFlushTime(instant);

    assertEquals(instant.toEpochMilli() + 990,
        rfsSink.nextFlush.toEpochMilli(),
        "The next roll time should have been 990 ms in the future");
  }

  /**
   * Test whether the roll interval is correctly calculated from the
   * configuration settings.
   */
  @Test
  public void testGetRollInterval() {
    doTestGetRollInterval(1, new String[] {"m", "min", "minute", "minutes"},
        60 * 1000L);
    doTestGetRollInterval(1, new String[] {"h", "hr", "hour", "hours"},
        60 * 60 * 1000L);
    doTestGetRollInterval(1, new String[] {"d", "day", "days"},
        24 * 60 * 60 * 1000L);

    ConfigBuilder builder = new ConfigBuilder();
    SubsetConfiguration conf =
        builder.add("sink.roll-interval", "1").subset("sink");
    // We can reuse the same sink evry time because we're setting the same
    // property every time.
    RollingFileSystemSink sink = new RollingFileSystemSink();

    sink.init(conf);

    assertEquals(3600000L, sink.getRollInterval());

    for (char c : "abcefgijklnopqrtuvwxyz".toCharArray()) {
      builder = new ConfigBuilder();
      conf = builder.add("sink.roll-interval", "90 " + c).subset("sink");

      try {
        sink.init(conf);
        sink.getRollInterval();
        fail("Allowed flush interval with bad units: " + c);
      } catch (MetricsException ex) {
        // Expected
      }
    }
  }

  /**
   * Test the basic unit conversions with the given unit name modifier applied.
   *
   * @param mod a unit name modifier
   */
  private void doTestGetRollInterval(int num, String[] units, long expected) {
    RollingFileSystemSink sink = new RollingFileSystemSink();
    ConfigBuilder builder = new ConfigBuilder();

    for (String unit : units) {
      sink.init(builder.add("sink.roll-interval", num + unit).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + unit.toUpperCase()).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + " " + unit).subset("sink"));
      assertEquals(expected, sink.getRollInterval());

      sink.init(builder.add("sink.roll-interval",
          num + " " + unit.toUpperCase()).subset("sink"));
      assertEquals(expected, sink.getRollInterval());
    }
  }
}
