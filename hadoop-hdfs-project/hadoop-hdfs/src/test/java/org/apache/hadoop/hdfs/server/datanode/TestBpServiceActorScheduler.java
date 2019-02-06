/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.server.datanode.BPServiceActor.Scheduler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static java.lang.Math.abs;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


/**
 * Verify the block report and heartbeat scheduling logic of BPServiceActor
 * using a few different values .
 */
public class TestBpServiceActorScheduler {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestBpServiceActorScheduler.class);

  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final long HEARTBEAT_INTERVAL_MS = 5000;      // 5 seconds
  private static final long LIFELINE_INTERVAL_MS = 3 * HEARTBEAT_INTERVAL_MS;
  private static final long BLOCK_REPORT_INTERVAL_MS = 10000;  // 10 seconds
  private static final long OUTLIER_REPORT_INTERVAL_MS = 10000;  // 10 seconds
  private final Random random = new Random(System.nanoTime());

  @Test
  public void testInit() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.isHeartbeatDue(now));
      assertTrue(scheduler.isBlockReportDue(scheduler.monotonicNow()));
    }
  }

  @Test
  public void testScheduleBlockReportImmediate() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleBlockReport(0);
      assertTrue(scheduler.resetBlockReportTime);
      assertThat(scheduler.nextBlockReportTime, is(now));
    }
  }

  @Test
  public void testScheduleBlockReportDelayed() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      final long delayMs = 10;
      scheduler.scheduleBlockReport(delayMs);
      assertTrue(scheduler.resetBlockReportTime);
      assertTrue(scheduler.nextBlockReportTime - now >= 0);
      assertTrue(scheduler.nextBlockReportTime - (now + delayMs) < 0);
    }
  }

  /**
   * If resetBlockReportTime is true then the next block report must be scheduled
   * in the range [now, now + BLOCK_REPORT_INTERVAL_SEC).
   */
  @Test
  public void testScheduleNextBlockReport() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.resetBlockReportTime);
      scheduler.scheduleNextBlockReport();
      assertTrue(scheduler.nextBlockReportTime - (now + BLOCK_REPORT_INTERVAL_MS) < 0);
    }
  }

  /**
   * If resetBlockReportTime is false then the next block report must be scheduled
   * exactly at (now + BLOCK_REPORT_INTERVAL_SEC).
   */
  @Test
  public void testScheduleNextBlockReport2() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.resetBlockReportTime = false;
      scheduler.scheduleNextBlockReport();
      assertThat(scheduler.nextBlockReportTime, is(now + BLOCK_REPORT_INTERVAL_MS));
    }
  }

  /**
   * Tests the case when a block report was delayed past its scheduled time.
   * In that case the next block report should not be delayed for a full interval.
   */
  @Test
  public void testScheduleNextBlockReport3() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.resetBlockReportTime = false;

      // Make it look like the block report was scheduled to be sent between 1-3
      // intervals ago but sent just now.
      final long blockReportDelay =
          BLOCK_REPORT_INTERVAL_MS + random.nextInt(2 * (int) BLOCK_REPORT_INTERVAL_MS);
      final long origBlockReportTime = now - blockReportDelay;
      scheduler.nextBlockReportTime = origBlockReportTime;
      scheduler.scheduleNextBlockReport();
      assertTrue(scheduler.nextBlockReportTime - now < BLOCK_REPORT_INTERVAL_MS);
      assertTrue(((scheduler.nextBlockReportTime - origBlockReportTime) % BLOCK_REPORT_INTERVAL_MS) == 0);
    }
  }

  @Test
  public void testScheduleHeartbeat() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleNextHeartbeat();
      assertFalse(scheduler.isHeartbeatDue(now));
      scheduler.scheduleHeartbeat();
      assertTrue(scheduler.isHeartbeatDue(now));
    }
  }


  /**
   * Regression test for HDFS-9305.
   * Delayed processing of a heartbeat can cause a subsequent heartbeat
   * storm.
   */
  @Test
  public void testScheduleDelayedHeartbeat() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleNextHeartbeat();
      assertFalse(scheduler.isHeartbeatDue(now));

      // Simulate a delayed heartbeat e.g. due to slow processing by NN.
      scheduler.nextHeartbeatTime = now - (HEARTBEAT_INTERVAL_MS * 10);
      scheduler.scheduleNextHeartbeat();

      // Ensure that the next heartbeat is not due immediately.
      assertFalse(scheduler.isHeartbeatDue(now));
    }
  }

  @Test
  public void testScheduleLifeline() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      scheduler.scheduleNextLifeline(now);
      assertFalse(scheduler.isLifelineDue(now));
      assertThat(scheduler.getLifelineWaitTime(), is(LIFELINE_INTERVAL_MS));
      scheduler.scheduleNextLifeline(now - LIFELINE_INTERVAL_MS);
      assertTrue(scheduler.isLifelineDue(now));
      assertThat(scheduler.getLifelineWaitTime(), is(0L));
    }
  }

  @Test
  public void testOutlierReportScheduling() {
    for (final long now : getTimestamps()) {
      Scheduler scheduler = makeMockScheduler(now);
      assertTrue(scheduler.isOutliersReportDue(now));
      scheduler.scheduleNextOutlierReport();
      assertFalse(scheduler.isOutliersReportDue(now));
      assertFalse(scheduler.isOutliersReportDue(now + 1));
      assertTrue(scheduler.isOutliersReportDue(
          now + OUTLIER_REPORT_INTERVAL_MS));
    }
  }

  private Scheduler makeMockScheduler(long now) {
    LOG.info("Using now = " + now);
    Scheduler mockScheduler = spy(new Scheduler(
        HEARTBEAT_INTERVAL_MS, LIFELINE_INTERVAL_MS,
        BLOCK_REPORT_INTERVAL_MS, OUTLIER_REPORT_INTERVAL_MS));
    doReturn(now).when(mockScheduler).monotonicNow();
    mockScheduler.nextBlockReportTime = now;
    mockScheduler.nextHeartbeatTime = now;
    mockScheduler.nextOutliersReportTime = now;
    return mockScheduler;
  }

  List<Long> getTimestamps() {
    return Arrays.asList(
        0L, Long.MIN_VALUE, Long.MAX_VALUE, // test boundaries
        Long.MAX_VALUE - 1,                 // test integer overflow
        abs(random.nextLong()),             // positive random
        -abs(random.nextLong()));           // negative random
  }
}
