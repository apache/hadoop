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

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.FakeTimer;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static org.junit.Assert.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;

/**
 * Tests the FSNamesystemLock, looking at lock compatibilities and
 * proper logging of lock hold times.
 */
public class TestFSNamesystemLock {

  @Test
  public void testFsLockFairness() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    conf.setBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY, true);
    FSNamesystemLock fsnLock = new FSNamesystemLock(conf, null);
    assertTrue(fsnLock.coarseLock.isFair());

    conf.setBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY, false);
    fsnLock = new FSNamesystemLock(conf, null);
    assertFalse(fsnLock.coarseLock.isFair());
  }

  @Test
  public void testFSNamesystemLockCompatibility() {
    FSNamesystemLock rwLock = new FSNamesystemLock(new Configuration(), null);

    assertEquals(0, rwLock.getReadHoldCount());
    rwLock.readLock();
    assertEquals(1, rwLock.getReadHoldCount());

    rwLock.readLock();
    assertEquals(2, rwLock.getReadHoldCount());

    rwLock.readUnlock();
    assertEquals(1, rwLock.getReadHoldCount());

    rwLock.readUnlock();
    assertEquals(0, rwLock.getReadHoldCount());

    assertFalse(rwLock.isWriteLockedByCurrentThread());
    assertEquals(0, rwLock.getWriteHoldCount());
    rwLock.writeLock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(1, rwLock.getWriteHoldCount());

    rwLock.writeLock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(2, rwLock.getWriteHoldCount());

    rwLock.writeUnlock();
    assertTrue(rwLock.isWriteLockedByCurrentThread());
    assertEquals(1, rwLock.getWriteHoldCount());

    rwLock.writeUnlock();
    assertFalse(rwLock.isWriteLockedByCurrentThread());
    assertEquals(0, rwLock.getWriteHoldCount());
  }

  @Test
  public void testFSLockGetWaiterCount() throws InterruptedException {
    final int threadCount = 3;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY, true);
    final FSNamesystemLock rwLock = new FSNamesystemLock(conf, null);
    rwLock.writeLock();
    ExecutorService helper = Executors.newFixedThreadPool(threadCount);

    for (int x = 0; x < threadCount; x++) {
      helper.execute(new Runnable() {
        @Override
        public void run() {
          latch.countDown();
          rwLock.readLock();
        }
      });
    }

    latch.await();
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return (threadCount == rwLock.getQueueLength());
        }
      }, 10, 1000);
    } catch (TimeoutException e) {
      fail("Expected number of blocked thread not found");
    }
  }

  /**
   * Test when FSNamesystem write lock is held for a long time,
   * logger will report it.
   */
  @Test(timeout=45000)
  public void testFSWriteLockLongHoldingReport() throws Exception {
    final long writeLockReportingThreshold = 100L;
    final long writeLockSuppressWarningInterval = 10000L;
    Configuration conf = new Configuration();
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
        writeLockReportingThreshold);
    conf.setTimeDuration(DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
        writeLockSuppressWarningInterval, TimeUnit.MILLISECONDS);

    final FakeTimer timer = new FakeTimer();
    final FSNamesystemLock fsnLock = new FSNamesystemLock(conf, null, timer);
    timer.advance(writeLockSuppressWarningInterval);

    LogCapturer logs = LogCapturer.captureLogs(FSNamesystem.LOG);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.INFO);

    // Don't report if the write lock is held for a short time
    fsnLock.writeLock();
    fsnLock.writeUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()));

    // Report if the write lock is held for a long time
    fsnLock.writeLock();
    timer.advance(writeLockReportingThreshold + 10);
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertTrue(logs.getOutput().contains(GenericTestUtils.getMethodName()));

    // Track but do not report if the write lock is held (interruptibly) for
    // a long time but time since last report does not exceed the suppress
    // warning interval
    fsnLock.writeLockInterruptibly();
    timer.advance(writeLockReportingThreshold + 10);
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()));

    // Track but do not report if it's held for a long time when re-entering
    // write lock but time since last report does not exceed the suppress
    // warning interval
    fsnLock.writeLock();
    timer.advance(writeLockReportingThreshold / 2 + 1);
    fsnLock.writeLockInterruptibly();
    timer.advance(writeLockReportingThreshold / 2 + 1);
    fsnLock.writeLock();
    timer.advance(writeLockReportingThreshold / 2);
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()));
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()));
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()));

    // Report if it's held for a long time and time since last report exceeds
    // the supress warning interval
    timer.advance(writeLockSuppressWarningInterval);
    fsnLock.writeLock();
    timer.advance(writeLockReportingThreshold + 100);
    logs.clearOutput();
    fsnLock.writeUnlock();
    assertTrue(logs.getOutput().contains(GenericTestUtils.getMethodName()));
    assertTrue(logs.getOutput().contains(
        "Number of suppressed write-lock reports: 2"));
  }

  /**
   * Test when FSNamesystem read lock is held for a long time,
   * logger will report it.
   */
  @Test(timeout=45000)
  public void testFSReadLockLongHoldingReport() throws Exception {
    final long readLockReportingThreshold = 100L;
    final long readLockSuppressWarningInterval = 10000L;
    final String readLockLogStmt = "FSNamesystem read lock held for ";
    Configuration conf = new Configuration();
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        readLockReportingThreshold);
    conf.setTimeDuration(DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
        readLockSuppressWarningInterval, TimeUnit.MILLISECONDS);

    final FakeTimer timer = new FakeTimer();
    final FSNamesystemLock fsnLock = new FSNamesystemLock(conf, null, timer);
    timer.advance(readLockSuppressWarningInterval);

    LogCapturer logs = LogCapturer.captureLogs(FSNamesystem.LOG);
    GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.INFO);

    // Don't report if the read lock is held for a short time
    fsnLock.readLock();
    fsnLock.readUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()) &&
        logs.getOutput().contains(readLockLogStmt));

    // Report the first read lock warning if it is held for a long time
    fsnLock.readLock();
    timer.advance(readLockReportingThreshold + 10);
    logs.clearOutput();
    fsnLock.readUnlock();
    assertTrue(logs.getOutput().contains(GenericTestUtils.getMethodName()) &&
        logs.getOutput().contains(readLockLogStmt));

    // Track but do not Report if the write lock is held for a long time but
    // time since last report does not exceed the suppress warning interval
    fsnLock.readLock();
    timer.advance(readLockReportingThreshold + 10);
    logs.clearOutput();
    fsnLock.readUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()) &&
        logs.getOutput().contains(readLockLogStmt));

    // Track but do not Report if it's held for a long time when re-entering
    // read lock but time since last report does not exceed the suppress
    // warning interval
    fsnLock.readLock();
    timer.advance(readLockReportingThreshold / 2 + 1);
    fsnLock.readLock();
    timer.advance(readLockReportingThreshold / 2 + 1);
    logs.clearOutput();
    fsnLock.readUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()) ||
        logs.getOutput().contains(readLockLogStmt));
    logs.clearOutput();
    fsnLock.readUnlock();
    assertFalse(logs.getOutput().contains(GenericTestUtils.getMethodName()) &&
        logs.getOutput().contains(readLockLogStmt));

    // Report if it's held for a long time (and time since last report
    // exceeds the suppress warning interval) while another thread also has the
    // read lock. Let one thread hold the lock long enough to activate an
    // alert, then have another thread grab the read lock to ensure that this
    // doesn't reset the timing.
    timer.advance(readLockSuppressWarningInterval);
    logs.clearOutput();
    final CountDownLatch barrier = new CountDownLatch(1);
    final CountDownLatch barrier2 = new CountDownLatch(1);
    Thread t1 = new Thread() {
      @Override
      public void run() {
        try {
          fsnLock.readLock();
          timer.advance(readLockReportingThreshold + 1);
          barrier.countDown(); // Allow for t2 to acquire the read lock
          barrier2.await(); // Wait until t2 has the read lock
          fsnLock.readUnlock();
        } catch (InterruptedException e) {
          fail("Interrupted during testing");
        }
      }
    };
    Thread t2 = new Thread() {
      @Override
      public void run() {
        try {
          barrier.await(); // Wait until t1 finishes sleeping
          fsnLock.readLock();
          barrier2.countDown(); // Allow for t1 to unlock
          fsnLock.readUnlock();
        } catch (InterruptedException e) {
          fail("Interrupted during testing");
        }
      }
    };
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    // Look for the differentiating class names in the stack trace
    String stackTracePatternString =
        String.format("INFO.+%s(.+\n){5}\\Q%%s\\E\\.run", readLockLogStmt);
    Pattern t1Pattern = Pattern.compile(
        String.format(stackTracePatternString, t1.getClass().getName()));
    assertTrue(t1Pattern.matcher(logs.getOutput()).find());
    Pattern t2Pattern = Pattern.compile(
        String.format(stackTracePatternString, t2.getClass().getName()));
    assertFalse(t2Pattern.matcher(logs.getOutput()).find());
    assertTrue(logs.getOutput().contains(
        "Number of suppressed read-lock reports: 2"));
  }

  @Test
  public void testDetailedHoldMetrics() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY, true);
    FakeTimer timer = new FakeTimer();
    MetricsRegistry registry = new MetricsRegistry("Test");
    MutableRatesWithAggregation rates =
        registry.newRatesWithAggregation("Test");
    FSNamesystemLock fsLock = new FSNamesystemLock(conf, rates, timer);

    fsLock.readLock();
    timer.advanceNanos(1300000);
    fsLock.readUnlock("foo");
    fsLock.readLock();
    timer.advanceNanos(2400000);
    fsLock.readUnlock("foo");

    fsLock.readLock();
    timer.advance(1);
    fsLock.readLock();
    timer.advance(1);
    fsLock.readUnlock("bar");
    fsLock.readUnlock("bar");

    fsLock.writeLock();
    timer.advance(1);
    fsLock.writeUnlock("baz");

    MetricsRecordBuilder rb = MetricsAsserts.mockMetricsRecordBuilder();
    rates.snapshot(rb, true);

    assertGauge("FSNReadLockFooNanosAvgTime", 1850000.0, rb);
    assertCounter("FSNReadLockFooNanosNumOps", 2L, rb);
    assertGauge("FSNReadLockBarNanosAvgTime", 2000000.0, rb);
    assertCounter("FSNReadLockBarNanosNumOps", 1L, rb);
    assertGauge("FSNWriteLockBazNanosAvgTime", 1000000.0, rb);
    assertCounter("FSNWriteLockBazNanosNumOps", 1L, rb);

    // Overall
    assertGauge("FSNReadLockOverallNanosAvgTime", 1900000.0, rb);
    assertCounter("FSNReadLockOverallNanosNumOps", 3L, rb);
    assertGauge("FSNWriteLockOverallNanosAvgTime", 1000000.0, rb);
    assertCounter("FSNWriteLockOverallNanosNumOps", 1L, rb);
  }

}