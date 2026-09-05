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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsStressTest.LatencyStats;
import org.apache.hadoop.hdfs.HdfsStressTest.RateLimiter;
import org.junit.Test;

/**
 * Fast, cluster-free unit tests for the {@link HdfsStressTest} rate-control and
 * latency-statistics helpers that pace and measure the write/read workloads.
 */
public class TestHdfsStressTestHelpers {

  /** A finite rate must space out {@code acquire()} calls. */
  @Test(timeout = 30_000)
  public void testRateLimiterPacesAcquires() throws Exception {
    // 20 permits/sec => ~50 ms between permits. Five acquires have four gaps,
    // so the run must take at least ~200 ms; assert a conservative lower bound.
    RateLimiter limiter = new RateLimiter(20);
    long start = System.nanoTime();
    for (int i = 0; i < 5; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue("5 acquires at 20/s should take >= ~120 ms but took " + elapsedMs
        + " ms", elapsedMs >= 120);
  }

  /** A very high rate must not introduce meaningful blocking. */
  @Test(timeout = 30_000)
  public void testRateLimiterHighRateDoesNotBlock() throws Exception {
    RateLimiter limiter = new RateLimiter(1_000_000);
    long start = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue("1000 acquires at 1e6/s should be fast but took " + elapsedMs
        + " ms", elapsedMs < 2000);
  }

  /** Increasing the rate at runtime must shorten the spacing. */
  @Test(timeout = 30_000)
  public void testRateLimiterSetRateTakesEffect() throws Exception {
    RateLimiter limiter = new RateLimiter(2); // 500 ms spacing
    limiter.acquire();                         // first is immediate
    limiter.setRate(1_000_000);                // speed up dramatically
    long start = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue("after speeding up, acquires should be fast but took "
        + elapsedMs + " ms", elapsedMs < 1000);
  }

  /** Latencies must be reported sorted and converted from nanos to millis. */
  @Test(timeout = 30_000)
  public void testLatencyStatsSortedAndConverted() {
    LatencyStats stats = new LatencyStats();
    stats.record(3_000_000L); // 3 ms
    stats.record(1_000_000L); // 1 ms
    stats.record(2_000_000L); // 2 ms

    assertEquals("count should reflect recorded samples", 3, stats.count());
    double[] ms = stats.sortedMillis();
    assertArrayEquals("latencies should be sorted ms",
        new double[] {1.0, 2.0, 3.0}, ms, 1e-9);
  }

  /** An empty collector must report zero count and no samples. */
  @Test(timeout = 30_000)
  public void testLatencyStatsEmpty() {
    LatencyStats stats = new LatencyStats();
    assertEquals(0, stats.count());
    assertEquals(0, stats.sortedMillis().length);
  }

  /**
   * The collector must bound its memory: {@code count()} stays exact for an
   * unbounded number of operations, while the retained sample is capped (via
   * reservoir sampling) rather than growing without limit.
   */
  @Test(timeout = 30_000)
  public void testLatencyStatsBoundedMemory() {
    LatencyStats stats = new LatencyStats();
    int n = 500_000; // well past the internal reservoir capacity
    for (int i = 0; i < n; i++) {
      stats.record(1_000_000L); // 1 ms each
    }
    assertEquals("count must be exact even beyond the sample cap", n,
        stats.count());
    double[] ms = stats.sortedMillis();
    assertTrue("retained sample must be bounded well below the op count, was "
        + ms.length, ms.length < n);
    assertTrue("retained sample must be non-empty", ms.length > 0);
    for (double v : ms) {
      assertEquals("sampled latency should be the recorded value", 1.0, v, 1e-9);
    }
  }

  /**
   * {@code blockSizeMB} sets the I/O unit and is a divisor when sizing the
   * corpus and computing per-op rates, so a non-positive value must be rejected
   * up front rather than dividing by zero at run time.
   */
  @Test(timeout = 30_000)
  public void testLoadConfigRejectsNonPositiveBlockSize() throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(new Configuration());
    File props = writeProps("blockSizeMB=0");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tool.loadConfig(props.getAbsolutePath()));
    assertTrue("message should name blockSizeMB, was: " + e.getMessage(),
        e.getMessage().contains("blockSizeMB"));
  }

  /**
   * Reads are served only from the pre-test corpus, so enabling the read
   * workload without a positive {@code testReadFileSizeGB} would silently run
   * zero readers and still exit success. That misconfiguration must fail fast.
   */
  @Test(timeout = 30_000)
  public void testLoadConfigRejectsReadWorkloadWithoutCorpusSize()
      throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(new Configuration());
    File props = writeProps(
        "blockSizeMB=1",
        "testReadDirectories=/stress/read",
        "readThroughputMB=8");
    // testReadFileSizeGB deliberately omitted (defaults to 0).
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tool.loadConfig(props.getAbsolutePath()));
    assertTrue("message should name testReadFileSizeGB, was: " + e.getMessage(),
        e.getMessage().contains("testReadFileSizeGB"));
  }

  /** A read workload with a positive corpus size must load without error. */
  @Test(timeout = 30_000)
  public void testLoadConfigAcceptsValidReadWorkload() throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(new Configuration());
    File props = writeProps(
        "blockSizeMB=1",
        "testReadDirectories=/stress/read",
        "readThroughputMB=8",
        "testReadFileSizeGB=1");
    tool.loadConfig(props.getAbsolutePath()); // must not throw
  }

  /** A write-only run must not require a read corpus size. */
  @Test(timeout = 30_000)
  public void testLoadConfigAcceptsWriteOnlyWorkload() throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(new Configuration());
    File props = writeProps(
        "blockSizeMB=1",
        "testWriteDirectory=/stress/write",
        "writeThroughputMB=8");
    tool.loadConfig(props.getAbsolutePath()); // must not throw
  }

  private static File writeProps(String... lines) throws Exception {
    File props = File.createTempFile("hdfs-stress-helpers", ".properties");
    props.deleteOnExit();
    try (FileWriter fw = new FileWriter(props)) {
      for (String line : lines) {
        fw.write(line);
        fw.write('\n');
      }
    }
    return props;
  }
}
