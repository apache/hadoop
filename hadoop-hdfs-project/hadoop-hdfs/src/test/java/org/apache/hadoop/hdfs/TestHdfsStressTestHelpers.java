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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsStressTest.LatencyStats;
import org.apache.hadoop.hdfs.HdfsStressTest.RateLimiter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Fast, cluster-free unit tests for the {@link HdfsStressTest} rate-control and
 * latency-statistics helpers that pace and measure the write/read workloads.
 */
public class TestHdfsStressTestHelpers {

  /** A finite rate must space out {@code acquire()} calls. */
  @Test
  @Timeout(30)
  public void testRateLimiterPacesAcquires() throws Exception {
    // 20 permits/sec => ~50 ms between permits. Five acquires have four gaps,
    // so the run must take at least ~200 ms; assert a conservative lower bound.
    RateLimiter limiter = new RateLimiter(20);
    long start = System.nanoTime();
    for (int i = 0; i < 5; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue(elapsedMs >= 120,
        "5 acquires at 20/s should take >= ~120 ms but took " + elapsedMs
            + " ms");
  }

  /** A very high rate must not introduce meaningful blocking. */
  @Test
  @Timeout(30)
  public void testRateLimiterHighRateDoesNotBlock() throws Exception {
    RateLimiter limiter = new RateLimiter(1_000_000);
    long start = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue(elapsedMs < 2000,
        "1000 acquires at 1e6/s should be fast but took " + elapsedMs + " ms");
  }

  /** Increasing the rate at runtime must shorten the spacing. */
  @Test
  @Timeout(30)
  public void testRateLimiterSetRateTakesEffect() throws Exception {
    RateLimiter limiter = new RateLimiter(2); // 500 ms spacing
    limiter.acquire();                         // first is immediate
    limiter.setRate(1_000_000);                // speed up dramatically
    long start = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      limiter.acquire();
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
    assertTrue(elapsedMs < 1000,
        "after speeding up, acquires should be fast but took " + elapsedMs
            + " ms");
  }

  /** Latencies must be reported sorted and converted from nanos to millis. */
  @Test
  @Timeout(30)
  public void testLatencyStatsSortedAndConverted() {
    LatencyStats stats = new LatencyStats();
    stats.record(3_000_000L); // 3 ms
    stats.record(1_000_000L); // 1 ms
    stats.record(2_000_000L); // 2 ms

    assertEquals(3, stats.count(), "count should reflect recorded samples");
    double[] ms = stats.sortedMillis();
    assertArrayEquals(new double[] {1.0, 2.0, 3.0}, ms, 1e-9,
        "latencies should be sorted ms");
  }

  /** An empty collector must report zero count and no samples. */
  @Test
  @Timeout(30)
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
  @Test
  @Timeout(30)
  public void testLatencyStatsBoundedMemory() {
    LatencyStats stats = new LatencyStats();
    int n = 500_000; // well past the internal reservoir capacity
    for (int i = 0; i < n; i++) {
      stats.record(1_000_000L); // 1 ms each
    }
    assertEquals(n, stats.count(),
        "count must be exact even beyond the sample cap");
    double[] ms = stats.sortedMillis();
    assertTrue(ms.length < n,
        "retained sample must be bounded well below the op count, was "
            + ms.length);
    assertTrue(ms.length > 0, "retained sample must be non-empty");
    for (double v : ms) {
      assertEquals(1.0, v, 1e-9,
          "sampled latency should be the recorded value");
    }
  }

  /**
   * {@code blockSizeMB} sets the I/O unit and is a divisor when sizing the
   * corpus and computing per-op rates, so a non-positive value must be rejected
   * up front rather than dividing by zero at run time.
   */
  @Test
  @Timeout(30)
  public void testLoadConfigRejectsNonPositiveBlockSize() throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(new Configuration());
    File props = writeProps("blockSizeMB=0");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> tool.loadConfig(props.getAbsolutePath()));
    assertTrue(e.getMessage().contains("blockSizeMB"),
        "message should name blockSizeMB, was: " + e.getMessage());
  }

  /**
   * Reads are served only from the pre-test corpus, so enabling the read
   * workload without a positive {@code testReadFileSizeGB} would silently run
   * zero readers and still exit success. That misconfiguration must fail fast.
   */
  @Test
  @Timeout(30)
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
    assertTrue(e.getMessage().contains("testReadFileSizeGB"),
        "message should name testReadFileSizeGB, was: " + e.getMessage());
  }

  /** A read workload with a positive corpus size must load without error. */
  @Test
  @Timeout(30)
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
  @Test
  @Timeout(30)
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
