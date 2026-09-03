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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional and latency tests for sequential read-ahead prefetch
 * ({@link BlockPrefetcher}) on {@link DFSInputStream}.
 *
 * <p>The latency tests simulate per-block DataNode latency with a fault
 * injector on {@code getBlockReader} (which fires on every read path), since
 * MiniDFSCluster reads are loopback. The heavyweight 2 GB tests allocate up to
 * ~2 GB of reusable prefetch buffers and should be run with a large -Xmx.
 */
public class TestDFSInputStreamPrefetch {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDFSInputStreamPrefetch.class);

  private MiniDFSCluster cluster;
  private Configuration baseConf;

  @BeforeEach
  public void setUp() {
    baseConf = new Configuration();
    // Force remote (non short-circuit) reads so injected fetch latency applies.
    baseConf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
  }

  @AfterEach
  public void tearDown() {
    DFSClientFaultInjector.set(new DFSClientFaultInjector());
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Inject a fixed sleep on every block-reader open to simulate per-block
   * DataNode/network latency. Fires on both the sequential and the prefetch
   * read paths (both open a reader via getBlockReader), so the baseline and
   * the prefetched read are charged the same per-block latency.
   */
  private static final class LatencyInjector extends DFSClientFaultInjector {
    private final long millis;
    LatencyInjector(long millis) {
      this.millis = millis;
    }
    @Override
    public void openBlockReaderDelay() {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private byte[] writeFile(FileSystem fs, Path p, int blockSize, int numBlocks)
      throws IOException {
    final long len = (long) blockSize * numBlocks;
    byte[] data = new byte[(int) len];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i * 31 + 7);
    }
    try (org.apache.hadoop.fs.FSDataOutputStream out =
             fs.create(p, true, 4096, (short) 1, blockSize)) {
      out.write(data);
    }
    return data;
  }

  /** Stream a file of {@code totalLen} bytes to {@code p} in 4 MB chunks. */
  private void writeLargeFile(FileSystem fs, Path p, int blockSize, long totalLen)
      throws IOException {
    try (org.apache.hadoop.fs.FSDataOutputStream out =
             fs.create(p, true, 4096, (short) 1, blockSize)) {
      byte[] chunk = new byte[4 * 1024 * 1024];
      for (int i = 0; i < chunk.length; i++) {
        chunk[i] = (byte) i;
      }
      for (long w = 0; w < totalLen; w += chunk.length) {
        out.write(chunk);
      }
    }
  }

  /** Fully read a stream in 1 MB chunks, returning all bytes. */
  private static byte[] readFully(InputStream in, int totalLen)
      throws IOException {
    byte[] out = new byte[totalLen];
    byte[] chunk = new byte[1024 * 1024];
    int filled = 0;
    int n;
    while (filled < totalLen && (n = in.read(chunk, 0, chunk.length)) > 0) {
      System.arraycopy(chunk, 0, out, filled, n);
      filled += n;
    }
    assertEquals(totalLen, filled, "did not read whole file");
    return out;
  }

  /** Time a streaming full read (bytes discarded). */
  private static long timeRead(FileSystem fs, Path p, long totalLen)
      throws IOException {
    byte[] chunk = new byte[4 * 1024 * 1024];
    long start = System.nanoTime();
    long read = 0;
    try (FSDataInputStream in = fs.open(p)) {
      int n;
      while ((n = in.read(chunk, 0, chunk.length)) > 0) {
        read += n;
      }
    }
    assertEquals(totalLen, read);
    return (System.nanoTime() - start) / 1_000_000L;
  }

  private static Configuration prefetchConf(Configuration base, long size,
      int threads, long maxBytes) {
    Configuration c = new Configuration(base);
    c.setBoolean(HdfsClientConfigKeys.Prefetch.ENABLED_KEY, true);
    c.setLong(HdfsClientConfigKeys.Prefetch.SIZE_KEY, size);
    c.setInt(HdfsClientConfigKeys.Prefetch.THREADS_KEY, threads);
    c.setInt(HdfsClientConfigKeys.Prefetch.THREADPOOL_SIZE_KEY, 8);
    c.setLong(HdfsClientConfigKeys.Prefetch.MAX_BYTES_KEY, maxBytes);
    return c;
  }

  /**
   * Correctness: with prefetch enabled on a multi-block file, reads return
   * exactly the bytes written, and the cache actually serves data. Small blocks
   * for speed.
   */
  @Test
  @Timeout(120)
  public void testPrefetchReadsAreCorrect() throws Exception {
    final int blockSize = 1024 * 1024;     // 1 MB
    final int numBlocks = 4;               // 4 MB file
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-correct");
    byte[] expected;
    try (FileSystem fs = cluster.getFileSystem()) {
      expected = writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      assertNotNull(dfsin.getPrefetcherForTesting(),
          "prefetcher should be active for multi-block file");

      byte[] got = readFully(in, expected.length);
      assertArrayEquals(expected, got, "prefetched bytes must match written bytes");

      BlockPrefetcher.Metrics m = dfsin.getPrefetcherForTesting().getMetrics();
      LOG.info("prefetch metrics: hits={} misses={} bytesServed={} "
              + "prefetchOk={} prefetchFailed={} cancellations={}",
          m.hits, m.misses, m.bytesServed, m.prefetchOk, m.prefetchFailed,
          m.cancellations);
      assertTrue(m.hits > 0, "cache should have served some reads");
      assertEquals(0, m.prefetchFailed, "no prefetch should have failed");
    }
  }

  /** A single-block file must never create a prefetcher. */
  @Test
  @Timeout(60)
  public void testSingleBlockFileNotPrefetched() throws Exception {
    final int blockSize = 1024 * 1024;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();
    Path file = new Path("/single-block");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeFile(fs, file, blockSize, 1);
    }
    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      assertNull(dfsin.getPrefetcherForTesting(), "single-block file must not prefetch");
    }
  }

  /** Run baseline (off) then prefetch (on) full reads; returns [baselineMs, prefetchMs]. */
  private long[] measure(Path file, long totalLen, Configuration on,
      BlockPrefetcher.Metrics[] metricsOut) throws IOException {
    Configuration off = new Configuration(baseConf);
    off.setBoolean(HdfsClientConfigKeys.Prefetch.ENABLED_KEY, false);

    long baselineMs;
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), off)) {
      baselineMs = timeRead(fs, file, totalLen);
    }

    long prefetchMs;
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on)) {
      byte[] chunk = new byte[4 * 1024 * 1024];
      long start = System.nanoTime();
      try (FSDataInputStream in = fs.open(file)) {
        DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
        assertNotNull(dfsin.getPrefetcherForTesting(), "prefetcher must be active");
        long read = 0;
        int n;
        while ((n = in.read(chunk, 0, chunk.length)) > 0) {
          read += n;
        }
        assertEquals(totalLen, read);
        metricsOut[0] = dfsin.getPrefetcherForTesting().getMetrics();
      }
      prefetchMs = (System.nanoTime() - start) / 1_000_000L;
    }
    return new long[] {baselineMs, prefetchMs};
  }

  private static void report(String tag, long baselineMs, long prefetchMs,
      BlockPrefetcher.Metrics m) {
    double speedup = (double) baselineMs / Math.max(1, prefetchMs);
    LOG.info("=== {} ===", tag);
    LOG.info("baseline (no prefetch): {} ms", baselineMs);
    LOG.info("with prefetch:          {} ms", prefetchMs);
    LOG.info("speedup:                {}x", String.format("%.2f", speedup));
    LOG.info("metrics: hits={} misses={} prefetchOk={} cancellations={} bytesServed={}",
        m.hits, m.misses, m.prefetchOk, m.cancellations, m.bytesServed);
    System.out.printf("PREFETCH PERF [%s]: baseline=%dms prefetch=%dms speedup=%.2fx "
            + "prefetchOk=%d cancellations=%d%n",
        tag, baselineMs, prefetchMs, speedup, m.prefetchOk, m.cancellations);
  }

  /**
   * Assert the deterministic invariants of a prefetch perf run: the cache was
   * actually used, and prefetch did not pathologically regress versus the
   * no-prefetch baseline. The exact wall-clock speedup is intentionally NOT
   * asserted: in a single-JVM {@link MiniDFSCluster} the emulated DataNodes
   * share the same CPU and OS page cache, so the parallel-transfer win is muted
   * and noisy (runs routinely land a few percent apart in either direction).
   * The measured speedup is logged by {@link #report}, and the real,
   * reproducible throughput numbers live in the design doc / commit message.
   * The generous 2x no-regression bound still catches a gross correctness or
   * performance regression without flaking on scheduling jitter.
   */
  private static void assertPrefetchEffective(BlockPrefetcher.Metrics m,
      long[] r) {
    assertTrue(m.hits > 0, "prefetch should serve from cache");
    assertTrue(r[1] < r[0] * 2, "prefetch (" + r[1] + "ms) should not regress badly vs baseline ("
        + r[0] + "ms)");
  }

  /**
   * Heavyweight latency/throughput scenarios allocate hundreds of MB up to
   * ~2 GB of prefetch buffers (plus the file payload), which OOMs or times out
   * the default Apache Yetus unit-test JVM. Skip them unless explicitly opted
   * in ({@code -Dtest.prefetch.heavy=true}) AND the JVM has enough heap
   * headroom, so they stay runnable on demand for perf validation without
   * breaking precommit CI. The small (1 MB-block) correctness tests always run.
   */
  private static void assumeHeavyPrefetchEnabled(long requiredBufferBytes) {
    Assumptions.assumeTrue(
        Boolean.getBoolean("test.prefetch.heavy"),
        "heavyweight prefetch perf test; enable with -Dtest.prefetch.heavy=true");
    long headroom = requiredBufferBytes + (512L << 20);
    Assumptions.assumeTrue(
        Runtime.getRuntime().maxMemory() >= headroom,
        "insufficient -Xmx for heavyweight prefetch test: need >= " + headroom
            + " bytes, have " + Runtime.getRuntime().maxMemory());
  }

  /** 512 MB file, 4 x 128 MB blocks, 512 MB budget (N=4), single DataNode. */
  @Test
  @Timeout(300)
  public void testPrefetchLatency512MBFile() throws Exception {
    assumeHeavyPrefetchEnabled(512L * 1024 * 1024);
    final int blockSize = 128 * 1024 * 1024;
    final int numBlocks = 4;
    final long totalLen = (long) blockSize * numBlocks;

    baseConf.setInt("dfs.bytes-per-checksum", 512);
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();
    Path file = new Path("/prefetch-512mb");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeLargeFile(fs, file, blockSize, totalLen);
    }
    DFSClientFaultInjector.set(new LatencyInjector(300));

    Configuration on = prefetchConf(baseConf, 4L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    BlockPrefetcher.Metrics[] m = new BlockPrefetcher.Metrics[1];
    long[] r = measure(file, totalLen, on, m);
    report("512MB file, 128MB blocks, N=4, 1 DataNode", r[0], r[1], m[0]);

    assertPrefetchEffective(m[0], r);
  }

  /** 2 GB file, 4 x 512 MB blocks, 1 GB budget (N=2 -> one block ahead). */
  @Test
  @Timeout(600)
  public void testPrefetchLatency2GBFile1GBBudget() throws Exception {
    assumeHeavyPrefetchEnabled(1024L * 1024 * 1024);
    final int blockSize = 512 * 1024 * 1024;
    final int numBlocks = 4;
    final long totalLen = (long) blockSize * numBlocks;

    baseConf.setInt("dfs.bytes-per-checksum", 512);
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();
    Path file = new Path("/prefetch-2gb-1gbudget");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeLargeFile(fs, file, blockSize, totalLen);
    }
    DFSClientFaultInjector.set(new LatencyInjector(300));

    Configuration on = prefetchConf(baseConf, 1024L * 1024 * 1024, 4,
        4L * 1024 * 1024 * 1024);
    BlockPrefetcher.Metrics[] m = new BlockPrefetcher.Metrics[1];
    long[] r = measure(file, totalLen, on, m);
    report("2GB file, 512MB blocks, 1GB budget (N=2), 1 DataNode", r[0], r[1], m[0]);

    assertPrefetchEffective(m[0], r);
  }

  /**
   * 2 GB file, 4 x 512 MB blocks, 2 GB budget so N=4 per-block buffers and all
   * blocks ahead of the cursor fetch concurrently (4 threads), each filled in
   * 8 MB chunks. Single DataNode.
   */
  @Test
  @Timeout(600)
  public void testPrefetchParallel2GBFile2GBBudget() throws Exception {
    assumeHeavyPrefetchEnabled(2L * 1024 * 1024 * 1024);
    final int blockSize = 512 * 1024 * 1024;
    final int numBlocks = 4;
    final long totalLen = (long) blockSize * numBlocks;

    baseConf.setInt("dfs.bytes-per-checksum", 512);
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();
    Path file = new Path("/prefetch-2gb-parallel");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeLargeFile(fs, file, blockSize, totalLen);
    }
    DFSClientFaultInjector.set(new LatencyInjector(300));

    Configuration on = prefetchConf(baseConf, 2L * 1024 * 1024 * 1024, 4,
        4L * 1024 * 1024 * 1024);
    BlockPrefetcher.Metrics[] m = new BlockPrefetcher.Metrics[1];
    long[] r = measure(file, totalLen, on, m);
    report("2GB file, 512MB blocks, 2GB budget (N=4 parallel), 1 DataNode",
        r[0], r[1], m[0]);

    assertPrefetchEffective(m[0], r);
  }

  /**
   * Same 2 GB / 512 MB-block / 2 GB-budget v2 config but on FOUR DataNodes, so
   * the blocks land on different nodes and the concurrent prefetches use
   * independent disks/NICs — revealing the parallel-transfer speedup that a
   * single DataNode's bandwidth hides.
   */
  @Test
  @Timeout(600)
  public void testPrefetchParallel2GBFileFourDataNodes() throws Exception {
    assumeHeavyPrefetchEnabled(2L * 1024 * 1024 * 1024);
    final int blockSize = 512 * 1024 * 1024;
    final int numBlocks = 4;
    final long totalLen = (long) blockSize * numBlocks;

    baseConf.setInt("dfs.bytes-per-checksum", 512);
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(4).build();
    cluster.waitActive();
    Path file = new Path("/prefetch-2gb-4dn");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeLargeFile(fs, file, blockSize, totalLen);
    }
    DFSClientFaultInjector.set(new LatencyInjector(300));

    Configuration on = prefetchConf(baseConf, 2L * 1024 * 1024 * 1024, 4,
        4L * 1024 * 1024 * 1024);
    BlockPrefetcher.Metrics[] m = new BlockPrefetcher.Metrics[1];
    long[] r = measure(file, totalLen, on, m);
    report("2GB file, 512MB blocks, 2GB budget (N=4 parallel), 4 DataNodes",
        r[0], r[1], m[0]);

    assertPrefetchEffective(m[0], r);
  }

  /**
   * Verifies the client usage pattern documented in
   * {@link PrefetchReadExample}: enable prefetch via Configuration, open and
   * read a multi-block file normally, and get identical bytes back.
   */
  @Test
  @Timeout(120)
  public void testUsageExample() throws Exception {
    final int blockSize = 1024 * 1024; // 1 MB
    final int numBlocks = 8;           // 8 MB, 8 blocks
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-usage");
    byte[] expected;
    try (FileSystem fs = cluster.getFileSystem()) {
      expected = writeFile(fs, file, blockSize, numBlocks);
    }

    // This is the documented client recipe (see PrefetchReadExample):
    Configuration conf = PrefetchReadExample.prefetchConfig();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);

    long total = 0;
    byte[] buffer = new byte[1024 * 1024];
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), conf);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      assertNotNull(dfsin.getPrefetcherForTesting(),
          "prefetch should be active for the usage example");
      int n;
      int pos = 0;
      while ((n = in.read(buffer, 0, buffer.length)) > 0) {
        for (int i = 0; i < n; i++) {
          assertEquals(expected[pos + i], buffer[i], "byte mismatch at " + (pos + i));
        }
        pos += n;
        total += n;
      }
      assertEquals(expected.length, total);
      assertTrue(dfsin.getPrefetcherForTesting().getMetrics().hits > 0,
          "cache should have served reads");
    }
  }

  /**
   * Verifies the read-cache metrics: bytes served from cache vs. read directly
   * from the DataNode are accounted separately and sum to the file length, and
   * the metrics-logging path is exercised when enabled.
   */
  @Test
  @Timeout(120)
  public void testCacheVsDirectMetrics() throws Exception {
    final int blockSize = 1024 * 1024; // 1 MB
    final int numBlocks = 8;           // 8 MB, 8 blocks
    final int totalLen = blockSize * numBlocks;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-metrics");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    on.setBoolean(HdfsClientConfigKeys.Prefetch.METRICS_LOG_ENABLED_KEY, true);
    on.setLong(HdfsClientConfigKeys.Prefetch.METRICS_LOG_INTERVAL_MS_KEY, 500);

    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      byte[] buffer = new byte[256 * 1024];
      while (in.read(buffer, 0, buffer.length) > 0) {
        // Drain the stream fully to exercise the prefetch read path.
      }
      BlockPrefetcher.Metrics m = dfsin.getPrefetcherForTesting().getMetrics();
      LOG.info("cache-vs-direct: cacheBytes={} directBytes={} hits={} misses={}",
          m.bytesServed, m.bytesReadDirect, m.hits, m.misses);

      assertEquals(totalLen, m.bytesServed + m.bytesReadDirect,
          "cache + direct must equal file length");
      assertTrue(m.bytesReadDirect > 0, "block 0 is read through directly");
      assertTrue(m.bytesServed > 0, "later blocks should be served from cache");
    }
  }

  /**
   * Regression test: prefetch-served bytes must be counted in
   * {@link org.apache.hadoop.hdfs.ReadStatistics} just like directly-read
   * bytes, so cache hits are not invisible to monitoring/billing. Without the
   * accounting in {@code consumePrefetched}, total bytes read would be
   * under-reported by the cache hit portion.
   */
  @Test
  @Timeout(120)
  public void testPrefetchReadStatisticsAccounted() throws Exception {
    final int blockSize = 1024 * 1024; // 1 MB
    final int numBlocks = 8;           // 8 MB, 8 blocks
    final long totalLen = (long) blockSize * numBlocks;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-readstats");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      byte[] buffer = new byte[256 * 1024];
      while (in.read(buffer, 0, buffer.length) > 0) {
        // Drain the stream fully to exercise the prefetch read path.
      }
      BlockPrefetcher.Metrics m = dfsin.getPrefetcherForTesting().getMetrics();
      assertTrue(m.hits > 0, "some bytes should have been served from cache");
      assertTrue(m.bytesServed > 0, "cache should have served bytes");

      // ReadStatistics must include cache-served bytes: the total must equal
      // the whole file, i.e. cache-served bytes are not dropped.
      assertEquals(totalLen, dfsin.getReadStatistics().getTotalBytesRead(),
          "ReadStatistics total must count prefetch-served bytes");
    }
  }

  /**
   * Verify per-client isolation: a client that disables prefetch never
   * prefetches, even when another client in the same JVM has already enabled
   * prefetch and created the process-wide thread pool.
   */
  @Test
  @Timeout(120)
  public void testPrefetchDisabledIsPerClient() throws Exception {
    final int blockSize = 1024 * 1024;
    final int numBlocks = 4;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-per-client");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    // Keep the prefetch-enabled client (and thus the process-wide pool) open
    // while checking the disabled client, so the disabled client is gated by
    // its own config rather than by the pool being absent.
    try (FileSystem fs1 = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in1 = fs1.open(file)) {
      DFSInputStream dfsin1 = (DFSInputStream) in1.getWrappedStream();
      assertNotNull(dfsin1.getPrefetcherForTesting(), "prefetch-enabled client should prefetch");

      Configuration off = new Configuration(baseConf);
      off.setBoolean(HdfsClientConfigKeys.Prefetch.ENABLED_KEY, false);
      try (FileSystem fs2 = FileSystem.newInstance(cluster.getURI(), off);
           FSDataInputStream in2 = fs2.open(file)) {
        DFSInputStream dfsin2 = (DFSInputStream) in2.getWrappedStream();
        assertNull(dfsin2.getPrefetcherForTesting(),
            "prefetch-disabled client must not prefetch even after "
            + "another client in the JVM enabled it");
      }
    }
  }

  /**
   * Verify cache-served bytes are attributed to the same
   * local/remote/short-circuit bucket as the reader that prefetched them,
   * rather than blindly counted as remote. The direct (landing block) read and
   * the prefetched reads open readers the same way, so they share one locality;
   * with short-circuit disabled every byte therefore lands in a single
   * non-short-circuit bucket.
   */
  @Test
  @Timeout(120)
  public void testPrefetchReadStatisticsLocalitySplit() throws Exception {
    final int blockSize = 1024 * 1024;
    final int numBlocks = 8;
    final long totalLen = (long) blockSize * numBlocks;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-locality");
    try (FileSystem fs = cluster.getFileSystem()) {
      writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      byte[] buffer = new byte[256 * 1024];
      while (in.read(buffer, 0, buffer.length) > 0) {
        // Drain the stream fully to exercise the prefetch read path.
      }
      BlockPrefetcher.Metrics m = dfsin.getPrefetcherForTesting().getMetrics();
      assertTrue(m.hits > 0, "some bytes should have been served from cache");

      ReadStatistics rs = dfsin.getReadStatistics();
      long total = rs.getTotalBytesRead();
      long remote = rs.getRemoteBytesRead();
      long local = rs.getTotalLocalBytesRead();
      long sc = rs.getTotalShortCircuitBytesRead();
      assertEquals(totalLen, total, "total must equal file length");
      assertEquals(0L, sc, "short-circuit is disabled for this test");
      assertTrue((remote == total && local == 0) || (local == total && remote == 0),
          "cache-served bytes must share the direct read's locality "
              + "bucket (no local/remote mixing): local=" + local
              + " remote=" + remote);
    }
  }

  /**
   * Verify {@code unbuffer()} releases the stream's pooled prefetch buffers so
   * an idle-but-open stream stops pinning heap: after {@code unbuffer()} the
   * free-buffer pool is drained and the allocation high-water mark is reset,
   * while the stream stays usable and re-allocates lazily on the next read.
   */
  @Test
  @Timeout(120)
  public void testUnbufferReleasesPrefetchBuffers() throws Exception {
    final int blockSize = 1024 * 1024;
    final int numBlocks = 8;
    cluster = new MiniDFSCluster.Builder(baseConf).numDataNodes(1).build();
    cluster.waitActive();

    Path file = new Path("/prefetch-unbuffer");
    byte[] expected;
    try (FileSystem fs = cluster.getFileSystem()) {
      expected = writeFile(fs, file, blockSize, numBlocks);
    }

    Configuration on = prefetchConf(baseConf, 8L * blockSize, 4,
        2L * 1024 * 1024 * 1024);
    try (FileSystem fs = FileSystem.newInstance(cluster.getURI(), on);
         FSDataInputStream in = fs.open(file)) {
      DFSInputStream dfsin = (DFSInputStream) in.getWrappedStream();
      BlockPrefetcher p = dfsin.getPrefetcherForTesting();
      byte[] buffer = new byte[256 * 1024];
      // Read the first few blocks so the prefetcher allocates backing arrays.
      for (int i = 0; i < 12; i++) {
        if (in.read(buffer, 0, buffer.length) <= 0) {
          break;
        }
      }
      assertTrue(p.getAllocatedBuffers() > 0, "prefetcher should have allocated buffers");

      // Quiesce outstanding fetches (no further reads schedule new ones) so the
      // drain assertions below are deterministic: a fetch completing after
      // unbuffer() would otherwise re-offer its array into the pool.
      long deadline = System.currentTimeMillis() + 30_000;
      while (p.getInFlight() > 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(0, p.getInFlight(), "no prefetch should still be in flight");

      in.unbuffer();
      assertEquals(0, p.getAllocatedBuffers(), "unbuffer must reset the allocation high-water mark");
      assertEquals(0, p.getFreeBufferCount(),
          "unbuffer must drain the free-buffer pool");

      // The stream is still usable after unbuffer(): a re-read returns the
      // correct bytes and the pool re-populates lazily.
      in.seek(0);
      byte[] got = readFully(in, expected.length);
      assertArrayEquals(expected, got, "bytes after unbuffer must match");
    }
  }
}
