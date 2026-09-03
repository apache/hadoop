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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A compact, self-contained HDFS DataNode read/write stress tester.
 *
 * <p>{@code TestDFSIO} is the standard HDFS I/O benchmark, but it launches a
 * MapReduce job scheduled by YARN over the whole cluster, which makes it hard
 * to (a) generate a <em>controlled</em> QPS / throughput, (b) target a specific
 * subset of DataNodes (e.g. a replica set), and (c) obtain client-side latency
 * <em>distributions</em> (p50/p95/p99). It also loads the entire cluster and
 * takes a long time to saturate a targeted set of nodes. This tool fills that
 * gap with a single-process, no-MapReduce load generator that:
 *
 * <ul>
 *   <li>drives a configurable, steady read and/or write throughput (MB/s) with
 *       a global rate limiter (optionally ramping up for acceleration testing);</li>
 *   <li>targets specific DataNodes via HDFS favored-nodes hints so load lands
 *       on a chosen replica set;</li>
 *   <li>uses a configurable block/file size;</li>
 *   <li>runs a pre-test phase that pre-creates a large set of files so that the
 *       measured reads are cold reads (larger than DataNode page cache);</li>
 *   <li>records client-side read/write latency distributions
 *       (p50, p75, p95, p99, min, max, mean, stddev) and effective QPS.</li>
 * </ul>
 *
 * <h3>How to run</h3>
 * <pre>
 *   # From a client host with the HDFS configuration on the classpath. The
 *   # tool ships in the hadoop-hdfs test jar:
 *   hadoop jar hadoop-hdfs-&lt;version&gt;-tests.jar \
 *       org.apache.hadoop.hdfs.HdfsStressTest /path/to/stress.properties
 * </pre>
 *
 * <p>The single argument is a Java properties file describing the workload.
 * Any property may also be supplied on the command line with
 * {@code -D<key>=<value>} (ToolRunner options), which takes precedence.
 *
 * <h3>Example {@code stress.properties}</h3>
 * <pre>
 *   # Target a specific replica set (host:port of the DataNode xfer port).
 *   favoredDataNodes=dn1.example.com:9866,dn2.example.com:9866,dn3.example.com:9866
 *   replication=3
 *   blockSizeMB=128
 *
 *   # Write workload.
 *   testWriteDirectory=/tmp/hdfs-stress/write
 *   writeThroughputMB=200
 *
 *   # Read workload (cold reads from files created in the pre-test phase).
 *   testReadDirectories=/tmp/hdfs-stress/read
 *   readThroughputMB=200
 *
 *   # Pre-test: create enough read files to exceed the DataNode page cache
 *   # (typically ~2x the DataNode memory) so reads are served from disk.
 *   testReadFileSizeGB=64
 *   preTestWriteThroughputMB=400
 *   preTestWriteDurationSeconds=600
 *
 *   # Main measurement window.
 *   testDurationSeconds=300
 *
 *   # Optional acceleration stress test: linearly ramp throughput from the
 *   # start value above to these end values over the test duration.
 *   endWriteThroughputMB=600
 *   endReadThroughputMB=600
 *
 *   # -1 => auto (a fixed worker pool; the rate limiter enforces throughput).
 *   writeThreads=-1
 *   readThreads=-1
 * </pre>
 *
 * <h3>Configuration reference</h3>
 * <p>All keys are read from the properties file (or overridden with
 * {@code -Dkey=value}). Sizes are plain numbers in the unit named by the key.
 * <table border="1">
 *   <caption>HdfsStressTest properties</caption>
 *   <tr><th>Property</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>{@code favoredDataNodes}</td><td>(none)</td>
 *       <td>Comma-separated {@code host:port} list of DataNode transfer ports.
 *       Written blocks are pinned to these nodes via HDFS favored-nodes hints,
 *       so load lands on a chosen replica set instead of the whole cluster.</td></tr>
 *   <tr><td>{@code replication}</td><td>3</td>
 *       <td>Replication factor for files created by the tool.</td></tr>
 *   <tr><td>{@code blockSizeMB}</td><td>128</td>
 *       <td>Block/file size in MB. Each write and each read operation moves one
 *       block-sized file, so this also sets the I/O unit for latency stats.</td></tr>
 *   <tr><td>{@code testWriteDirectory}</td><td>(none)</td>
 *       <td>HDFS directory for the write workload. Omit to disable writes.</td></tr>
 *   <tr><td>{@code writeThroughputMB}</td><td>0</td>
 *       <td>Target sustained write throughput in MB/s (0 disables writes).</td></tr>
 *   <tr><td>{@code endWriteThroughputMB}</td><td>0 (no ramp)</td>
 *       <td>If greater than {@code writeThroughputMB}, throughput ramps linearly
 *       from {@code writeThroughputMB} to this value over the run (acceleration
 *       / find-the-knee test); otherwise throughput stays constant.</td></tr>
 *   <tr><td>{@code writeThreads}</td><td>-1 (auto)</td>
 *       <td>Writer worker threads; {@code -1} uses a fixed pool and lets the
 *       rate limiter govern throughput.</td></tr>
 *   <tr><td>{@code testReadDirectories}</td><td>(none)</td>
 *       <td>Comma-separated HDFS directories for the read workload and for the
 *       pre-test cold-read corpus. Omit to disable reads.</td></tr>
 *   <tr><td>{@code readThroughputMB}</td><td>0</td>
 *       <td>Target sustained read throughput in MB/s (0 disables reads).</td></tr>
 *   <tr><td>{@code endReadThroughputMB}</td><td>0 (no ramp)</td>
 *       <td>Optional linear read-throughput ramp end value; ramps only when
 *       greater than {@code readThroughputMB} (see the write ramp).</td></tr>
 *   <tr><td>{@code readThreads}</td><td>-1 (auto)</td>
 *       <td>Reader worker threads; {@code -1} uses a fixed pool.</td></tr>
 *   <tr><td>{@code testReadFileSizeGB}</td><td>0</td>
 *       <td>Total size of the cold-read corpus to pre-create, in GB. Set this
 *       larger than the aggregate OS page cache of the target DataNodes (a good
 *       rule of thumb is ~2x their RAM) so reads cannot be served from cache.</td></tr>
 *   <tr><td>{@code preTestWriteThroughputMB}</td><td>0 (unlimited)</td>
 *       <td>Throughput (MB/s) used while generating the cold-read corpus. The
 *       default of {@code 0} (or any non-positive value) means "build the
 *       corpus as fast as the client can" (no pacing), which is usually what
 *       you want; set a positive value only to keep corpus creation from itself
 *       saturating the cluster.</td></tr>
 *   <tr><td>{@code preTestWriteDurationSeconds}</td><td>0 (unbounded)</td>
 *       <td>Safety cap on the pre-test phase; it stops when either the corpus
 *       reaches {@code testReadFileSizeGB} or this many seconds elapse.</td></tr>
 *   <tr><td>{@code testDurationSeconds}</td><td>60</td>
 *       <td>Length of the measured read/write window.</td></tr>
 * </table>
 *
 * <h3>Cold reads: avoiding OS page-cache hits with a pre-test corpus</h3>
 * <p>A read benchmark is only meaningful if it exercises the DataNode disks
 * rather than the operating-system page cache. If reads keep hitting the same
 * small set of recently written blocks, the DataNodes simply serve them from
 * RAM and the numbers reflect memory bandwidth, not HDFS/disk performance.
 *
 * <p>To force cold reads, the pre-test phase writes a corpus of size
 * {@code testReadFileSizeGB} <em>once</em>, sized deliberately larger than the
 * combined page cache (main memory) of the target DataNodes. Because the corpus
 * does not fit in memory, the kernel continuously evicts older pages as newer
 * blocks are written, so by the time the measured phase reads a given file
 * again its pages have already been evicted and are no longer in the page
 * cache. Every measured read therefore falls through to disk, giving a true
 * cold-read result. The read workload also spreads its picks across the whole
 * corpus (rather than replaying a hot subset) to keep the page-cache hit rate
 * near zero. Pick {@code testReadFileSizeGB} at roughly twice the target
 * DataNode RAM for a comfortable margin.
 *
 * <h3>Distributing load across multiple clients</h3>
 * <p>A single client process is limited by its own CPU, NIC and JVM. To drive
 * higher aggregate load, or to model many real writers/readers, run the tool
 * on several client hosts at once against the same cluster; the total offered
 * load is the sum of the per-client {@code writeThroughputMB} /
 * {@code readThroughputMB}. Give each client a distinct
 * {@code testWriteDirectory} (and, if pre-generating separate corpora, distinct
 * {@code testReadDirectories}) so the clients do not collide on paths, point
 * them at the same {@code favoredDataNodes} to concentrate load on one replica
 * set, and start them together so their measurement windows overlap. Aggregate
 * the per-client latency distributions and throughput to get the cluster-wide
 * result.
 */
public class HdfsStressTest extends Configured implements Tool {

  private static final long MB = 1024L * 1024L;
  private static final long GB = 1024L * MB;
  private static final int IO_BUFFER_BYTES = (int) MB;
  private static final int DEFAULT_AUTO_THREADS = 16;

  // ---- Workload configuration (populated from the properties file) ----
  private InetSocketAddress[] favoredNodes;
  private short replication;
  private long blockSizeBytes;

  private String writeDir;
  private double writeThroughputMB;
  private double endWriteThroughputMB;
  private int writeThreads;

  private List<String> readDirs;
  private double readThroughputMB;
  private double endReadThroughputMB;
  private int readThreads;

  private long readFileSizeBytes;
  private double preTestWriteThroughputMB;
  private long preTestWriteDurationSeconds;

  private long testDurationSeconds;

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: HdfsStressTest <config.properties>");
      System.err.println("See the class javadoc for the list of properties.");
      return 2;
    }
    loadConfig(args[0]);

    DistributedFileSystem dfs = asDistributedFileSystem();
    try {
      // Pre-test: create the cold-read corpus.
      List<Path> readFiles = new ArrayList<>();
      if (readThroughputMB > 0 && readFileSizeBytes > 0) {
        readFiles = preTestCreateReadFiles(dfs);
        if (readFiles.isEmpty()) {
          System.err.println("Pre-test produced no read files; disabling read "
              + "workload for this run.");
        }
      }

      // Test: run write and read workloads concurrently for the duration.
      runTestPhase(dfs, readFiles);
    } finally {
      dfs.close();
    }
    return 0;
  }

  private DistributedFileSystem asDistributedFileSystem() throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("HdfsStressTest requires an HDFS "
          + "DistributedFileSystem, but got " + fs.getClass().getName());
    }
    return (DistributedFileSystem) fs;
  }

  // -------------------------------------------------------------------------
  // Pre-test phase: pre-create cold-read files.
  // -------------------------------------------------------------------------
  @VisibleForTesting
  List<Path> preTestCreateReadFiles(DistributedFileSystem dfs)
      throws Exception {
    final Path[] dirs = new Path[readDirs.size()];
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new Path(readDirs.get(i));
      dfs.mkdirs(dirs[i]);
    }

    final long totalFiles = Math.max(1, readFileSizeBytes / blockSizeBytes);
    final List<Path> created = new ArrayList<>();
    // A non-positive throughput means "build the corpus as fast as possible"
    // (no pacing); a positive value paces corpus creation at that MB/s so the
    // pre-test does not itself saturate the cluster. The previous code floored
    // the rate at 1e-9 ops/s, which made the limiter sleep for ~centuries after
    // the first file whenever the (default) pre-test throughput was 0.
    final RateLimiter limiter = preTestWriteThroughputMB > 0
        ? new RateLimiter(opsPerSecond(preTestWriteThroughputMB)) : null;
    // A non-positive duration means "no time cap": run until the corpus reaches
    // testReadFileSizeGB. The previous code set the deadline to now, so a 0
    // (its documented "unbounded" default) created zero files and silently
    // disabled the entire read workload.
    final long deadline = preTestWriteDurationSeconds > 0
        ? System.nanoTime() + preTestWriteDurationSeconds * 1_000_000_000L
        : Long.MAX_VALUE;

    System.out.printf("Pre-test: creating up to %d cold-read file(s) of %d MB "
        + "across %d directory(ies)...%n",
        totalFiles, blockSizeBytes / MB, dirs.length);

    long index = 0;
    while (index < totalFiles && System.nanoTime() < deadline) {
      if (limiter != null) {
        limiter.acquire();
      }
      Path dir = dirs[(int) (index % dirs.length)];
      Path file = new Path(dir, "coldread-" + index + ".dat");
      writeBlockFile(dfs, file);
      created.add(file);
      index++;
    }
    System.out.printf("Pre-test: created %d read file(s).%n", created.size());
    if (created.size() < totalFiles) {
      // The corpus stopped short of the requested cold size (typically because
      // preTestWriteDurationSeconds capped it). A corpus that fits in the
      // combined page cache cannot guarantee cold reads: the kernel may still
      // hold recently written blocks in RAM, so the measured read numbers can
      // be optimistic. Surface this so the result is not misread as cold-disk.
      System.err.printf("WARNING: pre-test built only %d of %d target file(s) "
          + "(%d of %d MB). The cold-read corpus is smaller than requested, so "
          + "some reads may be served from the OS page cache and the reported "
          + "read throughput/latency may be optimistic. Increase "
          + "preTestWriteDurationSeconds (or remove the cap) so the corpus "
          + "reaches testReadFileSizeGB (ideally ~2x the target-DataNode RAM).%n",
          created.size(), totalFiles,
          created.size() * (blockSizeBytes / MB),
          totalFiles * (blockSizeBytes / MB));
    }
    return created;
  }

  // -------------------------------------------------------------------------
  // Test phase: paced write + read workers with latency capture.
  // -------------------------------------------------------------------------
  @VisibleForTesting
  void runTestPhase(DistributedFileSystem dfs, List<Path> readFiles)
      throws Exception {
    final long durationNanos = testDurationSeconds * 1_000_000_000L;
    final long startNanos = System.nanoTime();
    final long endNanos = startNanos + durationNanos;

    final List<Thread> threads = new ArrayList<>();
    final LatencyStats writeStats = new LatencyStats();
    final LatencyStats readStats = new LatencyStats();
    // Surfaces the first worker failure so the tool exits non-zero instead of
    // silently reporting "success" after an I/O error swallowed inside a worker.
    final AtomicReference<Throwable> workerError = new AtomicReference<>();

    final boolean doWrite = writeThroughputMB > 0 && writeDir != null;
    final boolean doRead = readThroughputMB > 0 && !readFiles.isEmpty();

    final int wThreads = doWrite
        ? (writeThreads > 0 ? writeThreads : DEFAULT_AUTO_THREADS) : 0;
    final int rThreads = doRead
        ? (readThreads > 0 ? readThreads : DEFAULT_AUTO_THREADS) : 0;

    final CountDownLatch done = new CountDownLatch(wThreads + rThreads);

    if (doWrite) {
      dfs.mkdirs(new Path(writeDir));
      final RateLimiter writeLimiter = new RateLimiter(
          opsPerSecond(writeThroughputMB));
      final AtomicLong seq = new AtomicLong();
      for (int i = 0; i < wThreads; i++) {
        Thread t = new Thread(() -> {
          try {
            while (System.nanoTime() < endNanos) {
              rampRate(writeLimiter, writeThroughputMB, endWriteThroughputMB,
                  startNanos, durationNanos);
              writeLimiter.acquire();
              Path file = new Path(writeDir,
                  "stress-write-" + seq.getAndIncrement() + ".dat");
              long t0 = System.nanoTime();
              writeBlockFile(dfs, file);
              writeStats.record(System.nanoTime() - t0);
            }
          } catch (Exception e) {
            workerError.compareAndSet(null, e);
            System.err.println("Write worker failed: " + e.getMessage());
          } finally {
            done.countDown();
          }
        }, "stress-write-" + i);
        t.start();
        threads.add(t);
      }
    }

    if (doRead) {
      final RateLimiter readLimiter = new RateLimiter(
          opsPerSecond(readThroughputMB));
      final Path[] files = readFiles.toArray(new Path[0]);
      for (int i = 0; i < rThreads; i++) {
        Thread t = new Thread(() -> {
          try {
            while (System.nanoTime() < endNanos) {
              rampRate(readLimiter, readThroughputMB, endReadThroughputMB,
                  startNanos, durationNanos);
              readLimiter.acquire();
              Path file = files[ThreadLocalRandom.current().nextInt(files.length)];
              long t0 = System.nanoTime();
              readWholeFile(dfs, file);
              readStats.record(System.nanoTime() - t0);
            }
          } catch (Exception e) {
            workerError.compareAndSet(null, e);
            System.err.println("Read worker failed: " + e.getMessage());
          } finally {
            done.countDown();
          }
        }, "stress-read-" + i);
        t.start();
        threads.add(t);
      }
    }

    System.out.printf("Test: running for %d second(s) with %d write and %d read "
        + "worker(s)...%n", testDurationSeconds, wThreads, rThreads);
    done.await();
    for (Thread t : threads) {
      t.join();
    }

    Throwable err = workerError.get();
    if (err != null) {
      throw new IOException("Stress workload failed: " + err, err);
    }

    double actualSeconds = (System.nanoTime() - startNanos) / 1e9;
    double opBytesMB = blockSizeBytes / (double) MB;
    printReport("WRITE", writeStats, actualSeconds, opBytesMB);
    printReport("READ", readStats, actualSeconds, opBytesMB);
  }

  // -------------------------------------------------------------------------
  // HDFS I/O helpers.
  // -------------------------------------------------------------------------
  @VisibleForTesting
  void writeBlockFile(DistributedFileSystem dfs, Path file)
      throws IOException {
    byte[] buf = new byte[IO_BUFFER_BYTES];
    Arrays.fill(buf, (byte) 'a');
    OutputStream out = dfs.create(file, FsPermission.getFileDefault(),
        true, IO_BUFFER_BYTES, replication, blockSizeBytes, null, favoredNodes);
    try {
      long remaining = blockSizeBytes;
      while (remaining > 0) {
        int n = (int) Math.min(buf.length, remaining);
        out.write(buf, 0, n);
        remaining -= n;
      }
    } finally {
      out.close();
    }
  }

  @VisibleForTesting
  void readWholeFile(DistributedFileSystem dfs, Path file)
      throws IOException {
    byte[] buf = new byte[IO_BUFFER_BYTES];
    InputStream in = dfs.open(file);
    try {
      while (in.read(buf) != -1) {
        // Discard: we only care about read latency/throughput, not the bytes.
      }
    } finally {
      in.close();
    }
  }

  // -------------------------------------------------------------------------
  // Rate control.
  // -------------------------------------------------------------------------
  /** Ops/sec required to sustain {@code throughputMB} at the block size. */
  private double opsPerSecond(double throughputMB) {
    double opMB = blockSizeBytes / (double) MB;
    return Math.max(throughputMB / opMB, 1e-9);
  }

  /**
   * Linearly ramp the limiter rate from the start throughput to the end
   * throughput across the test duration (acceleration stress testing). A no-op
   * when {@code endThroughputMB <= startThroughputMB}.
   */
  private void rampRate(RateLimiter limiter, double startThroughputMB,
      double endThroughputMB, long startNanos, long durationNanos) {
    if (endThroughputMB <= startThroughputMB || durationNanos <= 0) {
      return;
    }
    double fraction = Math.min(1.0,
        (System.nanoTime() - startNanos) / (double) durationNanos);
    double current = startThroughputMB
        + (endThroughputMB - startThroughputMB) * fraction;
    limiter.setRate(opsPerSecond(current));
  }

  /**
   * Minimal token-bucket rate limiter. {@link #acquire()} blocks so that calls
   * are spaced to average {@code permitsPerSecond}. Thread-safe and supports a
   * dynamically changing rate (for ramping).
   */
  static final class RateLimiter {
    private double permitsPerSecond;
    private long nextFreeTimeNanos;

    RateLimiter(double permitsPerSecond) {
      this.permitsPerSecond = Math.max(permitsPerSecond, 1e-9);
      this.nextFreeTimeNanos = System.nanoTime();
    }

    synchronized void setRate(double permitsPerSecond) {
      this.permitsPerSecond = Math.max(permitsPerSecond, 1e-9);
    }

    void acquire() throws InterruptedException {
      long waitNanos;
      synchronized (this) {
        long now = System.nanoTime();
        if (now > nextFreeTimeNanos) {
          nextFreeTimeNanos = now;
        }
        waitNanos = nextFreeTimeNanos - now;
        nextFreeTimeNanos += (long) (1_000_000_000L / permitsPerSecond);
      }
      if (waitNanos > 0) {
        Thread.sleep(waitNanos / 1_000_000L, (int) (waitNanos % 1_000_000L));
      }
    }
  }

  // -------------------------------------------------------------------------
  // Latency statistics.
  // -------------------------------------------------------------------------
  /**
   * Thread-safe collector of per-operation latencies (in nanoseconds).
   *
   * <p>Uses fixed-capacity reservoir sampling (Vitter's Algorithm R) so memory
   * stays bounded regardless of how many operations a long-running stress test
   * performs; the reported percentiles/mean/stddev are computed over a uniform
   * random sample of the observed latencies, while {@link #count()} still
   * reflects the exact total operation count (used for QPS/throughput).
   */
  static final class LatencyStats {
    private static final int MAX_SAMPLES = 200_000;
    private final long[] reservoir = new long[MAX_SAMPLES];
    private final Random rng = new Random(0);
    private int filled;
    private long total;

    synchronized void record(long nanos) {
      if (filled < reservoir.length) {
        reservoir[filled++] = nanos;
      } else {
        // total == number of items seen so far (0-indexed position of this one).
        long j = Math.floorMod(rng.nextLong(), total + 1);
        if (j < reservoir.length) {
          reservoir[(int) j] = nanos;
        }
      }
      total++;
    }

    synchronized long count() {
      return total;
    }

    /** Sorted copy of the sampled latencies, in milliseconds. */
    synchronized double[] sortedMillis() {
      double[] ms = new double[filled];
      for (int i = 0; i < filled; i++) {
        ms[i] = reservoir[i] / 1_000_000.0;
      }
      Arrays.sort(ms);
      return ms;
    }
  }

  private static double percentile(double[] sorted, double p) {
    if (sorted.length == 0) {
      return 0;
    }
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    idx = Math.max(0, Math.min(sorted.length - 1, idx));
    return sorted[idx];
  }

  private void printReport(String label, LatencyStats stats,
      double seconds, double opBytesMB) {
    long count = stats.count();
    if (count == 0) {
      return;
    }
    double[] sorted = stats.sortedMillis();
    double sum = 0;
    for (double v : sorted) {
      sum += v;
    }
    double mean = sum / sorted.length;
    double sqDiff = 0;
    for (double v : sorted) {
      sqDiff += (v - mean) * (v - mean);
    }
    double stddev = Math.sqrt(sqDiff / sorted.length);
    double qps = count / seconds;
    double throughputMB = count * opBytesMB / seconds;

    System.out.println("----------------------------------------");
    System.out.println(label + " results");
    System.out.println("----------------------------------------");
    System.out.printf("  operations       : %d%n", count);
    System.out.printf("  duration (s)     : %.1f%n", seconds);
    System.out.printf("  QPS              : %.2f%n", qps);
    System.out.printf("  throughput (MB/s): %.2f%n", throughputMB);
    System.out.printf("  latency ms  min  : %.2f%n", sorted[0]);
    System.out.printf("  latency ms  p50  : %.2f%n", percentile(sorted, 50));
    System.out.printf("  latency ms  p75  : %.2f%n", percentile(sorted, 75));
    System.out.printf("  latency ms  p95  : %.2f%n", percentile(sorted, 95));
    System.out.printf("  latency ms  p99  : %.2f%n", percentile(sorted, 99));
    System.out.printf("  latency ms  max  : %.2f%n", sorted[sorted.length - 1]);
    System.out.printf("  latency ms  mean : %.2f%n", mean);
    System.out.printf("  latency ms stddev: %.2f%n", stddev);
  }

  // -------------------------------------------------------------------------
  // Configuration parsing.
  // -------------------------------------------------------------------------
  @VisibleForTesting
  void loadConfig(String propsPath) throws IOException {
    Configuration conf = getConf();
    Properties props = new Properties();
    try (InputStream in = new java.io.FileInputStream(propsPath)) {
      props.load(in);
    }

    this.favoredNodes = parseFavoredNodes(get(conf, props, "favoredDataNodes",
        ""));
    this.replication = (short) getInt(conf, props, "replication", 3);
    this.blockSizeBytes = getLong(conf, props, "blockSizeMB", 128) * MB;

    this.writeDir = get(conf, props, "testWriteDirectory", null);
    this.writeThroughputMB = getDouble(conf, props, "writeThroughputMB", 0);
    this.endWriteThroughputMB = getDouble(conf, props, "endWriteThroughputMB",
        0);
    this.writeThreads = getInt(conf, props, "writeThreads", -1);

    String readDirsRaw = get(conf, props, "testReadDirectories", "");
    this.readDirs = new ArrayList<>();
    for (String d : readDirsRaw.split(",")) {
      if (!d.trim().isEmpty()) {
        readDirs.add(d.trim());
      }
    }
    this.readThroughputMB = getDouble(conf, props, "readThroughputMB", 0);
    this.endReadThroughputMB = getDouble(conf, props, "endReadThroughputMB", 0);
    this.readThreads = getInt(conf, props, "readThreads", -1);

    this.readFileSizeBytes = getLong(conf, props, "testReadFileSizeGB", 0) * GB;
    this.preTestWriteThroughputMB = getDouble(conf, props,
        "preTestWriteThroughputMB", 0);
    this.preTestWriteDurationSeconds = getLong(conf, props,
        "preTestWriteDurationSeconds", 0);

    this.testDurationSeconds = getLong(conf, props, "testDurationSeconds", 60);

    if (readDirs.isEmpty()) {
      this.readThroughputMB = 0;
    }

    // Fail fast on configurations that would otherwise divide by zero, silently
    // disable a workload the user explicitly asked for, or produce nonsense
    // throughput/latency numbers.
    if (blockSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "blockSizeMB must be a positive integer number of MB, got: "
              + (blockSizeBytes / MB));
    }
    if (readThroughputMB > 0 && readFileSizeBytes <= 0) {
      throw new IllegalArgumentException(
          "readThroughputMB=" + readThroughputMB + " enables the read workload,"
              + " but testReadFileSizeGB is not set to a positive value. The"
              + " cold-read corpus would be empty and the read workload would"
              + " silently not run. Set testReadFileSizeGB (ideally ~2x the"
              + " aggregate target-DataNode RAM) so reads exercise cold data.");
    }
  }

  private static InetSocketAddress[] parseFavoredNodes(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return null;
    }
    List<InetSocketAddress> nodes = new ArrayList<>();
    for (String hp : raw.split(",")) {
      hp = hp.trim();
      if (hp.isEmpty()) {
        continue;
      }
      int colon = hp.lastIndexOf(':');
      if (colon < 0) {
        throw new IllegalArgumentException(
            "favoredDataNodes entry must be host:port, got: " + hp);
      }
      String host = hp.substring(0, colon);
      int port = Integer.parseInt(hp.substring(colon + 1));
      nodes.add(new InetSocketAddress(host, port));
    }
    return nodes.isEmpty() ? null : nodes.toArray(new InetSocketAddress[0]);
  }

  // Configuration (-D) overrides the properties file, which overrides defaults.
  private static String get(Configuration conf, Properties props, String key,
      String dflt) {
    String v = conf.get(key);
    if (v != null) {
      return v;
    }
    return props.getProperty(key, dflt);
  }

  private static int getInt(Configuration conf, Properties props, String key,
      int dflt) {
    String v = get(conf, props, key, null);
    return v == null ? dflt : Integer.parseInt(v.trim());
  }

  private static long getLong(Configuration conf, Properties props, String key,
      long dflt) {
    String v = get(conf, props, key, null);
    return v == null ? dflt : Long.parseLong(v.trim());
  }

  private static double getDouble(Configuration conf, Properties props,
      String key, double dflt) {
    String v = get(conf, props, key, null);
    return v == null ? dflt : Double.parseDouble(v.trim());
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HdfsStressTest(), args);
    System.exit(res);
  }
}
