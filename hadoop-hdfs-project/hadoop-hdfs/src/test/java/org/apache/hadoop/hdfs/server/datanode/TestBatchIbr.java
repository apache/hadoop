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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.util.Time;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test verifies that incremental block reports are sent in batch mode
 * and the namenode allows closing a file with COMMITTED blocks.
 */
public class TestBatchIbr {
  public static final Logger LOG = LoggerFactory.getLogger(TestBatchIbr.class);

  private static final short NUM_DATANODES = 4;
  private static final int BLOCK_SIZE = 1024;
  private static final int MAX_BLOCK_NUM = 8;
  private static final int NUM_FILES = 1000;
  private static final int NUM_THREADS = 128;

  private static final ThreadLocalBuffer IO_BUF = new ThreadLocalBuffer();
  private static final ThreadLocalBuffer VERIFY_BUF = new ThreadLocalBuffer();

  static {
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(IncrementalBlockReportManager.class),
        Level.TRACE);
  }

  static HdfsConfiguration newConf(long ibrInterval) throws IOException {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(ReplaceDatanodeOnFailure.BEST_EFFORT_KEY, true);

    if (ibrInterval > 0) {
      conf.setLong(DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_KEY, ibrInterval);
    }
    return conf;
  }

  static ExecutorService createExecutor() throws Exception {
    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    final ExecutorCompletionService<Path> completion
        = new ExecutorCompletionService<>(executor);

    // initialize all threads and buffers
    for(int i = 0; i < NUM_THREADS; i++) {
      completion.submit(new Callable<Path>() {
        @Override
        public Path call() throws Exception {
          IO_BUF.get();
          VERIFY_BUF.get();
          return null;
        }
      });
    }
    for(int i = 0; i < NUM_THREADS; i++) {
      completion.take().get();
    }
    return executor;
  }

  static void runIbrTest(final long ibrInterval) throws Exception {
    final ExecutorService executor = createExecutor();
    final Random ran = new Random();

    final Configuration conf = newConf(ibrInterval);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES).build();
    final DistributedFileSystem dfs = cluster.getFileSystem();

    try {
      final String dirPathString = "/dir";
      final Path dir = new Path(dirPathString);
      dfs.mkdirs(dir);

      // start testing
      final long testStartTime = Time.monotonicNow();
      final ExecutorCompletionService<Path> createService
          = new ExecutorCompletionService<>(executor);
      final AtomicLong createFileTime = new AtomicLong();
      final AtomicInteger numBlockCreated = new AtomicInteger();

      // create files
      for(int i = 0; i < NUM_FILES; i++) {
        createService.submit(new Callable<Path>() {
          @Override
          public Path call() throws Exception {
            final long start = Time.monotonicNow();
            try {
              final long seed = ran.nextLong();
              final int numBlocks = ran.nextInt(MAX_BLOCK_NUM) + 1;
              numBlockCreated.addAndGet(numBlocks);
              return createFile(dir, numBlocks, seed, dfs);
            } finally {
              createFileTime.addAndGet(Time.monotonicNow() - start);
            }
          }
        });
      }

      // verify files
      final ExecutorCompletionService<Boolean> verifyService
          = new ExecutorCompletionService<>(executor);
      final AtomicLong verifyFileTime = new AtomicLong();
      for(int i = 0; i < NUM_FILES; i++) {
        final Path file = createService.take().get();
        verifyService.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            final long start = Time.monotonicNow();
            try {
              return verifyFile(file, dfs);
            } finally {
              verifyFileTime.addAndGet(Time.monotonicNow() - start);
            }
          }
        });
      }
      for(int i = 0; i < NUM_FILES; i++) {
        Assert.assertTrue(verifyService.take().get());
      }
      final long testEndTime = Time.monotonicNow();

      LOG.info("ibrInterval=" + ibrInterval + " ("
          + toConfString(DFS_BLOCKREPORT_INCREMENTAL_INTERVAL_MSEC_KEY, conf)
          + "), numBlockCreated=" + numBlockCreated);
      LOG.info("duration=" + toSecondString(testEndTime - testStartTime)
          + ", createFileTime=" + toSecondString(createFileTime.get())
          + ", verifyFileTime=" + toSecondString(verifyFileTime.get()));
      LOG.info("NUM_FILES=" + NUM_FILES
          + ", MAX_BLOCK_NUM=" + MAX_BLOCK_NUM
          + ", BLOCK_SIZE=" + BLOCK_SIZE
          + ", NUM_THREADS=" + NUM_THREADS
          + ", NUM_DATANODES=" + NUM_DATANODES);
      logIbrCounts(cluster.getDataNodes());
    } finally {
      executor.shutdown();
      cluster.shutdown();
    }
  }

  static String toConfString(String key, Configuration conf) {
    return key + "=" + conf.get(key);
  }

  static String toSecondString(long ms) {
    return (ms/1000.0) + "s";
  }

  static void logIbrCounts(List<DataNode> datanodes) {
    final String name = "IncrementalBlockReportsNumOps";
    for(DataNode dn : datanodes) {
      final MetricsRecordBuilder m = MetricsAsserts.getMetrics(
          dn.getMetrics().name());
      final long ibr = MetricsAsserts.getLongCounter(name, m);
      LOG.info(dn.getDisplayName() + ": " + name + "=" + ibr);
    }

  }

  static class ThreadLocalBuffer extends ThreadLocal<byte[]> {
    @Override
    protected byte[] initialValue() {
      return new byte[BLOCK_SIZE];
    }
  }

  static byte[] nextBytes(int blockIndex, long seed, byte[] bytes) {
    byte b = (byte)(seed ^ (seed >> blockIndex));
    for(int i = 0; i < bytes.length; i++) {
      bytes[i] = b++;
    }
    return bytes;
  }

  static Path createFile(Path dir, int numBlocks, long seed,
      DistributedFileSystem dfs) throws IOException {
    final Path f = new Path(dir, seed + "_" + numBlocks);
    final byte[] bytes = IO_BUF.get();

    try(FSDataOutputStream out = dfs.create(f)) {
      for(int i = 0; i < numBlocks; i++) {
        out.write(nextBytes(i, seed, bytes));
      }
    }
    return f;
  }

  static boolean verifyFile(Path f, DistributedFileSystem dfs) {
    final long seed;
    final int numBlocks;
    {
      final String name = f.getName();
      final int i = name.indexOf('_');
      seed = Long.parseLong(name.substring(0, i));
      numBlocks = Integer.parseInt(name.substring(i + 1));
    }

    final byte[] computed = IO_BUF.get();
    final byte[] expected = VERIFY_BUF.get();

    try(FSDataInputStream in = dfs.open(f)) {
      for(int i = 0; i < numBlocks; i++) {
        in.read(computed);
        nextBytes(i, seed, expected);
        Assert.assertArrayEquals(expected, computed);
      }
      return true;
    } catch(Exception e) {
      LOG.error("Failed to verify file " + f);
      return false;
    }
  }

  @Test
  public void testIbr() throws Exception {
    runIbrTest(0L);
    runIbrTest(100L);
  }
}
