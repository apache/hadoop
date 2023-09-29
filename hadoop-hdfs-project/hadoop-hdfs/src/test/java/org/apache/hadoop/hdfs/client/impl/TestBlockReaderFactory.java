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
package org.apache.hadoop.hdfs.client.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.PerDatanodeVisitorInfo;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.Visitor;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplicaInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.test.GenericTestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBlockReaderFactory {
  static final Logger LOG =
      LoggerFactory.getLogger(TestBlockReaderFactory.class);

  @Rule
  public final Timeout globalTimeout = new Timeout(180000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() {
    DomainSocket.disableBindPathValidation();
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  @After
  public void cleanup() {
    DFSInputStream.tcpReadsDisabledForTesting = false;
    BlockReaderFactory.createShortCircuitReplicaInfoCallback = null;
  }

  public static Configuration createShortCircuitConf(String testName,
      TemporarySocketDirectory sockDir) {
    Configuration conf = new Configuration();
    conf.set(DFS_CLIENT_CONTEXT, testName);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 4096);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        testName + "._PORT").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    return conf;
  }

  /**
   * If we have a UNIX domain socket configured,
   * and we have dfs.client.domain.socket.data.traffic set to true,
   * and short-circuit access fails, we should still be able to pass
   * data traffic over the UNIX domain socket.  Test this.
   */
  @Test(timeout=60000)
  public void testFallbackFromShortCircuitToUnixDomainTraffic()
      throws Exception {
    DFSInputStream.tcpReadsDisabledForTesting = true;
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();

    // The server is NOT configured with short-circuit local reads;
    // the client is.  Both support UNIX domain reads.
    Configuration clientConf = createShortCircuitConf(
        "testFallbackFromShortCircuitToUnixDomainTraffic", sockDir);
    clientConf.set(DFS_CLIENT_CONTEXT,
        "testFallbackFromShortCircuitToUnixDomainTraffic_clientContext");
    clientConf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
    Configuration serverConf = new Configuration(clientConf);
    serverConf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);

    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(serverConf).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem dfs = FileSystem.get(cluster.getURI(0), clientConf);
    String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 8193;
    final int SEED = 0xFADED;
    DFSTestUtil.createFile(dfs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    byte contents[] = DFSTestUtil.readFileBuffer(dfs, new Path(TEST_FILE));
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(contents, expected));
    cluster.shutdown();
    sockDir.close();
  }

  /**
   * Test the case where address passed to DomainSocketFactory#getPathInfo is
   * unresolved. In such a case an exception should be thrown.
   */
  @Test(timeout=60000)
  public void testGetPathInfoWithUnresolvedHost() throws Exception {
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();

    Configuration conf =
        createShortCircuitConf("testGetPathInfoWithUnresolvedHost", sockDir);
    conf.set(DFS_CLIENT_CONTEXT,
        "testGetPathInfoWithUnresolvedHost_Context");
    conf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);

    DfsClientConf.ShortCircuitConf shortCircuitConf =
        new DfsClientConf.ShortCircuitConf(conf);
    DomainSocketFactory domainSocketFactory =
        new DomainSocketFactory(shortCircuitConf);
    InetSocketAddress targetAddr =
        InetSocketAddress.createUnresolved("random", 32456);

    thrown.expect(IOException.class);
    thrown.expectMessage("Unresolved host: " + targetAddr);
    domainSocketFactory.getPathInfo(targetAddr, shortCircuitConf);
    sockDir.close();
  }

  /**
   * Test the case where we have multiple threads waiting on the
   * ShortCircuitCache delivering a certain ShortCircuitReplica.
   *
   * In this case, there should only be one call to
   * createShortCircuitReplicaInfo.  This one replica should be shared
   * by all threads.
   */
  @Test(timeout=60000)
  public void testMultipleWaitersOnShortCircuitCache()
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean creationIsBlocked = new AtomicBoolean(true);
    final AtomicBoolean testFailed = new AtomicBoolean(false);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    BlockReaderFactory.createShortCircuitReplicaInfoCallback =
      new ShortCircuitCache.ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          Uninterruptibles.awaitUninterruptibly(latch);
          if (!creationIsBlocked.compareAndSet(true, false)) {
            Assert.fail("there were multiple calls to "
                + "createShortCircuitReplicaInfo.  Only one was expected.");
          }
          return null;
        }
      };
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testMultipleWaitersOnShortCircuitCache", sockDir);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4000;
    final int SEED = 0xFADED;
    final int NUM_THREADS = 10;
    DFSTestUtil.createFile(dfs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    Runnable readerRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          byte contents[] = DFSTestUtil.readFileBuffer(dfs, new Path(TEST_FILE));
          Assert.assertFalse(creationIsBlocked.get());
          byte expected[] = DFSTestUtil.
              calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
          Assert.assertTrue(Arrays.equals(contents, expected));
        } catch (Throwable e) {
          LOG.error("readerRunnable error", e);
          testFailed.set(true);
        }
      }
    };
    Thread threads[] = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread(readerRunnable);
      threads[i].start();
    }
    Thread.sleep(500);
    latch.countDown();
    for (int i = 0; i < NUM_THREADS; i++) {
      Uninterruptibles.joinUninterruptibly(threads[i]);
    }
    cluster.shutdown();
    sockDir.close();
    Assert.assertFalse(testFailed.get());
  }

  /**
   * Test the case where we have a failure to complete a short circuit read
   * that occurs, and then later on, we have a success.
   * Any thread waiting on a cache load should receive the failure (if it
   * occurs);  however, the failure result should not be cached.  We want
   * to be able to retry later and succeed.
   */
  @Test
  public void testShortCircuitCacheTemporaryFailure()
      throws Exception {
    BlockReaderTestUtil.enableBlockReaderFactoryTracing();
    final AtomicBoolean replicaCreationShouldFail = new AtomicBoolean(true);
    final AtomicBoolean testFailed = new AtomicBoolean(false);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    BlockReaderFactory.createShortCircuitReplicaInfoCallback =
      new ShortCircuitCache.ShortCircuitReplicaCreator() {
        @Override
        public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
          if (replicaCreationShouldFail.get()) {
            // Insert a short delay to increase the chance that one client
            // thread waits for the other client thread's failure via
            // a condition variable.
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            return new ShortCircuitReplicaInfo();
          }
          return null;
        }
      };
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testShortCircuitCacheTemporaryFailure", sockDir);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4000;
    final int NUM_THREADS = 2;
    final int SEED = 0xFADED;
    final CountDownLatch gotFailureLatch = new CountDownLatch(NUM_THREADS);
    final CountDownLatch shouldRetryLatch = new CountDownLatch(1);
    DFSTestUtil.createFile(dfs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    Runnable readerRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          // First time should fail.
          List<LocatedBlock> locatedBlocks =
              cluster.getNameNode().getRpcServer().getBlockLocations(
              TEST_FILE, 0, TEST_FILE_LEN).getLocatedBlocks();
          LocatedBlock lblock = locatedBlocks.get(0); // first block
          BlockReader blockReader = null;
          try {
            blockReader = BlockReaderTestUtil.getBlockReader(
                cluster.getFileSystem(), lblock, 0, TEST_FILE_LEN);
            Assert.fail("expected getBlockReader to fail the first time.");
          } catch (Throwable t) {
            Assert.assertTrue("expected to see 'TCP reads were disabled " +
                "for testing' in exception " + t, t.getMessage().contains(
                "TCP reads were disabled for testing"));
          } finally {
            if (blockReader != null) blockReader.close(); // keep findbugs happy
          }
          gotFailureLatch.countDown();
          shouldRetryLatch.await();

          // Second time should succeed.
          try {
            blockReader = BlockReaderTestUtil.getBlockReader(
                cluster.getFileSystem(), lblock, 0, TEST_FILE_LEN);
          } catch (Throwable t) {
            LOG.error("error trying to retrieve a block reader " +
                "the second time.", t);
            throw t;
          } finally {
            if (blockReader != null) blockReader.close();
          }
        } catch (Throwable t) {
          LOG.error("getBlockReader failure", t);
          testFailed.set(true);
        }
      }
    };
    Thread threads[] = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      threads[i] = new Thread(readerRunnable);
      threads[i].start();
    }
    gotFailureLatch.await();
    replicaCreationShouldFail.set(false);
    shouldRetryLatch.countDown();
    for (int i = 0; i < NUM_THREADS; i++) {
      Uninterruptibles.joinUninterruptibly(threads[i]);
    }
    cluster.shutdown();
    sockDir.close();
    Assert.assertFalse(testFailed.get());
  }

  /**
   * Test that by default, reads after a failure does not go through SCR.
   */
  @Test
  public void testShortCircuitCacheUnbufferDefault() throws Exception {
    testShortCircuitCacheUnbufferWithDisableInterval(
        DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_DEFAULT, true);
  }
  /**
   * Test the case where if we disable the cache in
   * {@link org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory}, reads
   * after a failure still goes through SCR.
   */
  @Test
  public void testShortCircuitCacheUnbufferDisabled() throws Exception {
    testShortCircuitCacheUnbufferWithDisableInterval(0, false);
  }

  private void testShortCircuitCacheUnbufferWithDisableInterval(
      final long interval, final boolean disabled) throws Exception {
    final String testName = GenericTestUtils.getMethodName();
    BlockReaderTestUtil.enableBlockReaderFactoryTracing();
    try (TemporarySocketDirectory sockDir = new TemporarySocketDirectory()) {
      Configuration conf = createShortCircuitConf(testName, sockDir);
      conf.set(DFS_CLIENT_CONTEXT, testName + interval + disabled);
      conf.setLong(DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY, interval);
      Configuration serverConf = new Configuration(conf);
      MiniDFSCluster.Builder builder =
          new MiniDFSCluster.Builder(serverConf).numDataNodes(1);
      try (MiniDFSCluster cluster = builder.build();
          DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
              .get(cluster.getURI(0), conf)) {
        cluster.waitActive();
        final Path testFile = new Path("/test_file");
        final int testFileLen = 4000;
        final int seed = 0xFADED;
        DFSTestUtil.createFile(dfs, testFile, testFileLen, (short) 1, seed);
        final byte[] expected = DFSTestUtil.
            calculateFileContentsFromSeed(seed, testFileLen);

        try (FSDataInputStream in = dfs.open(testFile)) {
          Assert.assertEquals(0,
              dfs.getClient().getClientContext().getShortCircuitCache(0)
                  .getReplicaInfoMapSize());

          final byte[] buf = new byte[testFileLen];
          IOUtils.readFully(in, buf, 0, testFileLen);
          validateReadResult(dfs, expected, buf, 1);

          // Set cache size to 0 so the replica marked evictable by unbuffer
          // will be purged immediately.
          dfs.getClient().getClientContext().getShortCircuitCache(0)
              .setMaxTotalSize(0);
          LOG.info("Unbuffering");
          in.unbuffer();
          Assert.assertEquals(0,
              dfs.getClient().getClientContext().getShortCircuitCache(0)
                  .getReplicaInfoMapSize());

          DFSTestUtil.appendFile(dfs, testFile, "append more data");

          // This read will force a new replica read via TCP.
          Arrays.fill(buf, (byte) 0);
          in.seek(0);
          IOUtils.readFully(in, buf, 0, testFileLen);
          validateReadResult(dfs, expected, buf, 0);
        }

        LOG.info("Reading {} again.", testFile);
        try (FSDataInputStream in = dfs.open(testFile)) {
          final byte[] buf = new byte[testFileLen];
          Arrays.fill(buf, (byte) 0);
          IOUtils.readFully(in, buf, 0, testFileLen);
          final int expectedMapSize = disabled ? 0 : 1;
          validateReadResult(dfs, expected, buf, expectedMapSize);
        }
      }
    }
  }

  private void validateReadResult(final DistributedFileSystem dfs,
      final byte[] expected, final byte[] actual,
      final int expectedScrRepMapSize) {
    Assert.assertThat(expected, CoreMatchers.is(actual));
    Assert.assertEquals(expectedScrRepMapSize,
        dfs.getClient().getClientContext().getShortCircuitCache(0)
            .getReplicaInfoMapSize());
  }

  /**
   * Test that a client which supports short-circuit reads using
   * shared memory can fall back to not using shared memory when
   * the server doesn't support it.
   */
  @Test
  public void testShortCircuitReadFromServerWithoutShm() throws Exception {
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration clientConf = createShortCircuitConf(
        "testShortCircuitReadFromServerWithoutShm", sockDir);
    Configuration serverConf = new Configuration(clientConf);
    serverConf.setInt(
        DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS, 0);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(serverConf).numDataNodes(1).build();
    cluster.waitActive();
    clientConf.set(DFS_CLIENT_CONTEXT,
        "testShortCircuitReadFromServerWithoutShm_clientContext");
    final DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(0), clientConf);
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4000;
    final int SEED = 0xFADEC;
    DFSTestUtil.createFile(fs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    byte contents[] = DFSTestUtil.readFileBuffer(fs, new Path(TEST_FILE));
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(contents, expected));
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache(0);
    final DatanodeInfo datanode = new DatanodeInfoBuilder()
        .setNodeID(cluster.getDataNodes().get(0).getDatanodeId())
        .build();
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info)
          throws IOException {
        Assert.assertEquals(1,  info.size());
        PerDatanodeVisitorInfo vinfo = info.get(datanode);
        Assert.assertTrue(vinfo.disabled);
        Assert.assertEquals(0, vinfo.full.size());
        Assert.assertEquals(0, vinfo.notFull.size());
      }
    });
    cluster.shutdown();
    sockDir.close();
  }

  /**
   * Test that a client which does not support short-circuit reads using
   * shared memory can talk with a server which supports it.
   */
  @Test
  public void testShortCircuitReadFromClientWithoutShm() throws Exception {
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration clientConf = createShortCircuitConf(
        "testShortCircuitReadWithoutShm", sockDir);
    Configuration serverConf = new Configuration(clientConf);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(serverConf).numDataNodes(1).build();
    cluster.waitActive();
    clientConf.setInt(
        DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS, 0);
    clientConf.set(DFS_CLIENT_CONTEXT,
        "testShortCircuitReadFromClientWithoutShm_clientContext");
    final DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(0), clientConf);
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4000;
    final int SEED = 0xFADEC;
    DFSTestUtil.createFile(fs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    byte contents[] = DFSTestUtil.readFileBuffer(fs, new Path(TEST_FILE));
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(contents, expected));
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache(0);
    Assert.assertEquals(null, cache.getDfsClientShmManager());
    cluster.shutdown();
    sockDir.close();
  }

  /**
   * Test shutting down the ShortCircuitCache while there are things in it.
   */
  @Test
  public void testShortCircuitCacheShutdown() throws Exception {
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testShortCircuitCacheShutdown", sockDir);
    conf.set(DFS_CLIENT_CONTEXT, "testShortCircuitCacheShutdown");
    Configuration serverConf = new Configuration(conf);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(serverConf).numDataNodes(1).build();
    cluster.waitActive();
    final DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(0), conf);
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4000;
    final int SEED = 0xFADEC;
    DFSTestUtil.createFile(fs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);
    byte contents[] = DFSTestUtil.readFileBuffer(fs, new Path(TEST_FILE));
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(contents, expected));
    final ShortCircuitCache cache =
        fs.getClient().getClientContext().getShortCircuitCache(0);
    cache.close();
    Assert.assertTrue(cache.getDfsClientShmManager().
        getDomainSocketWatcher().isClosed());
    cluster.shutdown();
    sockDir.close();
  }

  /**
   * When an InterruptedException is sent to a thread calling
   * FileChannel#read, the FileChannel is immediately closed and the
   * thread gets an exception.  This effectively means that we might have
   * someone asynchronously calling close() on the file descriptors we use
   * in BlockReaderLocal.  So when unreferencing a ShortCircuitReplica in
   * ShortCircuitCache#unref, we should check if the FileChannel objects
   * are still open.  If not, we should purge the replica to avoid giving
   * it out to any future readers.
   *
   * This is a regression test for HDFS-6227: Short circuit read failed
   * due to ClosedChannelException.
   *
   * Note that you may still get ClosedChannelException errors if two threads
   * are reading from the same replica and an InterruptedException is delivered
   * to one of them.
   */
  @Test(timeout=120000)
  public void testPurgingClosedReplicas() throws Exception {
    BlockReaderTestUtil.enableBlockReaderFactoryTracing();
    final AtomicInteger replicasCreated = new AtomicInteger(0);
    final AtomicBoolean testFailed = new AtomicBoolean(false);
    DFSInputStream.tcpReadsDisabledForTesting = true;
    BlockReaderFactory.createShortCircuitReplicaInfoCallback =
        new ShortCircuitCache.ShortCircuitReplicaCreator() {
          @Override
          public ShortCircuitReplicaInfo createShortCircuitReplicaInfo() {
            replicasCreated.incrementAndGet();
            return null;
          }
        };
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    Configuration conf = createShortCircuitConf(
        "testPurgingClosedReplicas", sockDir);
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final String TEST_FILE = "/test_file";
    final int TEST_FILE_LEN = 4095;
    final int SEED = 0xFADE0;
    final DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(0), conf);
    DFSTestUtil.createFile(fs, new Path(TEST_FILE), TEST_FILE_LEN,
        (short)1, SEED);

    final Semaphore sem = new Semaphore(0);
    final List<LocatedBlock> locatedBlocks =
        cluster.getNameNode().getRpcServer().getBlockLocations(
            TEST_FILE, 0, TEST_FILE_LEN).getLocatedBlocks();
    final LocatedBlock lblock = locatedBlocks.get(0); // first block
    final byte[] buf = new byte[TEST_FILE_LEN];
    Runnable readerRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            BlockReader blockReader = null;
            try {
              blockReader = BlockReaderTestUtil.getBlockReader(
                  cluster.getFileSystem(), lblock, 0, TEST_FILE_LEN);
              sem.release();
              try {
                blockReader.readAll(buf, 0, TEST_FILE_LEN);
              } finally {
                sem.acquireUninterruptibly();
              }
            } catch (ClosedByInterruptException e) {
              LOG.info("got the expected ClosedByInterruptException", e);
              sem.release();
              break;
            } finally {
              if (blockReader != null) blockReader.close();
            }
            LOG.info("read another " + TEST_FILE_LEN + " bytes.");
          }
        } catch (Throwable t) {
          LOG.error("getBlockReader failure", t);
          testFailed.set(true);
          sem.release();
        }
      }
    };
    Thread thread = new Thread(readerRunnable);
    thread.start();

    // While the thread is reading, send it interrupts.
    // These should trigger a ClosedChannelException.
    while (thread.isAlive()) {
      sem.acquireUninterruptibly();
      thread.interrupt();
      sem.release();
    }
    Assert.assertFalse(testFailed.get());

    // We should be able to read from the file without
    // getting a ClosedChannelException.
    BlockReader blockReader = null;
    try {
      blockReader = BlockReaderTestUtil.getBlockReader(
          cluster.getFileSystem(), lblock, 0, TEST_FILE_LEN);
      blockReader.readFully(buf, 0, TEST_FILE_LEN);
    } finally {
      if (blockReader != null) blockReader.close();
    }
    byte expected[] = DFSTestUtil.
        calculateFileContentsFromSeed(SEED, TEST_FILE_LEN);
    Assert.assertTrue(Arrays.equals(buf, expected));

    // Another ShortCircuitReplica object should have been created.
    Assert.assertEquals(2, replicasCreated.get());

    dfs.close();
    cluster.shutdown();
    sockDir.close();
  }
}
