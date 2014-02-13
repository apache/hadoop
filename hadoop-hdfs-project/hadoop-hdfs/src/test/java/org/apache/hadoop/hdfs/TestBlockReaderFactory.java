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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.ShortCircuitCache;
import org.apache.hadoop.hdfs.client.ShortCircuitReplicaInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;

public class TestBlockReaderFactory {
  static final Log LOG = LogFactory.getLog(TestBlockReaderFactory.class);

  @Before
  public void init() {
    DomainSocket.disableBindPathValidation();
  }

  @After
  public void cleanup() {
    DFSInputStream.tcpReadsDisabledForTesting = false;
    BlockReaderFactory.createShortCircuitReplicaInfoCallback = null;
  }

  private static Configuration createShortCircuitConf(String testName,
      TemporarySocketDirectory sockDir) {
    Configuration conf = new Configuration();
    conf.set(DFS_CLIENT_CONTEXT, testName);
    conf.setLong(DFS_BLOCK_SIZE_KEY, 4096);
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        testName + "._PORT").getAbsolutePath());
    conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
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
    clientConf.setBoolean(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
    Configuration serverConf = new Configuration(clientConf);
    serverConf.setBoolean(DFS_CLIENT_READ_SHORTCIRCUIT_KEY, false);

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
  @Test(timeout=60000)
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
            blockReader = BlockReaderTestUtil.
                getBlockReader(cluster, lblock, 0, TEST_FILE_LEN);
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
            blockReader = BlockReaderTestUtil.
                getBlockReader(cluster, lblock, 0, TEST_FILE_LEN);
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
}
