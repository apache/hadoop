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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_MS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLocatedBlocksRefresher {
  private static final Logger LOG = LoggerFactory.getLogger(TestLocatedBlocksRefresher.class);

  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final short REPLICATION_FACTOR = (short) 4;
  private static final String[] RACKS = new String[] {
      "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3" };
  private static final int NUM_DATA_NODES = RACKS.length;

  private final int numOfBlocks = 24;
  private final int fileLength = numOfBlocks * BLOCK_SIZE;
  private final int dfsClientPrefetchSize = fileLength / 2;

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    cluster = null;
    conf = new HdfsConfiguration();

    // disable shortcircuit reading
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    // set replication factor
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_FACTOR);
    // set block size and other sizes
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY,
        dfsClientPrefetchSize);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown(true, true);
    }
  }

  private void setupTest(long refreshInterval) throws IOException {
    conf.setLong(DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_MS_KEY, refreshInterval);

    // this is necessary to ensure no caching between runs
    conf.set("dfs.client.context", UUID.randomUUID().toString());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).racks(RACKS).build();
    cluster.waitActive();
  }

  @Test
  public void testDisabledOnZeroInterval() throws IOException {
    setupTest(0);
    assertNull(cluster.getFileSystem().getClient().getLocatedBlockRefresher());
  }

  @Test
  public void testEnabledOnNonZeroInterval() throws Exception {
    setupTest(1000);
    LocatedBlocksRefresher refresher =
        cluster.getFileSystem().getClient().getLocatedBlockRefresher();
    assertNotNull(refresher);
    assertNoMoreRefreshes(refresher);
  }

  @Test
  public void testRefreshOnDeadNodes() throws Exception {
    setupTest(1000);
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.getClient();
    LocatedBlocksRefresher refresher = client.getLocatedBlockRefresher();

    String fileName = createTestFile(fs);

    try (DFSInputStream fin = client.open(fileName)) {
      LocatedBlocks locatedBlocks = fin.locatedBlocks;
      assertEquals(dfsClientPrefetchSize / BLOCK_SIZE,
          locatedBlocks.locatedBlockCount());

      // should not be tracked yet
      assertFalse(refresher.isInputStreamTracked(fin));

      // track and verify
      refresher.addInputStream(fin);
      assertTrue(refresher.isInputStreamTracked(fin));

      // no refreshes yet, as nothing has happened
      assertNoMoreRefreshes(refresher);
      synchronized (fin.infoLock) {
        assertSame(locatedBlocks, fin.locatedBlocks);
      }

      stopNodeHostingBlocks(fin, NUM_DATA_NODES - 1);

      // read blocks, which should trigger dead node for the one we stopped
      int chunkReadSize = BLOCK_SIZE / 4;
      byte[] readBuffer = new byte[chunkReadSize];
      fin.read(0, readBuffer, 0, readBuffer.length);

      assertEquals(1, fin.getLocalDeadNodes().size());

      // we should get a refresh now
      assertRefreshes(refresher, 1);

      // verify that it actually changed things
      synchronized (fin.infoLock) {
        assertNotSame(locatedBlocks, fin.locatedBlocks);
        assertTrue(fin.getLocalDeadNodes().isEmpty());
      }

      // no more refreshes because everything is happy again
      assertNoMoreRefreshes(refresher);

      // stop another node, and try to trigger a new deadNode
      stopNodeHostingBlocks(fin, NUM_DATA_NODES - 2);
      readBuffer = new byte[chunkReadSize];
      fin.read(0, readBuffer, 0, readBuffer.length);

      // we should refresh again now, and verify
      // may actually be more than 1, since the first dead node
      // may still be listed in the replicas for the bock
      assertTrue(fin.getLocalDeadNodes().size() > 0);

      assertRefreshes(refresher, 1);

      synchronized (fin.infoLock) {
        assertNotSame(locatedBlocks, fin.locatedBlocks);
        assertTrue(fin.getLocalDeadNodes().isEmpty());
      }

      // de-register, and expect no more refreshes below
      refresher.removeInputStream(fin);
    }

    assertNoMoreRefreshes(refresher);
  }

  private void stopNodeHostingBlocks(DFSInputStream fin, int expectedNodes) {
    synchronized (fin.infoLock) {
      int idx = fin.locatedBlocks.findBlock(0);
      for (int i = 0; i < REPLICATION_FACTOR; i++) {
        String deadNodeAddr = fin.locatedBlocks.get(idx).getLocations()[i].getXferAddr();

        DataNodeProperties dataNodeProperties = cluster.stopDataNode(deadNodeAddr);
        if (dataNodeProperties != null) {
          List<DataNode> datanodesPostStoppage = cluster.getDataNodes();
          assertEquals(expectedNodes, datanodesPostStoppage.size());
          return;
        }
      }

      throw new RuntimeException("Could not find a datanode to stop");
    }
  }

  private void assertNoMoreRefreshes(LocatedBlocksRefresher refresher) throws InterruptedException {
    long interval = refresher.getInterval();
    int runCount = refresher.getRunCount();
    int refreshCount = refresher.getRefreshCount();

    LOG.info("Waiting for at least {} runs, from current {}, expecting no refreshes",
        runCount + 3, runCount);
    // wait for it to run 3 times, with some buffer
    awaitWithTimeout(() -> refresher.getRunCount() > runCount + 3, 5 * interval);

    // it should not have refreshed anything, because no DFSInputStreams registered anymore
    assertEquals(refreshCount, refresher.getRefreshCount());
  }

  private void assertRefreshes(LocatedBlocksRefresher refresher, int expectedRefreshes)
      throws InterruptedException {
    int runCount = refresher.getRunCount();
    int refreshCount = refresher.getRefreshCount();
    int expectedRuns = 3;

    if (expectedRefreshes < 0) {
      expectedRefreshes = expectedRuns;
    }

    LOG.info(
        "Waiting for at least {} runs, from current {}. Expecting {} refreshes, from current {}",
        runCount + expectedRuns, runCount, refreshCount + expectedRefreshes, refreshCount
    );

    // wait for it to run 3 times
    awaitWithTimeout(() -> refresher.getRunCount() >= runCount + expectedRuns, 10_000);

    // the values may not be identical due to any refreshes that occurred before we opened
    // the DFSInputStream but the difference should be identical since we are refreshing
    // every time
    assertEquals(expectedRefreshes, refresher.getRefreshCount() - refreshCount);
  }

  private void awaitWithTimeout(Supplier<Boolean> test, long timeoutMillis)
      throws InterruptedException {
    long now = Time.monotonicNow();

    while(!test.get()) {
      if (Time.monotonicNow() - now > timeoutMillis) {
        fail("Timed out waiting for true condition");
        return;
      }

      Thread.sleep(50);
    }
  }

  private String createTestFile(FileSystem fs) throws IOException {
    String fileName = "/located_blocks_" + UUID.randomUUID().toString();
    Path filePath = new Path(fileName);
    try (FSDataOutputStream fout = fs.create(filePath, REPLICATION_FACTOR)) {
      fout.write(new byte[(fileLength)]);
    }
    fs.deleteOnExit(filePath);

    return fileName;
  }
}
