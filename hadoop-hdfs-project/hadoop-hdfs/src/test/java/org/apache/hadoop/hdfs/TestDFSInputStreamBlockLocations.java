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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the caches expiration of the block locations.
 */
public class TestDFSInputStreamBlockLocations {
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final String[] RACKS = new String[] {
      "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3" };
  private static final int NUM_DATA_NODES = RACKS.length;
  private static final short REPLICATION_FACTOR = (short) 4;
  private final int staleInterval = 8000;
  private final int numOfBlocks = 24;
  private final int fileLength = numOfBlocks * BLOCK_SIZE;
  private final int dfsClientPrefetchSize = fileLength / 2;
  // locatedBlocks expiration set to 1 hour
  private final long dfsInputLocationsTimeout = 60 * 60 * 1000L;

  private HdfsConfiguration conf;
  private MiniDFSCluster dfsCluster;
  private DFSClient dfsClient;
  private DistributedFileSystem fs;
  private Path filePath;
  private boolean enableBlkExpiration;

  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        {Boolean.TRUE},
        {Boolean.FALSE}
    });
  }

  public void initTestDFSInputStreamBlockLocations(Boolean pEnableExpiration)
      throws IOException {
    enableBlkExpiration = pEnableExpiration;
    setup();
  }

  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY, true);
    // set the heartbeat intervals and stale considerations
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        staleInterval);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        staleInterval / 2);
    // disable shortcircuit reading
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    // set replication factor
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION_FACTOR);
    // set block size and other sizes
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY,
        dfsClientPrefetchSize);
    if (enableBlkExpiration) {
      // set the refresh locations for every dfsInputLocationsTimeout
      conf.setLong(
          HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_MS_KEY,
          dfsInputLocationsTimeout);
    }
    // start the cluster and create a DFSClient
    dfsCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATA_NODES).racks(RACKS).build();
    dfsCluster.waitActive();
    assertEquals(NUM_DATA_NODES, dfsCluster.getDataNodes().size());
    InetSocketAddress addr = new InetSocketAddress("localhost",
        dfsCluster.getNameNodePort());
    dfsClient = new DFSClient(addr, conf);
    fs = dfsCluster.getFileSystem();
  }

  @AfterEach
  public void teardown() throws IOException {
    if (dfsClient != null) {
      dfsClient.close();
      dfsClient = null;
    }
    if (fs != null) {
      fs.deleteOnExit(filePath);
      fs.close();
      fs = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @MethodSource("getTestParameters")
  @ParameterizedTest
  public void testRefreshBlockLocations(Boolean pEnableExpiration) throws IOException {
    initTestDFSInputStreamBlockLocations(pEnableExpiration);
    final String fileName = "/test_cache_locations";
    filePath = createFile(fileName);

    try (DFSInputStream fin = dfsClient.open(fileName)) {
      LocatedBlocks existing = fin.locatedBlocks;
      long lastRefreshedAt = fin.getLastRefreshedBlocksAtForTesting();

      assertFalse(fin.refreshBlockLocations(null),
          "should not have attempted refresh");
      assertEquals(lastRefreshedAt, fin.getLastRefreshedBlocksAtForTesting(),
          "should not have updated lastRefreshedAt");
      assertSame(existing, fin.locatedBlocks,
          "should not have modified locatedBlocks");

      // fake a dead node to force refresh
      // refreshBlockLocations should return true, indicating we attempted a refresh
      // nothing should be changed, because locations have not changed
      fin.addToLocalDeadNodes(dfsClient.datanodeReport(DatanodeReportType.LIVE)[0]);
      assertTrue(fin.refreshBlockLocations(null),
          "should have attempted refresh");
      verifyChanged(fin, existing, lastRefreshedAt);

      // reset
      lastRefreshedAt = fin.getLastRefreshedBlocksAtForTesting();
      existing = fin.locatedBlocks;

      // It's hard to test explicitly for non-local nodes, but we can fake it
      // because we also treat unresolved as non-local. Pass in a cache where all the datanodes
      // are unresolved hosts.
      Map<String, InetSocketAddress> mockAddressCache = new HashMap<>();
      InetSocketAddress unresolved = InetSocketAddress.createUnresolved("www.google.com", 80);
      for (DataNode dataNode : dfsCluster.getDataNodes()) {
        mockAddressCache.put(dataNode.getDatanodeUuid(), unresolved);
      }

      assertTrue(fin.refreshBlockLocations(mockAddressCache),
          "should have attempted refresh");
      verifyChanged(fin, existing, lastRefreshedAt);
    }
  }

  private void verifyChanged(DFSInputStream fin, LocatedBlocks existing, long lastRefreshedAt) {
    assertTrue(fin.getLastRefreshedBlocksAtForTesting() > lastRefreshedAt,
        "lastRefreshedAt should have incremented");
    assertNotSame(existing, fin.locatedBlocks,
        "located blocks should have changed");
    assertTrue(fin.getLocalDeadNodes().isEmpty(),
        "deadNodes should be empty");
  }

  @MethodSource("getTestParameters")
  @ParameterizedTest
  public void testDeferredRegistrationStatefulRead(Boolean pEnableExpiration) throws IOException {
    initTestDFSInputStreamBlockLocations(pEnableExpiration);
    testWithRegistrationMethod(DFSInputStream::read);
  }

  @MethodSource("getTestParameters")
  @ParameterizedTest
  public void testDeferredRegistrationPositionalRead(Boolean pEnableExpiration) throws IOException {
    initTestDFSInputStreamBlockLocations(pEnableExpiration);
    testWithRegistrationMethod(fin -> fin.readFully(0, new byte[1]));
  }

  @MethodSource("getTestParameters")
  @ParameterizedTest
  public void testDeferredRegistrationGetAllBlocks(Boolean pEnableExpiration) throws IOException {
    initTestDFSInputStreamBlockLocations(pEnableExpiration);
    testWithRegistrationMethod(DFSInputStream::getAllBlocks);
  }

  /**
   * If the ignoreList contains all datanodes, the ignoredList should be cleared to take advantage
   * of retries built into chooseDataNode. This is needed for hedged reads
   * @throws IOException
   */
  @MethodSource("getTestParameters")
  @ParameterizedTest
  public void testClearIgnoreListChooseDataNode(Boolean pEnableExpiration) throws IOException {
    initTestDFSInputStreamBlockLocations(pEnableExpiration);
    final String fileName = "/test_cache_locations";
    filePath = createFile(fileName);

    try (DFSInputStream fin = dfsClient.open(fileName)) {
      LocatedBlocks existing = fin.locatedBlocks;
      LocatedBlock block = existing.getLastLocatedBlock();
      ArrayList<DatanodeInfo> ignoreList = new ArrayList<>(Arrays.asList(block.getLocations()));
      assertNotNull(fin.chooseDataNode(block, ignoreList, true));
      assertEquals(0, ignoreList.size());
    }
  }

  @FunctionalInterface
  interface ThrowingConsumer {
    void accept(DFSInputStream fin) throws IOException;
  }

  private void testWithRegistrationMethod(ThrowingConsumer registrationMethod) throws IOException {
    final String fileName = "/test_cache_locations";
    filePath = createFile(fileName);

    DFSInputStream fin = null;
    try {
      fin = dfsClient.open(fileName);
      assertFalse(dfsClient.getLocatedBlockRefresher().isInputStreamTracked(fin),
          "should not be tracking input stream on open");

      // still not registered because it hasn't been an hour by the time we call this
      registrationMethod.accept(fin);
      assertFalse(dfsClient.getLocatedBlockRefresher().isInputStreamTracked(fin),
          "should not be tracking input stream after first read");

      // artificially make it have been an hour
      fin.setLastRefreshedBlocksAtForTesting(Time.monotonicNow() - (dfsInputLocationsTimeout + 1));
      registrationMethod.accept(fin);
      assertEquals(enableBlkExpiration,
          dfsClient.getLocatedBlockRefresher().isInputStreamTracked(fin),
          "SHOULD be tracking input stream on read after interval, only if enabled");
    } finally {
      if (fin != null) {
        fin.close();
        assertFalse(dfsClient.getLocatedBlockRefresher().isInputStreamTracked(fin));
      }
      fs.delete(filePath, true);
    }
  }

  private Path createFile(String fileName) throws IOException {
    Path path = new Path(fileName);
    try (FSDataOutputStream fout = fs.create(path, REPLICATION_FACTOR)) {
      fout.write(new byte[(fileLength)]);
    }
    return path;
  }
}
