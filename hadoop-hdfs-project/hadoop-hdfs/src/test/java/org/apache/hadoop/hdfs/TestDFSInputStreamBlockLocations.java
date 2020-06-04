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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test the caches expiration of the block locations.
 */
@RunWith(Parameterized.class)
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

  @Parameterized.Parameters(name = "{index}: CacheExpirationConfig(Enable {0})")
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][] {
        {Boolean.TRUE},
        {Boolean.FALSE}
    });
  }

  public TestDFSInputStreamBlockLocations(Boolean enableExpiration) {
    enableBlkExpiration = enableExpiration;
  }

  @Before
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

  @After
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

  @Test
  public void testRead() throws Exception {
    final String fileName = "/test_cache_locations";
    filePath = new Path(fileName);
    DFSInputStream fin = null;
    FSDataOutputStream fout = null;
    try {
      // create a file and write for testing
      fout = fs.create(filePath, REPLICATION_FACTOR);
      fout.write(new byte[(fileLength)]);
      // finalize the file by closing the output stream
      fout.close();
      fout = null;
      // get the located blocks
      LocatedBlocks referenceLocatedBlocks =
          dfsClient.getLocatedBlocks(fileName, 0, fileLength);
      assertEquals(numOfBlocks, referenceLocatedBlocks.locatedBlockCount());
      String poolId = dfsCluster.getNamesystem().getBlockPoolId();
      fin = dfsClient.open(fileName);
      // get the located blocks from fin
      LocatedBlocks finLocatedBlocks = fin.locatedBlocks;
      assertEquals(dfsClientPrefetchSize / BLOCK_SIZE,
          finLocatedBlocks.locatedBlockCount());
      final int chunkReadSize = BLOCK_SIZE / 4;
      byte[] readBuffer = new byte[chunkReadSize];
      // read the first block
      DatanodeInfo prevDNInfo = null;
      DatanodeInfo currDNInfo = null;
      int bytesRead = 0;
      int firstBlockMark = BLOCK_SIZE;
      // get the second block locations
      LocatedBlock firstLocatedBlk =
          fin.locatedBlocks.getLocatedBlocks().get(0);
      DatanodeInfo[] firstBlkDNInfos = firstLocatedBlk.getLocations();
      while (fin.getPos() < firstBlockMark) {
        bytesRead = fin.read(readBuffer);
        Assert.assertTrue("Unexpected number of read bytes",
            chunkReadSize >= bytesRead);
        if (currDNInfo == null) {
          currDNInfo = fin.getCurrentDatanode();
          assertNotNull("current FIS datanode is null", currDNInfo);
          continue;
        }
        prevDNInfo = currDNInfo;
        currDNInfo = fin.getCurrentDatanode();
        assertEquals("the DFSInput stream does not read from same node",
            prevDNInfo, currDNInfo);
      }

      assertEquals("InputStream exceeds expected position",
          firstBlockMark, fin.getPos());
      // get the second block locations
      LocatedBlock secondLocatedBlk =
          fin.locatedBlocks.getLocatedBlocks().get(1);
      // get the nodeinfo for that block
      DatanodeInfo[] secondBlkDNInfos = secondLocatedBlk.getLocations();
      DatanodeInfo deadNodeInfo = secondBlkDNInfos[0];
      // stop the datanode in the list of the
      DataNode deadNode = getdataNodeFromHostName(dfsCluster,
          deadNodeInfo.getHostName());
      // Shutdown and wait for datanode to be marked dead
      DatanodeRegistration reg = InternalDataNodeTestUtils.
          getDNRegistrationForBP(dfsCluster.getDataNodes().get(0), poolId);
      DataNodeProperties stoppedDNProps =
          dfsCluster.stopDataNode(deadNodeInfo.getName());

      List<DataNode> datanodesPostStoppage = dfsCluster.getDataNodes();
      assertEquals(NUM_DATA_NODES - 1, datanodesPostStoppage.size());
      // get the located blocks
      LocatedBlocks afterStoppageLocatedBlocks =
          dfsClient.getLocatedBlocks(fileName, 0, fileLength);
      // read second block
      int secondBlockMark =  (int) (1.5 * BLOCK_SIZE);
      boolean firstIteration = true;
      if (this.enableBlkExpiration) {
        // set the time stamps to make sure that we do not refresh locations yet
        fin.setReadTimeStampsForTesting(Time.monotonicNow());
      }
      while (fin.getPos() < secondBlockMark) {
        bytesRead = fin.read(readBuffer);
        assertTrue("dead node used to read at position: " + fin.getPos(),
            fin.deadNodesContain(deadNodeInfo));
        Assert.assertTrue("Unexpected number of read bytes",
            chunkReadSize >= bytesRead);
        prevDNInfo = currDNInfo;
        currDNInfo = fin.getCurrentDatanode();
        assertNotEquals(deadNodeInfo, currDNInfo);
        if (firstIteration) {
          // currDNInfo has to be different unless first block locs is different
          assertFalse("FSInputStream should pick a different DN",
              firstBlkDNInfos[0].equals(deadNodeInfo)
                  && prevDNInfo.equals(currDNInfo));
          firstIteration = false;
        }
      }
      assertEquals("InputStream exceeds expected position",
          secondBlockMark, fin.getPos());
      // restart the dead node with the same port
      assertTrue(dfsCluster.restartDataNode(stoppedDNProps, true));
      dfsCluster.waitActive();
      List<DataNode> datanodesPostRestart = dfsCluster.getDataNodes();
      assertEquals(NUM_DATA_NODES, datanodesPostRestart.size());
      // continue reading from block 2 again. We should read from deadNode
      int thirdBlockMark =  2 * BLOCK_SIZE;
      firstIteration = true;
      while (fin.getPos() < thirdBlockMark) {
        bytesRead = fin.read(readBuffer);
        if (this.enableBlkExpiration) {
          assertEquals("node is removed from deadNodes after 1st iteration",
              firstIteration, fin.deadNodesContain(deadNodeInfo));
        } else {
          assertTrue(fin.deadNodesContain(deadNodeInfo));
        }
        Assert.assertTrue("Unexpected number of read bytes",
            chunkReadSize >= bytesRead);
        prevDNInfo = currDNInfo;
        currDNInfo = fin.getCurrentDatanode();
        if (!this.enableBlkExpiration) {
          assertNotEquals(deadNodeInfo, currDNInfo);
        }
        if (firstIteration) {
          assertEquals(prevDNInfo, currDNInfo);
          firstIteration = false;
          if (this.enableBlkExpiration) {
            // reset the time stamps of located blocks to force cache expiration
            fin.setReadTimeStampsForTesting(
                Time.monotonicNow() - (dfsInputLocationsTimeout + 1));
          }
        }
      }
      assertEquals("InputStream exceeds expected position",
          thirdBlockMark, fin.getPos());
    } finally {
      if (fout != null) {
        fout.close();
      }
      if (fin != null) {
        fin.close();
      }
    }
  }

  private DataNode getdataNodeFromHostName(MiniDFSCluster cluster,
      String hostName) {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getHostName().equals(hostName)) {
        return dn;
      }
    }
    return null;
  }
}