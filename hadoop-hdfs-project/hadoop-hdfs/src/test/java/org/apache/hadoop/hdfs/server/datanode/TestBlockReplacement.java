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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;

/**
 * This class tests if block replacement request to data nodes work correctly.
 */
public class TestBlockReplacement {
  private static final Logger LOG = LoggerFactory.getLogger(
  "org.apache.hadoop.hdfs.TestBlockReplacement");

  MiniDFSCluster cluster;
  @Test
  public void testThrottler() throws IOException {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    long bandwidthPerSec = 1024*1024L;
    final long TOTAL_BYTES =6*bandwidthPerSec; 
    long bytesToSend = TOTAL_BYTES; 
    long start = Time.monotonicNow();
    DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);
    long bytesSent = 1024*512L; // 0.5MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    bytesSent = 1024*768L; // 0.75MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {}
    throttler.throttle(bytesToSend);
    long end = Time.monotonicNow();
    assertTrue(TOTAL_BYTES * 1000 / (end - start) <= bandwidthPerSec);
  }
  
  @Test
  public void testBlockReplacement() throws Exception {
    final Configuration CONF = new HdfsConfiguration();
    final String[] INITIAL_RACKS = {"/RACK0", "/RACK1", "/RACK2"};
    final String[] NEW_RACKS = {"/RACK2"};

    final short REPLICATION_FACTOR = (short)3;
    final int DEFAULT_BLOCK_SIZE = 1024;
    final Random r = new Random();
    
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    CONF.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE/2);
    CONF.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,500);
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(REPLICATION_FACTOR)
                                              .racks(INITIAL_RACKS).build();

    try {
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/tmp.txt");
      
      // create a file with one block
      DFSTestUtil.createFile(fs, fileName,
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, r.nextLong());
      DFSTestUtil.waitReplication(fs,fileName, REPLICATION_FACTOR);
      
      // get all datanodes
      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, CONF);
      List<LocatedBlock> locatedBlocks = client.getNamenode().
        getBlockLocations("/tmp.txt", 0, DEFAULT_BLOCK_SIZE).getLocatedBlocks();
      assertEquals(1, locatedBlocks.size());
      LocatedBlock block = locatedBlocks.get(0);
      DatanodeInfo[]  oldNodes = block.getLocations();
      assertEquals(oldNodes.length, 3);
      ExtendedBlock b = block.getBlock();
      
      // add a fourth datanode to the cluster
      cluster.startDataNodes(CONF, 1, true, null, NEW_RACKS);
      cluster.waitActive();
      
      DatanodeInfo[] datanodes = client.datanodeReport(DatanodeReportType.ALL);

      // find out the new node
      DatanodeInfo newNode=null;
      for(DatanodeInfo node:datanodes) {
        Boolean isNewNode = true;
        for(DatanodeInfo oldNode:oldNodes) {
          if(node.equals(oldNode)) {
            isNewNode = false;
            break;
          }
        }
        if(isNewNode) {
          newNode = node;
          break;
        }
      }
      
      assertTrue(newNode!=null);
      DatanodeInfo source=null;
      ArrayList<DatanodeInfo> proxies = new ArrayList<DatanodeInfo>(2);
      for(DatanodeInfo node:datanodes) {
        if(node != newNode) {
          if( node.getNetworkLocation().equals(newNode.getNetworkLocation())) {
            source = node;
          } else {
            proxies.add( node );
          }
        }
      }
      //current state: the newNode is on RACK2, and "source" is the other dn on RACK2.
      //the two datanodes on RACK0 and RACK1 are in "proxies".
      //"source" and both "proxies" all contain the block, while newNode doesn't yet.
      assertTrue(source!=null && proxies.size()==2);
      
      // start to replace the block
      // case 1: proxySource does not contain the block
      LOG.info("Testcase 1: Proxy " + newNode
           + " does not contain the block " + b);
      assertFalse(replaceBlock(b, source, newNode, proxies.get(0)));
      // case 2: destination already contains the block
      LOG.info("Testcase 2: Destination " + proxies.get(1)
          + " contains the block " + b);
      assertFalse(replaceBlock(b, source, proxies.get(0), proxies.get(1)));
      // case 3: correct case
      LOG.info("Testcase 3: Source=" + source + " Proxy=" + 
          proxies.get(0) + " Destination=" + newNode );
      assertTrue(replaceBlock(b, source, proxies.get(0), newNode));
      // after cluster has time to resolve the over-replication,
      // block locations should contain two proxies and newNode
      // but not source
      checkBlocks(new DatanodeInfo[]{newNode, proxies.get(0), proxies.get(1)},
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
      // case 4: proxies.get(0) is not a valid del hint
      // expect either source or newNode replica to be deleted instead
      LOG.info("Testcase 4: invalid del hint " + proxies.get(0) );
      assertTrue(replaceBlock(b, proxies.get(0), proxies.get(1), source));
      // after cluster has time to resolve the over-replication,
      // block locations should contain any 3 of the blocks, since after the
      // deletion the number of racks is still >=2 for sure.
      // See HDFS-9314 for details, espacially the comment on 18/Nov/15 14:09.
      checkBlocks(new DatanodeInfo[]{},
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to verify that the copying of pinned block to a different destination
   * datanode will throw IOException with error code Status.ERROR_BLOCK_PINNED.
   *
   */
  @Test(timeout = 90000)
  public void testBlockReplacementWithPinnedBlocks() throws Exception {
    final Configuration conf = new HdfsConfiguration();

    // create only one datanode in the cluster with DISK and ARCHIVE storage
    // types.
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();

    try {
      cluster.waitActive();

      final DistributedFileSystem dfs = cluster.getFileSystem();
      String fileName = "/testBlockReplacementWithPinnedBlocks/file";
      final Path file = new Path(fileName);
      DFSTestUtil.createFile(dfs, file, 1024, (short) 1, 1024);

      LocatedBlock lb = dfs.getClient().getLocatedBlocks(fileName, 0).get(0);
      DatanodeInfo[] oldNodes = lb.getLocations();
      assertEquals("Wrong block locations", oldNodes.length, 1);
      DatanodeInfo source = oldNodes[0];
      ExtendedBlock b = lb.getBlock();

      DatanodeInfo[] datanodes = dfs.getDataNodeStats();
      DatanodeInfo destin = null;
      for (DatanodeInfo datanodeInfo : datanodes) {
        // choose different destination node
        if (!oldNodes[0].equals(datanodeInfo)) {
          destin = datanodeInfo;
          break;
        }
      }

      assertNotNull("Failed to choose destination datanode!", destin);

      assertFalse("Source and destin datanode should be different",
          source.equals(destin));

      // Mock FsDatasetSpi#getPinning to show that the block is pinned.
      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        DataNode dn = cluster.getDataNodes().get(i);
        LOG.info("Simulate block pinning in datanode " + dn);
        InternalDataNodeTestUtils.mockDatanodeBlkPinning(dn, true);
      }

      // Block movement to a different datanode should fail as the block is
      // pinned.
      assertTrue("Status code mismatches!", replaceBlock(b, source, source,
          destin, StorageType.ARCHIVE, Status.ERROR_BLOCK_PINNED));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockMoveAcrossStorageInSameNode() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // create only one datanode in the cluster to verify movement within
    // datanode.
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).storageTypes(
            new StorageType[] { StorageType.DISK, StorageType.ARCHIVE })
            .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      final Path file = new Path("/testBlockMoveAcrossStorageInSameNode/file");
      DFSTestUtil.createFile(dfs, file, 1024, (short) 1, 1024);
      LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(file.toString(), 0);
      // get the current 
      LocatedBlock locatedBlock = locatedBlocks.get(0);
      ExtendedBlock block = locatedBlock.getBlock();
      DatanodeInfo[] locations = locatedBlock.getLocations();
      assertEquals(1, locations.length);
      StorageType[] storageTypes = locatedBlock.getStorageTypes();
      // current block should be written to DISK
      assertTrue(storageTypes[0] == StorageType.DISK);
      
      DatanodeInfo source = locations[0];
      // move block to ARCHIVE by using same DataNodeInfo for source, proxy and
      // destination so that movement happens within datanode 
      assertTrue(replaceBlock(block, source, source, source,
          StorageType.ARCHIVE, Status.SUCCESS));
      
      // wait till namenode notified
      Thread.sleep(3000);
      locatedBlocks = dfs.getClient().getLocatedBlocks(file.toString(), 0);
      // get the current 
      locatedBlock = locatedBlocks.get(0);
      assertEquals("Storage should be only one", 1,
          locatedBlock.getLocations().length);
      assertTrue("Block should be moved to ARCHIVE", locatedBlock
          .getStorageTypes()[0] == StorageType.ARCHIVE);
    } finally {
      cluster.shutdown();
    }
  }

  /* check if file's blocks have expected number of replicas,
   * and exist at all of includeNodes
   */
  private void checkBlocks(DatanodeInfo[] includeNodes, String fileName, 
      long fileLen, short replFactor, DFSClient client) 
      throws IOException, TimeoutException {
    boolean notDone;
    final long TIMEOUT = 20000L;
    long starttime = Time.monotonicNow();
    long failtime = starttime + TIMEOUT;
    do {
      try {
        Thread.sleep(100);
      } catch(InterruptedException e) {
      }
      List<LocatedBlock> blocks = client.getNamenode().
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();
      assertEquals(1, blocks.size());
      DatanodeInfo[] nodes = blocks.get(0).getLocations();
      notDone = (nodes.length != replFactor);
      if (notDone) {
        LOG.info("Expected replication factor is " + replFactor +
            " but the real replication factor is " + nodes.length );
      } else {
        List<DatanodeInfo> nodeLocations = Arrays.asList(nodes);
        for (DatanodeInfo node : includeNodes) {
          if (!nodeLocations.contains(node) ) {
            notDone=true; 
            LOG.info("Block is not located at " + node );
            break;
          }
        }
      }
      if (Time.monotonicNow() > failtime) {
        String expectedNodesList = "";
        String currentNodesList = "";
        for (DatanodeInfo dn : includeNodes) 
          expectedNodesList += dn + ", ";
        for (DatanodeInfo dn : nodes) 
          currentNodesList += dn + ", ";
        LOG.info("Expected replica nodes are: " + expectedNodesList);
        LOG.info("Current actual replica nodes are: " + currentNodesList);
        throw new TimeoutException(
            "Did not achieve expected replication to expected nodes "
            + "after more than " + TIMEOUT + " msec.  See logs for details.");
      }
    } while(notDone);
    LOG.info("Achieved expected replication values in "
        + (Time.now() - starttime) + " msec.");
  }

  /* Copy a block from sourceProxy to destination. If the block becomes
   * over-replicated, preferably remove it from source.
   * 
   * Return true if a block is successfully copied; otherwise false.
   */
  private boolean replaceBlock( ExtendedBlock block, DatanodeInfo source,
      DatanodeInfo sourceProxy, DatanodeInfo destination) throws IOException {
    return DFSTestUtil.replaceBlock(block, source, sourceProxy, destination,
        StorageType.DEFAULT, Status.SUCCESS);
  }

  /*
   * Replace block
   */
  private boolean replaceBlock(
      ExtendedBlock block,
      DatanodeInfo source,
      DatanodeInfo sourceProxy,
      DatanodeInfo destination,
      StorageType targetStorageType,
      Status opStatus) throws IOException, SocketException {
    return DFSTestUtil.replaceBlock(block, source, sourceProxy, destination,
        targetStorageType, opStatus);
  }

  /**
   * Standby namenode doesn't queue Delete block request when the add block
   * request is in the edit log which are yet to be read.
   * @throws Exception
   */
  @Test
  public void testDeletedBlockWhenAddBlockIsInEdit() throws Exception {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
       .nnTopology(MiniDFSNNTopology.simpleHATopology())
       .numDataNodes(1).build();
    DFSClient client = null;
    try {
      cluster.waitActive();
      assertEquals("Number of namenodes is not 2", 2,
          cluster.getNumNameNodes());
      // Transitioning the namenode 0 to active.
      cluster.transitionToActive(0);
      assertTrue("Namenode 0 should be in active state",
          cluster.getNameNode(0).isActiveState());
      assertTrue("Namenode 1 should be in standby state",
          cluster.getNameNode(1).isStandbyState());

      // Trigger heartbeat to mark DatanodeStorageInfo#heartbeatedSinceFailover
      // to true.
      DataNodeTestUtils.triggerHeartbeat(cluster.getDataNodes().get(0));
      FileSystem fs = cluster.getFileSystem(0);

      // Trigger blockReport to mark DatanodeStorageInfo#blockContentsStale
      // to false.
      cluster.getDataNodes().get(0).triggerBlockReport(
          new BlockReportOptions.Factory().setIncremental(false).build());

      Path fileName = new Path("/tmp.txt");
      // create a file with one block
      DFSTestUtil.createFile(fs, fileName, 10L, (short)1, 1234L);
      DFSTestUtil.waitReplication(fs,fileName, (short)1);

      client = new DFSClient(cluster.getFileSystem(0).getUri(), conf);
      List<LocatedBlock> locatedBlocks = client.getNamenode().
          getBlockLocations("/tmp.txt", 0, 10L).getLocatedBlocks();
      assertTrue(locatedBlocks.size() == 1);
      assertTrue(locatedBlocks.get(0).getLocations().length == 1);

      // add a second datanode to the cluster
      cluster.startDataNodes(conf, 1, true, null, null, null, null);
      assertEquals("Number of datanodes should be 2", 2,
          cluster.getDataNodes().size());

      DataNode dn0 = cluster.getDataNodes().get(0);
      DataNode dn1 = cluster.getDataNodes().get(1);
      String activeNNBPId = cluster.getNamesystem(0).getBlockPoolId();
      DatanodeDescriptor sourceDnDesc = NameNodeAdapter.getDatanode(
          cluster.getNamesystem(0), dn0.getDNRegistrationForBP(activeNNBPId));
      DatanodeDescriptor destDnDesc = NameNodeAdapter.getDatanode(
          cluster.getNamesystem(0), dn1.getDNRegistrationForBP(activeNNBPId));

      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, fileName);

      LOG.info("replaceBlock:  " + replaceBlock(block,
          (DatanodeInfo)sourceDnDesc, (DatanodeInfo)sourceDnDesc,
          (DatanodeInfo)destDnDesc));
      // Waiting for the FsDatasetAsyncDsikService to delete the block
      for (int tries = 0; tries < 20; tries++) {
        Thread.sleep(1000);
        // Triggering the deletion block report to report the deleted block
        // to namnemode
        DataNodeTestUtils.triggerDeletionReport(cluster.getDataNodes().get(0));
        locatedBlocks =
            client.getNamenode().getBlockLocations("/tmp.txt", 0, 10L)
                .getLocatedBlocks();
        // If block was deleted and only on 1 datanode then break out
        if (locatedBlocks.get(0).getLocations().length == 1) {
          break;
        }
      }

      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      assertTrue("Namenode 1 should be in active state",
         cluster.getNameNode(1).isActiveState());
      assertTrue("Namenode 0 should be in standby state",
         cluster.getNameNode(0).isStandbyState());
      client.close();

      // Opening a new client for new active  namenode
      client = new DFSClient(cluster.getFileSystem(1).getUri(), conf);
      List<LocatedBlock> locatedBlocks1 = client.getNamenode()
          .getBlockLocations("/tmp.txt", 0, 10L).getLocatedBlocks();

      assertEquals(1, locatedBlocks1.size());
      assertEquals("The block should be only on 1 datanode ", 1,
          locatedBlocks1.get(0).getLocations().length);
    } finally {
      IOUtils.cleanupWithLogger(null, client);
      cluster.shutdown();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    (new TestBlockReplacement()).testBlockReplacement();
  }

}
