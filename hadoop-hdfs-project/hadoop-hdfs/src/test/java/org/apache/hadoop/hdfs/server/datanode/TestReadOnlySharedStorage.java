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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Test proper {@link BlockManager} replication counting for {@link DatanodeStorage}s
 * with {@link DatanodeStorage.State#READ_ONLY_SHARED READ_ONLY} state.
 * 
 * Uses {@link SimulatedFSDataset} to inject read-only replicas into a DataNode.
 */
public class TestReadOnlySharedStorage {

  public static final Log LOG = LogFactory.getLog(TestReadOnlySharedStorage.class);

  private static final short NUM_DATANODES = 3;
  private static final int RO_NODE_INDEX = 0;
  private static final int BLOCK_SIZE = 1024;
  private static final long seed = 0x1BADF00DL;
  private static final Path PATH = new Path("/" + TestReadOnlySharedStorage.class.getName() + ".dat");
  private static final int RETRIES = 10;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;

  private BlockManager blockManager;

  private DatanodeManager datanodeManager;
  private DatanodeInfo normalDataNode;
  private DatanodeInfo readOnlyDataNode;
  
  private Block block;
  private BlockInfo storedBlock;

  private ExtendedBlock extendedBlock;


  /**
   * Setup a {@link MiniDFSCluster}.
   * Create a block with both {@link State#NORMAL} and {@link State#READ_ONLY_SHARED} replicas.
   */
  @Before
  public void setup() throws IOException, InterruptedException {
    conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
    
    Configuration[] overlays = new Configuration[NUM_DATANODES];
    for (int i = 0; i < overlays.length; i++) {
      overlays[i] = new Configuration();
      if (i == RO_NODE_INDEX) {
        overlays[i].setEnum(SimulatedFSDataset.CONFIG_PROPERTY_STATE, 
            i == RO_NODE_INDEX 
              ? READ_ONLY_SHARED
              : NORMAL);
      }
    }
    
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .dataNodeConfOverlays(overlays)
        .build();
    fs = cluster.getFileSystem();
    blockManager = cluster.getNameNode().getNamesystem().getBlockManager();
    datanodeManager = blockManager.getDatanodeManager();
    client = new DFSClient(new InetSocketAddress("localhost", cluster.getNameNodePort()),
                           cluster.getConfiguration(0));
    
    for (int i = 0; i < NUM_DATANODES; i++) {
      DataNode dataNode = cluster.getDataNodes().get(i);
      validateStorageState(
          BlockManagerTestUtil.getStorageReportsForDatanode(
              datanodeManager.getDatanode(dataNode.getDatanodeId())),
              i == RO_NODE_INDEX 
                ? READ_ONLY_SHARED
                : NORMAL);
    }
    
    // Create a 1 block file
    DFSTestUtil.createFile(fs, PATH, BLOCK_SIZE, BLOCK_SIZE,
                           BLOCK_SIZE, (short) 1, seed);
    
    LocatedBlock locatedBlock = getLocatedBlock();
    extendedBlock = locatedBlock.getBlock();
    block = extendedBlock.getLocalBlock();
    storedBlock = blockManager.getStoredBlock(block);

    assertThat(locatedBlock.getLocations().length, is(1));
    normalDataNode = locatedBlock.getLocations()[0];
    readOnlyDataNode = datanodeManager.getDatanode(cluster.getDataNodes().get(RO_NODE_INDEX).getDatanodeId());
    assertThat(normalDataNode, is(not(readOnlyDataNode)));
    
    validateNumberReplicas(1);
    
    // Inject the block into the datanode with READ_ONLY_SHARED storage 
    cluster.injectBlocks(0, RO_NODE_INDEX, Collections.singleton(block));
    
    // There should now be 2 *locations* for the block
    // Must wait until the NameNode has processed the block report for the injected blocks
    waitForLocations(2);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(PATH, false);
    
    if (cluster != null) {
      fs.close();
      cluster.shutdown();
      cluster = null;
    }
  }  
  
  private void waitForLocations(int locations) throws IOException, InterruptedException {
    for (int tries = 0; tries < RETRIES; )
      try {
        LocatedBlock locatedBlock = getLocatedBlock();
        assertThat(locatedBlock.getLocations().length, is(locations));
        break;
      } catch (AssertionError e) {
        if (++tries < RETRIES) {
          Thread.sleep(1000);
        } else {
          throw e;
        }
      }
  }
  
  private LocatedBlock getLocatedBlock() throws IOException {
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(PATH.toString(), 0, BLOCK_SIZE);
    assertThat(locatedBlocks.getLocatedBlocks().size(), is(1));
    return Iterables.getOnlyElement(locatedBlocks.getLocatedBlocks());
  }
  
  private void validateStorageState(StorageReport[] storageReports, DatanodeStorage.State state) {
    for (StorageReport storageReport : storageReports) {
      DatanodeStorage storage = storageReport.getStorage();
      assertThat(storage.getState(), is(state));
    }
  }
  
  private void validateNumberReplicas(int expectedReplicas) throws IOException {
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.liveReplicas(), is(expectedReplicas));
    assertThat(numberReplicas.excessReplicas(), is(0));
    assertThat(numberReplicas.corruptReplicas(), is(0));
    assertThat(numberReplicas.decommissionedAndDecommissioning(), is(0));
    assertThat(numberReplicas.replicasOnStaleNodes(), is(0));
    
    BlockManagerTestUtil.updateState(blockManager);
    assertThat(blockManager.getLowRedundancyBlocksCount(), is(0L));
    assertThat(blockManager.getExcessBlocksCount(), is(0L));
  }
  
  /**
   * Verify that <tt>READ_ONLY_SHARED</tt> replicas are <i>not</i> counted towards the overall 
   * replication count, but <i>are</i> included as replica locations returned to clients for reads.
   */
  @Test
  public void testReplicaCounting() throws Exception {
    // There should only be 1 *replica* (the READ_ONLY_SHARED doesn't count)
    validateNumberReplicas(1);
    
    fs.setReplication(PATH, (short) 2);

    // There should now be 3 *locations* for the block, and 2 *replicas*
    waitForLocations(3);
    validateNumberReplicas(2);
  }

  /**
   * Verify that the NameNode is able to still use <tt>READ_ONLY_SHARED</tt> replicas even 
   * when the single NORMAL replica is offline (and the effective replication count is 0).
   */
  @Test
  public void testNormalReplicaOffline() throws Exception {
    // Stop the datanode hosting the NORMAL replica
    cluster.stopDataNode(normalDataNode.getXferAddr());
    
    // Force NameNode to detect that the datanode is down
    BlockManagerTestUtil.noticeDeadDatanode(
        cluster.getNameNode(), normalDataNode.getXferAddr());
    
    // The live replica count should now be zero (since the NORMAL replica is offline)
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.liveReplicas(), is(0));
    
    // The block should be reported as under-replicated
    BlockManagerTestUtil.updateState(blockManager);
    assertThat(blockManager.getLowRedundancyBlocksCount(), is(1L));
    
    // The BlockManager should be able to heal the replication count back to 1
    // by triggering an inter-datanode replication from one of the READ_ONLY_SHARED replicas
    BlockManagerTestUtil.computeAllPendingWork(blockManager);
    
    DFSTestUtil.waitForReplication(cluster, extendedBlock, 1, 1, 0);
    
    // There should now be 2 *locations* for the block, and 1 *replica*
    assertThat(getLocatedBlock().getLocations().length, is(2));
    validateNumberReplicas(1);
  }
  
  /**
   * Verify that corrupt <tt>READ_ONLY_SHARED</tt> replicas aren't counted 
   * towards the corrupt replicas total.
   */
  @Test
  public void testReadOnlyReplicaCorrupt() throws Exception {
    // "Corrupt" a READ_ONLY_SHARED replica by reporting it as a bad replica
    client.reportBadBlocks(new LocatedBlock[] { 
        new LocatedBlock(extendedBlock, new DatanodeInfo[] { readOnlyDataNode })
    });

    // There should now be only 1 *location* for the block as the READ_ONLY_SHARED is corrupt
    waitForLocations(1);
    
    // However, the corrupt READ_ONLY_SHARED replica should *not* affect the overall corrupt replicas count
    NumberReplicas numberReplicas = blockManager.countNodes(storedBlock);
    assertThat(numberReplicas.corruptReplicas(), is(0));
  }

}
