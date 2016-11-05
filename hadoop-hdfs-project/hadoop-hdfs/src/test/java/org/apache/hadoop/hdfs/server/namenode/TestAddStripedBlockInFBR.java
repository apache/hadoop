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
package org.apache.hadoop.hdfs.server.namenode;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_PARITY_BLOCKS;

public class TestAddStripedBlockInFBR {
  private final short GROUP_SIZE = (short) (NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS);

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(GROUP_SIZE).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testAddBlockInFullBlockReport() throws Exception {
    BlockManager spy = Mockito.spy(cluster.getNamesystem().getBlockManager());
    // let NN ignore one DataNode's IBR
    final DataNode dn = cluster.getDataNodes().get(0);
    final DatanodeID datanodeID = dn.getDatanodeId();
    Mockito.doNothing().when(spy)
        .processIncrementalBlockReport(Mockito.eq(datanodeID), Mockito.any());
    Whitebox.setInternalState(cluster.getNamesystem(), "blockManager", spy);

    final Path ecDir = new Path("/ec");
    final Path repDir = new Path("/rep");
    dfs.mkdirs(ecDir);
    dfs.mkdirs(repDir);
    dfs.getClient().setErasureCodingPolicy(ecDir.toString(), null);

    // create several non-EC files and one EC file
    final Path[] repFiles = new Path[GROUP_SIZE];
    for (int i = 0; i < GROUP_SIZE; i++) {
      repFiles[i] = new Path(repDir, "f" + i);
      DFSTestUtil.createFile(dfs, repFiles[i], 1L, (short) 3, 0L);
    }
    final Path ecFile = new Path(ecDir, "f");
    DFSTestUtil.createFile(dfs, ecFile,
        BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS, (short) 1, 0L);

    // trigger dn's FBR. The FBR will add block-dn mapping.
    DataNodeTestUtils.triggerBlockReport(dn);

    // make sure NN has correct block-dn mapping
    BlockInfoStriped blockInfo = (BlockInfoStriped) cluster.getNamesystem()
        .getFSDirectory().getINode(ecFile.toString()).asFile().getLastBlock();
    NumberReplicas nr = spy.countNodes(blockInfo);
    Assert.assertEquals(GROUP_SIZE, nr.liveReplicas());
    Assert.assertEquals(0, nr.excessReplicas());
  }
}
