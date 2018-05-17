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
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import com.google.common.base.Supplier;

import java.io.IOException;

public class TestAddStripedBlockInFBR {
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int cellSize = ecPolicy.getCellSize();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final short groupSize = (short) (dataBlocks + parityBlocks);

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    dfs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
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
    dfs.getClient().setErasureCodingPolicy(ecDir.toString(),
        StripedFileTestUtil.getDefaultECPolicy().getName());

    // create several non-EC files and one EC file
    final Path[] repFiles = new Path[groupSize];
    for (int i = 0; i < groupSize; i++) {
      repFiles[i] = new Path(repDir, "f" + i);
      DFSTestUtil.createFile(dfs, repFiles[i], 1L, (short) 3, 0L);
    }
    final Path ecFile = new Path(ecDir, "f");
    DFSTestUtil.createFile(dfs, ecFile,
        cellSize * dataBlocks, (short) 1, 0L);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {

      @Override
      public Boolean get() {
        try {
          // trigger dn's FBR. The FBR will add block-dn mapping.
          cluster.triggerBlockReports();

          // make sure NN has correct block-dn mapping
          BlockInfoStriped blockInfo = (BlockInfoStriped) cluster
              .getNamesystem().getFSDirectory().getINode(ecFile.toString())
              .asFile().getLastBlock();
          NumberReplicas nr = spy.countNodes(blockInfo);

          return nr.excessReplicas() == 0 && nr.liveReplicas() == groupSize;
        } catch (Exception ignored) {
          // Ignore the exception
        }

        return false;
      }
    }, 3000, 60000);
  }
}
