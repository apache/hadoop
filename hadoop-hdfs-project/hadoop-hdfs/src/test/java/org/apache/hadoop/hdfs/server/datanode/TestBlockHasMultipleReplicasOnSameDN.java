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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * This test verifies NameNode behavior when it gets unexpected block reports
 * from DataNodes. The same block is reported by two different storages on
 * the same DataNode. Excess replicas on the same DN should be ignored by the NN.
 */
public class TestBlockHasMultipleReplicasOnSameDN {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestBlockHasMultipleReplicasOnSameDN.class);

  private static final short NUM_DATANODES = 2;
  private static final int BLOCK_SIZE = 1024;
  private static final long NUM_BLOCKS = 5;
  private static final long seed = 0x1BADF00DL;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;
  private String bpid;

  @Before
  public void startUpCluster() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    bpid = cluster.getNamesystem().getBlockPoolId();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      fs.close();
      cluster.shutdown();
      cluster = null;
    }
  }

  private String makeFileName(String prefix) {
    return "/" + prefix + ".dat";
  }

  /**
   * Verify NameNode behavior when a given DN reports multiple replicas
   * of a given block.
   */
  @Test
  public void testBlockHasMultipleReplicasOnSameDN() throws IOException {
    String filename = makeFileName(GenericTestUtils.getMethodName());
    Path filePath = new Path(filename);

    // Write out a file with a few blocks.
    DFSTestUtil.createFile(fs, filePath, BLOCK_SIZE, BLOCK_SIZE * NUM_BLOCKS,
                           BLOCK_SIZE, NUM_DATANODES, seed);

    // Get the block list for the file with the block locations.
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(
        filePath.toString(), 0, BLOCK_SIZE * NUM_BLOCKS);

    // Generate a fake block report from one of the DataNodes, such
    // that it reports one copy of each block on either storage.
    DataNode dn = cluster.getDataNodes().get(0);
    DatanodeRegistration dnReg = dn.getDNRegistrationForBP(bpid);
    StorageBlockReport reports[] =
        new StorageBlockReport[cluster.getStoragesPerDatanode()];

    ArrayList<ReplicaInfo> blocks = new ArrayList<>();

    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      Block localBlock = locatedBlock.getBlock().getLocalBlock();
      blocks.add(new FinalizedReplica(localBlock, null, null));
    }
    Collections.sort(blocks);

    try (FsDatasetSpi.FsVolumeReferences volumes =
      dn.getFSDataset().getFsVolumeReferences()) {
      BlockListAsLongs bll = BlockListAsLongs.encode(blocks);
      for (int i = 0; i < cluster.getStoragesPerDatanode(); ++i) {
        DatanodeStorage dns = new DatanodeStorage(volumes.get(i).getStorageID());
        reports[i] = new StorageBlockReport(dns, bll);
      }
    }

    // Should not assert!
    cluster.getNameNodeRpc().blockReport(dnReg, bpid, reports,
        new BlockReportContext(1, 0, System.nanoTime(), 0L, true));

    // Get the block locations once again.
    locatedBlocks = client.getLocatedBlocks(filename, 0, BLOCK_SIZE * NUM_BLOCKS);

    // Make sure that each block has two replicas, one on each DataNode.
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      DatanodeInfo[] locations = locatedBlock.getLocations();
      assertThat(locations.length, is((int) NUM_DATANODES));
      assertThat(locations[0].getDatanodeUuid(), not(locations[1].getDatanodeUuid()));
    }
  }
}
