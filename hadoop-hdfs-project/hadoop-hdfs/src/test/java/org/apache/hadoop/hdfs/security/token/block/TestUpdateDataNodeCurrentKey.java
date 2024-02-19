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
package org.apache.hadoop.hdfs.security.token.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Random;

public class TestUpdateDataNodeCurrentKey {
  private static final Random RAN = new Random();
  private static final short REPLICATION = (short)1;
  private MiniDFSCluster cluster = null;
  private Configuration config;
  private  DistributedFileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    config = new Configuration();
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 8);
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY, 10);
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        12);
    config.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        300);
    config.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    config.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);

    cluster = new MiniDFSCluster.Builder(config)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(REPLICATION).build();
    cluster.transitionToActive(0);
    cluster.waitActive();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      fileSystem.close();
      cluster.shutdown();
      cluster = null;
    }
  }

  private void writeBlockToDataNode(DistributedFileSystem fs) throws IOException {
    final Path p = new Path("/test");
    final int size = (1 << 16) + RAN.nextInt(1 << 16);
    final FSDataOutputStream out = fs.create(p, REPLICATION);
    final byte[] bytes = new byte[1024];
    for (int remaining = size; remaining > 0; ) {
      RAN.nextBytes(bytes);
      final int len = bytes.length < remaining ? bytes.length : remaining;
      out.write(bytes, 0, len);
      out.hflush();
      remaining -= len;
    }
    out.close();
  }

  @Test
  public void TestUpdateDatanodeCurrentKeyWithHATopology()
      throws IOException, InterruptedException {
    //Step1: create a file, write some data.
    fileSystem = cluster.getFileSystem(0);
    writeBlockToDataNode(fileSystem);
    final DataNode oldDataNode = cluster.getDataNodes().get(0);

    //Step2: add a new datanode for copy block
    cluster.startDataNodes(config, 1, true, null, null);
    final DataNode newDataNode = cluster.getDataNodes().get(1);
    final String bpid = cluster.getNamesystem(0).getBlockPoolId();

    final DatanodeInfo[] dataNodeInfos = cluster.getNameNodeRpc(0).
        getDatanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    Assert.assertEquals(2, dataNodeInfos.length);

    int i = 0;
    DatanodeInfo oldNodeInfo = null;
    DatanodeInfo newNodeInfo = null;
    for (DatanodeRegistration dnReg = newDataNode.getDNRegistrationForBP(bpid);
         i < dataNodeInfos.length && !dataNodeInfos[i].equals(dnReg); i++) {
      Assert.assertTrue(i < dataNodeInfos.length);
      oldNodeInfo = dataNodeInfos[i];
      newNodeInfo = dataNodeInfos[1 - i];
    }

    final Collection<ReplicaInfo> replicas = FsDatasetTestUtil.getReplicas(
        oldDataNode.getFSDataset(), bpid);
    Assert.assertTrue(replicas.size() > 0);
    final ReplicaInfo r = replicas.iterator().next();
    final ExtendedBlock b = new ExtendedBlock(bpid, r.getBlockId(),
        r.getVisibleLength(), r.getGenerationStamp());

    //Step3: simulate new datanode losing contact to sbn
    newDataNode.scheduleAllBlockReport(0);
    newDataNode.setHeartbeatsDisabledForTests(true);
    newDataNode.setIBRDisabledForTest(true);
    newDataNode.setCacheReportsDisabledForTest(true);
    Thread.sleep(3000);
    cluster.restartNameNode(1, false);

    //Step4: trigger old datanode heartbeatToStandby to update block token
    final NameNode standbyNameNode = cluster.getNameNode(1);
    final BlockManager sbm = standbyNameNode.getNamesystem().getBlockManager();
    while (sbm.getDatanodeManager().getDatanodes().size() < 1) {
      Thread.sleep(100);
    }
    sbm.getBlockTokenSecretManager().updateKeys();
    DatanodeDescriptor datanode = sbm.getDatanodeManager().getDatanodeMap()
        .entrySet().iterator().next().getValue();
    datanode.setNeedKeyUpdate(true);
    Thread.sleep(4000);

    //Step4: trigger old datanode to transfer block to new block
    Token<BlockTokenIdentifier> token = oldDataNode.getBlockPoolTokenSecretManager().
        generateToken(b, EnumSet.of(BlockTokenIdentifier.AccessMode.COPY),
            new StorageType[]{StorageType.DEFAULT}, new String[0]);
    DFSTestUtil.transferFinalizedBlock(b, token, DFSClientAdapter.getDFSClient(fileSystem),
        oldNodeInfo, newNodeInfo);
    Thread.sleep(500);

    Assert.assertTrue(newDataNode.getMetrics().getInvalidTokenCount() == 0);
    Assert.assertTrue(newDataNode.getMetrics().getBlocksWrittenCount() == 1);
  }
}
