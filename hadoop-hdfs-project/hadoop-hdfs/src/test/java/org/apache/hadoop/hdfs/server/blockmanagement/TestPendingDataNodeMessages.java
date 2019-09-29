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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.junit.Test;

import com.google.common.base.Joiner;


public class TestPendingDataNodeMessages {
  final PendingDataNodeMessages msgs = new PendingDataNodeMessages();
  
  private final Block block1Gs1 = new Block(1, 0, 1);
  private final Block block1Gs2 = new Block(1, 0, 2);
  private final Block block1Gs2DifferentInstance =
    new Block(1, 0, 2);
  private final Block block2Gs1 = new Block(2, 0, 1);

  @Test
  public void testQueues() {
    DatanodeDescriptor fakeDN = DFSTestUtil.getLocalDatanodeDescriptor();
    DatanodeStorage storage = new DatanodeStorage("STORAGE_ID");
    DatanodeStorageInfo storageInfo = new DatanodeStorageInfo(fakeDN, storage);
    msgs.enqueueReportedBlock(storageInfo, block1Gs1, ReplicaState.FINALIZED);
    msgs.enqueueReportedBlock(storageInfo, block1Gs2, ReplicaState.FINALIZED);

    assertEquals(2, msgs.count());
    
    // Nothing queued yet for block 2
    assertNull(msgs.takeBlockQueue(block2Gs1));
    assertEquals(2, msgs.count());
    
    Queue<ReportedBlockInfo> q =
      msgs.takeBlockQueue(block1Gs2DifferentInstance);
    assertEquals(
        "ReportedBlockInfo [block=blk_1_1, dn=127.0.0.1:9866, reportedState=FINALIZED]," +
        "ReportedBlockInfo [block=blk_1_2, dn=127.0.0.1:9866, reportedState=FINALIZED]",
        Joiner.on(",").join(q));
    assertEquals(0, msgs.count());
    
    // Should be null if we pull again
    assertNull(msgs.takeBlockQueue(block1Gs1));
    assertEquals(0, msgs.count());
  }

  @Test
  public void testPendingDataNodeMessagesWithEC() throws Exception {
    ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies
        .getByID(SystemErasureCodingPolicies.XOR_2_1_POLICY_ID);
    Path dirPath = new Path("/testPendingDataNodeMessagesWithEC");
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 20 * 60000);

    int numDn = ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDn).nnTopology(MiniDFSNNTopology.simpleHATopology())
        .build();
    try {
      cluster.transitionToActive(0);

      DistributedFileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      fs.mkdirs(dirPath);
      fs.setErasureCodingPolicy(dirPath, ecPolicy.getName());

      DFSTestUtil.createFile(fs, new Path(dirPath, "file"),
          ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(), (short) 1, 0);

      cluster.getNameNode(0).getRpcServer().rollEditLog();
      cluster.getNameNode(1).getNamesystem().getEditLogTailer().doTailEdits();

      // PendingDataNodeMessages datanode message queue should be empty after
      // processing IBR
      int pendingIBRMsg = cluster.getNameNode(1).getNamesystem()
          .getBlockManager().getPendingDataNodeMessageCount();
      assertEquals("All DN message should processed after tail edits", 0,
          pendingIBRMsg);
    } finally {
      cluster.shutdown();
    }
  }
}
