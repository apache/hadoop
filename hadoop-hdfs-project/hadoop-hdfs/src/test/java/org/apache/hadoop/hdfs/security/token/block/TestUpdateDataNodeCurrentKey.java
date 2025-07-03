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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUpdateDataNodeCurrentKey {
  private static final short REPLICATION = (short)1;
  private MiniDFSCluster cluster = null;
  private Configuration config;

  @BeforeEach
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
    cluster.waitActive();
  }

  @AfterEach
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testUpdateDatanodeCurrentKeyWithStandbyNameNodes(){
    final String bpid = cluster.getNameNode(0).getFSImage().getBlockPoolID();
    final DataNode dataNode = cluster.getDataNodes().get(0);
    BlockKey currentKey = dataNode.getBlockPoolTokenSecretManager().
        get(bpid).getCurrentKey();
    assertTrue(currentKey != null);
  }

  @Test
  public void testUpdateDatanodeCurrentKeyWithFailover() throws IOException,
      InterruptedException {
    cluster.transitionToActive(0);
    final String bpid = cluster.getNameNode(0).getFSImage().getBlockPoolID();
    Thread.sleep(3000);
    BlockKey annCurrentKey = cluster.getNameNode(0).
        getNamesystem().getBlockManager().
        getBlockTokenSecretManager().
        getCurrentKey();
    final DataNode dataNode = cluster.getDataNodes().get(0);
    BlockKey currentKey = dataNode.getBlockPoolTokenSecretManager().
        get(bpid).getCurrentKey();
    assertEquals(annCurrentKey, currentKey);
  }

  @Test
  public void testUpdateDatanodeCurrentKeyFromActiveNameNode()
      throws IOException {
    cluster.transitionToActive(0);
    final DataNode oldDataNode = cluster.getDataNodes().get(0);
    //Add a new datanode
    cluster.startDataNodes(config, 1, true, null, null);
    final String bpid = cluster.getNamesystem(0).getBlockPoolId();

    final DatanodeInfo[] dataNodeInfos = cluster.getNameNodeRpc(0).
        getDatanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    assertEquals(2, dataNodeInfos.length);

    //Simulate nameNode restart
    cluster.restartNameNode(1, true);

    //DataNode currentKey is equals to active nameNode currentKey
    BlockKey currentKey = cluster.getNameNode(0).getNamesystem().
        getBlockManager().getBlockTokenSecretManager().
        getCurrentKey();
    final DataNode newDataNode = cluster.getDataNodes().get(1);
    BlockKey dnCurrentKey = oldDataNode.getBlockPoolTokenSecretManager().
        get(bpid).getCurrentKey();
    BlockKey dn2CurrentKey = newDataNode.getBlockPoolTokenSecretManager().
        get(bpid).getCurrentKey();
    assertEquals(dnCurrentKey, dn2CurrentKey);
    assertEquals(currentKey, dn2CurrentKey);
  }
}
