/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT;

public class TestNameNodeReconfigure {

  public static final Log LOG = LogFactory
      .getLog(TestNameNodeReconfigure.class);

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
  }

  @Test
  public void testReconfigureCallerContextEnabled()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final FSNamesystem nameSystem = nameNode.getNamesystem();

    // try invalid values
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "text");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, false);

    // enable CallerContext
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "true");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, true);

    // disable CallerContext
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, "false");
    verifyReconfigureCallerContextEnabled(nameNode, nameSystem, false);

    // revert to default
    nameNode.reconfigureProperty(HADOOP_CALLER_CONTEXT_ENABLED_KEY, null);

    // verify default
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value", false,
        nameSystem.getCallerContextEnabled());
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value", null,
        nameNode.getConf().get(HADOOP_CALLER_CONTEXT_ENABLED_KEY));
  }

  void verifyReconfigureCallerContextEnabled(final NameNode nameNode,
      final FSNamesystem nameSystem, boolean expected) {
    assertEquals(HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value",
        expected, nameNode.getNamesystem().getCallerContextEnabled());
    assertEquals(
        HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value",
        expected,
        nameNode.getConf().getBoolean(HADOOP_CALLER_CONTEXT_ENABLED_KEY,
            HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT));
  }

  /**
   * Test to reconfigure enable/disable IPC backoff
   */
  @Test
  public void testReconfigureIPCBackoff() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    NameNodeRpcServer nnrs = (NameNodeRpcServer) nameNode.getRpcServer();

    String ipcClientRPCBackoffEnable = NameNode.buildBackoffEnableKey(nnrs
        .getClientRpcServer().getPort());

    // try invalid values
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        false);

    // enable IPC_CLIENT_RPC_BACKOFF
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, "true");
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        true);

    // disable IPC_CLIENT_RPC_BACKOFF
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, "false");
    verifyReconfigureIPCBackoff(nameNode, nnrs, ipcClientRPCBackoffEnable,
        false);

    // revert to default
    nameNode.reconfigureProperty(ipcClientRPCBackoffEnable, null);
    assertEquals(ipcClientRPCBackoffEnable + " has wrong value", false,
        nnrs.getClientRpcServer().isClientBackoffEnabled());
    assertEquals(ipcClientRPCBackoffEnable + " has wrong value", null,
        nameNode.getConf().get(ipcClientRPCBackoffEnable));
  }

  void verifyReconfigureIPCBackoff(final NameNode nameNode,
      final NameNodeRpcServer nnrs, String property, boolean expected) {
    assertEquals(property + " has wrong value", expected, nnrs
        .getClientRpcServer().isClientBackoffEnabled());
    assertEquals(property + " has wrong value", expected, nameNode.getConf()
        .getBoolean(property, IPC_BACKOFF_ENABLE_DEFAULT));
  }

  /**
   * Test to reconfigure interval of heart beat check and re-check.
   */
  @Test
  public void testReconfigureHearbeatCheck() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();
    // change properties
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "" + 6);
    nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        "" + (10 * 60 * 1000));

    // try invalid values
    try {
      nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
      assertTrue(expected.getCause() instanceof NumberFormatException);
    }
    try {
      nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
          "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException expected) {
      assertTrue(expected.getCause() instanceof NumberFormatException);
    }

    // verify change
    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        6,
        nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
            DFS_HEARTBEAT_INTERVAL_DEFAULT));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", 6,
        datanodeManager.getHeartbeatInterval());

    assertEquals(
        DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY + " has wrong value",
        10 * 60 * 1000,
        nameNode.getConf().getInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
            DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", 10 * 60 * 1000,
        datanodeManager.getHeartbeatRecheckInterval());

    // revert to defaults
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, null);
    nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        null);

    // verify defaults
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", null,
        nameNode.getConf().get(DFS_HEARTBEAT_INTERVAL_KEY));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        DFS_HEARTBEAT_INTERVAL_DEFAULT, datanodeManager.getHeartbeatInterval());

    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", null,
        nameNode.getConf().get(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY));
    assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
        + " has wrong value", DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT,
        datanodeManager.getHeartbeatRecheckInterval());
  }

  @After
  public void shutDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}