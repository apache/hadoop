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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY;
import static org.junit.jupiter.api.Assertions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT;

public class TestNameNodeReconfigure {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestNameNodeReconfigure.class);

  private MiniDFSCluster cluster;
  private final int customizedBlockInvalidateLimit = 500;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_BLOCK_INVALIDATE_LIMIT_KEY,
        customizedBlockInvalidateLimit);
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
      assertEquals(false,
              nameSystem.getCallerContextEnabled(), HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value");
      assertEquals(null,
              nameNode.getConf().get(HADOOP_CALLER_CONTEXT_ENABLED_KEY), HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value");
  }

  void verifyReconfigureCallerContextEnabled(final NameNode nameNode,
      final FSNamesystem nameSystem, boolean expected) {
      assertEquals(
              expected, nameNode.getNamesystem().getCallerContextEnabled(), HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value");
      assertEquals(
              expected,
              nameNode.getConf().getBoolean(HADOOP_CALLER_CONTEXT_ENABLED_KEY,
                      HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT),
              HADOOP_CALLER_CONTEXT_ENABLED_KEY + " has wrong value");
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
      assertEquals(false,
              nnrs.getClientRpcServer().isClientBackoffEnabled(), ipcClientRPCBackoffEnable + " has wrong value");
      assertEquals(null,
              nameNode.getConf().get(ipcClientRPCBackoffEnable), ipcClientRPCBackoffEnable + " has wrong value");
  }

  void verifyReconfigureIPCBackoff(final NameNode nameNode,
      final NameNodeRpcServer nnrs, String property, boolean expected) {
      assertEquals(expected, nnrs
              .getClientRpcServer().isClientBackoffEnabled(), property + " has wrong value");
      assertEquals(expected, nameNode.getConf()
              .getBoolean(property, IPC_BACKOFF_ENABLE_DEFAULT), property + " has wrong value");
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
              6,
              nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
                      DFS_HEARTBEAT_INTERVAL_DEFAULT),
              DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");
      assertEquals(6,
              datanodeManager.getHeartbeatInterval(), DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");

      assertEquals(
              10 * 60 * 1000,
              nameNode.getConf().getInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                      DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT),
              DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY + " has wrong value");
      assertEquals(10 * 60 * 1000,
              datanodeManager.getHeartbeatRecheckInterval(), DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
              + " has wrong value");

    // change to a value with time unit
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "1m");

      assertEquals(
              60,
              nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
                      DFS_HEARTBEAT_INTERVAL_DEFAULT),
              DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");
      assertEquals(60,
              datanodeManager.getHeartbeatInterval(), DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");

    // revert to defaults
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, null);
    nameNode.reconfigureProperty(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        null);

      // verify defaults
      assertEquals(null,
              nameNode.getConf().get(DFS_HEARTBEAT_INTERVAL_KEY), DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");
      assertEquals(
              DFS_HEARTBEAT_INTERVAL_DEFAULT, datanodeManager.getHeartbeatInterval(), DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value");

      assertEquals(null,
              nameNode.getConf().get(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY), DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
              + " has wrong value");
      assertEquals(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT,
              datanodeManager.getHeartbeatRecheckInterval(), DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY
              + " has wrong value");
  }

  /**
   * Tests enable/disable Storage Policy Satisfier dynamically when
   * "dfs.storage.policy.enabled" feature is disabled.
   *
   * @throws ReconfigurationException
   * @throws IOException
   */
  @Test(timeout = 30000)
  public void testReconfigureSPSWithStoragePolicyDisabled()
      throws ReconfigurationException, IOException {
    // shutdown cluster
    cluster.shutdown();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY, false);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

    final NameNode nameNode = cluster.getNameNode();
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    // enable SPS internally by keeping DFS_STORAGE_POLICY_ENABLED_KEY
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());

      // Since DFS_STORAGE_POLICY_ENABLED_KEY is disabled, SPS can't be enabled.
      assertNull(
              nameNode.getNamesystem().getBlockManager().getSPSManager(), "SPS shouldn't start as "
              + DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY + " is disabled");
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL, false);

      assertEquals(
              StoragePolicySatisfierMode.EXTERNAL.toString(), nameNode.getConf()
              .get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
                      DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT), DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value");
  }

  /**
   * Tests enable/disable Storage Policy Satisfier dynamically.
   */
  @Test(timeout = 30000)
  public void testReconfigureStoragePolicySatisfierEnabled()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();

    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);
    // try invalid values
    try {
      nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
          "text");
      fail("ReconfigurationException expected");
    } catch (ReconfigurationException e) {
      GenericTestUtils.assertExceptionContains(
          "For enabling or disabling storage policy satisfier, must "
              + "pass either internal/external/none string value only",
          e.getCause());
    }

    // disable SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    // enable external SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL.toString());
      assertEquals(
              false, nameNode.getNamesystem().getBlockManager().getSPSManager()
              .isSatisfierRunning(), DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value");
      assertEquals(
              StoragePolicySatisfierMode.EXTERNAL.toString(),
              nameNode.getConf().get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
                      DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT), DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value");
  }

  /**
   * Test to satisfy storage policy after disabled storage policy satisfier.
   */
  @Test(timeout = 30000)
  public void testSatisfyStoragePolicyAfterSatisfierDisabled()
      throws ReconfigurationException, IOException {
    final NameNode nameNode = cluster.getNameNode();

    // disable SPS
    nameNode.reconfigureProperty(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE.toString());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.NONE, false);

    Path filePath = new Path("/testSPS");
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    fileSystem.create(filePath);
    fileSystem.setStoragePolicy(filePath, "COLD");
    try {
      fileSystem.satisfyStoragePolicy(filePath);
      fail("Expected to fail, as storage policy feature has disabled.");
    } catch (RemoteException e) {
      GenericTestUtils
          .assertExceptionContains("Cannot request to satisfy storage policy "
              + "when storage policy satisfier feature has been disabled"
              + " by admin. Seek for an admin help to enable it "
              + "or use Mover tool.", e);
    }
  }

  void verifySPSEnabled(final NameNode nameNode, String property,
      StoragePolicySatisfierMode expected, boolean isSatisfierRunning) {
    StoragePolicySatisfyManager spsMgr = nameNode
            .getNamesystem().getBlockManager().getSPSManager();
    boolean isSPSRunning = spsMgr != null ? spsMgr.isSatisfierRunning()
        : false;
      assertEquals(isSPSRunning, isSPSRunning, property + " has wrong value");
    String actual = nameNode.getConf().get(property,
        DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
      assertEquals(expected,
              StoragePolicySatisfierMode.fromString(actual), property + " has wrong value");
  }

  @Test
  public void testBlockInvalidateLimitAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

      assertEquals(
              customizedBlockInvalidateLimit,
              datanodeManager.getBlockInvalidateLimit(), DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not correctly set");

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(6));

      // 20 * 6 = 120 < 500
      // Invalid block limit should stay same as before after reconfiguration.
      assertEquals(
              customizedBlockInvalidateLimit,
              datanodeManager.getBlockInvalidateLimit(), DFS_BLOCK_INVALIDATE_LIMIT_KEY
              + " is not honored after reconfiguration");

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(50));

      // 20 * 50 = 1000 > 500
      // Invalid block limit should be reset to 1000
      assertEquals(
              1000,
              datanodeManager.getBlockInvalidateLimit(), DFS_BLOCK_INVALIDATE_LIMIT_KEY
              + " is not reconfigured correctly");
  }

  @Test
  public void testEnableParallelLoadAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();

    // By default, enableParallelLoad is false
    assertEquals(false, FSImageFormatProtobuf.getEnableParallelLoad());

    nameNode.reconfigureProperty(DFS_IMAGE_PARALLEL_LOAD_KEY,
        Boolean.toString(true));

    // After reconfigured, enableParallelLoad is true
    assertEquals(true, FSImageFormatProtobuf.getEnableParallelLoad());
  }

  @AfterEach
  public void shutDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}