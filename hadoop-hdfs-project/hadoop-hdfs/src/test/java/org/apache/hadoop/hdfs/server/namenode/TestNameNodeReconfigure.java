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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminBackoffMonitor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminMonitorInterface;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_NODES_TO_REPORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.StoragePolicySatisfierMode;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowPeerTracker;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfyManager;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT;

public class TestNameNodeReconfigure {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestNameNodeReconfigure.class);

  private MiniDFSCluster cluster;
  private final int customizedBlockInvalidateLimit = 500;

  @Before
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

    // change to a value with time unit
    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "1m");

    assertEquals(
        DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value",
        60,
        nameNode.getConf().getLong(DFS_HEARTBEAT_INTERVAL_KEY,
            DFS_HEARTBEAT_INTERVAL_DEFAULT));
    assertEquals(DFS_HEARTBEAT_INTERVAL_KEY + " has wrong value", 60,
        datanodeManager.getHeartbeatInterval());

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
    assertNull("SPS shouldn't start as "
        + DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY + " is disabled",
            nameNode.getNamesystem().getBlockManager().getSPSManager());
    verifySPSEnabled(nameNode, DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
        StoragePolicySatisfierMode.EXTERNAL, false);

    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        StoragePolicySatisfierMode.EXTERNAL.toString(), nameNode.getConf()
            .get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
            DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT));
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
    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        false, nameNode.getNamesystem().getBlockManager().getSPSManager()
            .isSatisfierRunning());
    assertEquals(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY + " has wrong value",
        StoragePolicySatisfierMode.EXTERNAL.toString(),
        nameNode.getConf().get(DFS_STORAGE_POLICY_SATISFIER_MODE_KEY,
            DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT));
    assertNotNull("SPS Manager should be created",
        nameNode.getNamesystem().getBlockManager().getSPSManager());
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
    assertNull("SPS Manager should be null",
        nameNode.getNamesystem().getBlockManager().getSPSManager());

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
    assertEquals(property + " has wrong value", isSatisfierRunning, isSPSRunning);
    String actual = nameNode.getConf().get(property,
        DFS_STORAGE_POLICY_SATISFIER_MODE_DEFAULT);
    assertEquals(property + " has wrong value", expected,
        StoragePolicySatisfierMode.fromString(actual));
  }

  @Test
  public void testBlockInvalidateLimitAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not correctly set",
        customizedBlockInvalidateLimit,
        datanodeManager.getBlockInvalidateLimit());

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(6));

    // 20 * 6 = 120 < 500
    // Invalid block limit should stay same as before after reconfiguration.
    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY
            + " is not honored after reconfiguration",
        customizedBlockInvalidateLimit,
        datanodeManager.getBlockInvalidateLimit());

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY,
        Integer.toString(50));

    // 20 * 50 = 1000 > 500
    // Invalid block limit should be reset to 1000
    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY
            + " is not reconfigured correctly",
        1000,
        datanodeManager.getBlockInvalidateLimit());
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

  @Test
  public void testEnableSlowNodesParametersAfterReconfigured()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final BlockManager blockManager = nameNode.namesystem.getBlockManager();
    final DatanodeManager datanodeManager = blockManager.getDatanodeManager();

    // By default, avoidSlowDataNodesForRead is false.
    assertEquals(false, datanodeManager.getEnableAvoidSlowDataNodesForRead());

    nameNode.reconfigureProperty(
        DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY, Boolean.toString(true));

    // After reconfigured, avoidSlowDataNodesForRead is true.
    assertEquals(true, datanodeManager.getEnableAvoidSlowDataNodesForRead());

    // By default, excludeSlowNodesEnabled is false.
    assertEquals(false, blockManager.
        getExcludeSlowNodesEnabled(BlockType.CONTIGUOUS));
    assertEquals(false, blockManager.
        getExcludeSlowNodesEnabled(BlockType.STRIPED));

    nameNode.reconfigureProperty(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY, Boolean.toString(true));

    // After reconfigured, excludeSlowNodesEnabled is true.
    assertEquals(true, blockManager.
        getExcludeSlowNodesEnabled(BlockType.CONTIGUOUS));
    assertEquals(true, blockManager.
        getExcludeSlowNodesEnabled(BlockType.STRIPED));
  }

  @Test
  public void testReconfigureMaxSlowpeerCollectNodes()
      throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

    // By default, DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY is 5.
    assertEquals(5, datanodeManager.getMaxSlowpeerCollectNodes());

    // Reconfigure.
    nameNode.reconfigureProperty(
        DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY, Integer.toString(10));

    // Assert DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY is 10.
    assertEquals(10, datanodeManager.getMaxSlowpeerCollectNodes());
  }

  @Test
  public void testBlockInvalidateLimit() throws ReconfigurationException {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem
        .getBlockManager().getDatanodeManager();

    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not correctly set",
        customizedBlockInvalidateLimit, datanodeManager.getBlockInvalidateLimit());

    try {
      nameNode.reconfigureProperty(DFS_BLOCK_INVALIDATE_LIMIT_KEY, "non-numeric");
      fail("Should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals(
          "Could not change property dfs.block.invalidate.limit from '500' to 'non-numeric'",
          e.getMessage());
    }

    nameNode.reconfigureProperty(DFS_BLOCK_INVALIDATE_LIMIT_KEY, "2500");

    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not honored after reconfiguration", 2500,
        datanodeManager.getBlockInvalidateLimit());

    nameNode.reconfigureProperty(DFS_HEARTBEAT_INTERVAL_KEY, "500");

    // 20 * 500 (10000) > 2500
    // Hence, invalid block limit should be reset to 10000
    assertEquals(DFS_BLOCK_INVALIDATE_LIMIT_KEY + " is not reconfigured correctly", 10000,
        datanodeManager.getBlockInvalidateLimit());
  }

  @Test
  public void testSlowPeerTrackerEnabled() throws Exception {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem.getBlockManager()
        .getDatanodeManager();

    assertFalse("SlowNode tracker is already enabled. It should be disabled by default",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());
    assertTrue(datanodeManager.isSlowPeerCollectorInitialized());

    try {
      nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "non-boolean");
      fail("should not reach here");
    } catch (ReconfigurationException e) {
      assertEquals(
          "Could not change property dfs.datanode.peer.stats.enabled from 'false' to 'non-boolean'",
          e.getMessage());
    }

    nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "True");
    assertTrue("SlowNode tracker is still disabled. Reconfiguration could not be successful",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());
    assertFalse(datanodeManager.isSlowPeerCollectorInitialized());

    nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, null);
    assertFalse("SlowNode tracker is still enabled. Reconfiguration could not be successful",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());

  }

  @Test
  public void testSlowPeerMaxNodesToReportReconf() throws Exception {
    final NameNode nameNode = cluster.getNameNode();
    final DatanodeManager datanodeManager = nameNode.namesystem.getBlockManager()
        .getDatanodeManager();
    nameNode.reconfigurePropertyImpl(DFS_DATANODE_PEER_STATS_ENABLED_KEY, "true");
    assertTrue("SlowNode tracker is still disabled. Reconfiguration could not be successful",
        datanodeManager.getSlowPeerTracker().isSlowPeerTrackerEnabled());

    SlowPeerTracker tracker = datanodeManager.getSlowPeerTracker();

    OutlierMetrics outlierMetrics1 = new OutlierMetrics(0.0, 0.0, 0.0, 1.1);
    tracker.addReport("node1", "node70", outlierMetrics1);
    OutlierMetrics outlierMetrics2 = new OutlierMetrics(0.0, 0.0, 0.0, 1.23);
    tracker.addReport("node2", "node71", outlierMetrics2);
    OutlierMetrics outlierMetrics3 = new OutlierMetrics(0.0, 0.0, 0.0, 2.13);
    tracker.addReport("node3", "node72", outlierMetrics3);
    OutlierMetrics outlierMetrics4 = new OutlierMetrics(0.0, 0.0, 0.0, 1.244);
    tracker.addReport("node4", "node73", outlierMetrics4);
    OutlierMetrics outlierMetrics5 = new OutlierMetrics(0.0, 0.0, 0.0, 0.2);
    tracker.addReport("node5", "node74", outlierMetrics4);
    OutlierMetrics outlierMetrics6 = new OutlierMetrics(0.0, 0.0, 0.0, 1.244);
    tracker.addReport("node6", "node75", outlierMetrics4);

    String jsonReport = tracker.getJson();
    LOG.info("Retrieved slow peer json report: {}", jsonReport);

    List<Boolean> containReport = validatePeerReport(jsonReport);
    assertEquals(1, containReport.stream().filter(reportVal -> !reportVal).count());

    nameNode.reconfigurePropertyImpl(DFS_DATANODE_MAX_NODES_TO_REPORT_KEY, "2");
    jsonReport = tracker.getJson();
    LOG.info("Retrieved slow peer json report: {}", jsonReport);

    containReport = validatePeerReport(jsonReport);
    assertEquals(4, containReport.stream().filter(reportVal -> !reportVal).count());
  }

  private List<Boolean> validatePeerReport(String jsonReport) {
    List<Boolean> containReport = new ArrayList<>();
    containReport.add(jsonReport.contains("node1"));
    containReport.add(jsonReport.contains("node2"));
    containReport.add(jsonReport.contains("node3"));
    containReport.add(jsonReport.contains("node4"));
    containReport.add(jsonReport.contains("node5"));
    containReport.add(jsonReport.contains("node6"));
    return containReport;
  }

  @Test
  public void testReconfigureDecommissionBackoffMonitorParameters()
      throws ReconfigurationException, IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS,
        DatanodeAdminBackoffMonitor.class, DatanodeAdminMonitorInterface.class);
    int defaultPendingRepLimit = 1000;
    conf.setInt(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT, defaultPendingRepLimit);
    int defaultBlocksPerLock = 1000;
    conf.setInt(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
        defaultBlocksPerLock);

    try (MiniDFSCluster newCluster = new MiniDFSCluster.Builder(conf).build()) {
      newCluster.waitActive();
      final NameNode nameNode = newCluster.getNameNode();
      final DatanodeManager datanodeManager = nameNode.namesystem
          .getBlockManager().getDatanodeManager();

      // verify defaultPendingRepLimit.
      assertEquals(datanodeManager.getDatanodeAdminManager().getPendingRepLimit(),
          defaultPendingRepLimit);

      // try invalid pendingRepLimit.
      try {
        nameNode.reconfigureProperty(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
            "non-numeric");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.decommission.backoff.monitor.pending.limit from '" +
            defaultPendingRepLimit + "' to 'non-numeric'", e.getMessage());
      }

      try {
        nameNode.reconfigureProperty(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
            "-1");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.decommission.backoff.monitor.pending.limit from '" +
            defaultPendingRepLimit + "' to '-1'", e.getMessage());
      }

      // try correct pendingRepLimit.
      nameNode.reconfigureProperty(DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
          "20000");
      assertEquals(datanodeManager.getDatanodeAdminManager().getPendingRepLimit(), 20000);

      // verify defaultBlocksPerLock.
      assertEquals(datanodeManager.getDatanodeAdminManager().getBlocksPerLock(),
          defaultBlocksPerLock);

      // try invalid blocksPerLock.
      try {
        nameNode.reconfigureProperty(
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
            "non-numeric");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.decommission.backoff.monitor.pending.blocks.per.lock from '" +
            defaultBlocksPerLock + "' to 'non-numeric'", e.getMessage());
      }

      try {
        nameNode.reconfigureProperty(
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK, "-1");
        fail("Should not reach here");
      } catch (ReconfigurationException e) {
        assertEquals("Could not change property " +
            "dfs.namenode.decommission.backoff.monitor.pending.blocks.per.lock from '" +
            defaultBlocksPerLock + "' to '-1'", e.getMessage());
      }

      // try correct blocksPerLock.
      nameNode.reconfigureProperty(
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK, "10000");
      assertEquals(datanodeManager.getDatanodeAdminManager().getBlocksPerLock(), 10000);
    }
  }

  @After
  public void shutDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}