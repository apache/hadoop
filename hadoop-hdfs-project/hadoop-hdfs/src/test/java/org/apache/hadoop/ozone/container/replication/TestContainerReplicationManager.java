/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.replication;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.TestUtils
    .ReplicationDatanodeStateManager;
import org.apache.hadoop.ozone.container.TestUtils.ReplicationNodeManagerMock;
import org.apache.hadoop.ozone.container.TestUtils
    .ReplicationNodePoolManagerMock;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.container.replication
    .ContainerReplicationManager;
import org.apache.hadoop.ozone.scm.container.replication.InProgressPool;
import org.apache.hadoop.ozone.scm.node.CommandQueue;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.ozone.scm.node.NodePoolManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.slf4j.event.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState.HEALTHY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT;
import static org.apache.ratis.shaded.com.google.common.util.concurrent
    .Uninterruptibles.sleepUninterruptibly;

/**
 * Tests for the container manager.
 */
public class TestContainerReplicationManager {
  final static String POOL_NAME_TEMPLATE = "Pool%d";
  static final int MAX_DATANODES = 72;
  static final int POOL_SIZE = 24;
  static final int POOL_COUNT = 3;
  private LogCapturer logCapturer = LogCapturer.captureLogs(
      LogFactory.getLog(ContainerReplicationManager.class));
  private List<DatanodeID> datanodes = new LinkedList<>();
  private NodeManager nodeManager;
  private NodePoolManager poolManager;
  private CommandQueue commandQueue;
  private ContainerReplicationManager replicationManager;
  private ReplicationDatanodeStateManager datanodeStateManager;

  @After
  public void tearDown() throws Exception {
    logCapturer.stopCapturing();
    GenericTestUtils.setLogLevel(ContainerReplicationManager.LOG, Level.INFO);
  }

  @Before
  public void setUp() throws Exception {
    GenericTestUtils.setLogLevel(ContainerReplicationManager.LOG, Level.DEBUG);
    Map<DatanodeID, NodeState> nodeStateMap = new HashMap<>();
    // We are setting up 3 pools with 24 nodes each in this cluster.
    // First we create 72 Datanodes.
    for (int x = 0; x < MAX_DATANODES; x++) {
      DatanodeID datanode = SCMTestUtils.getDatanodeID();
      datanodes.add(datanode);
      nodeStateMap.put(datanode, HEALTHY);
    }

    // All nodes in this cluster are healthy for time being.
    nodeManager = new ReplicationNodeManagerMock(nodeStateMap);
    poolManager = new ReplicationNodePoolManagerMock();
    commandQueue = new CommandQueue();

    Assert.assertEquals("Max datanodes should be equal to POOL_SIZE * " +
        "POOL_COUNT", POOL_COUNT * POOL_SIZE, MAX_DATANODES);

    // Start from 1 instead of zero so we can multiply and get the node index.
    for (int y = 1; y <= POOL_COUNT; y++) {
      String poolName = String.format(POOL_NAME_TEMPLATE, y);
      for (int z = 0; z < POOL_SIZE; z++) {
        DatanodeID id = datanodes.get(y * z);
        poolManager.addNode(poolName, id);
      }
    }
    OzoneConfiguration config = SCMTestUtils.getOzoneConf();
    config.setTimeDuration(OZONE_SCM_CONTAINER_REPORTS_WAIT_TIMEOUT, 1,
        TimeUnit.SECONDS);
    replicationManager = new ContainerReplicationManager(config,
        nodeManager, poolManager, commandQueue);
    datanodeStateManager = new ReplicationDatanodeStateManager(nodeManager,
        poolManager);
    // Sleep for one second to make sure all threads get time to run.
    sleepUninterruptibly(1, TimeUnit.SECONDS);
  }

  @Test
  /**
   * Asserts that at least one pool is picked up for processing.
   */
  public void testAssertPoolsAreProcessed() {
    // This asserts that replication manager has started processing at least
    // one pool.
    Assert.assertTrue(replicationManager.getInProgressPoolCount() > 0);

    // Since all datanodes are flagged as healthy in this test, for each
    // datanode we must have queued a command.
    Assert.assertEquals("Commands are in queue :", commandQueue
        .getCommandsInQueue(), POOL_SIZE * replicationManager
        .getInProgressPoolCount());
  }

  @Test
  /**
   * This test sends container reports for 2 containers to a pool in progress.
   * Asserts that we are able to find a container with single replica and do
   * not find container with 3 replicas.
   */
  public void testDetectSingleContainerReplica() throws TimeoutException,
      InterruptedException {
    String singleNodeContainer = "SingleNodeContainer";
    String threeNodeContainer = "ThreeNodeContainer";
    InProgressPool ppool = replicationManager.getInProcessPoolList().get(0);
    // Only single datanode reporting that "SingleNodeContainer" exists.
    List<ContainerReportsRequestProto> clist =
        datanodeStateManager.getContainerReport(singleNodeContainer,
            ppool.getPool().getPoolName(), 1);
    ppool.handleContainerReport(clist.get(0));

    // Three nodes are going to report that ThreeNodeContainer  exists.
    clist = datanodeStateManager.getContainerReport(threeNodeContainer,
        ppool.getPool().getPoolName(), 3);

    for (ContainerReportsRequestProto reportsProto : clist) {
      ppool.handleContainerReport(reportsProto);
    }
    GenericTestUtils.waitFor(() -> ppool.getContainerProcessedCount() == 4,
        200, 1000);
    ppool.setDoneProcessing();

    List<Map.Entry<String, Integer>> containers = ppool.filterContainer(p -> p
        .getValue() == 1);
    Assert.assertEquals(singleNodeContainer, containers.get(0).getKey());
    int count = containers.get(0).getValue();
    Assert.assertEquals(1L, count);
  }

  @Test
  /**
   * We create three containers, Normal,OveReplicated and WayOverReplicated
   * containers. This test asserts that we are able to find the
   * over replicated containers.
   */
  public void testDetectOverReplica() throws TimeoutException,
      InterruptedException {
    String normalContainer = "NormalContainer";
    String overReplicated = "OverReplicatedContainer";
    String wayOverReplicated = "WayOverReplicated";
    InProgressPool ppool = replicationManager.getInProcessPoolList().get(0);

    List<ContainerReportsRequestProto> clist =
        datanodeStateManager.getContainerReport(normalContainer,
            ppool.getPool().getPoolName(), 3);
    ppool.handleContainerReport(clist.get(0));

    clist = datanodeStateManager.getContainerReport(overReplicated,
        ppool.getPool().getPoolName(), 4);

    for (ContainerReportsRequestProto reportsProto : clist) {
      ppool.handleContainerReport(reportsProto);
    }

    clist = datanodeStateManager.getContainerReport(wayOverReplicated,
        ppool.getPool().getPoolName(), 7);

    for (ContainerReportsRequestProto reportsProto : clist) {
      ppool.handleContainerReport(reportsProto);
    }

    // We ignore container reports from the same datanodes.
    // it is possible that these each of these containers get placed
    // on same datanodes, so allowing for 4 duplicates in the set of 14.
    GenericTestUtils.waitFor(() -> ppool.getContainerProcessedCount() > 10,
        200, 1000);
    ppool.setDoneProcessing();

    List<Map.Entry<String, Integer>> containers = ppool.filterContainer(p -> p
        .getValue() > 3);
    Assert.assertEquals(2, containers.size());
  }

  @Test
  /**
   * This test verifies that all pools are picked up for replica processing.
   *
   */
  public void testAllPoolsAreProcessed() throws TimeoutException,
      InterruptedException {
    // Verify that we saw all three pools being picked up for processing.
    GenericTestUtils.waitFor(() -> replicationManager.getPoolProcessCount()
        >= 3, 200, 15 * 1000);
    Assert.assertTrue(logCapturer.getOutput().contains("Pool1") &&
        logCapturer.getOutput().contains("Pool2") &&
        logCapturer.getOutput().contains("Pool3"));
  }

  @Test
  /**
   * Adds a new pool and tests that we are able to pick up that new pool for
   * processing as well as handle container reports for datanodes in that pool.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public void testAddingNewPoolWorks()
      throws TimeoutException, InterruptedException, IOException {
    LogCapturer inProgressLog = LogCapturer.captureLogs(
        LogFactory.getLog(InProgressPool.class));
    GenericTestUtils.setLogLevel(InProgressPool.LOG, Level.DEBUG);
    try {
      DatanodeID id = SCMTestUtils.getDatanodeID();
      ((ReplicationNodeManagerMock) (nodeManager)).addNode(id, HEALTHY);
      poolManager.addNode("PoolNew", id);
      GenericTestUtils.waitFor(() ->
              logCapturer.getOutput().contains("PoolNew"),
          200, 15 * 1000);

      // Assert that we are able to send a container report to this new
      // pool and datanode.
      List<ContainerReportsRequestProto> clist =
          datanodeStateManager.getContainerReport("NewContainer1",
              "PoolNew", 1);
      replicationManager.handleContainerReport(clist.get(0));
      GenericTestUtils.waitFor(() ->
          inProgressLog.getOutput().contains("NewContainer1") && inProgressLog
              .getOutput().contains(id.getDatanodeUuid()), 200, 10 * 1000);
    } finally {
      inProgressLog.stopCapturing();
    }
  }
}
