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
package org.apache.hadoop.ozone.scm.node;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_DEADNODE_INTERVAL_MS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_MAX_HB_COUNT_TO_PROCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_STALENODE_INTERVAL_MS;
import static org.apache.hadoop.ozone.scm.node.NodeManager.NODESTATE.HEALTHY;
import static org.apache.hadoop.ozone.scm.node.NodeManager.NODESTATE.STALE;
import static org.apache.hadoop.ozone.scm.node.NodeManager.NODESTATE.DEAD;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the Node Manager class.
 */
public class TestNodeManager {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  Configuration getConf() {
    return new OzoneConfiguration();
  }

  /**
   * Create a new datanode ID.
   *
   * @return DatanodeID
   */
  DatanodeID getDatanodeID() {
    return getDatanodeID(UUID.randomUUID().toString());
  }

  /**
   * Create a new DatanodeID with NodeID set to the string.
   *
   * @param uuid - node ID, it is generally UUID.
   * @return DatanodeID.
   */
  DatanodeID getDatanodeID(String uuid) {
    Random random = new Random();
    String ipAddress = random.nextInt(256) + "."
        + random.nextInt(256) + "."
        + random.nextInt(256) + "."
        + random.nextInt(256);

    String hostName = RandomStringUtils.randomAscii(8);
    return new DatanodeID(ipAddress, hostName, uuid,
        0, 0, 0, 0);
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   * @throws IOException
   */

  SCMNodeManager createNodeManager(Configuration config) throws IOException {
    SCMNodeManager nodeManager = new SCMNodeManager(config);
    assertFalse("Node manager should be in chill mode",
        nodeManager.isOutOfNodeChillMode());
    return nodeManager;
  }

  /**
   * Tests that Node manager handles heartbeats correctly, and comes out of
   * chill Mode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmHeartbeat() throws IOException,
      InterruptedException, TimeoutException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {

      // Send some heartbeats from different nodes.
      for (int x = 0; x < nodeManager.getMinimumChillModeNodes(); x++) {
        nodeManager.updateHeartbeat(getDatanodeID());
      }

      // Wait for 4 seconds max.
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatThead(), 100,
          4 * 1000);

      assertTrue("Heartbeat thread should have picked up the scheduled " +
              "heartbeats and transitioned out of chill mode.",
          nodeManager.isOutOfNodeChillMode());
    }
  }

  /**
   * asserts that if we send no heartbeats node manager stays in chillmode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNoHeartbeats() throws IOException,
      InterruptedException, TimeoutException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatThead(), 100,
          4 * 1000);
      assertFalse("No heartbeats, Node manager should have been in chill mode.",
          nodeManager.isOutOfNodeChillMode());
    }
  }

  /**
   * Asserts that if we don't get enough unique nodes we stay in chillmode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNotEnoughHeartbeats() throws IOException,
      InterruptedException, TimeoutException {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {

      // Need 100 nodes to come out of chill mode, only one node is sending HB.
      nodeManager.setMinimumChillModeNodes(100);
      nodeManager.updateHeartbeat(getDatanodeID());
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatThead(), 100,
          4 * 1000);
      assertFalse("Not enough heartbeat, Node manager should have been in " +
          "chillmode.", nodeManager.isOutOfNodeChillMode());
    }
  }

  /**
   * Asserts that many heartbeat from the same node is counted as a single
   * node.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmSameNodeHeartbeats() throws IOException,
      InterruptedException, TimeoutException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      nodeManager.setMinimumChillModeNodes(3);
      DatanodeID datanodeID = getDatanodeID();

      // Send 10 heartbeat from same node, and assert we never leave chill mode.
      for (int x = 0; x < 10; x++) {
        nodeManager.updateHeartbeat(datanodeID);
      }

      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatThead(), 100,
          4 * 1000);
      assertFalse("Not enough nodes have send heartbeat to node manager.",
          nodeManager.isOutOfNodeChillMode());
    }
  }

  /**
   * Asserts that adding heartbeats after shutdown does not work. This implies
   * that heartbeat thread has been shutdown safely by closing the node
   * manager.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmShutdown() throws IOException, InterruptedException,
      TimeoutException {
    Configuration conf = getConf();
    conf.setInt(OzoneConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 100);
    SCMNodeManager nodeManager = createNodeManager(conf);
    nodeManager.close();

    // These should never be processed.
    nodeManager.updateHeartbeat(getDatanodeID());

    // Let us just wait for 2 seconds to prove that HBs are not processed.
    Thread.sleep(2 * 1000);

    assertFalse("Node manager executor service is shutdown, should never exit" +
        " chill mode", nodeManager.isOutOfNodeChillMode());

    assertEquals("Assert new HBs were never processed", 0,
        nodeManager.getLastHBProcessedCount());
  }

  /**
   * Asserts that we detect as many healthy nodes as we have generated heartbeat
   * for.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmHealthyNodeCount() throws IOException,
      InterruptedException, TimeoutException {
    Configuration conf = getConf();
    final int count = 10;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int x = 0; x < count; x++) {
        nodeManager.updateHeartbeat(getDatanodeID());
      }
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatThead(), 100,
          4 * 1000);
      assertEquals(count, nodeManager.getNodeCount(HEALTHY));
    }
  }

  /**
   * Asserts that if user provides a value less than 5 times the heartbeat
   * interval as the StaleNode Value, we throw since that is a QoS that we
   * cannot maintain.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */

  @Test
  public void testScmSanityOfUserConfig1() throws IOException,
      InterruptedException, TimeoutException {
    Configuration conf = getConf();
    final int interval = 100;
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, interval);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);

    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, interval);

    thrown.expect(IllegalArgumentException.class);

    // This string is a multiple of the interval value
    thrown.expectMessage(
        startsWith("100 is not within min = 500 or max = 100000"));
    createNodeManager(conf);
  }

  /**
   * Asserts that if Stale Interval value is more than 5 times the value of HB
   * processing thread it is a sane value.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmSanityOfUserConfig2() throws IOException,
      InterruptedException, TimeoutException {
    Configuration conf = getConf();
    final int interval = 100;
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, interval);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);

    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);
    createNodeManager(conf).close();
  }

  /**
   * Asserts that a single node moves from Healthy to stale node if it misses
   * the heartbeat.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmDetectStaleNode() throws IOException,
      InterruptedException, TimeoutException {
    Configuration conf = getConf();
    final int interval = 100;
    final int nodeCount = 10;
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, interval);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> nodeList = new LinkedList<>();
      DatanodeID staleNode = getDatanodeID();
      for (int x = 0; x < nodeCount; x++) {
        nodeList.add(getDatanodeID());
      }
      // Heartbeat once
      nodeManager.updateHeartbeat(staleNode);

      // Heartbeat all other nodes.
      nodeList.forEach(nodeManager::updateHeartbeat);

      // Wait for 2 seconds .. and heartbeat good nodes again.
      Thread.sleep(2 * 1000);
      nodeList.forEach(nodeManager::updateHeartbeat);

      // Wait for 2 more seconds, 3 seconds is the stale window for this test
      Thread.sleep(2 * 1000);

      List<DatanodeID> staleNodeList = nodeManager.getNodes(NodeManager
          .NODESTATE.STALE);
      assertEquals("Expected to find 1 stale node", 1, nodeManager
          .getNodeCount(STALE));
      assertEquals("Expected to find 1 stale node", 1, staleNodeList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getDatanodeUuid(), staleNodeList.get(0).getDatanodeUuid());
    }
  }

  /**
   * Asserts that a single node moves from Healthy to dead node if it misses
   * enough heartbeats.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmDetectDeadNode() throws IOException,
      InterruptedException, TimeoutException {
    final int interval = 100;
    final int nodeCount = 10;

    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, interval);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);
    conf.setInt(OZONE_SCM_DEADNODE_INTERVAL_MS, 6 * 1000);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> nodeList = new LinkedList<>();

      DatanodeID deadNode = getDatanodeID();
      for (int x = 0; x < nodeCount; x++) {
        nodeList.add(getDatanodeID());
      }
      // Heartbeat once
      nodeManager.updateHeartbeat(deadNode);

      // Heartbeat all other nodes.
      nodeList.forEach(nodeManager::updateHeartbeat);

      // Wait for 2 seconds .. and heartbeat good nodes again.
      Thread.sleep(2 * 1000);

      nodeList.forEach(nodeManager::updateHeartbeat);
      Thread.sleep(3 * 1000);

      // heartbeat good nodes again.
      nodeList.forEach(nodeManager::updateHeartbeat);

      //  6 seconds is the dead window for this test , so we wait a total of
      // 7 seconds to make sure that the node moves into dead state.
      Thread.sleep(2 * 1000);

      // Check for the dead node now.
      List<DatanodeID> deadNodeList = nodeManager
          .getNodes(DEAD);
      assertEquals("Expected to find 1 dead node", 1, nodeManager
          .getNodeCount(DEAD));
      assertEquals("Expected to find 1 dead node", 1, deadNodeList.size());
      assertEquals("Dead node is not the expected ID", deadNode
          .getDatanodeUuid(), deadNodeList.get(0).getDatanodeUuid());
    }
  }

  /**
   * Asserts that if we get duplicate registration calls for a datanode, we will
   * ignore it and LOG the error.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmDuplicateRegistrationLogsError() throws IOException,
      InterruptedException, TimeoutException {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      GenericTestUtils.LogCapturer logCapturer =
          GenericTestUtils.LogCapturer.captureLogs(SCMNodeManager.LOG);
      DatanodeID duplicateNodeID = getDatanodeID();
      nodeManager.registerNode(duplicateNodeID);
      nodeManager.registerNode(duplicateNodeID);
      logCapturer.stopCapturing();
      assertThat(logCapturer.getOutput(), containsString("Datanode is already" +
          " registered."));
    }
  }

  /**
   * Asserts that we log an error for null in datanode ID.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmLogErrorOnNullDatanode() throws IOException,
      InterruptedException, TimeoutException {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      GenericTestUtils.LogCapturer logCapturer =
          GenericTestUtils.LogCapturer.captureLogs(SCMNodeManager.LOG);
      nodeManager.updateHeartbeat(null);
      logCapturer.stopCapturing();
      assertThat(logCapturer.getOutput(), containsString("Datanode ID in " +
          "heartbeat is null"));
    }
  }

  /**
   * Asserts that a dead node, stale node and healthy nodes co-exist. The counts
   * , lists and node ID match the expected node state.
   * <p/>
   * This test is pretty complicated because it explores all states of Node
   * manager in a single test. Please read thru the comments to get an idea of
   * the current state of the node Manager.
   * <p/>
   * This test is written like a state machine to avoid threads and concurrency
   * issues. This test is replicated below with the use of threads. Avoiding
   * threads make it easy to debug the state machine.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmClusterIsInExpectedState1() throws IOException,
      InterruptedException, TimeoutException {

    DatanodeID healthyNode = getDatanodeID("HealthyNode");
    DatanodeID staleNode = getDatanodeID("StaleNode");
    DatanodeID deadNode = getDatanodeID("DeadNode");

    /**
     * These values are very important. Here is what it means so you don't
     * have to look it up while reading this code.
     *
     *  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS - This the frequency of the
     *  HB processing thread that is running in the SCM. This thread must run
     *  for the SCM  to process the Heartbeats.
     *
     *  OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS - This is the frequency at which
     *  datanodes will send heartbeats to SCM. Please note: This is the only
     *  config value for node manager that is specified in seconds. We don't
     *  want SCM heartbeat resolution to be more than in seconds.
     *  In this test it is not used, but we are forced to set it because we
     *  have validation code that checks Stale Node interval and Dead Node
     *  interval is larger than the value of
     *  OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS.
     *
     *  OZONE_SCM_STALENODE_INTERVAL_MS - This is the time that must elapse
     *  from the last heartbeat for us to mark a node as stale. In this test
     *  we set that to 3. That is if a node has not heartbeat SCM for last 3
     *  seconds we will mark it as stale.
     *
     *  OZONE_SCM_DEADNODE_INTERVAL_MS - This is the time that must elapse
     *  from the last heartbeat for a node to be marked dead. We have an
     *  additional constraint that this must be at least 2 times bigger than
     *  Stale node Interval.
     *
     *  With these we are trying to explore the state of this cluster with
     *  various timeouts. Each section is commented so that you can keep
     *  track of the state of the cluster nodes.
     *
     */

    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 100);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);
    conf.setInt(OZONE_SCM_DEADNODE_INTERVAL_MS, 6 * 1000);


    /**
     * Cluster state: Healthy: All nodes are heartbeat-ing like normal.
     */
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      nodeManager.updateHeartbeat(healthyNode);
      nodeManager.updateHeartbeat(staleNode);
      nodeManager.updateHeartbeat(deadNode);

      // Sleep so that heartbeat processing thread gets to run.
      Thread.sleep(500);

      //Assert all nodes are healthy.
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(HEALTHY));

      /**
       * Cluster state: Quiesced: We are going to sleep for 3 seconds. Which
       * means that no node is heartbeating. All nodes should move to Stale.
       */
      Thread.sleep(3 * 1000);
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(STALE));


      /**
       * Cluster State : Move healthy node back to healthy state, move other 2
       * nodes to Stale State.
       *
       * We heartbeat healthy node after 1 second and let other 2 nodes elapse
       * the 3 second windows.
       */

      nodeManager.updateHeartbeat(healthyNode);
      nodeManager.updateHeartbeat(staleNode);
      nodeManager.updateHeartbeat(deadNode);

      Thread.sleep(1500);
      nodeManager.updateHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);
      assertEquals(1, nodeManager.getNodeCount(HEALTHY));


      // 3.5 seconds from last heartbeat for the stale and deadNode. So those
      //  2 nodes must move to Stale state and the healthy node must
      // remain in the healthy State.
      List<DatanodeID> healthyList = nodeManager.getNodes(HEALTHY);
      assertEquals("Expected one healthy node", 1, healthyList.size());
      assertEquals("Healthy node is not the expected ID", healthyNode
          .getDatanodeUuid(), healthyList.get(0).getDatanodeUuid());

      assertEquals(2, nodeManager.getNodeCount(STALE));

      /**
       * Cluster State: Allow healthyNode to remain in healthy state and
       * staleNode to move to stale state and deadNode to move to dead state.
       */

      nodeManager.updateHeartbeat(healthyNode);
      nodeManager.updateHeartbeat(staleNode);
      Thread.sleep(1500);
      nodeManager.updateHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);

      // 3.5 seconds have elapsed for stale node, so it moves into Stale.
      // 7 seconds have elapsed for dead node, so it moves into dead.
      // 2 Seconds have elapsed for healthy node, so it stays in healhty state.
      healthyList = nodeManager.getNodes(HEALTHY);
      List<DatanodeID> staleList = nodeManager.getNodes(STALE);
      List<DatanodeID> deadList = nodeManager.getNodes(DEAD);

      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(1, nodeManager.getNodeCount(HEALTHY));
      assertEquals(1, nodeManager.getNodeCount(STALE));
      assertEquals(1, nodeManager.getNodeCount(DEAD));

      assertEquals("Expected one healthy node", 1, healthyList.size());
      assertEquals("Healthy node is not the expected ID", healthyNode
          .getDatanodeUuid(), healthyList.get(0).getDatanodeUuid());

      assertEquals("Expected one stale node", 1, staleList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getDatanodeUuid(), staleList.get(0).getDatanodeUuid());

      assertEquals("Expected one dead node", 1, deadList.size());
      assertEquals("Dead node is not the expected ID", deadNode
          .getDatanodeUuid(), deadList.get(0).getDatanodeUuid());

      /**
       * Cluster State : let us heartbeat all the nodes and verify that we get
       * back all the nodes in healthy state.
       */
      nodeManager.updateHeartbeat(healthyNode);
      nodeManager.updateHeartbeat(staleNode);
      nodeManager.updateHeartbeat(deadNode);
      Thread.sleep(500);
      //Assert all nodes are healthy.
      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(3, nodeManager.getNodeCount(HEALTHY));
    }
  }

  /**
   * Heartbeat a given set of nodes at a specified frequency.
   *
   * @param manager       - Node Manager
   * @param list          - List of datanodeIDs
   * @param sleepDuration - Duration to sleep between heartbeats.
   * @throws InterruptedException
   */
  private void heartbeatNodeSet(NodeManager manager, List<DatanodeID> list,
                                int sleepDuration) throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      list.forEach(manager::updateHeartbeat);
      Thread.sleep(sleepDuration);
    }
  }

  /**
   * Create a set of Nodes with a given prefix.
   *
   * @param count  - number of nodes.
   * @param prefix - A prefix string that can be used in verification.
   * @return List of Nodes.
   */
  private List<DatanodeID> createNodeSet(int count, String prefix) {
    List<DatanodeID> list = new LinkedList<>();
    for (int x = 0; x < count; x++) {
      list.add(getDatanodeID(prefix + x));
    }
    return list;
  }

  /**
   * Function that tells us if we found the right number of stale nodes.
   *
   * @param nodeManager - node manager
   * @param count       - number of stale nodes to look for.
   * @return true if we found the expected number.
   */
  private boolean findNodes(NodeManager nodeManager, int count,
                            NodeManager.NODESTATE state) {
    return count == nodeManager.getNodeCount(state);
  }

  /**
   * Asserts that we can create a set of nodes that send its heartbeats from
   * different threads and NodeManager behaves as expected.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testScmClusterIsInExpectedState2() throws IOException,
      InterruptedException, TimeoutException {
    final int healthyCount = 5000;
    final int staleCount = 100;
    final int deadCount = 10;

    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 100);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);
    conf.setInt(OZONE_SCM_DEADNODE_INTERVAL_MS, 6 * 1000);
    conf.setInt(OZONE_SCM_MAX_HB_COUNT_TO_PROCESS, 7000);

    List<DatanodeID> healthyNodeList = createNodeSet(healthyCount, "Healthy");
    List<DatanodeID> staleNodeList = createNodeSet(staleCount, "Stale");
    List<DatanodeID> deadNodeList = createNodeSet(deadCount, "Dead");


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      Runnable healthyNodeTask = () -> {
        try {
          // 2 second heartbeat makes these nodes stay healthy.
          heartbeatNodeSet(nodeManager, healthyNodeList, 2 * 1000);
        } catch (InterruptedException ignored) {
        }
      };

      Runnable staleNodeTask = () -> {
        try {
          // 4 second heartbeat makes these nodes go to stale and back to
          // healthy again.
          heartbeatNodeSet(nodeManager, staleNodeList, 4 * 1000);
        } catch (InterruptedException ignored) {
        }
      };


      // No Thread just one time HBs the node manager, so that these will be
      // marked as dead nodes eventually.
      deadNodeList.forEach(nodeManager::updateHeartbeat);

      Thread thread1 = new Thread(healthyNodeTask);
      thread1.setDaemon(true);
      thread1.start();


      Thread thread2 = new Thread(staleNodeTask);
      thread2.setDaemon(true);
      thread2.start();

      Thread.sleep(10 * 1000);

      // Assert all healthy nodes are healthy now, this has to be a greater
      // than check since Stale nodes can be healthy when we check the state.

      assertTrue(nodeManager.getNodeCount(HEALTHY) >= healthyCount);

      assertEquals(deadCount, nodeManager.getNodeCount(DEAD));

      List<DatanodeID> deadList = nodeManager.getNodes(DEAD);

      for (DatanodeID node : deadList) {
        assertThat(node.getDatanodeUuid(), CoreMatchers.startsWith("Dead"));
      }

      // Checking stale nodes is tricky since they have to move between
      // healthy and stale to avoid becoming dead nodes. So we search for
      // that state for a while, if we don't find that state waitfor will
      // throw.
      GenericTestUtils.waitFor(() -> findNodes(nodeManager, staleCount, STALE),
          500, 4 * 1000);

      thread1.interrupt();
      thread2.interrupt();
    }
  }

  /**
   * Asserts that we can handle 6000+ nodes heartbeating SCM.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmCanHandleScale() throws IOException,
      InterruptedException, TimeoutException {
    final int healthyCount = 3000;
    final int staleCount = 3000;
    List<DatanodeID> healthyList = createNodeSet(healthyCount, "h");
    List<DatanodeID> staleList = createNodeSet(staleCount, "s");
    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 100);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    conf.setInt(OZONE_SCM_STALENODE_INTERVAL_MS, 3 * 1000);
    conf.setInt(OZONE_SCM_DEADNODE_INTERVAL_MS, 6 * 1000);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      Runnable healthyNodeTask = () -> {
        try {
          heartbeatNodeSet(nodeManager, healthyList, 2 * 1000);
        } catch (InterruptedException ignored) {

        }
      };

      Runnable staleNodeTask = () -> {
        try {
          heartbeatNodeSet(nodeManager, staleList, 4 * 1000);
        } catch (InterruptedException ignored) {
        }
      };

      Thread thread1 = new Thread(healthyNodeTask);
      thread1.setDaemon(true);
      thread1.start();


      Thread thread2 = new Thread(staleNodeTask);
      thread2.setDaemon(true);
      thread2.start();
      Thread.sleep(3 * 1000);

      GenericTestUtils.waitFor(() -> findNodes(nodeManager, staleCount, STALE),
          500, 20 * 1000);
      assertEquals("Node count mismatch", healthyCount + staleCount, nodeManager
          .getAllNodes().size());

      thread1.interrupt();
      thread2.interrupt();
    }
  }

  /**
   * Asserts that SCM backs off from HB processing instead of going into an
   * infinite loop if SCM is flooded with too many heartbeats. This many not be
   * the best thing to do, but SCM tries to protect itself and logs an error
   * saying that it is getting flooded with heartbeats. In real world this can
   * lead to many nodes becoming stale or dead due to the fact that SCM is not
   * able to keep up with heartbeat processing. This test just verifies that SCM
   * will log that information.
   */
  @Test
  public void testScmLogsHeartbeatFlooding() throws IOException,
      InterruptedException {
    final int healthyCount = 3000;
    List<DatanodeID> healthyList = createNodeSet(healthyCount, "h");

    // Make the HB process thread run slower.
    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 500);
    conf.setInt(OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, 1);
    conf.setInt(OZONE_SCM_MAX_HB_COUNT_TO_PROCESS, 500);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      GenericTestUtils.LogCapturer logCapturer =
          GenericTestUtils.LogCapturer.captureLogs(SCMNodeManager.LOG);
      Runnable healthyNodeTask = () -> {
        try {
          // No wait in the HB sending loop.
          heartbeatNodeSet(nodeManager, healthyList, 0);
        } catch (InterruptedException ignored) {
        }
      };
      Thread thread1 = new Thread(healthyNodeTask);
      thread1.setDaemon(true);
      thread1.start();

      Thread.sleep(6 * 1000);


      thread1.interrupt();
      logCapturer.stopCapturing();

      assertThat(logCapturer.getOutput(), containsString("SCM is being " +
          "flooded by heartbeats. Not able to keep up with the heartbeat " +
          "counts."));
    }
  }

  @Test
  public void testScmEnterAndExistChillMode() throws IOException,
      InterruptedException {
    Configuration conf = getConf();
    conf.setInt(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, 100);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      nodeManager.setMinimumChillModeNodes(10);
      nodeManager.updateHeartbeat(getDatanodeID());
      String status = nodeManager.getChillModeStatus();
      Assert.assertThat(status, CoreMatchers.containsString("Still in chill " +
          "mode. Waiting on nodes to report in."));

      // Should not exist chill mode since 10 nodes have not heartbeat yet.
      assertFalse(nodeManager.isOutOfNodeChillMode());
      assertFalse((nodeManager.isInManualChillMode()));

      // Force exit chill mode.
      nodeManager.forceExitChillMode();
      assertTrue(nodeManager.isOutOfNodeChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Manual chill mode is set to false."));
      assertFalse((nodeManager.isInManualChillMode()));


      // Enter back to into chill mode.
      nodeManager.forceEnterChillMode();
      assertFalse(nodeManager.isOutOfNodeChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Manual chill mode is set to true."));
      assertTrue((nodeManager.isInManualChillMode()));


      // Assert that node manager force enter cannot be overridden by nodes HBs.
      for(int x= 0; x < 20; x++) {
        nodeManager.updateHeartbeat(getDatanodeID());
      }

      Thread.sleep(500);
      assertFalse(nodeManager.isOutOfNodeChillMode());

      // Make sure that once we clear the manual chill mode flag, we fall back
      // to the number of nodes to get out chill mode.
      nodeManager.clearChillModeFlag();
      assertTrue(nodeManager.isOutOfNodeChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Out of chill mode."));
      assertFalse(nodeManager.isInManualChillMode());
    }

  }
}
