/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm.node;

import com.google.common.base.Supplier;
import static java.util.concurrent.TimeUnit.*;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReportState;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.ozone.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState.DEAD;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState
    .HEALTHY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.NodeState
    .STALE;
import static org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.Type;

import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_MAX_HB_COUNT_TO_PROCESS;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
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

  private File testDir;

  private ReportState reportState = ReportState.newBuilder()
      .setState(ReportState.states.noContainerReports)
      .setCount(0).build();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
  }

  @Before
  public void setup() {
    testDir = PathUtils.getTestDir(
        TestNodeManager.class);
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(testDir);
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    return conf;
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   * @throws IOException
   */

  SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException {
    SCMNodeManager nodeManager = new SCMNodeManager(config,
        UUID.randomUUID().toString(), null);
    assertFalse("Node manager should be in chill mode",
        nodeManager.isOutOfChillMode());
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
        DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);
        nodeManager.sendHeartbeat(datanodeID, null, reportState);
      }

      // Wait for 4 seconds max.
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);

      assertTrue("Heartbeat thread should have picked up the" +
              "scheduled heartbeats and transitioned out of chill mode.",
          nodeManager.isOutOfChillMode());
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
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
      assertFalse("No heartbeats, Node manager should have been in" +
          " chill mode.", nodeManager.isOutOfChillMode());
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
      nodeManager.sendHeartbeat(SCMTestUtils.getDatanodeID(nodeManager),
          null, reportState);
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
      assertFalse("Not enough heartbeat, Node manager should have" +
          "been in chillmode.", nodeManager.isOutOfChillMode());
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
      DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);

      // Send 10 heartbeat from same node, and assert we never leave chill mode.
      for (int x = 0; x < 10; x++) {
        nodeManager.sendHeartbeat(datanodeID, null, reportState);
      }

      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
      assertFalse("Not enough nodes have send heartbeat to node" +
          "manager.", nodeManager.isOutOfChillMode());
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
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    SCMNodeManager nodeManager = createNodeManager(conf);
    DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);
    nodeManager.close();

    // These should never be processed.
    nodeManager.sendHeartbeat(datanodeID, null, reportState);

    // Let us just wait for 2 seconds to prove that HBs are not processed.
    Thread.sleep(2 * 1000);

    assertEquals("Assert new HBs were never processed", 0,
        nodeManager.getLastHBProcessedCount());
  }

  /**
   * Asserts scm informs datanodes to re-register with the nodemanager
   * on a restart.
   *
   * @throws Exception
   */
  @Test
  public void testScmHeartbeatAfterRestart() throws Exception {
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    DatanodeID datanodeID = SCMTestUtils.getDatanodeID();
    try (SCMNodeManager nodemanager = createNodeManager(conf)) {
      nodemanager.register(datanodeID);
      List<SCMCommand> command = nodemanager.sendHeartbeat(datanodeID,
          null, reportState);
      Assert.assertTrue(nodemanager.getAllNodes().contains(datanodeID));
      Assert.assertTrue("On regular HB calls, SCM responses a "
          + "datanode with an empty command list", command.isEmpty());
    }

    // Sends heartbeat without registering to SCM.
    // This happens when SCM restarts.
    try (SCMNodeManager nodemanager = createNodeManager(conf)) {
      Assert.assertFalse(nodemanager
          .getAllNodes().contains(datanodeID));
      try {
        // SCM handles heartbeat asynchronously.
        // It may need more than one heartbeat processing to
        // send the notification.
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override public Boolean get() {
            List<SCMCommand> command =
                nodemanager.sendHeartbeat(datanodeID, null,
                    reportState);
            return command.size() == 1 && command.get(0).getType()
                .equals(Type.reregisterCommand);
          }
        }, 100, 3 * 1000);
      } catch (TimeoutException e) {
        Assert.fail("Times out to verify that scm informs "
            + "datanode to re-register itself.");
      }
    }
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
    OzoneConfiguration conf = getConf();
    final int count = 10;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {

      for (int x = 0; x < count; x++) {
        DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);
        nodeManager.sendHeartbeat(datanodeID, null, reportState);
      }
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
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
    OzoneConfiguration conf = getConf();
    final int interval = 100;
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);

    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, interval, MILLISECONDS);

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
    OzoneConfiguration conf = getConf();
    final int interval = 100;
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);

    // This should be 5 times more than  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL
    // and 3 times more than OZONE_SCM_HEARTBEAT_INTERVAL
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3 * 1000, MILLISECONDS);
    createNodeManager(conf).close();
  }

  /**
   * Asserts that a single node moves from Healthy to stale node, then from
   * stale node to dead node if it misses enough heartbeats.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmDetectStaleAndDeadNode() throws IOException,
      InterruptedException, TimeoutException {
    final int interval = 100;
    final int nodeCount = 10;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> nodeList = createNodeSet(nodeManager, nodeCount,
          "Node");

      DatanodeID staleNode = SCMTestUtils.getDatanodeID(nodeManager);

      // Heartbeat once
      nodeManager.sendHeartbeat(staleNode, null, reportState);

      // Heartbeat all other nodes.
      for (DatanodeID dn : nodeList) {
        nodeManager.sendHeartbeat(dn, null, reportState);
      }

      // Wait for 2 seconds .. and heartbeat good nodes again.
      Thread.sleep(2 * 1000);

      for (DatanodeID dn : nodeList) {
        nodeManager.sendHeartbeat(dn, null, reportState);
      }

      // Wait for 2 seconds, wait a total of 4 seconds to make sure that the
      // node moves into stale state.
      Thread.sleep(2 * 1000);
      List<DatanodeID> staleNodeList = nodeManager.getNodes(STALE);
      assertEquals("Expected to find 1 stale node",
          1, nodeManager.getNodeCount(STALE));
      assertEquals("Expected to find 1 stale node",
          1, staleNodeList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getDatanodeUuid(), staleNodeList.get(0).getDatanodeUuid());
      Thread.sleep(1000);

      // heartbeat good nodes again.
      for (DatanodeID dn : nodeList) {
        nodeManager.sendHeartbeat(dn, null, reportState);
      }

      //  6 seconds is the dead window for this test , so we wait a total of
      // 7 seconds to make sure that the node moves into dead state.
      Thread.sleep(2 * 1000);

      // the stale node has been removed
      staleNodeList = nodeManager.getNodes(STALE);
      assertEquals("Expected to find 1 stale node",
          0, nodeManager.getNodeCount(STALE));
      assertEquals("Expected to find 1 stale node",
          0, staleNodeList.size());

      // Check for the dead node now.
      List<DatanodeID> deadNodeList = nodeManager.getNodes(DEAD);
      assertEquals("Expected to find 1 dead node", 1,
          nodeManager.getNodeCount(DEAD));
      assertEquals("Expected to find 1 dead node",
          1, deadNodeList.size());
      assertEquals("Dead node is not the expected ID", staleNode
          .getDatanodeUuid(), deadNodeList.get(0).getDatanodeUuid());
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
      nodeManager.sendHeartbeat(null, null, reportState);
      logCapturer.stopCapturing();
      assertThat(logCapturer.getOutput(),
          containsString("Datanode ID in heartbeat is null"));
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
    /**
     * These values are very important. Here is what it means so you don't
     * have to look it up while reading this code.
     *
     *  OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL - This the frequency of the
     *  HB processing thread that is running in the SCM. This thread must run
     *  for the SCM  to process the Heartbeats.
     *
     *  OZONE_SCM_HEARTBEAT_INTERVAL - This is the frequency at which
     *  datanodes will send heartbeats to SCM. Please note: This is the only
     *  config value for node manager that is specified in seconds. We don't
     *  want SCM heartbeat resolution to be more than in seconds.
     *  In this test it is not used, but we are forced to set it because we
     *  have validation code that checks Stale Node interval and Dead Node
     *  interval is larger than the value of
     *  OZONE_SCM_HEARTBEAT_INTERVAL.
     *
     *  OZONE_SCM_STALENODE_INTERVAL - This is the time that must elapse
     *  from the last heartbeat for us to mark a node as stale. In this test
     *  we set that to 3. That is if a node has not heartbeat SCM for last 3
     *  seconds we will mark it as stale.
     *
     *  OZONE_SCM_DEADNODE_INTERVAL - This is the time that must elapse
     *  from the last heartbeat for a node to be marked dead. We have an
     *  additional constraint that this must be at least 2 times bigger than
     *  Stale node Interval.
     *
     *  With these we are trying to explore the state of this cluster with
     *  various timeouts. Each section is commented so that you can keep
     *  track of the state of the cluster nodes.
     *
     */

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    /**
     * Cluster state: Healthy: All nodes are heartbeat-ing like normal.
     */
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeID healthyNode =
          SCMTestUtils.getDatanodeID(nodeManager, "HealthyNode");
      DatanodeID staleNode =
          SCMTestUtils.getDatanodeID(nodeManager, "StaleNode");
      DatanodeID deadNode =
          SCMTestUtils.getDatanodeID(nodeManager, "DeadNode");
      nodeManager.sendHeartbeat(healthyNode, null, reportState);
      nodeManager.sendHeartbeat(staleNode, null, reportState);
      nodeManager.sendHeartbeat(deadNode, null, reportState);

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

      nodeManager.sendHeartbeat(healthyNode, null, reportState);
      nodeManager.sendHeartbeat(staleNode, null, reportState);
      nodeManager.sendHeartbeat(deadNode, null, reportState);

      Thread.sleep(1500);
      nodeManager.sendHeartbeat(healthyNode, null, reportState);
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

      nodeManager.sendHeartbeat(healthyNode, null, reportState);
      nodeManager.sendHeartbeat(staleNode, null, reportState);
      Thread.sleep(1500);
      nodeManager.sendHeartbeat(healthyNode, null, reportState);
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

      assertEquals("Expected one healthy node",
          1, healthyList.size());
      assertEquals("Healthy node is not the expected ID", healthyNode
          .getDatanodeUuid(), healthyList.get(0).getDatanodeUuid());

      assertEquals("Expected one stale node",
          1, staleList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getDatanodeUuid(), staleList.get(0).getDatanodeUuid());

      assertEquals("Expected one dead node",
          1, deadList.size());
      assertEquals("Dead node is not the expected ID", deadNode
          .getDatanodeUuid(), deadList.get(0).getDatanodeUuid());
      /**
       * Cluster State : let us heartbeat all the nodes and verify that we get
       * back all the nodes in healthy state.
       */
      nodeManager.sendHeartbeat(healthyNode, null, reportState);
      nodeManager.sendHeartbeat(staleNode, null, reportState);
      nodeManager.sendHeartbeat(deadNode, null, reportState);
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
  private void heartbeatNodeSet(SCMNodeManager manager, List<DatanodeID> list,
      int sleepDuration) throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      for (DatanodeID dn : list) {
        manager.sendHeartbeat(dn, null, reportState);
      }
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
  private List<DatanodeID> createNodeSet(SCMNodeManager nodeManager, int
      count, String
      prefix) {
    List<DatanodeID> list = new LinkedList<>();
    for (int x = 0; x < count; x++) {
      list.add(SCMTestUtils.getDatanodeID(nodeManager, prefix + x));
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
      OzoneProtos.NodeState state) {
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

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    conf.setInt(OZONE_SCM_MAX_HB_COUNT_TO_PROCESS, 7000);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> healthyNodeList = createNodeSet(nodeManager,
          healthyCount, "Healthy");
      List<DatanodeID> staleNodeList = createNodeSet(nodeManager, staleCount,
          "Stale");
      List<DatanodeID> deadNodeList = createNodeSet(nodeManager, deadCount,
          "Dead");

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
      for (DatanodeID dn : deadNodeList) {
        nodeManager.sendHeartbeat(dn, null, reportState);
      }


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
        assertThat(node.getHostName(), CoreMatchers.startsWith("Dead"));
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
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1,
        SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3 * 1000,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6 * 1000,
        MILLISECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> healthyList = createNodeSet(nodeManager,
          healthyCount, "h");
      List<DatanodeID> staleList = createNodeSet(nodeManager, staleCount, "s");

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
      assertEquals("Node count mismatch",
          healthyCount + staleCount, nodeManager.getAllNodes().size());

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
   * @throws TimeoutException
   */
  @Test
  public void testScmLogsHeartbeatFlooding() throws IOException,
      InterruptedException, TimeoutException {
    final int healthyCount = 3000;

    // Make the HB process thread run slower.
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setInt(OZONE_SCM_MAX_HB_COUNT_TO_PROCESS, 500);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeID> healthyList = createNodeSet(nodeManager, healthyCount,
          "h");
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

      GenericTestUtils.waitFor(() -> logCapturer.getOutput()
          .contains("SCM is being "
              + "flooded by heartbeats. Not able to keep up"
              + " with the heartbeat counts."),
          500, 20 * 1000);

      thread1.interrupt();
      logCapturer.stopCapturing();
    }
  }

  @Test
  public void testScmEnterAndExitChillMode() throws IOException,
      InterruptedException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      nodeManager.setMinimumChillModeNodes(10);
      DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);
      nodeManager.sendHeartbeat(datanodeID, null, reportState);
      String status = nodeManager.getChillModeStatus();
      Assert.assertThat(status, CoreMatchers.containsString("Still in chill " +
          "mode, waiting on nodes to report in."));

      // Should not exit chill mode since 10 nodes have not heartbeat yet.
      assertFalse(nodeManager.isOutOfChillMode());

      // Force exit chill mode.
      nodeManager.forceExitChillMode();
      assertTrue(nodeManager.isOutOfChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Out of chill mode."));


      // Enter back to into chill mode.
      nodeManager.enterChillMode();
      assertFalse(nodeManager.isOutOfChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Out of startup chill mode," +
              " but in manual chill mode."));

      // Assert that node manager force enter cannot be overridden by nodes HBs.
      for (int x = 0; x < 20; x++) {
        DatanodeID datanode = SCMTestUtils.getDatanodeID(nodeManager);
        nodeManager.sendHeartbeat(datanode, null, reportState);
      }

      Thread.sleep(500);
      assertFalse(nodeManager.isOutOfChillMode());

      // Make sure that once we exit out of manual chill mode, we fall back
      // to the number of nodes to get out chill mode.
      nodeManager.exitChillMode();
      assertTrue(nodeManager.isOutOfChillMode());
      status = nodeManager.getChillModeStatus();
      Assert.assertThat(status,
          CoreMatchers.containsString("Out of chill mode."));
    }
  }

  /**
   * Test multiple nodes sending initial heartbeat with their node report.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmStatsFromNodeReport() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    final int nodeCount = 10;
    final long capacity = 2000;
    final long used = 100;
    final long remaining = capacity - used;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int x = 0; x < nodeCount; x++) {
        DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);

        SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
        SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
        srb.setStorageUuid(UUID.randomUUID().toString());
        srb.setCapacity(capacity).setScmUsed(used).
            setRemaining(capacity - used).build();
        nodeManager.sendHeartbeat(datanodeID,
            nrb.addStorageReport(srb).build(), reportState);
      }
      GenericTestUtils.waitFor(() -> nodeManager.waitForHeartbeatProcessed(),
          100, 4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeCount(HEALTHY));
      assertEquals(capacity * nodeCount, (long) nodeManager.getStats()
          .getCapacity().get());
      assertEquals(used * nodeCount, (long) nodeManager.getStats()
          .getScmUsed().get());
      assertEquals(remaining * nodeCount, (long) nodeManager.getStats()
          .getRemaining().get());
    }
  }

  /**
   * Test single node stat update based on nodereport from different heartbeat
   * status (healthy, stale and dead).
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNodeReportUpdate() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = getConf();
    final int heartbeatCount = 5;
    final int nodeCount = 1;
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeID datanodeID = SCMTestUtils.getDatanodeID(nodeManager);
      final long capacity = 2000;
      final long usedPerHeartbeat = 100;

      for (int x = 0; x < heartbeatCount; x++) {
        SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
        SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
        srb.setStorageUuid(UUID.randomUUID().toString());
        srb.setCapacity(capacity).setScmUsed(x * usedPerHeartbeat)
            .setRemaining(capacity - x * usedPerHeartbeat).build();
        nrb.addStorageReport(srb);

        nodeManager.sendHeartbeat(datanodeID, nrb.build(), reportState);
        Thread.sleep(100);
      }

      final long expectedScmUsed = usedPerHeartbeat * (heartbeatCount - 1);
      final long expectedRemaining = capacity - expectedScmUsed;

      GenericTestUtils.waitFor(
          () -> nodeManager.getStats().getScmUsed().get() == expectedScmUsed,
          100, 4 * 1000);

      long foundCapacity = nodeManager.getStats().getCapacity().get();
      assertEquals(capacity, foundCapacity);

      long foundScmUsed = nodeManager.getStats().getScmUsed().get();
      assertEquals(expectedScmUsed, foundScmUsed);

      long foundRemaining = nodeManager.getStats().getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Test NodeManager#getNodeStats
      assertEquals(nodeCount, nodeManager.getNodeStats().size());
      long nodeCapacity = nodeManager.getNodeStat(datanodeID).get()
          .getCapacity().get();
      assertEquals(capacity, nodeCapacity);

      foundScmUsed = nodeManager.getNodeStat(datanodeID).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeID).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Compare the result from
      // NodeManager#getNodeStats and NodeManager#getNodeStat
      SCMNodeStat stat1 = nodeManager.getNodeStats().
          get(datanodeID.getDatanodeUuid());
      SCMNodeStat stat2 = nodeManager.getNodeStat(datanodeID).get();
      assertEquals(stat1, stat2);

      // Wait up to 4s so that the node becomes stale
      // Verify the usage info should be unchanged.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(STALE) == 1, 100,
          4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());

      foundCapacity = nodeManager.getNodeStat(datanodeID).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeID).get()
          .getScmUsed().get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeID).get().
          getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Wait up to 4 more seconds so the node becomes dead
      // Verify usage info should be updated.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(DEAD) == 1, 100,
          4 * 1000);

      assertEquals(0, nodeManager.getNodeStats().size());
      foundCapacity = nodeManager.getStats().getCapacity().get();
      assertEquals(0, foundCapacity);

      foundScmUsed = nodeManager.getStats().getScmUsed().get();
      assertEquals(0, foundScmUsed);

      foundRemaining = nodeManager.getStats().getRemaining().get();
      assertEquals(0, foundRemaining);

      // Send a new report to bring the dead node back to healthy
      SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
      SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
      srb.setStorageUuid(UUID.randomUUID().toString());
      srb.setCapacity(capacity).setScmUsed(expectedScmUsed)
          .setRemaining(expectedRemaining).build();
      nrb.addStorageReport(srb);
      nodeManager.sendHeartbeat(datanodeID, nrb.build(), reportState);

      // Wait up to 5 seconds so that the dead node becomes healthy
      // Verify usage info should be updated.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(HEALTHY) == 1,
          100, 5 * 1000);
      GenericTestUtils.waitFor(
          () -> nodeManager.getStats().getScmUsed().get() == expectedScmUsed,
          100, 4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());
      foundCapacity = nodeManager.getNodeStat(datanodeID).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeID).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);
      foundRemaining = nodeManager.getNodeStat(datanodeID).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);
    }
  }
}
