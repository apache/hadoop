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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the SCM Node Manager class.
 */
public class TestSCMNodeManager {

  private File testDir;
  private StorageContainerManager scm;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
  }

  @Before
  public void setup() {
    testDir = PathUtils.getTestDir(
        TestSCMNodeManager.class);
  }

  @After
  public void cleanup() {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
    FileUtil.fullyDelete(testDir);
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
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
      throws IOException, AuthenticationException {
    scm = HddsTestUtils.getScm(config);
    return (SCMNodeManager) scm.getScmNodeManager();
  }

  /**
   * Tests that Node manager handles heartbeats correctly, and comes out of
   * safe Mode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmHeartbeat()
      throws IOException, InterruptedException, AuthenticationException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      int registeredNodes = 5;
      // Send some heartbeats from different nodes.
      for (int x = 0; x < registeredNodes; x++) {
        DatanodeDetails datanodeDetails = TestUtils
            .createRandomDatanodeAndRegister(nodeManager);
        nodeManager.processHeartbeat(datanodeDetails);
      }

      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertTrue("Heartbeat thread should have picked up the" +
              "scheduled heartbeats.",
          nodeManager.getAllNodes().size() == registeredNodes);
    }
  }

  /**
   * asserts that if we send no heartbeats node manager stays in safemode.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  public void testScmNoHeartbeats()
      throws IOException, InterruptedException, AuthenticationException {

    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertTrue("No heartbeats, 0 nodes should be registered",
          nodeManager.getAllNodes().size() == 0);
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
  public void testScmShutdown()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    SCMNodeManager nodeManager = createNodeManager(conf);
    DatanodeDetails datanodeDetails = TestUtils
        .createRandomDatanodeAndRegister(nodeManager);
    nodeManager.close();

    // These should never be processed.
    nodeManager.processHeartbeat(datanodeDetails);

    // Let us just wait for 2 seconds to prove that HBs are not processed.
    Thread.sleep(2 * 1000);

    //TODO: add assertion
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
  public void testScmHealthyNodeCount()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int count = 10;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {

      for (int x = 0; x < count; x++) {
        DatanodeDetails datanodeDetails = TestUtils
            .createRandomDatanodeAndRegister(nodeManager);
        nodeManager.processHeartbeat(datanodeDetails);
      }
      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
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
  public void testScmSanityOfUserConfig1()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int interval = 100;
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);

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
  public void testScmSanityOfUserConfig2()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int interval = 100;
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);

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
  public void testScmDetectStaleAndDeadNode()
      throws IOException, InterruptedException, AuthenticationException {
    final int interval = 100;
    final int nodeCount = 10;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> nodeList = createNodeSet(nodeManager, nodeCount);


      DatanodeDetails staleNode = TestUtils.createRandomDatanodeAndRegister(
          nodeManager);

      // Heartbeat once
      nodeManager.processHeartbeat(staleNode);

      // Heartbeat all other nodes.
      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
      }

      // Wait for 2 seconds .. and heartbeat good nodes again.
      Thread.sleep(2 * 1000);

      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
      }

      // Wait for 2 seconds, wait a total of 4 seconds to make sure that the
      // node moves into stale state.
      Thread.sleep(2 * 1000);
      List<DatanodeDetails> staleNodeList = nodeManager.getNodes(STALE);
      assertEquals("Expected to find 1 stale node",
          1, nodeManager.getNodeCount(STALE));
      assertEquals("Expected to find 1 stale node",
          1, staleNodeList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getUuid(), staleNodeList.get(0).getUuid());
      Thread.sleep(1000);

      // heartbeat good nodes again.
      for (DatanodeDetails dn : nodeList) {
        nodeManager.processHeartbeat(dn);
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
      List<DatanodeDetails> deadNodeList = nodeManager.getNodes(DEAD);
      assertEquals("Expected to find 1 dead node", 1,
          nodeManager.getNodeCount(DEAD));
      assertEquals("Expected to find 1 dead node",
          1, deadNodeList.size());
      assertEquals("Dead node is not the expected ID", staleNode
          .getUuid(), deadNodeList.get(0).getUuid());
    }
  }

  /**
   * Check for NPE when datanodeDetails is passed null for sendHeartbeat.
   *
   * @throws IOException
   */
  @Test
  public void testScmCheckForErrorOnNullDatanodeDetails()
      throws IOException, AuthenticationException {
    try (SCMNodeManager nodeManager = createNodeManager(getConf())) {
      nodeManager.processHeartbeat(null);
    } catch (NullPointerException npe) {
      GenericTestUtils.assertExceptionContains("Heartbeat is missing " +
          "DatanodeDetails.", npe);
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

  @Test
  public void testScmClusterIsInExpectedState1()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    /**
     * Cluster state: Healthy: All nodes are heartbeat-ing like normal.
     */
    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails healthyNode =
          TestUtils.createRandomDatanodeAndRegister(nodeManager);
      DatanodeDetails staleNode =
          TestUtils.createRandomDatanodeAndRegister(nodeManager);
      DatanodeDetails deadNode =
          TestUtils.createRandomDatanodeAndRegister(nodeManager);
      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);

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

      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);

      Thread.sleep(1500);
      nodeManager.processHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);
      assertEquals(1, nodeManager.getNodeCount(HEALTHY));


      // 3.5 seconds from last heartbeat for the stale and deadNode. So those
      //  2 nodes must move to Stale state and the healthy node must
      // remain in the healthy State.
      List<DatanodeDetails> healthyList = nodeManager.getNodes(HEALTHY);
      assertEquals("Expected one healthy node", 1, healthyList.size());
      assertEquals("Healthy node is not the expected ID", healthyNode
          .getUuid(), healthyList.get(0).getUuid());

      assertEquals(2, nodeManager.getNodeCount(STALE));

      /**
       * Cluster State: Allow healthyNode to remain in healthy state and
       * staleNode to move to stale state and deadNode to move to dead state.
       */

      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      Thread.sleep(1500);
      nodeManager.processHeartbeat(healthyNode);
      Thread.sleep(2 * 1000);

      // 3.5 seconds have elapsed for stale node, so it moves into Stale.
      // 7 seconds have elapsed for dead node, so it moves into dead.
      // 2 Seconds have elapsed for healthy node, so it stays in healthy state.
      healthyList = nodeManager.getNodes(HEALTHY);
      List<DatanodeDetails> staleList = nodeManager.getNodes(STALE);
      List<DatanodeDetails> deadList = nodeManager.getNodes(DEAD);

      assertEquals(3, nodeManager.getAllNodes().size());
      assertEquals(1, nodeManager.getNodeCount(HEALTHY));
      assertEquals(1, nodeManager.getNodeCount(STALE));
      assertEquals(1, nodeManager.getNodeCount(DEAD));

      assertEquals("Expected one healthy node",
          1, healthyList.size());
      assertEquals("Healthy node is not the expected ID", healthyNode
          .getUuid(), healthyList.get(0).getUuid());

      assertEquals("Expected one stale node",
          1, staleList.size());
      assertEquals("Stale node is not the expected ID", staleNode
          .getUuid(), staleList.get(0).getUuid());

      assertEquals("Expected one dead node",
          1, deadList.size());
      assertEquals("Dead node is not the expected ID", deadNode
          .getUuid(), deadList.get(0).getUuid());
      /**
       * Cluster State : let us heartbeat all the nodes and verify that we get
       * back all the nodes in healthy state.
       */
      nodeManager.processHeartbeat(healthyNode);
      nodeManager.processHeartbeat(staleNode);
      nodeManager.processHeartbeat(deadNode);
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
  private void heartbeatNodeSet(SCMNodeManager manager,
                                List<DatanodeDetails> list,
                                int sleepDuration) throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      for (DatanodeDetails dn : list) {
        manager.processHeartbeat(dn);
      }
      Thread.sleep(sleepDuration);
    }
  }

  /**
   * Create a set of Nodes with a given prefix.
   *
   * @param count  - number of nodes.
   * @return List of Nodes.
   */
  private List<DatanodeDetails> createNodeSet(SCMNodeManager nodeManager, int
      count) {
    List<DatanodeDetails> list = new ArrayList<>();
    for (int x = 0; x < count; x++) {
      DatanodeDetails datanodeDetails = TestUtils
          .createRandomDatanodeAndRegister(nodeManager);
      list.add(datanodeDetails);
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
      HddsProtos.NodeState state) {
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
  public void testScmClusterIsInExpectedState2()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    final int healthyCount = 5000;
    final int staleCount = 100;
    final int deadCount = 10;

    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);


    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> healthyNodeList = createNodeSet(nodeManager,
          healthyCount);
      List<DatanodeDetails> staleNodeList = createNodeSet(nodeManager,
          staleCount);
      List<DatanodeDetails> deadNodeList = createNodeSet(nodeManager,
          deadCount);

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
      for (DatanodeDetails dn : deadNodeList) {
        nodeManager.processHeartbeat(dn);
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

      List<DatanodeDetails> deadList = nodeManager.getNodes(DEAD);

      for (DatanodeDetails node : deadList) {
        assertTrue(deadNodeList.contains(node));
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
  public void testScmCanHandleScale()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    final int healthyCount = 3000;
    final int staleCount = 3000;
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1,
        SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3 * 1000,
        MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6 * 1000,
        MILLISECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      List<DatanodeDetails> healthyList = createNodeSet(nodeManager,
          healthyCount);
      List<DatanodeDetails> staleList = createNodeSet(nodeManager,
          staleCount);

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
   * Test multiple nodes sending initial heartbeat with their node report.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  @Test
  @Ignore
  // TODO: Enable this after we implement NodeReportEvent handler.
  public void testScmStatsFromNodeReport()
      throws IOException, InterruptedException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 1000,
        MILLISECONDS);
    final int nodeCount = 10;
    final long capacity = 2000;
    final long used = 100;
    final long remaining = capacity - used;

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      for (int x = 0; x < nodeCount; x++) {
        DatanodeDetails datanodeDetails = TestUtils
            .createRandomDatanodeAndRegister(nodeManager);
        UUID dnId = datanodeDetails.getUuid();
        long free = capacity - used;
        String storagePath = testDir.getAbsolutePath() + "/" + dnId;
        StorageReportProto report = TestUtils
            .createStorageReport(dnId, storagePath, capacity, used, free, null);
        nodeManager.processHeartbeat(datanodeDetails);
      }
      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
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
  @Ignore
  // TODO: Enable this after we implement NodeReportEvent handler.
  public void testScmNodeReportUpdate()
      throws IOException, InterruptedException, TimeoutException,
      AuthenticationException {
    OzoneConfiguration conf = getConf();
    final int heartbeatCount = 5;
    final int nodeCount = 1;
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    try (SCMNodeManager nodeManager = createNodeManager(conf)) {
      DatanodeDetails datanodeDetails =
          TestUtils.createRandomDatanodeAndRegister(nodeManager);
      final long capacity = 2000;
      final long usedPerHeartbeat = 100;
      UUID dnId = datanodeDetails.getUuid();
      for (int x = 0; x < heartbeatCount; x++) {
        long scmUsed = x * usedPerHeartbeat;
        long remaining = capacity - scmUsed;
        String storagePath = testDir.getAbsolutePath() + "/" + dnId;
        StorageReportProto report = TestUtils
            .createStorageReport(dnId, storagePath, capacity, scmUsed,
                remaining, null);

        nodeManager.processHeartbeat(datanodeDetails);
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
      long nodeCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, nodeCapacity);

      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);

      // Compare the result from
      // NodeManager#getNodeStats and NodeManager#getNodeStat
      SCMNodeStat stat1 = nodeManager.getNodeStats().
          get(datanodeDetails.getUuid());
      SCMNodeStat stat2 = nodeManager.getNodeStat(datanodeDetails).get();
      assertEquals(stat1, stat2);

      // Wait up to 4s so that the node becomes stale
      // Verify the usage info should be unchanged.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(STALE) == 1, 100,
          4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());

      foundCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get()
          .getScmUsed().get();
      assertEquals(expectedScmUsed, foundScmUsed);

      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get().
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

      nodeManager.processHeartbeat(datanodeDetails);

      // Wait up to 5 seconds so that the dead node becomes healthy
      // Verify usage info should be updated.
      GenericTestUtils.waitFor(
          () -> nodeManager.getNodeCount(HEALTHY) == 1,
          100, 5 * 1000);
      GenericTestUtils.waitFor(
          () -> nodeManager.getStats().getScmUsed().get() == expectedScmUsed,
          100, 4 * 1000);
      assertEquals(nodeCount, nodeManager.getNodeStats().size());
      foundCapacity = nodeManager.getNodeStat(datanodeDetails).get()
          .getCapacity().get();
      assertEquals(capacity, foundCapacity);
      foundScmUsed = nodeManager.getNodeStat(datanodeDetails).get().getScmUsed()
          .get();
      assertEquals(expectedScmUsed, foundScmUsed);
      foundRemaining = nodeManager.getNodeStat(datanodeDetails).get()
          .getRemaining().get();
      assertEquals(expectedRemaining, foundRemaining);
    }
  }

  @Test
  public void testHandlingSCMCommandEvent()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = getConf();
    conf.getTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);

    DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
    UUID dnId = datanodeDetails.getUuid();
    String storagePath = testDir.getAbsolutePath() + "/" + dnId;
    StorageReportProto report =
        TestUtils.createStorageReport(dnId, storagePath, 100, 10, 90, null);

    EventQueue eq = new EventQueue();
    try (SCMNodeManager nodemanager = createNodeManager(conf)) {
      eq.addHandler(DATANODE_COMMAND, nodemanager);

      nodemanager
          .register(datanodeDetails, TestUtils.createNodeReport(report),
                  TestUtils.getRandomPipelineReports());
      eq.fireEvent(DATANODE_COMMAND,
          new CommandForDatanode<>(datanodeDetails.getUuid(),
              new CloseContainerCommand(1L,
                  PipelineID.randomId())));

      eq.processAll(1000L);
      List<SCMCommand> command =
          nodemanager.processHeartbeat(datanodeDetails);
      Assert.assertEquals(1, command.size());
      Assert
          .assertEquals(command.get(0).getClass(), CloseContainerCommand.class);
    } catch (IOException e) {
      e.printStackTrace();
      throw  e;
    }
  }

}
