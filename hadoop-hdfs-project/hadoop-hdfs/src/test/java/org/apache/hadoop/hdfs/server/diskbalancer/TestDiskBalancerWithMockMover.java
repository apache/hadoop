/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.diskbalancer;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import java.util.function.Supplier;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancer;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkItem;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ClusterConnector;
import org.apache.hadoop.hdfs.server.diskbalancer.connectors.ConnectorFactory;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.GreedyPlanner;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.MoveStep;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.NodePlan;
import org.apache.hadoop.hdfs.server.diskbalancer.planner.Step;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus.Result.NO_PLAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests diskbalancer with a mock mover.
 */
public class TestDiskBalancerWithMockMover {
  static final Logger LOG =
      LoggerFactory.getLogger(TestDiskBalancerWithMockMover.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String PLAN_FILE = "/system/current.plan.json";
  private MiniDFSCluster cluster;
  private String sourceName;
  private String destName;
  private String sourceUUID;
  private String destUUID;
  private String nodeID;
  private DataNode dataNode;

  /**
   * Checks that we return the right error if diskbalancer is not enabled.
   */
  @Test
  public void testDiskBalancerDisabled() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, false);
    restartDataNode();

    TestMover blockMover = new TestMover(cluster.getDataNodes()
        .get(0).getFSDataset());

    DiskBalancer balancer = new DiskBalancerBuilder(conf)
        .setMover(blockMover)
        .build();

    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.DISK_BALANCER_NOT_ENABLED));

    balancer.queryWorkStatus();
  }

  /**
   * Checks that Enable flag works correctly.
   *
   * @throws DiskBalancerException
   */
  @Test
  public void testDiskBalancerEnabled() throws DiskBalancerException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);

    TestMover blockMover = new TestMover(cluster.getDataNodes()
        .get(0).getFSDataset());

    DiskBalancer balancer = new DiskBalancerBuilder(conf)
        .setMover(blockMover)
        .build();

    DiskBalancerWorkStatus status = balancer.queryWorkStatus();
    assertEquals(NO_PLAN, status.getResult());

  }

  private void executeSubmitPlan(NodePlan plan, DiskBalancer balancer,
                                 int version) throws IOException {
    String planJson = plan.toJson();
    String planID = DigestUtils.shaHex(planJson);
    balancer.submitPlan(planID, version, PLAN_FILE, planJson, false);
  }

  private void executeSubmitPlan(NodePlan plan, DiskBalancer balancer)
      throws IOException {
    executeSubmitPlan(plan, balancer, 1);
  }

  /**
   * Test a second submit plan fails.
   *
   * @throws Exception
   */
  @Test
  public void testResubmitDiskBalancerPlan() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();

    // ask block mover to get stuck in copy block
    mockMoverHelper.getBlockMover().setSleep();
    executeSubmitPlan(plan, balancer);
    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.PLAN_ALREADY_IN_PROGRESS));
    executeSubmitPlan(plan, balancer);

    // Not needed but this is the cleanup step.
    mockMoverHelper.getBlockMover().clearSleep();
  }

  @Test
  public void testSubmitDiskBalancerPlan() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    final DiskBalancer balancer = mockMoverHelper.getBalancer();

    executeSubmitPlan(plan, balancer);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return balancer.queryWorkStatus().getResult() ==
              DiskBalancerWorkStatus.Result.PLAN_DONE;
        } catch (IOException ex) {
          return false;
        }
      }
    }, 1000, 100000);

    // Asserts that submit plan caused an execution in the background.
    assertTrue(mockMoverHelper.getBlockMover().getRunCount() == 1);
  }

  @Test
  public void testSubmitWithOlderPlan() throws Exception {
    final long millisecondInAnHour = 1000 * 60 * 60L;
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();

    plan.setTimeStamp(Time.now() - (32 * millisecondInAnHour));
    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.OLD_PLAN_SUBMITTED));
    executeSubmitPlan(plan, balancer);
  }

  @Test
  public void testSubmitWithOldInvalidVersion() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();

    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.INVALID_PLAN_VERSION));

    // Plan version is invalid -- there is no version 0.
    executeSubmitPlan(plan, balancer, 0);
  }

  @Test
  public void testSubmitWithNullPlan() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();
    String planJson = plan.toJson();
    String planID = DigestUtils.shaHex(planJson);

    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.INVALID_PLAN));

    balancer.submitPlan(planID, 1, "no-plan-file.json", null, false);
  }

  @Test
  public void testSubmitWithInvalidHash() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();


    String planJson = plan.toJson();
    String planID = DigestUtils.shaHex(planJson);
    char repChar = planID.charAt(0);
    repChar++;

    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.INVALID_PLAN_HASH));
    balancer.submitPlan(planID.replace(planID.charAt(0), repChar),
        1, PLAN_FILE, planJson, false);

  }

  /**
   * Test Cancel Plan.
   *
   * @throws Exception
   */
  @Test
  public void testCancelDiskBalancerPlan() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();


    // ask block mover to delay execution
    mockMoverHelper.getBlockMover().setSleep();
    executeSubmitPlan(plan, balancer);


    String planJson = plan.toJson();
    String planID = DigestUtils.shaHex(planJson);
    balancer.cancelPlan(planID);

    DiskBalancerWorkStatus status = balancer.queryWorkStatus();
    assertEquals(DiskBalancerWorkStatus.Result.PLAN_CANCELLED,
        status.getResult());


    executeSubmitPlan(plan, balancer);

    // Send a Wrong cancellation request.
    char first = planID.charAt(0);
    first++;
    thrown.expect(DiskBalancerException.class);
    thrown.expect(new DiskBalancerResultVerifier(DiskBalancerException
        .Result.NO_SUCH_PLAN));
    balancer.cancelPlan(planID.replace(planID.charAt(0), first));

    // Now cancel the real one
    balancer.cancelPlan(planID);
    mockMoverHelper.getBlockMover().clearSleep(); // unblock mover.

    status = balancer.queryWorkStatus();
    assertEquals(DiskBalancerWorkStatus.Result.PLAN_CANCELLED,
        status.getResult());

  }


  /**
   * Test Custom bandwidth.
   *
   * @throws Exception
   */
  @Test
  public void testCustomBandwidth() throws Exception {
    MockMoverHelper mockMoverHelper = new MockMoverHelper().invoke();
    NodePlan plan = mockMoverHelper.getPlan();
    DiskBalancer balancer = mockMoverHelper.getBalancer();

    for(Step step : plan.getVolumeSetPlans()){
      MoveStep tempStep = (MoveStep) step;
      tempStep.setBandwidth(100);
    }
    executeSubmitPlan(plan, balancer);
    DiskBalancerWorkStatus status = balancer
        .queryWorkStatus();
    assertNotNull(status);

    DiskBalancerWorkStatus.DiskBalancerWorkEntry entry =
        balancer.queryWorkStatus().getCurrentState().get(0);
    assertEquals(100L, entry.getWorkItem().getBandwidth());

  }


  @Before
  public void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int numStoragesPerDn = 2;
    cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(3)
        .storagesPerDatanode(numStoragesPerDn)
        .build();
    cluster.waitActive();
    dataNode = cluster.getDataNodes().get(0);
    FsDatasetSpi.FsVolumeReferences references = dataNode.getFSDataset()
        .getFsVolumeReferences();

    nodeID = dataNode.getDatanodeUuid();
    sourceName = references.get(0).getBaseURI().getPath();
    destName = references.get(1).getBaseURI().getPath();
    sourceUUID = references.get(0).getStorageID();
    destUUID = references.get(1).getStorageID();
    references.close();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void restartDataNode() throws IOException {
    if (cluster != null) {
      cluster.restartDataNode(0);
    }
  }

  /**
   * Allows us to control mover class for test purposes.
   */
  public static class TestMover implements DiskBalancer.BlockMover {

    private AtomicBoolean shouldRun;
    private FsDatasetSpi dataset;
    private int runCount;
    private volatile boolean sleepInCopyBlocks;
    private long delay;

    public TestMover(FsDatasetSpi dataset) {
      this.dataset = dataset;
      this.shouldRun = new AtomicBoolean(false);
    }

    public void setSleep() {
      sleepInCopyBlocks = true;
    }

    public void clearSleep() {
      sleepInCopyBlocks = false;
    }

    public void setDelay(long milliseconds) {
      this.delay = milliseconds;
    }

    /**
     * Copies blocks from a set of volumes.
     *
     * @param pair - Source and Destination Volumes.
     * @param item - Number of bytes to move from volumes.
     */
    @Override
    public void copyBlocks(DiskBalancer.VolumePair pair,
                           DiskBalancerWorkItem item) {

      try {
        // get stuck if we are asked to sleep.
        while (sleepInCopyBlocks) {
          if (!this.shouldRun()) {
            return;
          }
          Thread.sleep(10);
        }
        if (delay > 0) {
          Thread.sleep(delay);
        }
        synchronized (this) {
          if (shouldRun()) {
            runCount++;
          }
        }
      } catch (InterruptedException ex) {
        // A failure here can be safely ignored with no impact for tests.
        LOG.error(ex.toString());
      }
    }

    /**
     * Sets copyblocks into runnable state.
     */
    @Override
    public void setRunnable() {
      this.shouldRun.set(true);
    }

    /**
     * Signals copy block to exit.
     */
    @Override
    public void setExitFlag() {
      this.shouldRun.set(false);
    }

    /**
     * Returns the shouldRun boolean flag.
     */
    public boolean shouldRun() {
      return this.shouldRun.get();
    }

    @Override
    public FsDatasetSpi getDataset() {
      return this.dataset;
    }

    /**
     * Returns time when this plan started executing.
     *
     * @return Start time in milliseconds.
     */
    @Override
    public long getStartTime() {
      return 0;
    }

    /**
     * Number of seconds elapsed.
     *
     * @return time in seconds
     */
    @Override
    public long getElapsedSeconds() {
      return 0;
    }

    public int getRunCount() {
      synchronized (this) {
        LOG.info("Run count : " + runCount);
        return runCount;
      }
    }
  }

  private class MockMoverHelper {
    private DiskBalancer balancer;
    private NodePlan plan;
    private TestMover blockMover;

    public DiskBalancer getBalancer() {
      return balancer;
    }

    public NodePlan getPlan() {
      return plan;
    }

    public TestMover getBlockMover() {
      return blockMover;
    }

    public MockMoverHelper invoke() throws Exception {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_DISK_BALANCER_ENABLED, true);
      restartDataNode();

      blockMover = new TestMover(dataNode.getFSDataset());
      blockMover.setRunnable();

      balancer = new DiskBalancerBuilder(conf)
          .setMover(blockMover)
          .setNodeID(nodeID)
          .build();

      DiskBalancerCluster diskBalancerCluster = new DiskBalancerClusterBuilder()
          .setClusterSource("/diskBalancer/data-cluster-3node-3disk.json")
          .build();

      plan = new PlanBuilder(diskBalancerCluster, nodeID)
          .setPathMap(sourceName, destName)
          .setUUIDMap(sourceUUID, destUUID)
          .build();
      return this;
    }
  }

  private static class DiskBalancerBuilder {
    private TestMover blockMover;
    private Configuration conf;
    private String nodeID;

    public DiskBalancerBuilder(Configuration conf) {
      this.conf = conf;
    }

    public DiskBalancerBuilder setNodeID(String nodeID) {
      this.nodeID = nodeID;
      return this;
    }

    public DiskBalancerBuilder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public DiskBalancerBuilder setMover(TestMover mover) {
      this.blockMover = mover;
      return this;
    }

    public DiskBalancerBuilder setRunnable() {
      blockMover.setRunnable();
      return this;
    }

    public DiskBalancer build() {
      Preconditions.checkNotNull(blockMover);
      return new DiskBalancer(nodeID, conf,
          blockMover);
    }
  }

  private static class DiskBalancerClusterBuilder {
    private String jsonFilePath;
    private Configuration conf;

    public DiskBalancerClusterBuilder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public DiskBalancerClusterBuilder setClusterSource(String jsonFilePath)
        throws Exception {
      this.jsonFilePath = jsonFilePath;
      return this;
    }

    public DiskBalancerCluster build() throws Exception {
      DiskBalancerCluster diskBalancerCluster;
      URI clusterJson = getClass().getResource(jsonFilePath).toURI();
      ClusterConnector jsonConnector =
          ConnectorFactory.getCluster(clusterJson, conf);
      diskBalancerCluster = new DiskBalancerCluster(jsonConnector);
      diskBalancerCluster.readClusterInfo();
      diskBalancerCluster.setNodesToProcess(diskBalancerCluster.getNodes());
      return diskBalancerCluster;
    }
  }

  private static class PlanBuilder {
    private String sourcePath;
    private String destPath;
    private String sourceUUID;
    private String destUUID;
    private DiskBalancerCluster balancerCluster;
    private String nodeID;

    public PlanBuilder(DiskBalancerCluster balancerCluster, String nodeID) {
      this.balancerCluster = balancerCluster;
      this.nodeID = nodeID;
    }

    public PlanBuilder setPathMap(String sourcePath, String destPath) {
      this.sourcePath = sourcePath;
      this.destPath = destPath;
      return this;
    }

    public PlanBuilder setUUIDMap(String sourceUUID, String destUUID) {
      this.sourceUUID = sourceUUID;
      this.destUUID = destUUID;
      return this;
    }

    public NodePlan build() throws Exception {
      final int dnIndex = 0;
      Preconditions.checkNotNull(balancerCluster);
      Preconditions.checkState(nodeID.length() > 0);

      DiskBalancerDataNode node = balancerCluster.getNodes().get(dnIndex);
      node.setDataNodeUUID(nodeID);
      GreedyPlanner planner = new GreedyPlanner(10.0f, node);
      NodePlan plan = new NodePlan(node.getDataNodeName(),
          node.getDataNodePort());
      planner.balanceVolumeSet(node, node.getVolumeSets().get("DISK"), plan);
      setVolumeNames(plan);
      return plan;
    }

    private void setVolumeNames(NodePlan plan) {
      Iterator<Step> iter = plan.getVolumeSetPlans().iterator();
      while (iter.hasNext()) {
        MoveStep nextStep = (MoveStep) iter.next();
        nextStep.getSourceVolume().setPath(sourcePath);
        nextStep.getSourceVolume().setUuid(sourceUUID);
        nextStep.getDestinationVolume().setPath(destPath);
        nextStep.getDestinationVolume().setUuid(destUUID);
      }
    }

  }
}

