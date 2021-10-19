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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

/**
 * Test for Distributed Shell With Multiple Node Managers.
 * Parameter 0 tests with Single Node Placement and
 * parameter 1 tests with Multiple Node Placement.
 */
@RunWith(value = Parameterized.class)
public class TestDSWithMultipleNodeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDSWithMultipleNodeManager.class);

  private static final int NUM_NMS = 2;
  private static final String POLICY_CLASS_NAME =
      ResourceUsageMultiNodeLookupPolicy.class.getName();
  private final Boolean multiNodePlacementEnabled;
  @Rule
  public TestName name = new TestName();
  @Rule
  public Timeout globalTimeout =
      new Timeout(DistributedShellBaseTest.TEST_TIME_OUT,
          TimeUnit.MILLISECONDS);
  private DistributedShellBaseTest distShellTest;
  private Client dsClient;

  public TestDSWithMultipleNodeManager(Boolean multiNodePlacementEnabled) {
    this.multiNodePlacementEnabled = multiNodePlacementEnabled;
  }

  @Parameterized.Parameters
  public static Collection<Boolean> getParams() {
    return Arrays.asList(false, true);
  }

  private YarnConfiguration getConfiguration(
      boolean multiNodePlacementConfigs) {
    YarnConfiguration conf = new YarnConfiguration();
    if (multiNodePlacementConfigs) {
      conf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
          "resource-based");
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          "resource-based");
      String policyName =
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
          + ".resource-based" + ".class";
      conf.set(policyName, POLICY_CLASS_NAME);
      conf.setBoolean(
          CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED, true);
    }
    return conf;
  }

  @BeforeClass
  public static void setupUnitTests() throws Exception {
    TestDSTimelineV10.setupUnitTests();
  }

  @AfterClass
  public static void tearDownUnitTests() throws Exception {
    TestDSTimelineV10.tearDownUnitTests();
  }

  @Before
  public void setup() throws Exception {
    distShellTest = new TestDSTimelineV10();
    distShellTest.setupInternal(NUM_NMS,
        getConfiguration(multiNodePlacementEnabled));
  }

  @After
  public void tearDown() throws Exception {
    if (dsClient != null) {
      dsClient.sendStopSignal();
      dsClient = null;
    }
    if (distShellTest != null) {
      distShellTest.tearDown();
      distShellTest = null;
    }
  }

  private void initializeNodeLabels() throws IOException {
    RMContext rmContext = distShellTest.getResourceManager(0).getRMContext();
    // Setup node labels
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    Set<String> labels = new HashSet<>();
    labels.add("x");
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(labels);

    // Setup queue access to node labels
    distShellTest.setConfiguration(PREFIX + "root.accessible-node-labels", "x");
    distShellTest.setConfiguration(
        PREFIX + "root.accessible-node-labels.x.capacity", "100");
    distShellTest.setConfiguration(
        PREFIX + "root.default.accessible-node-labels", "x");
    distShellTest.setConfiguration(PREFIX
        + "root.default.accessible-node-labels.x.capacity", "100");

    rmContext.getScheduler().reinitialize(distShellTest.getConfiguration(),
        rmContext);

    // Fetch node-ids from yarn cluster
    NodeId[] nodeIds = new NodeId[NUM_NMS];
    for (int i = 0; i < NUM_NMS; i++) {
      NodeManager mgr = distShellTest.getNodeManager(i);
      nodeIds[i] = mgr.getNMContext().getNodeId();
    }

    // Set label x to NM[1]
    labelsMgr.addLabelsToNode(ImmutableMap.of(nodeIds[1], labels));
  }

  @Test
  public void testDSShellWithNodeLabelExpression() throws Exception {
    NMContainerMonitor containerMonitorRunner = null;
    initializeNodeLabels();

    try {
      // Start NMContainerMonitor
      containerMonitorRunner = new NMContainerMonitor();
      containerMonitorRunner.start();

      // Submit a job which will sleep for 60 sec
      String[] args =
          DistributedShellBaseTest.createArguments(() -> generateAppName(),
              "--num_containers",
              "4",
              "--shell_command",
              "sleep",
              "--shell_args",
              "15",
              "--master_memory",
              "512",
              "--master_vcores",
              "2",
              "--container_memory",
              "128",
              "--container_vcores",
              "1",
              "--node_label_expression",
              "x"
          );

      LOG.info("Initializing DS Client");
      dsClient =
          new Client(
              new Configuration(distShellTest.getYarnClusterConfiguration()));
      Assert.assertTrue(dsClient.init(args));
      LOG.info("Running DS Client");
      boolean result = dsClient.run();
      LOG.info("Client run completed. Result={}", result);

      containerMonitorRunner.stopMonitoring();

      // Check maximum number of containers on each NMs
      int[] maxRunningContainersOnNMs =
          containerMonitorRunner.getMaxRunningContainersReport();
      // Check no container allocated on NM[0]
      Assert.assertEquals(0, maxRunningContainersOnNMs[0]);
      // Check there are some containers allocated on NM[1]
      Assert.assertTrue(maxRunningContainersOnNMs[1] > 0);
    } finally {
      if (containerMonitorRunner != null) {
        containerMonitorRunner.stopMonitoring();
        containerMonitorRunner.join();
      }
    }
  }

  @Test
  public void testDistributedShellWithPlacementConstraint()
      throws Exception {
    NMContainerMonitor containerMonitorRunner = null;
    String[] args =
        DistributedShellBaseTest.createArguments(() -> generateAppName(),
            "1",
            "--shell_command",
            DistributedShellBaseTest.getSleepCommand(15),
            "--placement_spec",
            "zk(1),NOTIN,NODE,zk:spark(1),NOTIN,NODE,zk"
        );
    try {
      containerMonitorRunner = new NMContainerMonitor();
      containerMonitorRunner.start();

      LOG.info("Initializing DS Client with args {}", Arrays.toString(args));
      dsClient =
          new Client(
              new Configuration(distShellTest.getYarnClusterConfiguration()));
      Assert.assertTrue(dsClient.init(args));
      LOG.info("Running DS Client");
      boolean result = dsClient.run();
      LOG.info("Client run completed. Result={}", result);

      containerMonitorRunner.stopMonitoring();

      ConcurrentMap<ApplicationId, RMApp> apps =
          distShellTest.getResourceManager().getRMContext().getRMApps();
      RMApp app = apps.values().iterator().next();
      RMAppAttempt appAttempt = app.getAppAttempts().values().iterator().next();
      NodeId masterNodeId = appAttempt.getMasterContainer().getNodeId();
      NodeManager nm1 = distShellTest.getNodeManager(0);

      int[] expectedNMsCount = new int[]{1, 1};
      if (nm1.getNMContext().getNodeId().equals(masterNodeId)) {
        expectedNMsCount[0]++;
      } else {
        expectedNMsCount[1]++;
      }

      int[] maxRunningContainersOnNMs =
          containerMonitorRunner.getMaxRunningContainersReport();
      Assert.assertEquals(expectedNMsCount[0], maxRunningContainersOnNMs[0]);
      Assert.assertEquals(expectedNMsCount[1], maxRunningContainersOnNMs[1]);
    } finally {
      if (containerMonitorRunner != null) {
        containerMonitorRunner.stopMonitoring();
        containerMonitorRunner.join();
      }
    }
  }

  @Test
  public void testDistributedShellWithAllocationTagNamespace()
      throws Exception {
    NMContainerMonitor containerMonitorRunner = null;
    Client clientB = null;
    YarnClient yarnClient = null;

    String[] argsA =
        DistributedShellBaseTest.createArguments(() -> generateAppName("001"),
            "--shell_command",
            DistributedShellBaseTest.getSleepCommand(30),
            "--placement_spec",
            "bar(1),notin,node,bar"
        );
    String[] argsB =
        DistributedShellBaseTest.createArguments(() -> generateAppName("002"),
            "1",
            "--shell_command",
            DistributedShellBaseTest.getListCommand(),
            "--placement_spec",
            "foo(3),notin,node,all/bar"
        );

    try {
      containerMonitorRunner = new NMContainerMonitor();
      containerMonitorRunner.start();
      dsClient =
          new Client(
              new Configuration(distShellTest.getYarnClusterConfiguration()));
      dsClient.init(argsA);
      Thread dsClientRunner = new Thread(() -> {
        try {
          dsClient.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      dsClientRunner.start();

      NodeId taskContainerNodeIdA;
      ConcurrentMap<ApplicationId, RMApp> apps;
      AtomicReference<RMApp> appARef = new AtomicReference<>(null);
      AtomicReference<NodeId> masterContainerNodeIdARef =
          new AtomicReference<>(null);
      int[] expectedNMCounts = new int[]{0, 0};

      waitForExpectedNMsCount(expectedNMCounts, appARef,
          masterContainerNodeIdARef);

      NodeId nodeA = distShellTest.getNodeManager(0).getNMContext().
          getNodeId();
      NodeId nodeB = distShellTest.getNodeManager(1).getNMContext().
          getNodeId();
      Assert.assertEquals(2, (expectedNMCounts[0] + expectedNMCounts[1]));
      if (expectedNMCounts[0] != expectedNMCounts[1]) {
        taskContainerNodeIdA = masterContainerNodeIdARef.get();
      } else {
        taskContainerNodeIdA =
            masterContainerNodeIdARef.get().equals(nodeA) ? nodeB : nodeA;
      }

      clientB =
          new Client(
              new Configuration(distShellTest.getYarnClusterConfiguration()));
      clientB.init(argsB);
      Assert.assertTrue(clientB.run());
      containerMonitorRunner.stopMonitoring();
      apps = distShellTest.getResourceManager().getRMContext().getRMApps();
      Iterator<RMApp> it = apps.values().iterator();
      RMApp appB = it.next();
      if (appARef.get().equals(appB)) {
        appB = it.next();
      }
      LOG.info("Allocation Tag NameSpace Applications are={} and {}",
          appARef.get().getApplicationId(), appB.getApplicationId());

      RMAppAttempt appAttemptB =
          appB.getAppAttempts().values().iterator().next();
      NodeId masterContainerNodeIdB =
          appAttemptB.getMasterContainer().getNodeId();

      if (nodeA.equals(masterContainerNodeIdB)) {
        expectedNMCounts[0]++;
      } else {
        expectedNMCounts[1]++;
      }
      if (nodeA.equals(taskContainerNodeIdA)) {
        expectedNMCounts[1] += 3;
      } else {
        expectedNMCounts[0] += 3;
      }
      int[] maxRunningContainersOnNMs =
          containerMonitorRunner.getMaxRunningContainersReport();
      Assert.assertEquals(expectedNMCounts[0], maxRunningContainersOnNMs[0]);
      Assert.assertEquals(expectedNMCounts[1], maxRunningContainersOnNMs[1]);

      try {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(
            new Configuration(distShellTest.getYarnClusterConfiguration()));
        yarnClient.start();
        yarnClient.killApplication(appARef.get().getApplicationId());
      } catch (Exception e) {
        // Ignore Exception while killing a job
        LOG.warn("Exception killing the job: {}", e.getMessage());
      }
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
      if (clientB != null) {
        clientB.sendStopSignal();
      }
      if (containerMonitorRunner != null) {
        containerMonitorRunner.stopMonitoring();
        containerMonitorRunner.join();
      }
    }
  }

  protected String generateAppName() {
    return generateAppName(null);
  }

  protected String generateAppName(String postFix) {
    return name.getMethodName().replaceFirst("test", "")
        .concat(postFix == null ? "" : "-" + postFix);
  }

  private void waitForExpectedNMsCount(int[] expectedNMCounts,
      AtomicReference<RMApp> appARef,
      AtomicReference<NodeId> masterContainerNodeIdARef) throws Exception {
    GenericTestUtils.waitFor(() -> {
      if ((expectedNMCounts[0] + expectedNMCounts[1]) < 2) {
        expectedNMCounts[0] =
            distShellTest.getNodeManager(0).getNMContext()
                .getContainers().size();
        expectedNMCounts[1] =
            distShellTest.getNodeManager(1).getNMContext()
                .getContainers().size();
        return false;
      }
      ConcurrentMap<ApplicationId, RMApp> appIDsMap =
          distShellTest.getResourceManager().getRMContext().getRMApps();
      if (appIDsMap.isEmpty()) {
        return false;
      }
      appARef.set(appIDsMap.values().iterator().next());
      if (appARef.get().getAppAttempts().isEmpty()) {
        return false;
      }
      RMAppAttempt appAttemptA =
          appARef.get().getAppAttempts().values().iterator().next();
      if (appAttemptA.getMasterContainer() == null) {
        return false;
      }
      masterContainerNodeIdARef.set(
          appAttemptA.getMasterContainer().getNodeId());
      return true;
    }, 10, 60000);
  }

  /**
   * Monitor containers running on NMs.
   */
  class NMContainerMonitor extends Thread {
    // The interval of milliseconds of sampling (500ms)
    private final static int SAMPLING_INTERVAL_MS = 500;

    // The maximum number of containers running on each NMs
    private final int[] maxRunningContainersOnNMs = new int[NUM_NMS];
    private final Object quitSignal = new Object();
    private volatile boolean isRunning = true;

    @Override
    public void run() {
      while (isRunning) {
        for (int i = 0; i < NUM_NMS; i++) {
          int nContainers =
              distShellTest.getNodeManager(i).getNMContext()
                  .getContainers().size();
          if (nContainers > maxRunningContainersOnNMs[i]) {
            maxRunningContainersOnNMs[i] = nContainers;
          }
        }
        synchronized (quitSignal) {
          try {
            if (!isRunning) {
              break;
            }
            quitSignal.wait(SAMPLING_INTERVAL_MS);
          } catch (InterruptedException e) {
            LOG.warn("NMContainerMonitor interrupted");
            isRunning = false;
            break;
          }
        }
      }
    }

    public int[] getMaxRunningContainersReport() {
      return maxRunningContainersOnNMs;
    }

    public void stopMonitoring() {
      if (!isRunning) {
        return;
      }
      synchronized (quitSignal) {
        isRunning = false;
        quitSignal.notifyAll();
      }
    }
  }
}
