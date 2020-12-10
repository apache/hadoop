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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDSWithMultipleNodeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDSWithMultipleNodeManager.class);

  static final int NUM_NMS = 2;
  TestDistributedShell distShellTest;

  @Before
  public void setup() throws Exception {
    distShellTest = new TestDistributedShell();
    distShellTest.setupInternal(NUM_NMS);
  }

  @After
  public void tearDown() throws Exception {
    distShellTest.tearDown();
  }

  private void initializeNodeLabels() throws IOException {
    RMContext rmContext = distShellTest.yarnCluster.getResourceManager(0).getRMContext();

    // Setup node labels
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    Set<String> labels = new HashSet<String>();
    labels.add("x");
    labelsMgr.addToCluserNodeLabelsWithDefaultExclusivity(labels);

    // Setup queue access to node labels
    distShellTest.conf.set(PREFIX + "root.accessible-node-labels", "x");
    distShellTest.conf.set(PREFIX + "root.accessible-node-labels.x.capacity",
        "100");
    distShellTest.conf.set(PREFIX + "root.default.accessible-node-labels", "x");
    distShellTest.conf.set(PREFIX
        + "root.default.accessible-node-labels.x.capacity", "100");

    rmContext.getScheduler().reinitialize(distShellTest.conf, rmContext);

    // Fetch node-ids from yarn cluster
    NodeId[] nodeIds = new NodeId[NUM_NMS];
    for (int i = 0; i < NUM_NMS; i++) {
      NodeManager mgr = distShellTest.yarnCluster.getNodeManager(i);
      nodeIds[i] = mgr.getNMContext().getNodeId();
    }

    // Set label x to NM[1]
    labelsMgr.addLabelsToNode(ImmutableMap.of(nodeIds[1], labels));
  }

  @Test(timeout=90000)
  public void testDSShellWithNodeLabelExpression() throws Exception {
    initializeNodeLabels();

    // Start NMContainerMonitor
    NMContainerMonitor mon = new NMContainerMonitor();
    Thread t = new Thread(mon);
    t.start();

    // Submit a job which will sleep for 60 sec
    String[] args = {
        "--jar",
        TestDistributedShell.APPMASTER_JAR,
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
    };

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(distShellTest.yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);

    t.interrupt();

    // Check maximum number of containers on each NMs
    int[] maxRunningContainersOnNMs = mon.getMaxRunningContainersReport();
    // Check no container allocated on NM[0]
    Assert.assertEquals(0, maxRunningContainersOnNMs[0]);
    // Check there're some containers allocated on NM[1]
    Assert.assertTrue(maxRunningContainersOnNMs[1] > 0);
  }

  @Test(timeout = 90000)
  public void testDistributedShellWithPlacementConstraint()
      throws Exception {
    NMContainerMonitor mon = new NMContainerMonitor();
    Thread t = new Thread(mon);
    t.start();

    String[] args = {
        "--jar",
        distShellTest.APPMASTER_JAR,
        "1",
        "--shell_command",
        distShellTest.getSleepCommand(15),
        "--placement_spec",
        "zk(1),NOTIN,NODE,zk:spark(1),NOTIN,NODE,zk"
    };
    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(distShellTest.yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);

    t.interrupt();

    ConcurrentMap<ApplicationId, RMApp> apps = distShellTest.yarnCluster.
        getResourceManager().getRMContext().getRMApps();
    RMApp app = apps.values().iterator().next();
    RMAppAttempt appAttempt = app.getAppAttempts().values().iterator().next();
    NodeId masterNodeId = appAttempt.getMasterContainer().getNodeId();
    NodeManager nm1 = distShellTest.yarnCluster.getNodeManager(0);

    int expectedNM1Count = 1;
    int expectedNM2Count = 1;
    if (nm1.getNMContext().getNodeId().equals(masterNodeId)) {
      expectedNM1Count++;
    } else {
      expectedNM2Count++;
    }

    int[] maxRunningContainersOnNMs = mon.getMaxRunningContainersReport();
    Assert.assertEquals(expectedNM1Count, maxRunningContainersOnNMs[0]);
    Assert.assertEquals(expectedNM2Count, maxRunningContainersOnNMs[1]);
  }

  @Test(timeout = 90000)
  public void testDistributedShellWithAllocationTagNamespace()
      throws Exception {
    NMContainerMonitor mon = new NMContainerMonitor();
    Thread monitorThread = new Thread(mon);
    monitorThread.start();

    String[] argsA = {
        "--jar",
        distShellTest.APPMASTER_JAR,
        "--shell_command",
        distShellTest.getSleepCommand(30),
        "--placement_spec",
        "bar(1),notin,node,bar"
    };
    final Client clientA =
        new Client(new Configuration(distShellTest.yarnCluster.getConfig()));
    clientA.init(argsA);
    final AtomicBoolean resultA = new AtomicBoolean(false);
    Thread t = new Thread() {
      public void run() {
        try {
          resultA.set(clientA.run());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();

    NodeId masterContainerNodeIdA;
    NodeId taskContainerNodeIdA;
    ConcurrentMap<ApplicationId, RMApp> apps;
    RMApp appA;

    int expectedNM1Count = 0;
    int expectedNM2Count = 0;
    while (true) {
      if ((expectedNM1Count + expectedNM2Count) < 2) {
        expectedNM1Count = distShellTest.yarnCluster.getNodeManager(0).
            getNMContext().getContainers().size();
        expectedNM2Count = distShellTest.yarnCluster.getNodeManager(1).
            getNMContext().getContainers().size();
        continue;
      }
      apps = distShellTest.yarnCluster.getResourceManager().getRMContext().
          getRMApps();
      if (apps.isEmpty()) {
        Thread.sleep(10);
        continue;
      }
      appA = apps.values().iterator().next();
      if (appA.getAppAttempts().isEmpty()) {
        Thread.sleep(10);
        continue;
      }
      RMAppAttempt appAttemptA = appA.getAppAttempts().values().iterator().
          next();
      if (appAttemptA.getMasterContainer() == null) {
        Thread.sleep(10);
        continue;
      }
      masterContainerNodeIdA = appAttemptA.getMasterContainer().getNodeId();
      break;
    }

    NodeId nodeA = distShellTest.yarnCluster.getNodeManager(0).getNMContext().
        getNodeId();
    NodeId nodeB = distShellTest.yarnCluster.getNodeManager(1).getNMContext().
        getNodeId();
    Assert.assertEquals(2, (expectedNM1Count + expectedNM2Count));

    if (expectedNM1Count != expectedNM2Count) {
      taskContainerNodeIdA = masterContainerNodeIdA;
    } else {
      taskContainerNodeIdA = masterContainerNodeIdA.equals(nodeA) ? nodeB :
          nodeA;
    }

    String[] argsB = {
        "--jar",
        distShellTest.APPMASTER_JAR,
        "1",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--placement_spec",
        "foo(3),notin,node,all/bar"
    };
    final Client clientB = new Client(new Configuration(distShellTest.
        yarnCluster.getConfig()));
    clientB.init(argsB);
    boolean resultB = clientB.run();
    Assert.assertTrue(resultB);

    monitorThread.interrupt();
    apps = distShellTest.yarnCluster.getResourceManager().getRMContext().
        getRMApps();
    Iterator<RMApp> it = apps.values().iterator();
    RMApp appB = it.next();
    if (appA.equals(appB)) {
      appB = it.next();
    }
    LOG.info("Allocation Tag NameSpace Applications are=" + appA.
        getApplicationId() + " and " + appB.getApplicationId());

    RMAppAttempt appAttemptB = appB.getAppAttempts().values().iterator().
        next();
    NodeId masterContainerNodeIdB = appAttemptB.getMasterContainer().
        getNodeId();

    if (nodeA.equals(masterContainerNodeIdB)) {
      expectedNM1Count += 1;
    } else {
      expectedNM2Count += 1;
    }
    if (nodeA.equals(taskContainerNodeIdA)) {
      expectedNM2Count += 3;
    } else {
      expectedNM1Count += 3;
    }
    int[] maxRunningContainersOnNMs = mon.getMaxRunningContainersReport();
    Assert.assertEquals(expectedNM1Count, maxRunningContainersOnNMs[0]);
    Assert.assertEquals(expectedNM2Count, maxRunningContainersOnNMs[1]);

    try {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(new Configuration(distShellTest.yarnCluster.
          getConfig()));
      yarnClient.start();
      yarnClient.killApplication(appA.getApplicationId());
    } catch (Exception e) {
     // Ignore Exception while killing a job
    }
  }

  /**
   * Monitor containers running on NMs
   */
  class NMContainerMonitor implements Runnable {
    // The interval of milliseconds of sampling (500ms)
    final static int SAMPLING_INTERVAL_MS = 500;

    // The maximum number of containers running on each NMs
    int[] maxRunningContainersOnNMs = new int[NUM_NMS];

    @Override
    public void run() {
      while (true) {
        for (int i = 0; i < NUM_NMS; i++) {
          int nContainers =
              distShellTest.yarnCluster.getNodeManager(i).getNMContext()
                  .getContainers().size();
          if (nContainers > maxRunningContainersOnNMs[i]) {
            maxRunningContainersOnNMs[i] = nContainers;
          }
        }
        try {
          Thread.sleep(SAMPLING_INTERVAL_MS);
        } catch (InterruptedException e) {
          e.printStackTrace();
          break;
        }
      }
    }

    public int[] getMaxRunningContainersReport() {
      return maxRunningContainersOnNMs;
    }
  }
}
