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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestDistributedShellWithNodeLabels {
  private static final Log LOG =
      LogFactory.getLog(TestDistributedShellWithNodeLabels.class);
  
  static final int NUM_NMS = 2;
  TestDistributedShell distShellTest;
 
  @Before
  public void setup() throws Exception {
    distShellTest = new TestDistributedShell();
    distShellTest.setupInternal(NUM_NMS);
  }
  
  private void initializeNodeLabels() throws IOException {
    RMContext rmContext = distShellTest.yarnCluster.getResourceManager(0).getRMContext();

    // Setup node labels
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    Set<String> labels = new HashSet<String>();
    labels.add("x");
    labelsMgr.addToCluserNodeLabels(labels);

    // Setup queue access to node labels
    distShellTest.conf.set("yarn.scheduler.capacity.root.accessible-node-labels", "x");
    distShellTest.conf.set("yarn.scheduler.capacity.root.accessible-node-labels.x.capacity",
        "100");
    distShellTest.conf.set("yarn.scheduler.capacity.root.default.accessible-node-labels", "x");
    distShellTest.conf.set(
        "yarn.scheduler.capacity.root.default.accessible-node-labels.x.capacity",
        "100");

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
