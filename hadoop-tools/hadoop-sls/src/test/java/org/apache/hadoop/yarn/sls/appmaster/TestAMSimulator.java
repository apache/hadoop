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
package org.apache.hadoop.yarn.sls.appmaster;

import com.codahale.metrics.MetricRegistry;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.tools.rumen.datatypes.UserName;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.AMDefinitionRumen;
import org.apache.hadoop.yarn.sls.TaskContainerDefinition;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.*;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestAMSimulator {
  private ResourceManager rm;
  private YarnConfiguration conf;
  private Path metricOutputDir;

  private Class<?> slsScheduler;
  private Class<?> scheduler;

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][] {
        {SLSFairScheduler.class, FairScheduler.class},
        {SLSCapacityScheduler.class, CapacityScheduler.class}
    });
  }

  public TestAMSimulator(Class<?> slsScheduler, Class<?> scheduler) {
    this.slsScheduler = slsScheduler;
    this.scheduler = scheduler;
  }

  @Before
  public void setup() {
    createMetricOutputDir();

    conf = new YarnConfiguration();
    conf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricOutputDir.toString());
    conf.set(YarnConfiguration.RM_SCHEDULER, slsScheduler.getName());
    conf.set(SLSConfiguration.RM_SCHEDULER, scheduler.getName());
    conf.set(YarnConfiguration.NODE_LABELS_ENABLED, "true");
    conf.setBoolean(SLSConfiguration.METRICS_SWITCH, true);
    rm = new ResourceManager();
    rm.init(conf);
    rm.start();
  }

  class MockAMSimulator extends AMSimulator {
    @Override
    protected void processResponseQueue()
        throws InterruptedException, YarnException, IOException {
    }

    @Override
    protected void sendContainerRequest()
        throws YarnException, IOException, InterruptedException {
    }

    @Override
    public void initReservation(ReservationId id, long deadline, long now){
    }

    @Override
    protected void checkStop() {
    }
  }

  private void verifySchedulerMetrics(String appId) {
    if (scheduler.equals(FairScheduler.class)) {
      SchedulerMetrics schedulerMetrics = ((SchedulerWrapper)
          rm.getResourceScheduler()).getSchedulerMetrics();
      MetricRegistry metricRegistry = schedulerMetrics.getMetrics();
      for (FairSchedulerMetrics.Metric metric :
          FairSchedulerMetrics.Metric.values()) {
        String key = "variable.app." + appId + "." + metric.getValue() +
            ".memory";
        Assert.assertTrue(metricRegistry.getGauges().containsKey(key));
        Assert.assertNotNull(metricRegistry.getGauges().get(key).getValue());
      }
    }
  }

  private void createMetricOutputDir() {
    Path testDir =
        Paths.get(System.getProperty("test.build.data", "target/test-dir"));
    try {
      metricOutputDir = Files.createTempDirectory(testDir, "output");
    } catch (IOException e) {
      Assert.fail(e.toString());
    }
  }

  private void deleteMetricOutputDir() {
    try {
      FileUtils.deleteDirectory(metricOutputDir.toFile());
    } catch (IOException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testAMSimulator() throws Exception {
    // Register one app
    MockAMSimulator app = new MockAMSimulator();
    String appId = "app1";
    String queue = "default";
    List<ContainerSimulator> containers = new ArrayList<>();
    HashMap<ApplicationId, AMSimulator> map = new HashMap<>();

    UserName mockUser = mock(UserName.class);
    when(mockUser.getValue()).thenReturn("user1");
    AMDefinitionRumen amDef =
        AMDefinitionRumen.Builder.create()
        .withUser(mockUser)
        .withQueue(queue)
        .withJobId(appId)
        .withJobStartTime(0)
        .withJobFinishTime(1000000L)
        .withAmResource(SLSConfiguration.getAMContainerResource(conf))
        .withTaskContainers(containers)
        .build();
    app.init(amDef, rm, null, true, 0, 1000, map);
    app.firstStep();

    verifySchedulerMetrics(appId);

    Assert.assertEquals(1, rm.getRMContext().getRMApps().size());
    Assert.assertNotNull(rm.getRMContext().getRMApps().get(app.appId));

    // Finish this app
    app.lastStep();
  }

  @Test
  public void testAMSimulatorWithNodeLabels() throws Exception {
    if (scheduler.equals(CapacityScheduler.class)) {
      // add label to the cluster
      RMAdminCLI rmAdminCLI = new RMAdminCLI(conf);
      String[] args = {"-addToClusterNodeLabels", "label1"};
      rmAdminCLI.run(args);

      MockAMSimulator app = new MockAMSimulator();
      String appId = "app1";
      String queue = "default";
      List<ContainerSimulator> containers = new ArrayList<>();
      HashMap<ApplicationId, AMSimulator> map = new HashMap<>();

      UserName mockUser = mock(UserName.class);
      when(mockUser.getValue()).thenReturn("user1");
      AMDefinitionRumen amDef =
          AMDefinitionRumen.Builder.create()
              .withUser(mockUser)
              .withQueue(queue)
              .withJobId(appId)
              .withJobStartTime(0)
              .withJobFinishTime(1000000L)
              .withAmResource(SLSConfiguration.getAMContainerResource(conf))
              .withTaskContainers(containers)
              .withLabelExpression("label1")
              .build();
      app.init(amDef, rm, null, true, 0, 1000, map);
      app.firstStep();

      verifySchedulerMetrics(appId);

      ConcurrentMap<ApplicationId, RMApp> rmApps =
          rm.getRMContext().getRMApps();
      Assert.assertEquals(1, rmApps.size());
      RMApp rmApp = rmApps.get(app.appId);
      Assert.assertNotNull(rmApp);
      Assert.assertEquals("label1", rmApp.getAmNodeLabelExpression());
    }
  }

  @Test
  public void testPackageRequests() throws YarnException {
    MockAMSimulator app = new MockAMSimulator();
    List<ContainerSimulator> containerSimulators = new ArrayList<>();
    Resource resource = Resources.createResource(1024);
    int priority = 1;
    ExecutionType execType = ExecutionType.GUARANTEED;
    String type = "map";

    TaskContainerDefinition.Builder builder =
        TaskContainerDefinition.Builder.create()
        .withResource(resource)
        .withDuration(100)
        .withPriority(1)
        .withType(type)
        .withExecutionType(execType)
        .withAllocationId(-1)
        .withRequestDelay(0);

    ContainerSimulator s1 = ContainerSimulator
        .createFromTaskContainerDefinition(
            builder.withHostname("/default-rack/h1").build());
    ContainerSimulator s2 = ContainerSimulator
        .createFromTaskContainerDefinition(
            builder.withHostname("/default-rack/h1").build());
    ContainerSimulator s3 = ContainerSimulator
        .createFromTaskContainerDefinition(
            builder.withHostname("/default-rack/h2").build());

    containerSimulators.add(s1);
    containerSimulators.add(s2);
    containerSimulators.add(s3);

    List<ResourceRequest> res = app.packageRequests(containerSimulators,
        priority);

    // total 4 resource requests: any -> 1, rack -> 1, node -> 2
    // All resource requests for any would be packaged into 1.
    // All resource requests for racks would be packaged into 1 as all of them
    // are for same rack.
    // All resource requests for nodes would be packaged into 2 as there are
    // two different nodes.
    Assert.assertEquals(4, res.size());
    int anyRequestCount = 0;
    int rackRequestCount = 0;
    int nodeRequestCount = 0;

    for (ResourceRequest request : res) {
      String resourceName = request.getResourceName();
      if (resourceName.equals("*")) {
        anyRequestCount++;
      } else if (resourceName.equals("/default-rack")) {
        rackRequestCount++;
      } else {
        nodeRequestCount++;
      }
    }

    Assert.assertEquals(1, anyRequestCount);
    Assert.assertEquals(1, rackRequestCount);
    Assert.assertEquals(2, nodeRequestCount);

    containerSimulators.clear();
    s1 = ContainerSimulator.createFromTaskContainerDefinition(
        createDefaultTaskContainerDefMock(resource, priority, execType, type,
            "/default-rack/h1", 1));
    s2 = ContainerSimulator.createFromTaskContainerDefinition(
        createDefaultTaskContainerDefMock(resource, priority, execType, type,
            "/default-rack/h1", 2));
    s3 = ContainerSimulator.createFromTaskContainerDefinition(
        createDefaultTaskContainerDefMock(resource, priority, execType, type,
            "/default-rack/h2", 1));

    containerSimulators.add(s1);
    containerSimulators.add(s2);
    containerSimulators.add(s3);

    res = app.packageRequests(containerSimulators, priority);

    // total 7 resource requests: any -> 2, rack -> 2, node -> 3
    // All resource requests for any would be packaged into 2 as there are
    // two different allocation id.
    // All resource requests for racks would be packaged into 2 as all of them
    // are for same rack but for two different allocation id.
    // All resource requests for nodes would be packaged into 3 as either node
    // or allocation id is different for each request.
    Assert.assertEquals(7, res.size());

    anyRequestCount = 0;
    rackRequestCount = 0;
    nodeRequestCount = 0;

    for (ResourceRequest request : res) {
      String resourceName = request.getResourceName();
      long allocationId = request.getAllocationRequestId();
      // allocation id should be either 1 or 2
      Assert.assertTrue(allocationId == 1 || allocationId == 2);
      if (resourceName.equals("*")) {
        anyRequestCount++;
      } else if (resourceName.equals("/default-rack")) {
        rackRequestCount++;
      } else {
        nodeRequestCount++;
      }
    }

    Assert.assertEquals(2, anyRequestCount);
    Assert.assertEquals(2, rackRequestCount);
    Assert.assertEquals(3, nodeRequestCount);
  }

  @Test
  public void testAMSimulatorRanNodesCleared() throws Exception {
    NMSimulator nm = new NMSimulator();
    nm.init("/rack1/testNode1", Resources.createResource(1024 * 10, 10), 0, 1000,
        rm, -1f);

    Map<NodeId, NMSimulator> nmMap = new HashMap<>();
    nmMap.put(nm.getNode().getNodeID(), nm);

    MockAMSimulator app = new MockAMSimulator();
    app.appId = ApplicationId.newInstance(0l, 1);
    SLSRunner slsRunner = Mockito.mock(SLSRunner.class);
    app.se = slsRunner;
    when(slsRunner.getNmMap()).thenReturn(nmMap);
    app.getRanNodes().add(nm.getNode().getNodeID());
    nm.getNode().getRunningApps().add(app.appId);
    Assert.assertTrue(nm.getNode().getRunningApps().contains(app.appId));

    app.lastStep();
    Assert.assertFalse(nm.getNode().getRunningApps().contains(app.appId));
    Assert.assertTrue(nm.getNode().getRunningApps().isEmpty());
  }
  private TaskContainerDefinition createDefaultTaskContainerDefMock(
      Resource resource, int priority, ExecutionType execType, String type,
      String hostname, long allocationId) {
    TaskContainerDefinition taskContainerDef =
        mock(TaskContainerDefinition.class);
    when(taskContainerDef.getResource()).thenReturn(resource);
    when(taskContainerDef.getDuration()).thenReturn(100L);
    when(taskContainerDef.getPriority()).thenReturn(priority);
    when(taskContainerDef.getType()).thenReturn(type);
    when(taskContainerDef.getExecutionType()).thenReturn(execType);
    when(taskContainerDef.getHostname()).thenReturn(hostname);
    when(taskContainerDef.getAllocationId()).thenReturn(allocationId);
    return taskContainerDef;
  }

  @After
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }

    deleteMetricOutputDir();
  }
}