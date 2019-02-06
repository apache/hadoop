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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

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
    app.init(1000, containers, rm, null, 0, 1000000L, "user1", queue, true,
        appId, 0, SLSConfiguration.getAMContainerResource(conf), null, null);
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
      app.init(1000, containers, rm, null, 0, 1000000L, "user1", queue, true,
          appId, 0, SLSConfiguration.getAMContainerResource(conf),
          "label1", null);
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

  @After
  public void tearDown() {
    if (rm != null) {
      rm.stop();
    }

    deleteMetricOutputDir();
  }
}