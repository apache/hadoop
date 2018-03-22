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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Test;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import static org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerPerf {
  private final int GB = 1024;

  private String getResourceName(int idx) {
    return "resource-" + idx;
  }

  private void testUserLimitThroughputWithNumberOfResourceTypes(
      int numOfResourceTypes)
      throws Exception {
    if (numOfResourceTypes > 2) {
      // Initialize resource map
      Map<String, ResourceInformation> riMap = new HashMap<>();

      // Initialize mandatory resources
      riMap.put(ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB);
      riMap.put(ResourceInformation.VCORES_URI, ResourceInformation.VCORES);

      for (int i = 2; i < numOfResourceTypes; i++) {
        String resourceName = getResourceName(i);
        riMap.put(resourceName, ResourceInformation
            .newInstance(resourceName, "", 0, ResourceTypes.COUNTABLE, 0,
                Integer.MAX_VALUE));
      }

      ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
    }

    // Since this is more of a performance unit test, only run if
    // RunUserLimitThroughput is set (-DRunUserLimitThroughput=true)
    Assume.assumeTrue(Boolean.valueOf(
        System.getProperty("RunCapacitySchedulerPerfTests")));

    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setMaximumApplicationMasterResourcePerQueuePercent("root", 100.0f);
    csconf.setMaximumAMResourcePercentPerPartition("root", "", 100.0f);
    csconf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
        100.0f);
    csconf.setMaximumAMResourcePercentPerPartition("root.default", "", 100.0f);
    csconf.setResourceComparator(DominantResourceCalculator.class);

    YarnConfiguration conf = new YarnConfiguration(csconf);
    // Don't reset resource types since we have already configured resource types
    conf.setBoolean(TEST_CONF_RESET_RESOURCE_TYPES, false);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    LeafQueue qb = (LeafQueue)cs.getQueue("default");

    // For now make user limit large so we can activate all applications
    qb.setUserLimitFactor((float)100.0);
    qb.setupConfigurableCapacities();

    SchedulerEvent addAppEvent;
    SchedulerEvent addAttemptEvent;
    Container container = mock(Container.class);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);

    final int appCount = 100;
    ApplicationId[] appids = new ApplicationId[appCount];
    RMAppAttemptImpl[] attempts = new RMAppAttemptImpl[appCount];
    ApplicationAttemptId[] appAttemptIds = new ApplicationAttemptId[appCount];
    RMAppImpl[] apps = new RMAppImpl[appCount];
    RMAppAttemptMetrics[] attemptMetrics = new RMAppAttemptMetrics[appCount];
    for (int i=0; i<appCount; i++) {
      appids[i] = BuilderUtils.newApplicationId(100, i);
      appAttemptIds[i] =
          BuilderUtils.newApplicationAttemptId(appids[i], 1);

      attemptMetrics[i] =
          new RMAppAttemptMetrics(appAttemptIds[i], rm.getRMContext());
      apps[i] = mock(RMAppImpl.class);
      when(apps[i].getApplicationId()).thenReturn(appids[i]);
      attempts[i] = mock(RMAppAttemptImpl.class);
      when(attempts[i].getMasterContainer()).thenReturn(container);
      when(attempts[i].getSubmissionContext()).thenReturn(submissionContext);
      when(attempts[i].getAppAttemptId()).thenReturn(appAttemptIds[i]);
      when(attempts[i].getRMAppAttemptMetrics()).thenReturn(attemptMetrics[i]);
      when(apps[i].getCurrentAppAttempt()).thenReturn(attempts[i]);

      rm.getRMContext().getRMApps().put(appids[i], apps[i]);
      addAppEvent =
          new AppAddedSchedulerEvent(appids[i], "default", "user1");
      cs.handle(addAppEvent);
      addAttemptEvent =
          new AppAttemptAddedSchedulerEvent(appAttemptIds[i], false);
      cs.handle(addAttemptEvent);
    }

    // add nodes  to cluster, so cluster has 20GB and 20 vcores
    Resource nodeResource = Resource.newInstance(10 * GB, 10);
    if (numOfResourceTypes > 2) {
      for (int i = 2; i < numOfResourceTypes; i++) {
        nodeResource.setResourceValue(getResourceName(i), 10);
      }
    }

    RMNode node = MockNodes.newNodeInfo(0, nodeResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    RMNode node2 = MockNodes.newNodeInfo(0, nodeResource, 1, "127.0.0.2");
    cs.handle(new NodeAddedSchedulerEvent(node2));

    Priority u0Priority = TestUtils.createMockPriority(1);
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(null);

    FiCaSchedulerApp[] fiCaApps = new FiCaSchedulerApp[appCount];
    for (int i=0;i<appCount;i++) {
      fiCaApps[i] =
          cs.getSchedulerApplications().get(apps[i].getApplicationId())
              .getCurrentAppAttempt();

      ResourceRequest resourceRequest = TestUtils.createResourceRequest(
          ResourceRequest.ANY, 1 * GB, 1, true, u0Priority, recordFactory);
      if (numOfResourceTypes > 2) {
        for (int j = 2; j < numOfResourceTypes; j++) {
          resourceRequest.getCapability().setResourceValue(getResourceName(j),
              10);
        }
      }

      // allocate container for app2 with 1GB memory and 1 vcore
      fiCaApps[i].updateResourceRequests(
          Collections.singletonList(resourceRequest));
    }
    // Now force everything to be over user limit
    qb.setUserLimitFactor((float)0.0);

    // Quiet the loggers while measuring throughput
    for (Enumeration<?> loggers = LogManager.getCurrentLoggers();
         loggers.hasMoreElements(); )  {
      Logger logger = (Logger) loggers.nextElement();
      logger.setLevel(Level.WARN);
    }
    final int topn = 20;
    final int iterations = 2000000;
    final int printInterval = 20000;
    final float numerator = 1000.0f * printInterval;
    PriorityQueue<Long> queue = new PriorityQueue<>(topn,
        Collections.reverseOrder());

    long n = Time.monotonicNow();
    long timespent = 0;
    for (int i = 0; i < iterations; i+=2) {
      if (i > 0  && i % printInterval == 0){
        long ts = (Time.monotonicNow() - n);
        if (queue.size() < topn) {
          queue.offer(ts);
        } else {
          Long last = queue.peek();
          if (last > ts) {
            queue.poll();
            queue.offer(ts);
          }
        }
        System.out.println(i + " " + (numerator / ts));
        n= Time.monotonicNow();
      }
      cs.handle(new NodeUpdateSchedulerEvent(node));
      cs.handle(new NodeUpdateSchedulerEvent(node2));
    }
    timespent=0;
    int entries = queue.size();
    while(queue.size() > 0){
      long l = queue.poll();
      timespent += l;
    }
    System.out.println(
        "#ResourceTypes = " + numOfResourceTypes + ". Avg of fastest " + entries
            + ": " + numerator / (timespent / entries));
    rm.stop();
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForTwoResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(2);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForThreeResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(3);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForFourResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(4);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForFiveResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(5);
  }
}
