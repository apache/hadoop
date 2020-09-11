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

import org.apache.hadoop.test.GenericTestUtils;
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
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.event.Level;
import org.junit.Assume;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.yarn.server.resourcemanager.resource.TestResourceProfiles.TEST_CONF_RESET_RESOURCE_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerPerf {
  private final int GB = 1024;

  private String getResourceName(int idx) {
    return "resource-" + idx;
  }

  public static class CapacitySchedulerPerf extends CapacityScheduler {
    volatile boolean enable = false;
    AtomicLong count = new AtomicLong(0);

    public CapacitySchedulerPerf() {
      super();
    }

    @Override
    CSAssignment allocateContainersToNode(
        CandidateNodeSet<FiCaSchedulerNode> candidates,
        boolean withNodeHeartbeat) {
      CSAssignment retVal = super.allocateContainersToNode(candidates,
          withNodeHeartbeat);

      if (enable) {
        count.incrementAndGet();
      }

      return retVal;
    }
  }

  // This test is run only when when -DRunCapacitySchedulerPerfTests=true is set
  // on the command line. In addition, this test has tunables for the following:
  //   Number of queues: -DNumberOfQueues (default=100)
  //   Number of total apps: -DNumberOfApplications (default=200)
  //   Percentage of queues with apps: -DPercentActiveQueues (default=100)
  // E.G.:
  // mvn test -Dtest=TestCapacitySchedulerPerf -Dsurefire.fork.timeout=1800 \
  //    -DRunCapacitySchedulerPerfTests=true -DNumberOfQueues=50 \
  //    -DNumberOfApplications=200 -DPercentActiveQueues=100
  // Note that the surefire.fork.timeout flag is added because these tests could
  // take longer than the surefire timeout.
  private void testUserLimitThroughputWithNumberOfResourceTypes(
      int numOfResourceTypes, int numQueues, int pctActiveQueues, int appCount)
      throws Exception {
    Assume.assumeTrue(Boolean.valueOf(
        System.getProperty("RunCapacitySchedulerPerfTests")));
    int numThreads = Integer.valueOf(System.getProperty(
        "CapacitySchedulerPerfTestsNumThreads", "0"));

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

    final int activeQueues = (int) (numQueues * (pctActiveQueues/100f));
    final int totalApps = appCount + activeQueues;
    // extra apps to get started with user limit

    CapacitySchedulerConfiguration csconf =
        createCSConfWithManyQueues(numQueues);
    if (numThreads > 0) {
      csconf.setScheduleAynschronously(true);
      csconf.setInt(
          CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_MAXIMUM_THREAD,
          numThreads);
      csconf.setLong(
          CapacitySchedulerConfiguration.SCHEDULE_ASYNCHRONOUSLY_PREFIX
              + ".scheduling-interval-ms", 0);
    }

    YarnConfiguration conf = new YarnConfiguration(csconf);
    // Don't reset resource types since we have already configured resource
    // types
    conf.setBoolean(TEST_CONF_RESET_RESOURCE_TYPES, false);

    if (numThreads > 0) {
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacitySchedulerPerf.class,
          ResourceScheduler.class);
      // avoid getting skipped (see CapacityScheduler.shouldSkipNodeSchedule)
      conf.setLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 600000);
    } else {
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
              ResourceScheduler.class);
    }

    MockRM rm = new MockRM(conf);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    LeafQueue[] lqs = new LeafQueue[numQueues];
    for (int i = 0; i < numQueues; i++) {
      String queueName = String.format("%03d", i);
      LeafQueue qb = (LeafQueue)cs.getQueue(queueName);
      // For now make user limit large so we can activate all applications
      qb.setUserLimitFactor((float)100.0);
      qb.setupConfigurableCapacities();
      lqs[i] = qb;
    }

    SchedulerEvent addAppEvent;
    SchedulerEvent addAttemptEvent;
    Container container = mock(Container.class);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);

    ApplicationId[] appids = new ApplicationId[totalApps];
    RMAppAttemptImpl[] attempts = new RMAppAttemptImpl[totalApps];
    ApplicationAttemptId[] appAttemptIds = new ApplicationAttemptId[totalApps];
    RMAppImpl[] apps = new RMAppImpl[totalApps];
    RMAppAttemptMetrics[] attemptMetrics = new RMAppAttemptMetrics[totalApps];
    for (int i=0; i<totalApps; i++) {
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
      String queueName = lqs[i % activeQueues].getQueuePath();
      addAppEvent =
          new AppAddedSchedulerEvent(appids[i], queueName, "user1");
      cs.handle(addAppEvent);
      addAttemptEvent =
          new AppAttemptAddedSchedulerEvent(appAttemptIds[i], false);
      cs.handle(addAttemptEvent);
    }

    // add nodes to cluster with enough resources to satisfy all apps
    Resource newResource = Resource.newInstance(totalApps * GB, totalApps);
    if (numOfResourceTypes > 2) {
      for (int i = 2; i < numOfResourceTypes; i++) {
        newResource.setResourceValue(getResourceName(i), totalApps);
      }
    }
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    RMNode node2 = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.2");
    cs.handle(new NodeAddedSchedulerEvent(node2));

    Priority u0Priority = TestUtils.createMockPriority(1);
    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(null);

    if (numThreads > 0) {
      // disable async scheduling threads
      for (CapacityScheduler.AsyncScheduleThread t : cs.asyncSchedulerThreads) {
        t.suspendSchedule();
      }
    }

    FiCaSchedulerApp[] fiCaApps = new FiCaSchedulerApp[totalApps];
    for (int i=0;i<totalApps;i++) {
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
    // Now force everything to be at user limit
    for (int i = 0; i < numQueues; i++) {
      lqs[i].setUserLimitFactor((float)0.0);
    }

    if (numThreads > 0) {
      // enable async scheduling threads
      for (CapacityScheduler.AsyncScheduleThread t : cs.asyncSchedulerThreads) {
        t.beginSchedule();
      }

      // let the threads allocate resources for extra apps
      while (CapacitySchedulerMetrics.getMetrics().commitSuccess.lastStat()
          .numSamples() < activeQueues) {
        Thread.sleep(1000);
      }

      // count the number of apps with allocated containers
      int numNotPending = 0;
      for (int i = 0; i < totalApps; i++) {
        boolean pending = fiCaApps[i].getAppSchedulingInfo().isPending();
        if (!pending) {
          numNotPending++;
          assertEquals(0,
              fiCaApps[i].getTotalPendingRequestsPerPartition().size());
        } else {
          assertEquals(1*GB,
              fiCaApps[i].getTotalPendingRequestsPerPartition()
                  .get(RMNodeLabelsManager.NO_LABEL).getMemorySize());
        }
      }

      // make sure only extra apps have allocated containers
      assertEquals(activeQueues, numNotPending);
    } else {
      // allocate one container for each extra apps since
      //  LeafQueue.canAssignToUser() checks for used > limit, not used >= limit
      cs.handle(new NodeUpdateSchedulerEvent(node));
      cs.handle(new NodeUpdateSchedulerEvent(node2));

      // make sure only the extra apps have allocated containers
      for (int i=0;i<totalApps;i++) {
        boolean pending = fiCaApps[i].getAppSchedulingInfo().isPending();
        if (i < activeQueues) {
          assertFalse(pending);
          assertEquals(0,
              fiCaApps[i].getTotalPendingRequestsPerPartition().size());
        } else {
          assertTrue(pending);
          assertEquals(1*GB,
              fiCaApps[i].getTotalPendingRequestsPerPartition()
                  .get(RMNodeLabelsManager.NO_LABEL).getMemorySize());
        }
      }
    }

    // Quiet the loggers while measuring throughput
    GenericTestUtils.setRootLogLevel(Level.WARN);

    if (numThreads > 0) {
      System.out.println("Starting now");
      ((CapacitySchedulerPerf) cs).enable = true;
      long start = Time.monotonicNow();
      Thread.sleep(60000);
      long end = Time.monotonicNow();
      ((CapacitySchedulerPerf) cs).enable = false;
      long numOps = ((CapacitySchedulerPerf) cs).count.get();
      System.out.println("Number of operations: " + numOps);
      System.out.println("Time taken: " + (end - start) + " ms");
      System.out.println("" + (numOps * 1000 / (end - start))
          + " ops / second");
    } else {
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
          n = Time.monotonicNow();
        }
        cs.handle(new NodeUpdateSchedulerEvent(node));
        cs.handle(new NodeUpdateSchedulerEvent(node2));
      }
      timespent = 0;
      int entries = queue.size();
      while (queue.size() > 0) {
        long l = queue.poll();
        timespent += l;
      }
      System.out.println("#ResourceTypes = " + numOfResourceTypes
          + ". Avg of fastest " + entries
          + ": " + numerator / (timespent / entries) + " ops/sec of "
          + appCount + " apps on " + pctActiveQueues + "% of " + numQueues
          + " queues.");
    }

    if (numThreads > 0) {
      // count the number of apps with allocated containers
      int numNotPending = 0;
      for (int i = 0; i < totalApps; i++) {
        boolean pending = fiCaApps[i].getAppSchedulingInfo().isPending();
        if (!pending) {
          numNotPending++;
          assertEquals(0,
                  fiCaApps[i].getTotalPendingRequestsPerPartition().size());
        } else {
          assertEquals(1*GB,
                  fiCaApps[i].getTotalPendingRequestsPerPartition()
                          .get(RMNodeLabelsManager.NO_LABEL).getMemorySize());
        }
      }

      // make sure only extra apps have allocated containers
      assertEquals(activeQueues, numNotPending);
    } else {
      // make sure only the extra apps have allocated containers
      for (int i = 0; i < totalApps; i++) {
        boolean pending = fiCaApps[i].getAppSchedulingInfo().isPending();
        if (i < activeQueues) {
          assertFalse(pending);
          assertEquals(0,
                  fiCaApps[i].getTotalPendingRequestsPerPartition().size());
        } else {
          assertTrue(pending);
          assertEquals(1 * GB,
                  fiCaApps[i].getTotalPendingRequestsPerPartition()
                          .get(RMNodeLabelsManager.NO_LABEL).getMemorySize());
        }
      }
    }

    rm.close();
    rm.stop();
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForTwoResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(2, 1, 100, 100);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForThreeResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(3, 1, 100, 100);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForFourResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(4, 1, 100, 100);
  }

  @Test(timeout = 300000)
  public void testUserLimitThroughputForFiveResources() throws Exception {
    testUserLimitThroughputWithNumberOfResourceTypes(5, 1, 100, 100);
  }

  @Test(timeout = 1800000)
  public void testUserLimitThroughputWithManyQueues() throws Exception {

    int numQueues = Integer.getInteger("NumberOfQueues", 40);
    int pctActiveQueues = Integer.getInteger("PercentActiveQueues", 100);
    int appCount = Integer.getInteger("NumberOfApplications", 100);

    testUserLimitThroughputWithNumberOfResourceTypes(
        2, numQueues, pctActiveQueues, appCount);
  }

  CapacitySchedulerConfiguration createCSConfWithManyQueues(int numQueues)
      throws Exception {
    CapacitySchedulerConfiguration csconf =
        new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculator.class);
    csconf.setMaximumApplicationMasterResourcePerQueuePercent("root", 100.0f);
    csconf.setMaximumAMResourcePercentPerPartition("root", "", 100.0f);
    csconf.setCapacity("root.default", 0.0f);
    csconf.setOffSwitchPerHeartbeatLimit(numQueues);

    float capacity = 100.0f / numQueues;
    String[] subQueues = new String[numQueues];
    for (int i = 0; i < numQueues; i++) {
      String queueName = String.format("%03d", i);
      String queuePath = "root." + queueName;
      subQueues[i] = queueName;
      csconf.setMaximumApplicationMasterResourcePerQueuePercent(
          queuePath, 100.0f);
      csconf.setMaximumAMResourcePercentPerPartition(queuePath, "", 100.0f);
      csconf.setCapacity(queuePath, capacity);
      csconf.setUserLimitFactor(queuePath, 100.0f);
      csconf.setMaximumCapacity(queuePath, 100.0f);
    }

    csconf.setQueues("root", subQueues);

    return csconf;
  }
}
