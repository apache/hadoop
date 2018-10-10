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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test ResourceUtilizationAggregator functionality.
 */
public class TestResourceUtilizationAggregator {

  private static final int GB = 1024;

  private MockRM rm;
  private DrainDispatcher dispatcher;
  private MockNM nm1, nm2, nm3, nm4;
  private ResourceUtilizationAggregator agg;

  @Before
  public void createAndStartRM() throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    startRM(conf);

    nm1 = new MockNM("h1:1234", 8 * GB, rm.getResourceTrackerService());
    nm2 = new MockNM("h1:4321", 8 * GB, rm.getResourceTrackerService());
    nm3 = new MockNM("h2:1234", 8 * GB, rm.getResourceTrackerService());
    nm4 = new MockNM("h2:4321", 8 * GB, rm.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode();
    nm3.registerNode();
    nm4.registerNode();

    agg = rm.getRMContext().getResourceManager().getResUtilizationAggregator();
  }

  private void startRM(final YarnConfiguration conf) {
    dispatcher = new DrainDispatcher();
    rm = new MockRM(conf) {
      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.start();
  }

  @After
  public void stopRM() {
    if (rm != null) {
      rm.stop();
    }
  }

  /**
   * Check if Resource Utilization Aggregation works correctly.
   * Start 3 Apps across 4 nodes : 2 apps by 'user1' and 1 by 'user2'
   * .. but all on the same queue.
   *
   * Step 1: Send Node Heartbeats with App Resource Utilization.
   * Ensure the Resource utilization is correctly aggregated across
   * apps, users and queues.
   *
   * Step 2: Resend Node Heartbeats with Increase in one App's Utilization
   * Ensure the Resource utilization is correctly aggregated across
   * apps, users and queues.
   *
   * Step 3: Resend Node Heatbeats with Decrease in utilization across
   * all app. Ensure the Resource utilization is correctly aggregated across
   * apps, users and queues.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testResourceUtilizationAggregation() throws Exception {

    RMApp app1 = rm.submitApp(1 * GB, "app1", "user1", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm2);

    RMApp app2 = rm.submitApp(1 * GB, "app2", "user2", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm3);

    RMApp app3 = rm.submitApp(1 * GB, "app3", "user2", null, "default");
    MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm4);

    am1.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.GUARANTEED, true))),
        null);

    am2.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.GUARANTEED, true))),
        null);

    am3.allocate(
        Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(1 * GB), 1, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.GUARANTEED, true))),
        null);

    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    nm3.nodeHeartbeat(true);
    nm4.nodeHeartbeat(true);
    rm.drainEvents();
    dispatcher.waitForEventThreadToWait();

    AllocateResponse alloc1 = am1.allocate(
        new ArrayList<>(), new ArrayList<>());
    assertEquals(1, alloc1.getAllocatedContainers().size());

    AllocateResponse alloc2 = am2.allocate(
        new ArrayList<>(), new ArrayList<>());
    assertEquals(1, alloc2.getAllocatedContainers().size());

    AllocateResponse alloc3 = am3.allocate(
        new ArrayList<>(), new ArrayList<>());
    assertEquals(1, alloc3.getAllocatedContainers().size());

    ApplicationId appId1 = app1.getApplicationId();
    ApplicationId appId2 = app2.getApplicationId();
    ApplicationId appId3 = app3.getApplicationId();

    AbstractYarnScheduler sched =
        (AbstractYarnScheduler) rm.getRMContext().getScheduler();
    SchedulerApplicationAttempt appAttempt1 =
        sched.getApplicationAttempt(
            app1.getCurrentAppAttempt().getAppAttemptId());
    SchedulerApplicationAttempt appAttempt2 =
        sched.getApplicationAttempt(
            app2.getCurrentAppAttempt().getAppAttemptId());

    // START Step 1 ========>
    // Send Node Heartbeats with App Resource Utilization.
    // Ensure the Resource utilization is correctly aggregated across
    // apps, users and queues.
    sendHeartBeatsWithAppUtil(
        mkmap(
            e(appId1, ResourceUtilization.newInstance(1, 1, 0.1f)),
            e(appId2, ResourceUtilization.newInstance(3, 3, 0.3f))
        ),
        mkmap(
            e(appId1, ResourceUtilization.newInstance(2, 2, 0.2f))
        ),
        mkmap(
            e(appId3, ResourceUtilization.newInstance(5, 5, 0.5f))
        ),
        mkmap(
            e(appId2, ResourceUtilization.newInstance(4, 4, 0.4f))
        ));

    ResourceUtilization aRU1 =
        agg.getAppResourceUtilization(app1.getApplicationId());
    ResourceUtilization aRU2 =
        agg.getAppResourceUtilization(app2.getApplicationId());
    ResourceUtilization aRU3 =
        agg.getAppResourceUtilization(app3.getApplicationId());

    ResourceUtilization uRU1 = agg.getUserResourceUtilization("user1");
    ResourceUtilization uRU2 = agg.getUserResourceUtilization("user2");

    // Check aggregated utilization across nodes for
    // each application
    assertEquals(3, aRU1.getPhysicalMemory());
    assertEquals(3, aRU1.getVirtualMemory());
    assertEquals(7, aRU2.getPhysicalMemory());
    assertEquals(7, aRU2.getVirtualMemory());
    assertEquals(5, aRU3.getPhysicalMemory());
    assertEquals(5, aRU3.getVirtualMemory());

    // Check aggregated utilization across nodes for
    // each user
    assertEquals(3, uRU1.getPhysicalMemory());
    assertEquals(3, uRU1.getVirtualMemory());
    assertEquals(12, uRU2.getPhysicalMemory());
    assertEquals(12, uRU2.getVirtualMemory());

    assertEquals(appAttempt1.getQueue(), appAttempt2.getQueue());

    // All three applications are bound to the same queue,
    // so the queue utilization should be the total aggregate..
    ResourceUtilization qRU =
        agg.getQueueResourceUtilization(appAttempt1.getQueue());
    assertEquals(15, qRU.getPhysicalMemory());
    assertEquals(15, qRU.getVirtualMemory());
    // <======== END Step 1

    Queue queue = appAttempt1.getQueue();
    // Step 2: Resend Node Heartbeats with Increase in one App's Utilization
    // Ensure the Resource utilization is correctly aggregated across
    // apps, users and queues.
    checkAggAfterUtilIncrease(appId1, appId2, appId3, queue, "user1", "user2");

    // Step 3: Resend Node Heatbeats with Decrease in utilization across
    // all app. Ensure the Resource utilization is correctly aggregated across
    // apps, users and queues.
    checkAggAfterUtilDecrease(appId1, appId2, appId3, queue, "user1", "user2");
  }


  private void checkAggAfterUtilDecrease(ApplicationId appId1,
      ApplicationId appId2, ApplicationId appId3,
      Queue queue, String user1, String user2)
      throws Exception {
    sendHeartBeatsWithAppUtil(
        mkmap(
            e(appId1, ResourceUtilization.newInstance(1, 1, 0.1f)),
            e(appId2, ResourceUtilization.newInstance(1, 1, 0.1f))
        ),
        mkmap(
            e(appId1, ResourceUtilization.newInstance(1, 1, 0.1f))
        ),
        mkmap(
            e(appId3, ResourceUtilization.newInstance(1, 1, 0.1f))
        ),
        mkmap(
            e(appId2, ResourceUtilization.newInstance(1, 1, 0.1f))
        ));

    // All Utilizations should decrease..
    assertEquals(2,
        agg.getAppResourceUtilization(appId1).getPhysicalMemory());
    assertEquals(2,
        agg.getAppResourceUtilization(appId2).getPhysicalMemory());
    assertEquals(1,
        agg.getAppResourceUtilization(appId3).getPhysicalMemory());
    assertEquals(2,
        agg.getUserResourceUtilization(user1).getPhysicalMemory());
    assertEquals(3,
        agg.getUserResourceUtilization(user2).getPhysicalMemory());
    assertEquals(5,
        agg.getQueueResourceUtilization(queue).getPhysicalMemory());
  }


  private void checkAggAfterUtilIncrease(ApplicationId appId1,
      ApplicationId appId2, ApplicationId appId3,
      Queue queue, String user1, String user2)
      throws Exception {
    sendHeartBeatsWithAppUtil(
        mkmap(
            e(appId1, ResourceUtilization.newInstance(2, 2, 0.1f)),
            e(appId2, ResourceUtilization.newInstance(3, 3, 0.3f))
        ),
        mkmap(
            e(appId1, ResourceUtilization.newInstance(2, 2, 0.2f))
        ),
        mkmap(
            e(appId3, ResourceUtilization.newInstance(5, 5, 0.5f))
        ),
        mkmap(
            e(appId2, ResourceUtilization.newInstance(4, 4, 0.4f))
        ));

    // App1, User1 and overall Queue utilization should increase
    // Everything else should stay the same..
    assertEquals(4,
        agg.getAppResourceUtilization(appId1).getPhysicalMemory());
    assertEquals(7,
        agg.getAppResourceUtilization(appId2).getPhysicalMemory());
    assertEquals(5,
        agg.getAppResourceUtilization(appId3).getPhysicalMemory());
    assertEquals(4,
        agg.getUserResourceUtilization(user1).getPhysicalMemory());
    assertEquals(12,
        agg.getUserResourceUtilization(user2).getPhysicalMemory());
    assertEquals(16,
        agg.getQueueResourceUtilization(queue).getPhysicalMemory());
  }


  private void sendHeartBeatsWithAppUtil(
      Map<ApplicationId, ResourceUtilization> nm1AppUtil,
      Map<ApplicationId, ResourceUtilization> nm2AppUtil,
      Map<ApplicationId, ResourceUtilization> nm3AppUtil,
      Map<ApplicationId, ResourceUtilization> nm4AppUtil) throws Exception{
    nm1.nodeHeartbeat(nm1AppUtil);
    nm2.nodeHeartbeat(nm2AppUtil);
    nm3.nodeHeartbeat(nm3AppUtil);
    nm4.nodeHeartbeat(nm4AppUtil);

    // Wait for scheduler to process all events
    rm.drainEvents();
    dispatcher.waitForEventThreadToWait();

    agg.kickoffAggregation();
  }
  /**
   * Utility function to create a map.
   */
  private static <K, V> Map<K, V> mkmap(AbstractMap.SimpleEntry<K, V>... es) {
    return Stream.of(es).collect(
        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Utility function to create a map entry to me used by above function.
   */
  private static <K, V> AbstractMap.SimpleEntry<K, V> e(K key, V val) {
    return new AbstractMap.SimpleEntry<>(key, val);
  }
}
