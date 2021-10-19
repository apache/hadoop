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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRMApplicationHistoryWriter {

  private static int MAX_RETRIES = 10;

  private RMApplicationHistoryWriter writer;
  private ApplicationHistoryStore store;
  private List<CounterDispatcher> dispatchers =
      new ArrayList<CounterDispatcher>();

  @Before
  public void setup() {
    store = new MemoryApplicationHistoryStore();
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, true);
    conf.setClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
        MemoryApplicationHistoryStore.class, ApplicationHistoryStore.class);
    writer = new RMApplicationHistoryWriter() {

      @Override
      protected ApplicationHistoryStore createApplicationHistoryStore(
          Configuration conf) {
        return store;
      }

      @Override
      protected Dispatcher createDispatcher(Configuration conf) {
        MultiThreadedDispatcher dispatcher =
            new MultiThreadedDispatcher(
              conf
                .getInt(
                  YarnConfiguration.RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE,
                  YarnConfiguration.DEFAULT_RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE));
        dispatcher.setDrainEventsOnStop();
        return dispatcher;
      }

      class MultiThreadedDispatcher extends
          RMApplicationHistoryWriter.MultiThreadedDispatcher {

        public MultiThreadedDispatcher(int num) {
          super(num);
        }

        @Override
        protected AsyncDispatcher createDispatcher() {
          CounterDispatcher dispatcher = new CounterDispatcher();
          dispatchers.add(dispatcher);
          return dispatcher;
        }

      }
    };
    writer.init(conf);
    writer.start();
  }

  @After
  public void tearDown() {
    writer.stop();
  }

  private static RMApp createRMApp(ApplicationId appId) {
    RMApp app = mock(RMApp.class);
    when(app.getApplicationId()).thenReturn(appId);
    when(app.getName()).thenReturn("test app");
    when(app.getApplicationType()).thenReturn("test app type");
    when(app.getUser()).thenReturn("test user");
    when(app.getQueue()).thenReturn("test queue");
    when(app.getSubmitTime()).thenReturn(0L);
    when(app.getStartTime()).thenReturn(1L);
    when(app.getFinishTime()).thenReturn(2L);
    when(app.getDiagnostics()).thenReturn(
      new StringBuilder("test diagnostics info"));
    when(app.getFinalApplicationStatus()).thenReturn(
      FinalApplicationStatus.UNDEFINED);
    return app;
  }

  private static RMAppAttempt createRMAppAttempt(
      ApplicationAttemptId appAttemptId) {
    RMAppAttempt appAttempt = mock(RMAppAttempt.class);
    when(appAttempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(appAttempt.getHost()).thenReturn("test host");
    when(appAttempt.getRpcPort()).thenReturn(-100);
    Container container = mock(Container.class);
    when(container.getId())
      .thenReturn(ContainerId.newContainerId(appAttemptId, 1));
    when(appAttempt.getMasterContainer()).thenReturn(container);
    when(appAttempt.getDiagnostics()).thenReturn("test diagnostics info");
    when(appAttempt.getTrackingUrl()).thenReturn("test url");
    when(appAttempt.getFinalApplicationStatus()).thenReturn(
      FinalApplicationStatus.UNDEFINED);
    return appAttempt;
  }

  private static RMContainer createRMContainer(ContainerId containerId) {
    RMContainer container = mock(RMContainer.class);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getAllocatedNode()).thenReturn(
      NodeId.newInstance("test host", -100));
    when(container.getAllocatedResource()).thenReturn(
      Resource.newInstance(-1, -1));
    when(container.getAllocatedPriority()).thenReturn(Priority.UNDEFINED);
    when(container.getCreationTime()).thenReturn(0L);
    when(container.getFinishTime()).thenReturn(1L);
    when(container.getDiagnosticsInfo()).thenReturn("test diagnostics info");
    when(container.getLogURL()).thenReturn("test log url");
    when(container.getContainerExitStatus()).thenReturn(-1);
    when(container.getContainerState()).thenReturn(ContainerState.COMPLETE);
    return container;
  }

  @Test
  public void testDefaultStoreSetup() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED, true);
    RMApplicationHistoryWriter writer = new RMApplicationHistoryWriter();
    writer.init(conf);
    writer.start();
    try {
      Assert.assertFalse(writer.historyServiceEnabled);
      Assert.assertNull(writer.writer);
    } finally {
      writer.stop();
      writer.close();
    }
  }

  @Test
  public void testWriteApplication() throws Exception {
    RMApp app = createRMApp(ApplicationId.newInstance(0, 1));

    writer.applicationStarted(app);
    ApplicationHistoryData appHD = null;
    for (int i = 0; i < MAX_RETRIES; ++i) {
      appHD = store.getApplication(ApplicationId.newInstance(0, 1));
      if (appHD != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertNotNull(appHD);
    Assert.assertEquals("test app", appHD.getApplicationName());
    Assert.assertEquals("test app type", appHD.getApplicationType());
    Assert.assertEquals("test user", appHD.getUser());
    Assert.assertEquals("test queue", appHD.getQueue());
    Assert.assertEquals(0L, appHD.getSubmitTime());
    Assert.assertEquals(1L, appHD.getStartTime());

    writer.applicationFinished(app, RMAppState.FINISHED);
    for (int i = 0; i < MAX_RETRIES; ++i) {
      appHD = store.getApplication(ApplicationId.newInstance(0, 1));
      if (appHD.getYarnApplicationState() != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertEquals(2L, appHD.getFinishTime());
    Assert.assertEquals("test diagnostics info", appHD.getDiagnosticsInfo());
    Assert.assertEquals(FinalApplicationStatus.UNDEFINED,
      appHD.getFinalApplicationStatus());
    Assert.assertEquals(YarnApplicationState.FINISHED,
      appHD.getYarnApplicationState());
  }

  @Test
  public void testWriteApplicationAttempt() throws Exception {
    RMAppAttempt appAttempt =
        createRMAppAttempt(ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(0, 1), 1));
    writer.applicationAttemptStarted(appAttempt);
    ApplicationAttemptHistoryData appAttemptHD = null;
    for (int i = 0; i < MAX_RETRIES; ++i) {
      appAttemptHD =
          store.getApplicationAttempt(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1));
      if (appAttemptHD != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertNotNull(appAttemptHD);
    Assert.assertEquals("test host", appAttemptHD.getHost());
    Assert.assertEquals(-100, appAttemptHD.getRPCPort());
    Assert.assertEquals(ContainerId.newContainerId(
      ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1), 1),
      appAttemptHD.getMasterContainerId());

    writer.applicationAttemptFinished(appAttempt, RMAppAttemptState.FINISHED);
    for (int i = 0; i < MAX_RETRIES; ++i) {
      appAttemptHD =
          store.getApplicationAttempt(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1));
      if (appAttemptHD.getYarnApplicationAttemptState() != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertEquals("test diagnostics info",
      appAttemptHD.getDiagnosticsInfo());
    Assert.assertEquals("test url", appAttemptHD.getTrackingURL());
    Assert.assertEquals(FinalApplicationStatus.UNDEFINED,
      appAttemptHD.getFinalApplicationStatus());
    Assert.assertEquals(YarnApplicationAttemptState.FINISHED,
      appAttemptHD.getYarnApplicationAttemptState());
  }

  @Test
  public void testWriteContainer() throws Exception {
    RMContainer container =
        createRMContainer(ContainerId.newContainerId(
          ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1),
          1));
    writer.containerStarted(container);
    ContainerHistoryData containerHD = null;
    for (int i = 0; i < MAX_RETRIES; ++i) {
      containerHD =
          store.getContainer(ContainerId.newContainerId(ApplicationAttemptId
            .newInstance(ApplicationId.newInstance(0, 1), 1), 1));
      if (containerHD != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertNotNull(containerHD);
    Assert.assertEquals(NodeId.newInstance("test host", -100),
      containerHD.getAssignedNode());
    Assert.assertEquals(Resource.newInstance(-1, -1),
      containerHD.getAllocatedResource());
    Assert.assertEquals(Priority.UNDEFINED, containerHD.getPriority());
    Assert.assertEquals(0L, container.getCreationTime());

    writer.containerFinished(container);
    for (int i = 0; i < MAX_RETRIES; ++i) {
      containerHD =
          store.getContainer(ContainerId.newContainerId(ApplicationAttemptId
            .newInstance(ApplicationId.newInstance(0, 1), 1), 1));
      if (containerHD.getContainerState() != null) {
        break;
      } else {
        Thread.sleep(100);
      }
    }
    Assert.assertEquals("test diagnostics info",
      containerHD.getDiagnosticsInfo());
    Assert.assertEquals(-1, containerHD.getContainerExitStatus());
    Assert.assertEquals(ContainerState.COMPLETE,
      containerHD.getContainerState());
  }

  @Test
  public void testParallelWrite() throws Exception {
    List<ApplicationId> appIds = new ArrayList<ApplicationId>();
    for (int i = 0; i < 10; ++i) {
      Random rand = new Random(i);
      ApplicationId appId = ApplicationId.newInstance(0, rand.nextInt());
      appIds.add(appId);
      RMApp app = createRMApp(appId);
      writer.applicationStarted(app);
      for (int j = 1; j <= 10; ++j) {
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, j);
        RMAppAttempt appAttempt = createRMAppAttempt(appAttemptId);
        writer.applicationAttemptStarted(appAttempt);
        for (int k = 1; k <= 10; ++k) {
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          RMContainer container = createRMContainer(containerId);
          writer.containerStarted(container);
          writer.containerFinished(container);
        }
        writer.applicationAttemptFinished(
            appAttempt, RMAppAttemptState.FINISHED);
      }
      writer.applicationFinished(app, RMAppState.FINISHED);
    }
    for (int i = 0; i < MAX_RETRIES; ++i) {
      if (allEventsHandled(20 * 10 * 10 + 20 * 10 + 20)) {
        break;
      } else {
        Thread.sleep(500);
      }
    }
    Assert.assertTrue(allEventsHandled(20 * 10 * 10 + 20 * 10 + 20));
    // Validate all events of one application are handled by one dispatcher
    for (ApplicationId appId : appIds) {
      Assert.assertTrue(handledByOne(appId));
    }
  }

  private boolean allEventsHandled(int expected) {
    int actual = 0;
    for (CounterDispatcher dispatcher : dispatchers) {
      for (Integer count : dispatcher.counts.values()) {
        actual += count;
      }
    }
    return actual == expected;
  }

  @Test
  public void testRMWritingMassiveHistoryForFairSche() throws Exception {
    //test WritingMassiveHistory for Fair Scheduler.
    testRMWritingMassiveHistory(true);
  }

  @Test
  public void testRMWritingMassiveHistoryForCapacitySche() throws Exception {
    //test WritingMassiveHistory for Capacity Scheduler.
    testRMWritingMassiveHistory(false);
  }

  private void testRMWritingMassiveHistory(boolean isFS) throws Exception {
    // 1. Show RM can run with writing history data
    // 2. Test additional workload of processing history events
    YarnConfiguration conf = new YarnConfiguration();
    if (isFS) {
      conf.setBoolean(FairSchedulerConfiguration.ASSIGN_MULTIPLE, true);
      conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    } else {
      conf.set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
    }
    // don't process history events
    MockRM rm = new MockRM(conf) {
      @Override
      protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
        return new RMApplicationHistoryWriter() {
          @Override
          public void applicationStarted(RMApp app) {
          }

          @Override
          public void applicationFinished(RMApp app, RMAppState finalState) {
          }

          @Override
          public void applicationAttemptStarted(RMAppAttempt appAttempt) {
          }

          @Override
          public void applicationAttemptFinished(
              RMAppAttempt appAttempt, RMAppAttemptState finalState) {
          }

          @Override
          public void containerStarted(RMContainer container) {
          }

          @Override
          public void containerFinished(RMContainer container) {
          }
        };
      }
    };
    long startTime1 = System.currentTimeMillis();
    testRMWritingMassiveHistory(rm);
    long finishTime1 = System.currentTimeMillis();
    long elapsedTime1 = finishTime1 - startTime1;
    rm = new MockRM(conf);
    long startTime2 = System.currentTimeMillis();
    testRMWritingMassiveHistory(rm);
    long finishTime2 = System.currentTimeMillis();
    long elapsedTime2 = finishTime2 - startTime2;
    // No more than 10% additional workload
    // Should be much less, but computation time is fluctuated
    Assert.assertTrue(elapsedTime2 - elapsedTime1 < elapsedTime1 / 10);
  }

  private void testRMWritingMassiveHistory(MockRM rm) throws Exception {
    rm.start();
    MockNM nm = rm.registerNode("127.0.0.1:1234", 1024 * 10100);

    RMApp app = MockRMAppSubmitter.submitWithMemory(1024, rm);
    //Wait to make sure the attempt has the right state
    //TODO explore a better way than sleeping for a while (YARN-4929)
    Thread.sleep(1000);
    nm.nodeHeartbeat(true);
    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    int request = 10000;
    am.allocate("127.0.0.1", 1024, request, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    List<Container> allocated =
        am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
    int waitCount = 0;
    int allocatedSize = allocated.size();
    while (allocatedSize < request && waitCount++ < 200) {
      Thread.sleep(300);
      allocated =
          am.allocate(new ArrayList<ResourceRequest>(),
            new ArrayList<ContainerId>()).getAllocatedContainers();
      allocatedSize += allocated.size();
      nm.nodeHeartbeat(true);
    }
    Assert.assertEquals(request, allocatedSize);

    am.unregisterAppAttempt();
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHING);
    nm.nodeHeartbeat(am.getApplicationAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    NodeHeartbeatResponse resp = nm.nodeHeartbeat(true);
    List<ContainerId> cleaned = resp.getContainersToCleanup();
    int cleanedSize = cleaned.size();
    waitCount = 0;
    while (cleanedSize < allocatedSize && waitCount++ < 200) {
      Thread.sleep(300);
      resp = nm.nodeHeartbeat(true);
      cleaned = resp.getContainersToCleanup();
      cleanedSize += cleaned.size();
    }
    Assert.assertEquals(allocatedSize, cleanedSize);
    rm.waitForState(app.getApplicationId(), RMAppState.FINISHED);

    rm.stop();
  }

  private boolean handledByOne(ApplicationId appId) {
    int count = 0;
    for (CounterDispatcher dispatcher : dispatchers) {
      if (dispatcher.counts.containsKey(appId)) {
        ++count;
      }
    }
    return count == 1;
  }

  private static class CounterDispatcher extends AsyncDispatcher {

    private Map<ApplicationId, Integer> counts =
        new HashMap<ApplicationId, Integer>();

    @SuppressWarnings("rawtypes")
    @Override
    protected void dispatch(Event event) {
      if (event instanceof WritingApplicationHistoryEvent) {
        WritingApplicationHistoryEvent ashEvent =
            (WritingApplicationHistoryEvent) event;
        switch (ashEvent.getType()) {
          case APP_START:
            incrementCounts(((WritingApplicationStartEvent) event)
              .getApplicationId());
            break;
          case APP_FINISH:
            incrementCounts(((WritingApplicationFinishEvent) event)
              .getApplicationId());
            break;
          case APP_ATTEMPT_START:
            incrementCounts(((WritingApplicationAttemptStartEvent) event)
              .getApplicationAttemptId().getApplicationId());
            break;
          case APP_ATTEMPT_FINISH:
            incrementCounts(((WritingApplicationAttemptFinishEvent) event)
              .getApplicationAttemptId().getApplicationId());
            break;
          case CONTAINER_START:
            incrementCounts(((WritingContainerStartEvent) event)
              .getContainerId().getApplicationAttemptId().getApplicationId());
            break;
          case CONTAINER_FINISH:
            incrementCounts(((WritingContainerFinishEvent) event)
              .getContainerId().getApplicationAttemptId().getApplicationId());
            break;
        }
      }
      super.dispatch(event);
    }

    private void incrementCounts(ApplicationId appId) {
      Integer val = counts.get(appId);
      if (val == null) {
        counts.put(appId, 1);
      } else {
        counts.put(appId, val + 1);
      }
    }
  }

}
