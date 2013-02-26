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

package org.apache.hadoop.mapreduce.v2.app;

import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestRMContainerAllocator {

  static final Log LOG = LogFactory
      .getLog(TestRMContainerAllocator.class);
  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @After
  public void tearDown() {
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testSimple() throws Exception {

    LOG.info("Running testSimple");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0, 
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    Assert.assertEquals(4, rm.getMyFifoScheduler().lastAsk.size());

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h3" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    Assert.assertEquals(3, rm.getMyFifoScheduler().lastAsk.size());
    
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, rm.getMyFifoScheduler().lastAsk.size());
    checkAssignments(new ContainerRequestEvent[] { event1, event2, event3 },
        assigned, false);
    
    // check that the assigned container requests are cancelled
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(5, rm.getMyFifoScheduler().lastAsk.size());    
  }
  
  @Test 
  public void testMapNodeLocality() throws Exception {
    // test checks that ordering of allocated containers list from the RM does 
    // not affect the map->container assignment done by the AM. If there is a 
    // node local container available for a map then it should be assigned to 
    // that container and not a rack-local container that happened to be seen 
    // earlier in the allocated containers list from the RM.
    // Regression test for MAPREDUCE-4893
    LOG.info("Running testMapNodeLocality");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0, 
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 3072); // can assign 2 maps 
    rm.registerNode("h2:1234", 10240); // wont heartbeat on node local node
    MockNM nodeManager3 = rm.registerNode("h3:1234", 1536); // assign 1 map
    dispatcher.await();

    // create the container requests for maps
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    // Node heartbeat from rack-local first. This makes node h3 the first in the
    // list of allocated containers but it should not be assigned to task1.
    nodeManager3.nodeHeartbeat(true);
    // Node heartbeat from node-local next. This allocates 2 node local 
    // containers for task1 and task2. These should be matched with those tasks.
    nodeManager1.nodeHeartbeat(true);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event2, event3 },
        assigned, false);
    // remove the rack-local assignment that should have happened for task3
    for(TaskAttemptContainerAssignedEvent event : assigned) {
      if(event.getTaskAttemptID().equals(event3.getAttemptID())) {
        assigned.remove(event);
        Assert.assertTrue(
                    event.getContainer().getNodeId().getHost().equals("h3"));
        break;
      }
    }
    checkAssignments(new ContainerRequestEvent[] { event1, event2},
        assigned, true);
  }

  @Test
  public void testResource() throws Exception {

    LOG.info("Running testResource");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 2048,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event2 },
        assigned, false);
  }

  @Test
  public void testMapReduceScheduling() throws Exception {

    LOG.info("Running testMapReduceScheduling");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 1024);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    // send MAP request
    ContainerRequestEvent event1 = createReq(jobId, 1, 2048, new String[] {
        "h1", "h2" }, true, false);
    allocator.sendRequest(event1);

    // send REDUCE request
    ContainerRequestEvent event2 = createReq(jobId, 2, 3000,
        new String[] { "h1" }, false, true);
    allocator.sendRequest(event2);

    // send MAP request
    ContainerRequestEvent event3 = createReq(jobId, 3, 2048,
        new String[] { "h3" }, false, false);
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event3 },
        assigned, false);

    // validate that no container is assigned to h1 as it doesn't have 2048
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertFalse("Assigned count not correct", "h1".equals(assig
          .getContainer().getNodeId().getHost()));
    }
  }

  private static class MyResourceManager extends MockRM {
    
    public MyResourceManager(Configuration conf) {
      super(conf);
    }

    @Override
    protected Dispatcher createDispatcher() {
      return new DrainDispatcher();
    }

    @Override
    protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
      // Dispatch inline for test sanity
      return new EventHandler<SchedulerEvent>() {
        @Override
        public void handle(SchedulerEvent event) {
          scheduler.handle(event);
        }
      };
    }
    @Override
    protected ResourceScheduler createScheduler() {
      return new MyFifoScheduler(this.getRMContext());
    }
    
    MyFifoScheduler getMyFifoScheduler() {
      return (MyFifoScheduler) scheduler;
    }
  }

  @Test
  public void testReportedAppProgress() throws Exception {

    LOG.info("Running testReportedAppProgress");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp rmApp = rm.submitApp(1024);
    rmDispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 21504);
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();

    final ApplicationAttemptId appAttemptId = rmApp.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rmDispatcher.await();

    MRApp mrApp = new MRApp(appAttemptId, BuilderUtils.newContainerId(
      appAttemptId, 0), 10, 10, false, this.getClass().getName(), true, 1) {
      @Override
      protected Dispatcher createDispatcher() {
        return new DrainDispatcher();
      }
      protected ContainerAllocator createContainerAllocator(
          ClientService clientService, AppContext context) {
        return new MyContainerAllocator(rm, appAttemptId, context);
      };
    };

    Assert.assertEquals(0.0, rmApp.getProgress(), 0.0);

    mrApp.submit(conf);
    Job job = mrApp.getContext().getAllJobs().entrySet().iterator().next()
        .getValue();

    DrainDispatcher amDispatcher = (DrainDispatcher) mrApp.getDispatcher();

    MyContainerAllocator allocator = (MyContainerAllocator) mrApp
      .getContainerAllocator();

    mrApp.waitForState(job, JobState.RUNNING);

    amDispatcher.await();
    // Wait till all map-attempts request for containers
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.MAP) {
        mrApp.waitForInternalState((TaskAttemptImpl) t.getAttempts().values()
            .iterator().next(), TaskAttemptStateInternal.UNASSIGNED);
      }
    }
    amDispatcher.await();

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all map-tasks to be running
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.MAP) {
        mrApp.waitForState(t, TaskState.RUNNING);
      }
    }

    allocator.schedule(); // Send heartbeat
    rmDispatcher.await();
    Assert.assertEquals(0.05f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.05f, rmApp.getProgress(), 0.001f);

    // Finish off 1 map.
    Iterator<Task> it = job.getTasks().values().iterator();
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.095f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.095f, rmApp.getProgress(), 0.001f);

    // Finish off 7 more so that map-progress is 80%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 7);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.41f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.41f, rmApp.getProgress(), 0.001f);

    // Finish off the 2 remaining maps
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all reduce-tasks to be running
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.REDUCE) {
        mrApp.waitForState(t, TaskState.RUNNING);
      }
    }

    // Finish off 2 reduces
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);

    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.59f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.59f, rmApp.getProgress(), 0.001f);

    // Finish off the remaining 8 reduces.
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 8);
    allocator.schedule();
    rmDispatcher.await();
    // Remaining is JobCleanup
    Assert.assertEquals(0.95f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.95f, rmApp.getProgress(), 0.001f);
  }

  private void finishNextNTasks(DrainDispatcher rmDispatcher, MockNM node,
      MRApp mrApp, Iterator<Task> it, int nextN) throws Exception {
    Task task;
    for (int i=0; i<nextN; i++) {
      task = it.next();
      finishTask(rmDispatcher, node, mrApp, task);
    }
  }

  private void finishTask(DrainDispatcher rmDispatcher, MockNM node,
      MRApp mrApp, Task task) throws Exception {
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    List<ContainerStatus> contStatus = new ArrayList<ContainerStatus>(1);
    contStatus.add(BuilderUtils.newContainerStatus(attempt.getAssignedContainerID(),
        ContainerState.COMPLETE, "", 0));
    Map<ApplicationId,List<ContainerStatus>> statusUpdate =
        new HashMap<ApplicationId,List<ContainerStatus>>(1);
    statusUpdate.put(mrApp.getAppID(), contStatus);
    node.nodeHeartbeat(statusUpdate, true);
    rmDispatcher.await();
    mrApp.getContext().getEventHandler().handle(
          new TaskAttemptEvent(attempt.getID(), TaskAttemptEventType.TA_DONE));
    mrApp.waitForState(task, TaskState.SUCCEEDED);
  }

  @Test
  public void testReportedAppProgressWithOnlyMaps() throws Exception {

    LOG.info("Running testReportedAppProgressWithOnlyMaps");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp rmApp = rm.submitApp(1024);
    rmDispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 11264);
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();

    final ApplicationAttemptId appAttemptId = rmApp.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rmDispatcher.await();

    MRApp mrApp = new MRApp(appAttemptId, BuilderUtils.newContainerId(
      appAttemptId, 0), 10, 0, false, this.getClass().getName(), true, 1) {
      @Override
        protected Dispatcher createDispatcher() {
          return new DrainDispatcher();
        }
      protected ContainerAllocator createContainerAllocator(
          ClientService clientService, AppContext context) {
        return new MyContainerAllocator(rm, appAttemptId, context);
      };
    };

    Assert.assertEquals(0.0, rmApp.getProgress(), 0.0);

    mrApp.submit(conf);
    Job job = mrApp.getContext().getAllJobs().entrySet().iterator().next()
        .getValue();

    DrainDispatcher amDispatcher = (DrainDispatcher) mrApp.getDispatcher();

    MyContainerAllocator allocator = (MyContainerAllocator) mrApp
      .getContainerAllocator();

    mrApp.waitForState(job, JobState.RUNNING);

    amDispatcher.await();
    // Wait till all map-attempts request for containers
    for (Task t : job.getTasks().values()) {
      mrApp.waitForInternalState((TaskAttemptImpl) t.getAttempts().values()
          .iterator().next(), TaskAttemptStateInternal.UNASSIGNED);
    }
    amDispatcher.await();

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all map-tasks to be running
    for (Task t : job.getTasks().values()) {
      mrApp.waitForState(t, TaskState.RUNNING);
    }

    allocator.schedule(); // Send heartbeat
    rmDispatcher.await();
    Assert.assertEquals(0.05f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.05f, rmApp.getProgress(), 0.001f);

    Iterator<Task> it = job.getTasks().values().iterator();

    // Finish off 1 map so that map-progress is 10%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.14f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.14f, rmApp.getProgress(), 0.001f);

    // Finish off 5 more map so that map-progress is 60%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 5);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.59f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.59f, rmApp.getProgress(), 0.001f);

    // Finish off remaining map so that map-progress is 100%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 4);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.95f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.95f, rmApp.getProgress(), 0.001f);
  }
  
  @Test
  public void testUpdatedNodes() throws Exception {
    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nm1 = rm.registerNode("h1:1234", 10240);
    MockNM nm2 = rm.registerNode("h2:1234", 10240);
    dispatcher.await();

    // create the map container request
    ContainerRequestEvent event = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event);
    TaskAttemptId attemptId = event.getAttemptID();
    
    TaskAttempt mockTaskAttempt = mock(TaskAttempt.class);
    when(mockTaskAttempt.getNodeId()).thenReturn(nm1.getNodeId());
    Task mockTask = mock(Task.class);
    when(mockTask.getAttempt(attemptId)).thenReturn(mockTaskAttempt);
    when(mockJob.getTask(attemptId.getTaskId())).thenReturn(mockTask);

    // this tells the scheduler about the requests
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();

    nm1.nodeHeartbeat(true);
    dispatcher.await();
    // get the assignment
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(1, assigned.size());
    Assert.assertEquals(nm1.getNodeId(), assigned.get(0).getContainer().getNodeId());
    // no updated nodes reported
    Assert.assertTrue(allocator.getJobUpdatedNodeEvents().isEmpty());
    Assert.assertTrue(allocator.getTaskAttemptKillEvents().isEmpty());
    
    // mark nodes bad
    nm1.nodeHeartbeat(false);
    nm2.nodeHeartbeat(false);
    dispatcher.await();
    
    // schedule response returns updated nodes
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, assigned.size());
    // updated nodes are reported
    Assert.assertEquals(1, allocator.getJobUpdatedNodeEvents().size());
    Assert.assertEquals(1, allocator.getTaskAttemptKillEvents().size());
    Assert.assertEquals(2, allocator.getJobUpdatedNodeEvents().get(0).getUpdatedNodes().size());
    Assert.assertEquals(attemptId, allocator.getTaskAttemptKillEvents().get(0).getTaskAttemptID());
    allocator.getJobUpdatedNodeEvents().clear();
    allocator.getTaskAttemptKillEvents().clear();
    
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, assigned.size());
    // no updated nodes reported
    Assert.assertTrue(allocator.getJobUpdatedNodeEvents().isEmpty());
    Assert.assertTrue(allocator.getTaskAttemptKillEvents().isEmpty());
  }

  @Test
  public void testBlackListedNodes() throws Exception {
    
    LOG.info("Running testBlackListedNodes");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);
    
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h3" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);            
    allocator.sendFailure(f1);
    ContainerFailedEvent f2 = createFailEvent(jobId, 1, "h2", false);            
    allocator.sendFailure(f2);

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());    

    // mark h1/h2 as bad nodes
    nodeManager1.nodeHeartbeat(false);
    nodeManager2.nodeHeartbeat(false);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());    

    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    assigned = allocator.schedule();    
    dispatcher.await();
        
    Assert.assertTrue("No of assignments must be 3", assigned.size() == 3);
    
    // validate that all containers are assigned to h3
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertTrue("Assigned container host not correct", "h3".equals(assig
          .getContainer().getNodeId().getHost()));
    }
  }
  
  @Test
  public void testIgnoreBlacklisting() throws Exception {
    LOG.info("Running testIgnoreBlacklisting");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, 33);

    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM[] nodeManagers = new MockNM[10];
    int nmNum = 0;
    List<TaskAttemptContainerAssignedEvent> assigned = null;
    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    nodeManagers[0].nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator =
        new MyContainerAllocator(rm, conf, appAttemptId, mockJob);

    // Known=1, blacklisted=0, ignore should be false - assign first container
    assigned =
        getContainerOnHost(jobId, 1, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    LOG.info("Failing container _1 on H1 (Node should be blacklisted and"
        + " ignore blacklisting enabled");
    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);
    allocator.sendFailure(f1);

    // Test single node.
    // Known=1, blacklisted=1, ignore should be true - assign 1
    assigned =
        getContainerOnHost(jobId, 2, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=2, blacklisted=1, ignore should be true - assign 1 anyway.
    assigned =
        getContainerOnHost(jobId, 3, 1024, new String[] { "h2" },
            nodeManagers[1], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=3, blacklisted=1, ignore should be true - assign 1 anyway.
    assigned =
        getContainerOnHost(jobId, 4, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Known=3, blacklisted=1, ignore should be true - assign 1
    assigned =
        getContainerOnHost(jobId, 5, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=4, blacklisted=1, ignore should be false - assign 1 anyway
    assigned =
        getContainerOnHost(jobId, 6, 1024, new String[] { "h4" },
            nodeManagers[3], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Test blacklisting re-enabled.
    // Known=4, blacklisted=1, ignore should be false - no assignment on h1
    assigned =
        getContainerOnHost(jobId, 7, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    // RMContainerRequestor would have created a replacement request.

    // Blacklist h2
    ContainerFailedEvent f2 = createFailEvent(jobId, 3, "h2", false);
    allocator.sendFailure(f2);

    // Test ignore blacklisting re-enabled
    // Known=4, blacklisted=2, ignore should be true. Should assign 2
    // containers.
    assigned =
        getContainerOnHost(jobId, 8, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 2", 2, assigned.size());

    // Known=4, blacklisted=2, ignore should be true.
    assigned =
        getContainerOnHost(jobId, 9, 1024, new String[] { "h2" },
            nodeManagers[1], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Test blacklist while ignore blacklisting enabled
    ContainerFailedEvent f3 = createFailEvent(jobId, 4, "h3", false);
    allocator.sendFailure(f3);

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=5, blacklisted=3, ignore should be true.
    assigned =
        getContainerOnHost(jobId, 10, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());
    
    // Assign on 5 more nodes - to re-enable blacklisting
    for (int i = 0; i < 5; i++) {
      nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
      assigned =
          getContainerOnHost(jobId, 11 + i, 1024,
              new String[] { String.valueOf(5 + i) }, nodeManagers[4 + i],
              dispatcher, allocator);
      Assert.assertEquals("No of assignments must be 1", 1, assigned.size());
    }

    // Test h3 (blacklisted while ignoring blacklisting) is blacklisted.
    assigned =
        getContainerOnHost(jobId, 20, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
  }

  private MockNM registerNodeManager(int i, MyResourceManager rm,
      DrainDispatcher dispatcher) throws Exception {
    MockNM nm = rm.registerNode("h" + (i + 1) + ":1234", 10240);
    dispatcher.await();
    return nm;
  }

  private
      List<TaskAttemptContainerAssignedEvent> getContainerOnHost(JobId jobId,
          int taskAttemptId, int memory, String[] hosts, MockNM mockNM,
          DrainDispatcher dispatcher, MyContainerAllocator allocator)
          throws Exception {
    ContainerRequestEvent reqEvent =
        createReq(jobId, taskAttemptId, memory, hosts);
    allocator.sendRequest(reqEvent);

    // Send the request to the RM
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Heartbeat from the required nodeManager
    mockNM.nodeHeartbeat(true);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    return assigned;
  }
 
  @Test
  public void testBlackListedNodesWithSchedulingToThatNode() throws Exception {
    LOG.info("Running testBlackListedNodesWithSchedulingToThatNode");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);
    
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    LOG.info("Requesting 1 Containers _1 on H1");
    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    LOG.info("RM Heartbeat (to send the container requests)");
    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    LOG.info("h1 Heartbeat (To actually schedule the containers)");
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());    
    
    LOG.info("Failing container _1 on H1 (should blacklist the node)");
    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);
    allocator.sendFailure(f1);

    //At this stage, a request should be created for a fast fail map
    //Create a FAST_FAIL request for a previously failed map.
    ContainerRequestEvent event1f = createReq(jobId, 1, 1024,
        new String[] { "h1" }, true, false);
    allocator.sendRequest(event1f);

    //Update the Scheduler with the new requests.
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h1", "h3" });
    allocator.sendRequest(event3);
    
    //Allocator is aware of prio:5 container, and prio:20 (h1+h3) container.
    //RM is only aware of the prio:5 container
    
    LOG.info("h1 Heartbeat (To actually schedule the containers)");
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());    
    
    //RMContainerAllocator gets assigned a p:5 on a blacklisted node.

    //Send a release for the p:5 container + another request.
    LOG.info("RM Heartbeat (To process the re-scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    
    //Hearbeat from H3 to schedule on this host.
    LOG.info("h3 Heartbeat (To re-schedule the containers)");
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the re-scheduled containers for H3)");
    assigned = allocator.schedule();
    dispatcher.await();
     
    // For debugging
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      LOG.info(assig.getTaskAttemptID() +
          " assgined to " + assig.getContainer().getId() +
          " with priority " + assig.getContainer().getPriority());
    }
    
    Assert.assertEquals("No of assignments must be 2", 2, assigned.size());
    
    // validate that all containers are assigned to h3
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertEquals("Assigned container " + assig.getContainer().getId()
          + " host not correct", "h3", assig.getContainer().getNodeId().getHost());
    }
  }
  
  private static class MyFifoScheduler extends FifoScheduler {

    public MyFifoScheduler(RMContext rmContext) {
      super();
      try {
        Configuration conf = new Configuration();
        reinitialize(conf, rmContext);
      } catch (IOException ie) {
        LOG.info("add application failed with ", ie);
        assert (false);
      }
    }
    
    List<ResourceRequest> lastAsk = null;
    
    // override this to copy the objects otherwise FifoScheduler updates the
    // numContainers in same objects as kept by RMContainerAllocator
    @Override
    public synchronized Allocation allocate(
        ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
        List<ContainerId> release) {
      List<ResourceRequest> askCopy = new ArrayList<ResourceRequest>();
      for (ResourceRequest req : ask) {
        ResourceRequest reqCopy = BuilderUtils.newResourceRequest(req
            .getPriority(), req.getHostName(), req.getCapability(), req
            .getNumContainers());
        askCopy.add(reqCopy);
      }
      lastAsk = ask;
      return super.allocate(applicationAttemptId, askCopy, release);
    }
  }

  private ContainerRequestEvent createReq(JobId jobId, int taskAttemptId,
      int memory, String[] hosts) {
    return createReq(jobId, taskAttemptId, memory, hosts, false, false);
  }

  private ContainerRequestEvent
      createReq(JobId jobId, int taskAttemptId, int memory, String[] hosts,
          boolean earlierFailedAttempt, boolean reduce) {
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
        taskAttemptId);
    Resource containerNeed = BuilderUtils.newResource(memory, 1);
    if (earlierFailedAttempt) {
      return ContainerRequestEvent
          .createContainerRequestEventForFailedContainer(attemptId,
              containerNeed);
    }
    return new ContainerRequestEvent(attemptId, containerNeed, hosts,
        new String[] { NetworkTopology.DEFAULT_RACK });
  }

  private ContainerFailedEvent createFailEvent(JobId jobId, int taskAttemptId,
      String host, boolean reduce) {
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
        taskAttemptId);
    return new ContainerFailedEvent(attemptId, host);    
  }
  
  private void checkAssignments(ContainerRequestEvent[] requests,
      List<TaskAttemptContainerAssignedEvent> assignments,
      boolean checkHostMatch) {
    Assert.assertNotNull("Container not assigned", assignments);
    Assert.assertEquals("Assigned count not correct", requests.length,
        assignments.size());

    // check for uniqueness of containerIDs
    Set<ContainerId> containerIds = new HashSet<ContainerId>();
    for (TaskAttemptContainerAssignedEvent assigned : assignments) {
      containerIds.add(assigned.getContainer().getId());
    }
    Assert.assertEquals("Assigned containers must be different", assignments
        .size(), containerIds.size());

    // check for all assignment
    for (ContainerRequestEvent req : requests) {
      TaskAttemptContainerAssignedEvent assigned = null;
      for (TaskAttemptContainerAssignedEvent ass : assignments) {
        if (ass.getTaskAttemptID().equals(req.getAttemptID())) {
          assigned = ass;
          break;
        }
      }
      checkAssignment(req, assigned, checkHostMatch);
    }
  }

  private void checkAssignment(ContainerRequestEvent request,
      TaskAttemptContainerAssignedEvent assigned, boolean checkHostMatch) {
    Assert.assertNotNull("Nothing assigned to attempt "
        + request.getAttemptID(), assigned);
    Assert.assertEquals("assigned to wrong attempt", request.getAttemptID(),
        assigned.getTaskAttemptID());
    if (checkHostMatch) {
      Assert.assertTrue("Not assigned to requested host", Arrays.asList(
          request.getHosts()).contains(
          assigned.getContainer().getNodeId().getHost()));
    }
  }

  // Mock RMContainerAllocator
  // Instead of talking to remote Scheduler,uses the local Scheduler
  private static class MyContainerAllocator extends RMContainerAllocator {
    static final List<TaskAttemptContainerAssignedEvent> events
      = new ArrayList<TaskAttemptContainerAssignedEvent>();
    static final List<TaskAttemptKillEvent> taskAttemptKillEvents 
      = new ArrayList<TaskAttemptKillEvent>();
    static final List<JobUpdatedNodesEvent> jobUpdatedNodeEvents 
    = new ArrayList<JobUpdatedNodesEvent>();
    private MyResourceManager rm;

    private static AppContext createAppContext(
        ApplicationAttemptId appAttemptId, Job job) {
      AppContext context = mock(AppContext.class);
      ApplicationId appId = appAttemptId.getApplicationId();
      when(context.getApplicationID()).thenReturn(appId);
      when(context.getApplicationAttemptId()).thenReturn(appAttemptId);
      when(context.getJob(isA(JobId.class))).thenReturn(job);
      when(context.getClusterInfo()).thenReturn(
          new ClusterInfo(BuilderUtils.newResource(1024, 1), BuilderUtils
              .newResource(10240, 1)));
      when(context.getEventHandler()).thenReturn(new EventHandler() {
        @Override
        public void handle(Event event) {
          // Only capture interesting events.
          if (event instanceof TaskAttemptContainerAssignedEvent) {
            events.add((TaskAttemptContainerAssignedEvent) event);
          } else if (event instanceof TaskAttemptKillEvent) {
            taskAttemptKillEvents.add((TaskAttemptKillEvent)event);
          } else if (event instanceof JobUpdatedNodesEvent) {
            jobUpdatedNodeEvents.add((JobUpdatedNodesEvent)event);
          }
        }
      });
      return context;
    }

    private static AppContext createAppContext(
        ApplicationAttemptId appAttemptId, Job job, Clock clock) {
      AppContext context = createAppContext(appAttemptId, job);
      when(context.getClock()).thenReturn(clock);
      return context;
    }

    private static ClientService createMockClientService() {
      ClientService service = mock(ClientService.class);
      when(service.getBindAddress()).thenReturn(
          NetUtils.createSocketAddr("localhost:4567"));
      when(service.getHttpPort()).thenReturn(890);
      return service;
    }

    // Use this constructor when using a real job.
    MyContainerAllocator(MyResourceManager rm,
        ApplicationAttemptId appAttemptId, AppContext context) {
      super(createMockClientService(), context);
      this.rm = rm;
    }

    // Use this constructor when you are using a mocked job.
    public MyContainerAllocator(MyResourceManager rm, Configuration conf,
        ApplicationAttemptId appAttemptId, Job job) {
      super(createMockClientService(), createAppContext(appAttemptId, job));
      this.rm = rm;
      super.init(conf);
      super.start();
    }

    public MyContainerAllocator(MyResourceManager rm, Configuration conf,
        ApplicationAttemptId appAttemptId, Job job, Clock clock) {
      super(createMockClientService(),
          createAppContext(appAttemptId, job, clock));
      this.rm = rm;
      super.init(conf);
      super.start();
    }

    @Override
    protected AMRMProtocol createSchedulerProxy() {
      return this.rm.getApplicationMasterService();
    }

    @Override
    protected void register() {
      super.register();
    }

    @Override
    protected void unregister() {
    }

    @Override
    protected Resource getMinContainerCapability() {
      return BuilderUtils.newResource(1024, 1);
    }

    @Override
    protected Resource getMaxContainerCapability() {
      return BuilderUtils.newResource(10240, 1);
    }

    public void sendRequest(ContainerRequestEvent req) {
      sendRequests(Arrays.asList(new ContainerRequestEvent[] { req }));
    }

    public void sendRequests(List<ContainerRequestEvent> reqs) {
      for (ContainerRequestEvent req : reqs) {
        super.handleEvent(req);
      }
    }

    public void sendFailure(ContainerFailedEvent f) {
      super.handleEvent(f);
    }
    
    // API to be used by tests
    public List<TaskAttemptContainerAssignedEvent> schedule() {
      // run the scheduler
      try {
        super.heartbeat();
      } catch (Exception e) {
        LOG.error("error in heartbeat ", e);
        throw new YarnException(e);
      }

      List<TaskAttemptContainerAssignedEvent> result
        = new ArrayList<TaskAttemptContainerAssignedEvent>(events);
      events.clear();
      return result;
    }
    
    List<TaskAttemptKillEvent> getTaskAttemptKillEvents() {
      return taskAttemptKillEvents;
    }
    
    List<JobUpdatedNodesEvent> getJobUpdatedNodeEvents() {
      return jobUpdatedNodeEvents;
    }

    @Override
    protected void startAllocatorThread() {
      // override to NOT start thread
    }
        
  }

  @Test
  public void testReduceScheduling() throws Exception {
    int totalMaps = 10;
    int succeededMaps = 1;
    int scheduledMaps = 10;
    int scheduledReduces = 0;
    int assignedMaps = 2;
    int assignedReduces = 0;
    int mapResourceReqt = 1024;
    int reduceResourceReqt = 2*1024;
    int numPendingReduces = 4;
    float maxReduceRampupLimit = 0.5f;
    float reduceSlowStart = 0.2f;
    
    RMContainerAllocator allocator = mock(RMContainerAllocator.class);
    doCallRealMethod().when(allocator).
        scheduleReduces(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), 
            anyInt(), anyInt(), anyInt(), anyInt(), anyFloat(), anyFloat());
    
    // Test slow-start
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, never()).setIsReduceStarted(true);
    
    // verify slow-start still in effect when no more maps need to
    // be scheduled but some have yet to complete
    allocator.scheduleReduces(
        totalMaps, succeededMaps,
        0, scheduledReduces,
        totalMaps - succeededMaps, assignedReduces,
        mapResourceReqt, reduceResourceReqt,
        numPendingReduces,
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, never()).setIsReduceStarted(true);
    verify(allocator, never()).scheduleAllReduces();

    succeededMaps = 3;
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, times(1)).setIsReduceStarted(true);
    
    // Test reduce ramp-up
    doReturn(100 * 1024).when(allocator).getMemLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator).rampUpReduces(anyInt());
    verify(allocator, never()).rampDownReduces(anyInt());

    // Test reduce ramp-down
    scheduledReduces = 3;
    doReturn(10 * 1024).when(allocator).getMemLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator).rampDownReduces(anyInt());
  }

  private static class RecalculateContainerAllocator extends MyContainerAllocator {
    public boolean recalculatedReduceSchedule = false;

    public RecalculateContainerAllocator(MyResourceManager rm,
        Configuration conf, ApplicationAttemptId appAttemptId, Job job) {
      super(rm, conf, appAttemptId, job);
    }

    @Override
    public void scheduleReduces(int totalMaps, int completedMaps,
        int scheduledMaps, int scheduledReduces, int assignedMaps,
        int assignedReduces, int mapResourceReqt, int reduceResourceReqt,
        int numPendingReduces, float maxReduceRampupLimit, float reduceSlowStart) {
      recalculatedReduceSchedule = true;
    }
  }
  
  @Test
  public void testCompletedTasksRecalculateSchedule() throws Exception {
    LOG.info("Running testCompletedTasksRecalculateSchedule");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job job = mock(Job.class);
    when(job.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    doReturn(10).when(job).getTotalMaps();
    doReturn(10).when(job).getTotalReduces();
    doReturn(0).when(job).getCompletedMaps();
    RecalculateContainerAllocator allocator =
        new RecalculateContainerAllocator(rm, conf, appAttemptId, job);
    allocator.schedule();

    allocator.recalculatedReduceSchedule = false;
    allocator.schedule();
    Assert.assertFalse("Unexpected recalculate of reduce schedule",
        allocator.recalculatedReduceSchedule);

    doReturn(1).when(job).getCompletedMaps();
    allocator.schedule();
    Assert.assertTrue("Expected recalculate of reduce schedule",
        allocator.recalculatedReduceSchedule);
  }

  @Test
  public void testHeartbeatHandler() throws Exception {
    LOG.info("Running testHeartbeatHandler");

    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS, 1);
    ControlledClock clock = new ControlledClock(new SystemClock());
    AppContext appContext = mock(AppContext.class);
    when(appContext.getClock()).thenReturn(clock);
    when(appContext.getApplicationID()).thenReturn(
        BuilderUtils.newApplicationId(1, 1));

    RMContainerAllocator allocator = new RMContainerAllocator(
        mock(ClientService.class), appContext) {
          @Override
          protected void register() {
          }
          @Override
          protected AMRMProtocol createSchedulerProxy() {
            return mock(AMRMProtocol.class);
          }
          @Override
          protected synchronized void heartbeat() throws Exception {
          }
    };
    allocator.init(conf);
    allocator.start();

    clock.setTime(5);
    int timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 5 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(5, allocator.getLastHeartbeatTime());
    clock.setTime(7);
    timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 7 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(7, allocator.getLastHeartbeatTime());

    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    allocator.runOnNextHeartbeat(new Runnable() {
      @Override
      public void run() {
        callbackCalled.set(true);
      }
    });
    clock.setTime(8);
    timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 8 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(8, allocator.getLastHeartbeatTime());
    Assert.assertTrue(callbackCalled.get());
  }

  @Test
  public void testCompletedContainerEvent() {
    RMContainerAllocator allocator = new RMContainerAllocator(
        mock(ClientService.class), mock(AppContext.class));
    
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(
        MRBuilderUtils.newTaskId(
            MRBuilderUtils.newJobId(1, 1, 1), 1, TaskType.MAP), 1);
    ContainerId containerId = BuilderUtils.newContainerId(1, 1, 1, 1);
    ContainerStatus status = BuilderUtils.newContainerStatus(
        containerId, ContainerState.RUNNING, "", 0);

    ContainerStatus abortedStatus = BuilderUtils.newContainerStatus(
        containerId, ContainerState.RUNNING, "",
        YarnConfiguration.ABORTED_CONTAINER_EXIT_STATUS);
    
    TaskAttemptEvent event = allocator.createContainerFinishedEvent(status,
        attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_CONTAINER_COMPLETED,
        event.getType());
    
    TaskAttemptEvent abortedEvent = allocator.createContainerFinishedEvent(
        abortedStatus, attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_KILL, abortedEvent.getType());
  }
  
  public static void main(String[] args) throws Exception {
    TestRMContainerAllocator t = new TestRMContainerAllocator();
    t.testSimple();
    t.testResource();
    t.testMapReduceScheduling();
    t.testReportedAppProgress();
    t.testReportedAppProgressWithOnlyMaps();
    t.testBlackListedNodes();
    t.testCompletedTasksRecalculateSchedule();
  }

}
