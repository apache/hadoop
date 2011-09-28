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

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
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
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Test;

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
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING,
            0, 0, 0, 0, 0, 0));
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

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h3" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event2, event3 },
        assigned, false);
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
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING,
            0, 0, 0, 0, 0, 0));
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
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING,
            0, 0, 0, 0, 0, 0));
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
      return new MyFifoScheduler(getRMContext());
    }
  }

  private static class FakeJob extends JobImpl {

    public FakeJob(ApplicationAttemptId appAttemptID, Configuration conf,
        int numMaps, int numReduces) {
      super(appAttemptID, conf, null, null, null, null, null, null, null,
          null);
      this.jobId = MRBuilderUtils
          .newJobId(appAttemptID.getApplicationId(), 0);
      this.numMaps = numMaps;
      this.numReduces = numReduces;
    }

    private float setupProgress;
    private float mapProgress;
    private float reduceProgress;
    private float cleanupProgress;
    private final int numMaps;
    private final int numReduces;
    private JobId jobId;

    void setProgress(float setupProgress, float mapProgress,
        float reduceProgress, float cleanupProgress) {
      this.setupProgress = setupProgress;
      this.mapProgress = mapProgress;
      this.reduceProgress = reduceProgress;
      this.cleanupProgress = cleanupProgress;
    }

    @Override
    public int getTotalMaps() { return this.numMaps; }
    @Override
    public int getTotalReduces() { return this.numReduces;}

    @Override
    public JobReport getReport() {
      return MRBuilderUtils.newJobReport(this.jobId, "job", "user",
          JobState.RUNNING, 0, 0, this.setupProgress, this.mapProgress,
          this.reduceProgress, this.cleanupProgress);
    }
  }

  @Test
  public void testReportedAppProgress() throws Exception {

    LOG.info("Running testReportedAppProgress");

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

    FakeJob job = new FakeJob(appAttemptId, conf, 2, 2);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, job);

    allocator.schedule(); // Send heartbeat
    dispatcher.await();
    Assert.assertEquals(0.0, app.getProgress(), 0.0);

    job.setProgress(100, 10, 0, 0);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(9.5f, app.getProgress(), 0.0);

    job.setProgress(100, 80, 0, 0);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(41.0f, app.getProgress(), 0.0);

    job.setProgress(100, 100, 20, 0);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(59.0f, app.getProgress(), 0.0);

    job.setProgress(100, 100, 100, 100);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(100.0f, app.getProgress(), 0.0);
  }

  @Test
  public void testReportedAppProgressWithOnlyMaps() throws Exception {

    LOG.info("Running testReportedAppProgressWithOnlyMaps");

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

    FakeJob job = new FakeJob(appAttemptId, conf, 2, 0);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, job);

    allocator.schedule(); // Send heartbeat
    dispatcher.await();
    Assert.assertEquals(0.0, app.getProgress(), 0.0);

    job.setProgress(100, 10, 0, 0);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(14f, app.getProgress(), 0.0);

    job.setProgress(100, 60, 0, 0);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(59.0f, app.getProgress(), 0.0);

    job.setProgress(100, 100, 0, 100);
    allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(100.0f, app.getProgress(), 0.0);
  }

  private static class MyFifoScheduler extends FifoScheduler {

    public MyFifoScheduler(RMContext rmContext) {
      super();
      try {
        reinitialize(new Configuration(), new ContainerTokenSecretManager(),
            rmContext);
      } catch (IOException ie) {
        LOG.info("add application failed with ", ie);
        assert (false);
      }
    }

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
    Resource containerNeed = BuilderUtils.newResource(memory);
    if (earlierFailedAttempt) {
      return ContainerRequestEvent
          .createContainerRequestEventForFailedContainer(attemptId,
              containerNeed);
    }
    return new ContainerRequestEvent(attemptId, containerNeed, hosts,
        new String[] { NetworkTopology.DEFAULT_RACK });
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
          assigned.getContainer().getNodeId().toString()));
    }
  }

  // Mock RMContainerAllocator
  // Instead of talking to remote Scheduler,uses the local Scheduler
  private static class MyContainerAllocator extends RMContainerAllocator {
    static final List<TaskAttemptContainerAssignedEvent> events
      = new ArrayList<TaskAttemptContainerAssignedEvent>();

    private MyResourceManager rm;

    @SuppressWarnings("rawtypes")
    private static AppContext createAppContext(
        ApplicationAttemptId appAttemptId, Job job) {
      AppContext context = mock(AppContext.class);
      ApplicationId appId = appAttemptId.getApplicationId();
      when(context.getApplicationID()).thenReturn(appId);
      when(context.getApplicationAttemptId()).thenReturn(appAttemptId);
      when(context.getJob(isA(JobId.class))).thenReturn(job);
      when(context.getEventHandler()).thenReturn(new EventHandler() {
        @Override
        public void handle(Event event) {
          // Only capture interesting events.
          if (event instanceof TaskAttemptContainerAssignedEvent) {
            events.add((TaskAttemptContainerAssignedEvent) event);
          }
        }
      });
      return context;
    }

    private static ClientService createMockClientService() {
      ClientService service = mock(ClientService.class);
      when(service.getBindAddress()).thenReturn(
          NetUtils.createSocketAddr("localhost:4567"));
      when(service.getHttpPort()).thenReturn(890);
      return service;
    }

    MyContainerAllocator(MyResourceManager rm, Configuration conf,
        ApplicationAttemptId appAttemptId, Job job) {
      super(createMockClientService(), createAppContext(appAttemptId, job));
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
      return BuilderUtils.newResource(1024);
    }

    @Override
    protected Resource getMaxContainerCapability() {
      return BuilderUtils.newResource(10240);
    }

    public void sendRequest(ContainerRequestEvent req) {
      sendRequests(Arrays.asList(new ContainerRequestEvent[] { req }));
    }

    public void sendRequests(List<ContainerRequestEvent> reqs) {
      for (ContainerRequestEvent req : reqs) {
        super.handle(req);
      }
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

    protected void startAllocatorThread() {
      // override to NOT start thread
    }
  }

  public static void main(String[] args) throws Exception {
    TestRMContainerAllocator t = new TestRMContainerAllocator();
    t.testSimple();
    t.testResource();
    t.testMapReduceScheduling();
    t.testReportedAppProgress();
    t.testReportedAppProgressWithOnlyMaps();
  }
}
