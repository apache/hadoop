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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRMContainerAllocator {
  private static final Log LOG = LogFactory.getLog(TestRMContainerAllocator.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  @BeforeClass
  public static void preTests() {
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testSimple() throws Exception {
    FifoScheduler scheduler = createScheduler();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler, new Configuration());

    //add resources to scheduler
    RMNode nodeManager1 = addNode(scheduler, "h1", 10240);
    RMNode nodeManager2 = addNode(scheduler, "h2", 10240);
    RMNode nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    ContainerRequestEvent event1 = 
      createReq(1, 1024, new String[]{"h1"});
    allocator.sendRequest(event1);

    //send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(2, 1024, new String[]{"h2"});
    allocator.sendRequest(event2);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(3, 1024, new String[]{"h3"});
    allocator.sendRequest(event3);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3); // Node heartbeat


    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event2, event3}, assigned, false);
  }

  //TODO: Currently Scheduler seems to have bug where it does not work
  //for Application asking for containers with different capabilities.
  //@Test
  public void testResource() throws Exception {
    FifoScheduler scheduler = createScheduler();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler, new Configuration());

    //add resources to scheduler
    RMNode nodeManager1 = addNode(scheduler, "h1", 10240);
    RMNode nodeManager2 = addNode(scheduler, "h2", 10240);
    RMNode nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    ContainerRequestEvent event1 = 
      createReq(1, 1024, new String[]{"h1"});
    allocator.sendRequest(event1);

    //send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(2, 2048, new String[]{"h2"});
    allocator.sendRequest(event2);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3); // Node heartbeat

    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event2}, assigned, false);
  }

  @Test
  public void testMapReduceScheduling() throws Exception {
    FifoScheduler scheduler = createScheduler();
    Configuration conf = new Configuration();
    LocalRMContainerAllocator allocator = new LocalRMContainerAllocator(
        scheduler, conf);

    //add resources to scheduler
    RMNode nodeManager1 = addNode(scheduler, "h1", 1024);
    RMNode nodeManager2 = addNode(scheduler, "h2", 10240);
    RMNode nodeManager3 = addNode(scheduler, "h3", 10240);

    //create the container request
    //send MAP request
    ContainerRequestEvent event1 = 
      createReq(1, 2048, new String[]{"h1", "h2"}, true, false);
    allocator.sendRequest(event1);

    //send REDUCE request
    ContainerRequestEvent event2 = createReq(2, 3000, new String[]{"h1"}, false, true);
    allocator.sendRequest(event2);

    //send MAP request
    ContainerRequestEvent event3 = createReq(3, 2048, new String[]{"h3"}, false, false);
    allocator.sendRequest(event3);

    //this tells the scheduler about the requests
    //as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    //update resources in scheduler
    scheduler.nodeUpdate(nodeManager1); // Node heartbeat
    scheduler.nodeUpdate(nodeManager2); // Node heartbeat
    scheduler.nodeUpdate(nodeManager3); // Node heartbeat

    assigned = allocator.schedule();
    checkAssignments(
        new ContainerRequestEvent[]{event1, event3}, assigned, false);

    //validate that no container is assigned to h1 as it doesn't have 2048
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertFalse("Assigned count not correct", 
          "h1".equals(assig.getContainer().getContainerManagerAddress()));
    }
  }



  private RMNode addNode(FifoScheduler scheduler, 
      String nodeName, int memory) {
    NodeId nodeId = recordFactory.newRecordInstance(NodeId.class);
    nodeId.setId(0);
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemory(memory);
    RMNode nodeManager = new RMNodeImpl(nodeId, nodeName, 0, 0,
        ResourceTrackerService.resolve(nodeName), resource
        );
    scheduler.addNode(nodeManager); // Node registration
    return nodeManager;
  }

  private FifoScheduler createScheduler() throws YarnRemoteException {
    FifoScheduler fsc = new FifoScheduler() {
      //override this to copy the objects
      //otherwise FifoScheduler updates the numContainers in same objects as kept by
      //RMContainerAllocator
      
      @Override
      public synchronized Allocation allocate(ApplicationId applicationId,
          List<ResourceRequest> ask, List<Container> release) 
          throws IOException {
        List<ResourceRequest> askCopy = new ArrayList<ResourceRequest>();
        for (ResourceRequest req : ask) {
          ResourceRequest reqCopy = recordFactory.newRecordInstance(ResourceRequest.class);
          reqCopy.setPriority(req.getPriority());
          reqCopy.setHostName(req.getHostName());
          reqCopy.setCapability(req.getCapability());
          reqCopy.setNumContainers(req.getNumContainers());
          askCopy.add(reqCopy);
        }
        //no need to copy release
        return super.allocate(applicationId, askCopy, release);
      }
    };
    try {
      fsc.reinitialize(new Configuration(), new ContainerTokenSecretManager(), null);
      fsc.addApplication(recordFactory.newRecordInstance(ApplicationId.class),
          recordFactory.newRecordInstance(ApplicationMaster.class),
          "test", null, null, StoreFactory.createVoidAppStore());
    } catch(IOException ie) {
      LOG.info("add application failed with ", ie);
      assert(false);
    }
    return fsc;
  }

  private ContainerRequestEvent createReq(
      int attemptid, int memory, String[] hosts) {
    return createReq(attemptid, memory, hosts, false, false);
  }
  
  private ContainerRequestEvent createReq(
      int attemptid, int memory, String[] hosts, boolean earlierFailedAttempt, boolean reduce) {
    ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(0);
    appId.setId(0);
    JobId jobId = recordFactory.newRecordInstance(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(0);
    TaskId taskId = recordFactory.newRecordInstance(TaskId.class);
    taskId.setId(0);
    taskId.setJobId(jobId);
    if (reduce) {
      taskId.setTaskType(TaskType.REDUCE);
    } else {
      taskId.setTaskType(TaskType.MAP);
    }
    TaskAttemptId attemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    attemptId.setId(attemptid);
    attemptId.setTaskId(taskId);
    Resource containerNeed = recordFactory.newRecordInstance(Resource.class);
    containerNeed.setMemory(memory);
    if (earlierFailedAttempt) {
      return ContainerRequestEvent.
           createContainerRequestEventForFailedContainer(attemptId, containerNeed);
    }
    return new ContainerRequestEvent(attemptId, 
        containerNeed, 
        hosts, new String[] {NetworkTopology.DEFAULT_RACK});
  }

  private void checkAssignments(ContainerRequestEvent[] requests, 
      List<TaskAttemptContainerAssignedEvent> assignments, 
      boolean checkHostMatch) {
    Assert.assertNotNull("Container not assigned", assignments);
    Assert.assertEquals("Assigned count not correct", 
        requests.length, assignments.size());

    //check for uniqueness of containerIDs
    Set<ContainerId> containerIds = new HashSet<ContainerId>();
    for (TaskAttemptContainerAssignedEvent assigned : assignments) {
      containerIds.add(assigned.getContainer().getId());
    }
    Assert.assertEquals("Assigned containers must be different", 
        assignments.size(), containerIds.size());

    //check for all assignment
    for (ContainerRequestEvent req : requests) {
      TaskAttemptContainerAssignedEvent assigned = null;
      for (TaskAttemptContainerAssignedEvent ass : assignments) {
        if (ass.getTaskAttemptID().equals(req.getAttemptID())){
          assigned = ass;
          break;
        }
      }
      checkAssignment(req, assigned, checkHostMatch);
    }
  }

  private void checkAssignment(ContainerRequestEvent request, 
      TaskAttemptContainerAssignedEvent assigned, boolean checkHostMatch) {
    Assert.assertNotNull("Nothing assigned to attempt " + request.getAttemptID(),
        assigned);
    Assert.assertEquals("assigned to wrong attempt", request.getAttemptID(),
        assigned.getTaskAttemptID());
    if (checkHostMatch) {
      Assert.assertTrue("Not assigned to requested host", Arrays.asList
          (request.getHosts()).contains(assigned.getContainer().getContainerManagerAddress()));
    }

  }

  //Mock RMContainerAllocator
  //Instead of talking to remote Scheduler,uses the local Scheduler
  public static class LocalRMContainerAllocator extends RMContainerAllocator {
    private static final List<TaskAttemptContainerAssignedEvent> events = 
      new ArrayList<TaskAttemptContainerAssignedEvent>();

    public static class AMRMProtocolImpl implements AMRMProtocol {

      private ResourceScheduler resourceScheduler;

      public AMRMProtocolImpl(ResourceScheduler resourceScheduler) {
        this.resourceScheduler = resourceScheduler;
      }

      @Override
      public RegisterApplicationMasterResponse registerApplicationMaster(RegisterApplicationMasterRequest request) throws YarnRemoteException {
        ApplicationMaster applicationMaster = request.getApplicationMaster();
        RegisterApplicationMasterResponse response = recordFactory.newRecordInstance(RegisterApplicationMasterResponse.class);
        return null;
      }

      public AllocateResponse allocate(AllocateRequest request) throws YarnRemoteException {
        ApplicationStatus status = request.getApplicationStatus();
        List<ResourceRequest> ask = request.getAskList();
        List<Container> release = request.getReleaseList();
        try {
          AMResponse response = recordFactory.newRecordInstance(AMResponse.class);
          Allocation allocation = resourceScheduler.allocate(status.getApplicationId(), ask, release);
          response.addAllNewContainers(allocation.getContainers());
          response.setAvailableResources(allocation.getResourceLimit());
          AllocateResponse allocateResponse = recordFactory.newRecordInstance(AllocateResponse.class);
          allocateResponse.setAMResponse(response);
          return allocateResponse;
        } catch(IOException ie) {
          throw RPCUtil.getRemoteException(ie);
        }
      }

      @Override
      public FinishApplicationMasterResponse finishApplicationMaster(FinishApplicationMasterRequest request) throws YarnRemoteException {
        ApplicationMaster applicationMaster = request.getApplicationMaster();
        FinishApplicationMasterResponse response = recordFactory.newRecordInstance(FinishApplicationMasterResponse.class);
        return response;
      }

    }

    private ResourceScheduler scheduler;
    LocalRMContainerAllocator(ResourceScheduler scheduler, Configuration conf) {
      super(null, new TestContext(events));
      this.scheduler = scheduler;
      super.init(conf);
      super.start();
    }

    protected AMRMProtocol createSchedulerProxy() {
      return new AMRMProtocolImpl(scheduler);
    }

    @Override
    protected void register() {}
    @Override
    protected void unregister() {}

    @Override
    protected Resource getMinContainerCapability() {
      Resource res = recordFactory.newRecordInstance(Resource.class);
      res.setMemory(1024);
      return res;
    }
    
    @Override
    protected Resource getMaxContainerCapability() {
      Resource res = recordFactory.newRecordInstance(Resource.class);
      res.setMemory(10240);
      return res;
    }
    
    public void sendRequest(ContainerRequestEvent req) {
      sendRequests(Arrays.asList(new ContainerRequestEvent[]{req}));
    }

    public void sendRequests(List<ContainerRequestEvent> reqs) {
      for (ContainerRequestEvent req : reqs) {
        handle(req);
      }
    }

    //API to be used by tests
    public List<TaskAttemptContainerAssignedEvent> schedule() {
      //run the scheduler
      try {
        heartbeat();
      } catch (Exception e) {
        LOG.error("error in heartbeat ", e);
        throw new YarnException(e);
      }

      List<TaskAttemptContainerAssignedEvent> result = new ArrayList(events);
      events.clear();
      return result;
    }

    protected void startAllocatorThread() {
      //override to NOT start thread
    }

    static class TestContext implements AppContext {
      private List<TaskAttemptContainerAssignedEvent> events;
      TestContext(List<TaskAttemptContainerAssignedEvent> events) {
        this.events = events;
      }
      @Override
      public Map<JobId, Job> getAllJobs() {
        return null;
      }
      @Override
      public ApplicationAttemptId getApplicationAttemptId() {
        return recordFactory.newRecordInstance(ApplicationAttemptId.class);
      }
      @Override
      public ApplicationId getApplicationID() {
        return recordFactory.newRecordInstance(ApplicationId.class);
      }
      @Override
      public EventHandler getEventHandler() {
        return new EventHandler() {
          @Override
          public void handle(Event event) {
            if (event instanceof TaskAttemptContainerAssignedEvent) {
              events.add((TaskAttemptContainerAssignedEvent) event);
            } //Ignoring JobCounterUpdateEvents
          }
        };
      }
      @Override
      public Job getJob(JobId jobID) {
        return null;
      }

      @Override
      public String getUser() {
        return null;
      }

      @Override
      public Clock getClock() {
        return null;
      }

      @Override
      public String getApplicationName() {
        return null;
      }

      @Override
      public long getStartTime() {
        return 0;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    TestRMContainerAllocator t = new TestRMContainerAllocator();
    t.testSimple();
    //t.testResource();
    t.testMapReduceScheduling();
  }
}
