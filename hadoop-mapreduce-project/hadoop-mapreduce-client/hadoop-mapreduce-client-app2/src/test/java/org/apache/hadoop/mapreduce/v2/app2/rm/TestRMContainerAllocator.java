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

package org.apache.hadoop.mapreduce.v2.app2.rm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.ContainerHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerAssignTAEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerMap;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventNodeCountUpdated;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeMap;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;


public class TestRMContainerAllocator {

  static final Log LOG = LogFactory.getLog(TestRMContainerAllocator.class);

  static final Priority MAP_PRIORITY = RMContainerAllocatorForTest.getMapPriority();
  static final Priority REDUCE_PRIORITY = RMContainerAllocatorForTest.getReducePriority();
  static final Priority FAST_FAIL_MAP_PRIORITY = RMContainerAllocatorForTest.getFailedMapPriority();
  
  private static final int port = 3333;

  /**
   * Verifies simple allocation. Matches pending requets to assigned containers.
   */
  @Test
  public void testSimple() {
    LOG.info("Running testSimple");

    YarnConfiguration conf = new YarnConfiguration();
    TrackingEventHandler eventHandler = new TrackingEventHandler();
    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RMContainerAllocatorForTest scheduler = new RMContainerAllocatorForTest(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));

    AMSchedulerTALaunchRequestEvent event1 = createTALaunchReq(jobId, 1, 1024,
        new String[] { "h1" });
    AMSchedulerTALaunchRequestEvent event2 = createTALaunchReq(jobId, 2, 1024,
        new String[] { "h2" });
    AMSchedulerTALaunchRequestEvent event3 = createTALaunchReq(jobId, 3, 1024,
        new String[] { "h3" });

    scheduler.handleEvent(event1);
    scheduler.handleEvent(event2);
    scheduler.handleEvent(event3);

    assertEquals(3, rmComm.addRequests.size());

    Container container1 = newContainer(appContext, 1, "h1", 1024, MAP_PRIORITY);
    Container container2 = newContainer(appContext, 2, "h2", 1024, MAP_PRIORITY);
    Container container3 = newContainer(appContext, 3, "h3", 1024, MAP_PRIORITY);

    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(container1.getId());
    containerIds.add(container2.getId());
    containerIds.add(container3.getId());

    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);
    scheduler.handleEvent(allocatedEvent);

    checkAssignments(new AMSchedulerTALaunchRequestEvent[] { event1, event2,
        event3 }, eventHandler.launchRequests, eventHandler.assignEvents, true,
        appContext);
  }
  
  /**
   * Verifies allocation based on resource ask and allocation.
   */
  @Test
  public void testResource() {
    LOG.info("Running testResource");

    YarnConfiguration conf = new YarnConfiguration();
    TrackingEventHandler eventHandler = new TrackingEventHandler();
    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RMContainerAllocatorForTest scheduler = new RMContainerAllocatorForTest(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));

    AMSchedulerTALaunchRequestEvent event1 = createTALaunchReq(jobId, 1, 1024,
        new String[] { "h1" });
    AMSchedulerTALaunchRequestEvent event2 = createTALaunchReq(jobId, 2, 2048,
        new String[] { "h2" });

    // Register the asks with the AMScheduler
    scheduler.handleEvent(event1);
    scheduler.handleEvent(event2);

    assertEquals(2, rmComm.addRequests.size());

    Container container1 = newContainer(appContext, 1, "h1", 1024, MAP_PRIORITY);
    Container container2 = newContainer(appContext, 2, "h2", 2048, MAP_PRIORITY);

    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(container1.getId());
    containerIds.add(container2.getId());

    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);
    scheduler.handleEvent(allocatedEvent);

    checkAssignments(new AMSchedulerTALaunchRequestEvent[] { event1, event2 },
        eventHandler.launchRequests, eventHandler.assignEvents, true,
        appContext);
  }
  
  @Test
  public void testMapReduceScheduling() {
    LOG.info("Running testMapReduceScheduling");

    YarnConfiguration conf = new YarnConfiguration();
    TrackingEventHandler eventHandler = new TrackingEventHandler();

    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RMContainerAllocatorForTest scheduler = new RMContainerAllocatorForTest(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));

    // Map, previously failed.
    AMSchedulerTALaunchRequestEvent event1 = createTALaunchReq(jobId, 1, 2048,
        new String[] { "h1", "h2" }, true, false);

    // Reduce, first attempt.
    AMSchedulerTALaunchRequestEvent event2 = createTALaunchReq(jobId, 2, 3000,
        new String[] { "h1" }, false, true);

    // Map, first attempt.
    AMSchedulerTALaunchRequestEvent event3 = createTALaunchReq(jobId, 3, 2048,
        new String[] { "h3" }, false, false);

    // Register the asks with the AMScheduler
    scheduler.handleEvent(event1);
    scheduler.handleEvent(event2);
    scheduler.handleEvent(event3);

    // The reduce should not have been asked for yet - event2
    assertEquals(2, rmComm.addRequests.size());

   
    Container container1 = newContainer(appContext, 1, "h1", 1024, MAP_PRIORITY);
    Container container2 = newContainer(appContext, 2, "h1", 3072,
        REDUCE_PRIORITY);
    Container container3 = newContainer(appContext, 3, "h3", 2048, MAP_PRIORITY);
    Container container4 = newContainer(appContext, 4, "h1", 2048,
        FAST_FAIL_MAP_PRIORITY);
    // Container1 - release low mem.
    // Container2 - release no pending reduce request.
    // Container3 - assign to t3
    // Container4 - assign to t1

    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(container1.getId());
    containerIds.add(container2.getId());
    containerIds.add(container3.getId());
    containerIds.add(container4.getId());

    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);
    scheduler.handleEvent(allocatedEvent);

    // Two stop container events for Container1 and Container2 ?
    assertEquals(2, eventHandler.stopEvents.size());

    // Since maps have been assigned containers. Verify that a request is sent
    // out for the pending reduce task.
    assertEquals(3, rmComm.addRequests.size());

    // Verify map assignments.
    checkAssignments(new AMSchedulerTALaunchRequestEvent[] { event1, event3 },
        eventHandler.launchRequests, eventHandler.assignEvents, true,
        appContext);

    eventHandler.reset();

    Container container5 = newContainer(appContext, 5, "h1", 3072,
        REDUCE_PRIORITY); // assign to t2
    containerIds.clear();
    containerIds.add(container5.getId());
    allocatedEvent = new AMSchedulerEventContainersAllocated(containerIds,
        false);
    scheduler.handleEvent(allocatedEvent);

    // Verify reduce assignment.
    checkAssignments(new AMSchedulerTALaunchRequestEvent[] { event2 },
        eventHandler.launchRequests, eventHandler.assignEvents, true,
        appContext);
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

  @Test
  public void testBlackListedNodes() throws Exception {
    LOG.info("Running testBlackListedNodes");

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);
    TrackingEventHandler eventHandler = new TrackingEventHandler();
    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RMContainerAllocatorForTest scheduler = new RMContainerAllocatorForTest(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));

    // 3 requests on 3 hosts.
    AMSchedulerTALaunchRequestEvent event1 = createTALaunchReq(jobId, 1, 1024,
        new String[] { "h1", "h4" });
    AMSchedulerTALaunchRequestEvent event2 = createTALaunchReq(jobId, 2, 1024,
        new String[] { "h2" });
    AMSchedulerTALaunchRequestEvent event3 = createTALaunchReq(jobId, 3, 1024,
        new String[] { "h3" });

    scheduler.handleEvent(event1);
    scheduler.handleEvent(event2);
    scheduler.handleEvent(event3);

    assertEquals(3, rmComm.addRequests.size());

    // 3 containers on 3 hosts.
    Container container1 = newContainer(appContext, 1, "h1", 1024, MAP_PRIORITY);
    Container container2 = newContainer(appContext, 2, "h2", 1024, MAP_PRIORITY);
    Container container3 = newContainer(appContext, 3, "h3", 1024, MAP_PRIORITY);

    // Simulate two blacklisted nodes.
    appContext.getAllNodes().handle(
        new AMNodeEvent(container1.getNodeId(),
            AMNodeEventType.N_NODE_WAS_BLACKLISTED));
    appContext.getAllNodes().handle(
        new AMNodeEvent(container2.getNodeId(),
            AMNodeEventType.N_NODE_WAS_BLACKLISTED));

    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(container1.getId());
    containerIds.add(container2.getId());
    containerIds.add(container3.getId());

    rmComm.reset();

    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);
    scheduler.handleEvent(allocatedEvent);

    // Only contianer 3 should have been assigned.
    checkAssignments(new AMSchedulerTALaunchRequestEvent[] { event3 },
        eventHandler.launchRequests, eventHandler.assignEvents, true,
        appContext);

    // Verify container stop events for the remaining two containers.
    assertEquals(2, eventHandler.stopEvents.size());
    Set<ContainerId> tmpSet = new HashSet<ContainerId>();
    tmpSet.add(container1.getId());
    tmpSet.add(container2.getId());
    for (AMContainerEvent ame : eventHandler.stopEvents) {
      tmpSet.remove(ame.getContainerId());
    }
    assertEquals(0, tmpSet.size());

    // Verify new request events were sent out to replace these containers.
    assertEquals(2, rmComm.addRequests.size());
    // One of the requests should refer to host4.
    boolean hostSeen = false;
    for (ContainerRequest c : rmComm.addRequests) {
      if (c.hosts.length != 0) {
        if (!hostSeen) {
          assertEquals(1, c.hosts.length);
          assertEquals("h4", c.hosts[0]);
          hostSeen = true;
        } else {
          fail("Only one request should have a host");
        }
      }
    }
  }
  
  @Test
  public void testIgnoreBlacklisting() throws Exception {
    LOG.info("Running testIgnoreBlacklisting");

    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, 33);

    TrackingEventHandler eventHandler = new TrackingEventHandler();
    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RMContainerAllocatorForTest scheduler = new RMContainerAllocatorForTest(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));

    int attemptId = 0;
    int currentHostNum = 0;
    NodeId[] nodeIds = new NodeId[10];

    // Add a node.
    nodeIds[currentHostNum] = addNode(currentHostNum, appContext);

    // known=1, blacklisted=0. IgnoreBlacklisting=false, Assign 1
    assertFalse(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    // Blacklist node1.
    blacklistNode(nodeIds[0], appContext);

    // known=1, blacklisted=1. IgnoreBlacklisting=true, Assign 1
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    currentHostNum++;
    nodeIds[currentHostNum] = addNode(currentHostNum, appContext);
    // Known=2, blacklisted=1, ignore should be true - assign 1 anyway.
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    currentHostNum++;
    nodeIds[currentHostNum] = addNode(currentHostNum, appContext);
    // Known=3, blacklisted=1, ignore should be true - assign 1 anyway. (Request
    // on non blacklisted)
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    // Known=3, blacklisted=1, ignore should be true - assign 1 anyway. (Request
    // on blacklisted)
    assignContainerOnHost(jobId, nodeIds[0], ++attemptId, scheduler,
        eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    currentHostNum++;
    nodeIds[currentHostNum] = addNode(currentHostNum, appContext);
    // Known=4, blacklisted=1, ignore should be false - assign 1 anyway.
    // (Request on non blacklisted)
    assertFalse(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    // Known=4, blacklisted=1, ignore should be false - assign 1 anyway.
    // (Request on blacklisted)
    assignContainerOnHost(jobId, nodeIds[0], ++attemptId, scheduler,
        eventHandler, appContext);
    assertEquals(0, eventHandler.assignEvents.size());

    // Blacklist node2.
    blacklistNode(nodeIds[1], appContext);

    // Known=4, blacklisted=2, ignore should be true - assign 1 anyway. (Request
    // on blacklisted)
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[0], ++attemptId, scheduler,
        eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    // Blacklist node3. (While ignore is enabled)
    blacklistNode(nodeIds[2], appContext);
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());

    currentHostNum++;
    nodeIds[currentHostNum] = addNode(currentHostNum, appContext);
    // Known=4, blacklisted=2, ignore should be true - assign 1 anyway. (Request
    // on non-blacklisted)
    assertTrue(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[currentHostNum], ++attemptId,
        scheduler, eventHandler, appContext);
    assertEquals(1, eventHandler.assignEvents.size());

    // Add 5 more nodes.
    for (int i = 0; i < 5; i++) {
      currentHostNum++;
      nodeIds[currentHostNum] = addNode(currentHostNum, appContext);
    }

    // Known=9, blacklisted=3, ignore should be false - assign 1 on host3.
    assertFalse(appContext.getAllNodes().isBlacklistingIgnored());
    assignContainerOnHost(jobId, nodeIds[2], ++attemptId, scheduler,
        eventHandler, appContext);
    assertEquals(0, eventHandler.assignEvents.size());

  }

  @Test
  public void testCompletedTasksRecalculateSchedule() throws Exception {
    LOG.info("Running testCompletedTasksRecalculateSchedule");
    YarnConfiguration conf = new YarnConfiguration();

    TrackingEventHandler eventHandler = new TrackingEventHandler();
    AppContext appContext = setupDefaultTestContext(eventHandler, conf);

    TrackingAMContainerRequestor rmComm = new TrackingAMContainerRequestor(
        appContext);
    rmComm.init(conf);
    rmComm.start();
    RecalculateContainerAllocator scheduler = new RecalculateContainerAllocator(
        rmComm, appContext);
    scheduler.init(conf);
    scheduler.start();

    JobId jobId = TypeConverter.toYarn(TypeConverter.fromYarn(appContext
        .getApplicationID()));
    Job job = appContext.getJob(jobId);
    doReturn(10).when(job).getTotalMaps();
    doReturn(10).when(job).getTotalReduces();
    doReturn(2).when(job).getCompletedMaps();
    doReturn(0).when(job).getCompletedReduces();

    List<ContainerId> containerIds = new LinkedList<ContainerId>();

    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);

    // Ignore the first allocate.
    scheduler.handleEvent(allocatedEvent);

    // Since nothing changed, recalculate reduce = false.
    scheduler.recalculatedReduceSchedule = false;
    scheduler.handleEvent(allocatedEvent);
    assertFalse("Unexpected recalculate of reduce schedule",
        scheduler.recalculatedReduceSchedule);

    // Change completedMaps. Recalcualte reduce should be true.
    doReturn(1).when(job).getCompletedMaps();
    scheduler.handleEvent(allocatedEvent);
    assertTrue("Expected recalculate of reduce schedule",
        scheduler.recalculatedReduceSchedule);

    // Since nothing changed, recalculate reduce = false.
    scheduler.recalculatedReduceSchedule = false;
    scheduler.handleEvent(allocatedEvent);
    assertFalse("Unexpected recalculate of reduce schedule",
        scheduler.recalculatedReduceSchedule);
  }
  
  // TODO XXX Unit test for AMNode to simulate node health status change.
  
  
  
  
  
  
  private void blacklistNode(NodeId nodeId, AppContext appContext) {
    appContext.getAllNodes().handle(
        new AMNodeEvent(nodeId, AMNodeEventType.N_NODE_WAS_BLACKLISTED));
  }

  private NodeId addNode(int currentHostNum, AppContext appContext) {
    NodeId nodeId = BuilderUtils.newNodeId("h" + currentHostNum, port);
    appContext.getAllNodes().nodeSeen(nodeId);
    appContext.getAllNodes().handle(
        new AMNodeEventNodeCountUpdated(appContext.getAllNodes().size()));
    return nodeId;
  }

  /**
   * Generates the events for a TaskAttempt start request and an associated 
   * container assignment. Resets TrackingEventHandler statistics.
   */
  void assignContainerOnHost(JobId jobId, NodeId nodeId, int attemptId,
      RMContainerAllocatorForTest scheduler, TrackingEventHandler eventHandler,
      AppContext appContext) {
    eventHandler.reset();
    AMSchedulerTALaunchRequestEvent launchRequest = createTALaunchReq(jobId,
        attemptId, 1024, new String[] { nodeId.getHost() });
    scheduler.handleEvent(launchRequest);
    Container container = newContainer(appContext, attemptId, nodeId.getHost(),
        1024, MAP_PRIORITY);
    List<ContainerId> containerIds = new LinkedList<ContainerId>();
    containerIds.add(container.getId());
    AMSchedulerEventContainersAllocated allocatedEvent = new AMSchedulerEventContainersAllocated(
        containerIds, false);
    scheduler.handleEvent(allocatedEvent);
  }

  /**
   * Verify all assignments and launch requests against the requests.
   */
  private void checkAssignments(AMSchedulerTALaunchRequestEvent[] requests,
      List<AMContainerLaunchRequestEvent> launchRequests,
      List<AMContainerAssignTAEvent> assignEvents, boolean checkHostMatch,
      AppContext appContext) {

    assertNotNull("Containers not assigned", launchRequests);
    assertNotNull("Containers not assigned", assignEvents);
    assertEquals("LaunchRequest Count not correct", requests.length,
        launchRequests.size());
    assertEquals("Assigned Count not correct", requests.length,
        assignEvents.size());

    Set<ContainerId> containerIds = new HashSet<ContainerId>();
    // Check for uniqueness of container id launch requests
    for (AMContainerLaunchRequestEvent launchRequest : launchRequests) {
      containerIds.add(launchRequest.getContainerId());
    }
    assertEquals("Multiple launch requests for same container id",
        assignEvents.size(), containerIds.size());

    // Check for uniqueness of container Id assignments.
    containerIds.clear();
    for (AMContainerAssignTAEvent assignEvent : assignEvents) {
      containerIds.add(assignEvent.getContainerId());
    }
    assertEquals("Assigned container Ids not unique", assignEvents.size(),
        containerIds.size());

    AMContainerAssignTAEvent assignment = null;
    // Check that all requests were assigned a container.
    for (AMSchedulerTALaunchRequestEvent request : requests) {
      for (AMContainerAssignTAEvent assignEvent : assignEvents) {
        if (request.getAttemptID().equals(assignEvent.getTaskAttemptId())) {
          assignment = assignEvent;
          break;
        }
      }
      checkAssignment(request, assignment, checkHostMatch, appContext);
      assignment = null;
    }
  }

  
  /**
   * Verify assignment for a single request / allocation, optionally checking
   * for the requested host.
   */
  private void checkAssignment(AMSchedulerTALaunchRequestEvent request,
      AMContainerAssignTAEvent assignEvent, boolean checkHostMatch,
      AppContext appContext) {

    Assert.assertNotNull(
        "Nothing assigned to attempt " + request.getAttemptID(), assignEvent);
    if (checkHostMatch) {
      if (request.getHosts().length == 0) {
        return;
      } else {
        Assert.assertTrue(
            "Not assigned to requested host",
            Arrays.asList(request.getHosts()).contains(
                appContext.getAllContainers().get(assignEvent.getContainerId())
                    .getContainer().getNodeId().getHost()));
      }
    }
  }

  
  /**
   * Create containers for allocation. Will also register the associated node 
   * with the AMNodeMap, and the container with the AMContainerMap.
   */
  private Container newContainer(AppContext appContext,
      int containerNum, String host, int memory, Priority priority) {
    ContainerId containerId = BuilderUtils.newContainerId(appContext.getApplicationAttemptId(),
        containerNum);
    NodeId nodeId = BuilderUtils.newNodeId(host, port);
    
    appContext.getAllNodes().nodeSeen(nodeId);
    Resource resource = BuilderUtils.newResource(memory);
    Container container = BuilderUtils.newContainer(containerId, nodeId, host
        + ":8000", resource, priority, null);
    appContext.getAllContainers().addContainerIfNew(container);
    return container;
  }
  
  
  private AMSchedulerTALaunchRequestEvent createTALaunchReq(JobId jobId,
      int taskAttemptId, int memory, String[] hosts) {
    return createTALaunchReq(jobId, taskAttemptId, memory, hosts, false, false);
  }
  
  private AMSchedulerTALaunchRequestEvent createTALaunchReq(JobId jobId,
      int taskAttemptId, int memory, String[] hosts,
      boolean earlierFailedAttempt, boolean reduce) {
    Resource resource = BuilderUtils.newResource(memory);
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    String[] hostArray;
    String[] rackArray;
    if (earlierFailedAttempt) {
      hostArray = new String[0];
      rackArray = new String[0];
    } else {
      hostArray = hosts;
      rackArray = new String[] { NetworkTopology.DEFAULT_RACK };
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
        taskAttemptId);
    AMSchedulerTALaunchRequestEvent event = new AMSchedulerTALaunchRequestEvent(
        attemptId, earlierFailedAttempt, resource, null, null, null, null,
        hostArray, rackArray);
    return event;
  }
  
  static class RMContainerAllocatorForTest extends RMContainerAllocator {

    public RMContainerAllocatorForTest(ContainerRequestor requestor,
        AppContext appContext) {
      super(requestor, appContext);
    }
    
    @Override
    public void start() {
      this.job = appContext.getJob(jobId);
    }
    
    @Override
    protected void handleEvent(AMSchedulerEvent event) {
      super.handleEvent(event);
    }
    
    @Override
    protected boolean shouldProfileTaskAttempt(JobConf conf,
        org.apache.hadoop.mapred.Task remoteTask) {
      return false;
    }
    
    static Priority getMapPriority() {
      return BuilderUtils.newPriority(PRIORITY_MAP.getPriority());
    }
    
    static Priority getReducePriority() {
      return BuilderUtils.newPriority(PRIORITY_REDUCE.getPriority());
    }
    
    static Priority getFailedMapPriority() {
      return BuilderUtils.newPriority(PRIORITY_FAST_FAIL_MAP.getPriority());
    }
  }

  static class RecalculateContainerAllocator extends
      RMContainerAllocatorForTest {

    boolean recalculatedReduceSchedule = false;

    public RecalculateContainerAllocator(ContainerRequestor requestor,
        AppContext appContext) {
      super(requestor, appContext);
    }

    @Override
    public void scheduleReduces(int totalMaps, int completedMaps,
        int scheduledMaps, int scheduledReduces, int assignedMaps,
        int assignedReduces, int mapResourceReqt, int reduceResourceReqt,
        int numPendingReduces, float maxReduceRampupLimit, float reduceSlowStart) {
      recalculatedReduceSchedule = true;
    }
    
    @Override
    protected boolean shouldProfileTaskAttempt(JobConf conf,
        org.apache.hadoop.mapred.Task remoteTask) {
      return false;
    }
  }

  class TrackingAMContainerRequestor extends RMContainerRequestor {

    List<ContainerRequest> addRequests = new LinkedList<RMContainerRequestor.ContainerRequest>();
    List<ContainerRequest> decRequests = new LinkedList<RMContainerRequestor.ContainerRequest>();
    private Resource minContainerMemory = Records.newRecord(Resource.class);
    private Resource maxContainerMemory = Records.newRecord(Resource.class);
    
    void reset() {
      addRequests.clear();
      decRequests.clear();
    }

    public TrackingAMContainerRequestor(AppContext context) {
      this(context, 1024, 10240);
    }

    public TrackingAMContainerRequestor(AppContext context,
        int minContainerMemory, int maxContainerMemory) {
      super(null, context);
      this.minContainerMemory.setMemory(minContainerMemory);
      this.maxContainerMemory.setMemory(maxContainerMemory);
    }
   
    @Override
    public void addContainerReq(ContainerRequest request) {
      addRequests.add(request);
    }
    
    @Override
    public void decContainerReq(ContainerRequest request) {
      decRequests.add(request);
    }
    
    @Override
    public Map<ApplicationAccessType, String> getApplicationAcls() {
      return null;
    }
    
    @Override
    public Resource getAvailableResources() {
      return BuilderUtils.newResource(0);
    }
    
    @Override
    protected Resource getMaxContainerCapability() {
      return maxContainerMemory;
    }
    
    @Override
    protected Resource getMinContainerCapability() {
      return minContainerMemory;
    }

    @Override
    public void register() {
    }

    @Override
    public void unregister() {
    }

    @Override
    public void startAllocatorThread() {
    }
    
    @Override
    public AMRMProtocol createSchedulerProxy() {
      return null;
    }
  }
  
  @SuppressWarnings("rawtypes")
  private class TrackingEventHandler implements EventHandler {

    List<AMContainerLaunchRequestEvent> launchRequests = new LinkedList<AMContainerLaunchRequestEvent>();
    List<AMContainerAssignTAEvent> assignEvents = new LinkedList<AMContainerAssignTAEvent>();
    List<AMContainerEvent> stopEvents = new LinkedList<AMContainerEvent>();
    List<RMCommunicatorContainerDeAllocateRequestEvent> releaseEvents = new LinkedList<RMCommunicatorContainerDeAllocateRequestEvent>();
    
    @Override
    public void handle(Event event) {
      if (event.getType() == AMContainerEventType.C_LAUNCH_REQUEST) {
        launchRequests.add((AMContainerLaunchRequestEvent)event);
      } else if (event.getType() == AMContainerEventType.C_ASSIGN_TA) {
        assignEvents.add((AMContainerAssignTAEvent)event);
      } else if (event.getType() == AMContainerEventType.C_STOP_REQUEST) {
        stopEvents.add((AMContainerEvent)event);
      } else if (event.getType() == RMCommunicatorEventType.CONTAINER_DEALLOCATE) {
        releaseEvents.add((RMCommunicatorContainerDeAllocateRequestEvent)event);
      }
    }
    
    public void reset() {
      this.launchRequests.clear();
      this.assignEvents.clear();
      this.stopEvents.clear();
    }
  }

  // TODO Allow specifying the jobId as a parameter
  @SuppressWarnings("rawtypes") 
  private AppContext setupDefaultTestContext(EventHandler eventHandler,
      Configuration conf) {
    AppContext appContext = mock(AppContext.class);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 1);
    JobID id = TypeConverter.fromYarn(appId);
    JobId jobId = TypeConverter.toYarn(id);

    Job mockJob = mock(Job.class);
    when(mockJob.getID()).thenReturn(jobId);
    when(mockJob.getProgress()).thenReturn(0.0f);
    when(mockJob.getConf()).thenReturn(conf);

    Clock clock = new ControlledClock(new SystemClock());

    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    amNodeMap.init(conf);
    amNodeMap.start();
    

    AMContainerMap amContainerMap = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        appContext);
    amContainerMap.init(conf);
    amContainerMap.start();
    when(appContext.getAllContainers()).thenReturn(amContainerMap);

    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getApplicationAttemptId()).thenReturn(appAttemptId);
    when(appContext.getEventHandler()).thenReturn(eventHandler);
    when(appContext.getJob(jobId)).thenReturn(mockJob);
    when(appContext.getClock()).thenReturn(clock);
    when(appContext.getAllNodes()).thenReturn(amNodeMap);
    when(appContext.getClusterInfo()).thenReturn(
        new ClusterInfo(BuilderUtils.newResource(1024), BuilderUtils
            .newResource(10240)));

    return appContext;
  }
  
  // TODO Add a unit test to verify a correct launchContainer invocation with
  // security.
}
