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

package org.apache.hadoop.mapreduce.v2.app.rm;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerReuseRequestor.EventType;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerReuseRequestor.HostInfo;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for RMContainerReuseRequestor.
 */
public class TestRMContainerReuseRequestor {

  private RMContainerReuseRequestor reuseRequestor;

  @Before
  public void setup() throws IOException {
    RMContainerAllocator allocator = mock(RMContainerAllocator.class);
    Job job = mock(Job.class);
    Task task = mock(Task.class);
    TaskAttempt taskAttempt = mock(TaskAttempt.class);
    when(taskAttempt.getShufflePort()).thenReturn(0);
    when(task.getAttempt(any(TaskAttemptId.class))).thenReturn(taskAttempt);
    when(job.getTask(any(TaskId.class))).thenReturn(task);
    when(allocator.getJob()).thenReturn(job);
    reuseRequestor = new RMContainerReuseRequestor(null,
        allocator);
  }

  @Test
  public void testNoOfTimesEachMapTaskContainerCanReuseWithDefaultConfig() {
    // Verify that no of times each map task container can be reused with
    // default configuration for
    // 'yarn.app.mapreduce.am.container.reuse.max-maptasks'.
    testNoOfTimesEachContainerCanReuseWithDefaultConfig(TaskType.MAP,
        RMContainerAllocator.PRIORITY_MAP);
  }

  @Test
  public void testNoOfTimesEachMapTaskContainerCanReuseWithConfigLimit() {
    // Verify that no of times each map task container can be reused when
    // 'yarn.app.mapreduce.am.container.reuse.max-maptasks' configured with a
    // value.
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_MAPTASKS, 1);
    testNoOfTimesEachContainerCanReuseWithConfigLimit(TaskType.MAP,
        RMContainerAllocator.PRIORITY_MAP, conf);
  }

  @Test
  public void testNoOfTimesEachRedTaskContainerCanReuseWithDefaultConfig() {
    // Verify that no of times each reduce task container can be reused with
    // default configuration for
    // 'yarn.app.mapreduce.am.container.reuse.max-reducetasks'.
    testNoOfTimesEachContainerCanReuseWithDefaultConfig(TaskType.REDUCE,
        RMContainerAllocator.PRIORITY_REDUCE);
  }

  @Test
  public void testNoOfTimesEachRedTaskContainerCanReuseWithConfigLimit() {
    // Verify that no of times each map task container can be reused when
    // 'yarn.app.mapreduce.am.container.reuse.max-reducetasks' configured with a
    // value.
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_REDUCETASKS, 1);
    testNoOfTimesEachContainerCanReuseWithConfigLimit(TaskType.REDUCE,
        RMContainerAllocator.PRIORITY_REDUCE, conf);
  }

  @Test
  public void testNoOfMaxMapTaskContainersCanReuseWithDefaultConfig() {
    // Verify that no of maximum map containers can be reused at any time with
    // default configuration for
    // 'yarn.app.mapreduce.am.container.reuse.max-maptaskcontainers'.
    testNoOfMaxContainersCanReuseWithDefaultConfig(TaskType.MAP,
        RMContainerAllocator.PRIORITY_MAP);
  }

  @Test
  public void testNoOfMaxMapTaskContainersCanReuseWithConfigLimit() {
    // Verify that no of maximum map containers can be reused at any time when
    // 'yarn.app.mapreduce.am.container.reuse.max-maptaskcontainers' configured
    // with a limit value.
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_MAPTASKCONTAINERS, 1);
    testNoOfMaxContainersCanReuseWithConfigLimit(TaskType.MAP,
        RMContainerAllocator.PRIORITY_MAP, conf);
  }

  @Test
  public void testNoOfMaxRedTaskContainersCanReuseWithDefaultConfig() {
    // Verify that no of maximum reduce containers can be reused at any time
    // with default configuration for
    // 'yarn.app.mapreduce.am.container.reuse.max-reducetasks'.
    testNoOfMaxContainersCanReuseWithDefaultConfig(TaskType.REDUCE,
        RMContainerAllocator.PRIORITY_REDUCE);
  }

  @Test
  public void testNoOfMaxRedTaskContainersCanReuseWithConfigLimit() {
    // Verify that no of maximum reduce containers can be reused at any time
    // when 'yarn.app.mapreduce.am.container.reuse.max-reducetasks' configured
    // with a limit value.
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_CONTAINER_REUSE_MAX_REDUCETASKCONTAINERS, 1);
    testNoOfMaxContainersCanReuseWithConfigLimit(TaskType.REDUCE,
        RMContainerAllocator.PRIORITY_REDUCE, conf);
  }

  @Test
  public void testContainerFailedOnHost() throws Exception {
    reuseRequestor.serviceInit(new Configuration());
    Map<Container, HostInfo> containersToReuse = reuseRequestor
        .getContainersToReuse();
    containersToReuse
        .put(newContainerInstance("container_1472171035081_0009_01_000008",
            RMContainerAllocator.PRIORITY_REDUCE), new HostInfo("node1", 1999));
    containersToReuse
        .put(newContainerInstance("container_1472171035081_0009_01_000009",
            RMContainerAllocator.PRIORITY_REDUCE), new HostInfo("node2", 1999));
    reuseRequestor.getBlacklistedNodes().add("node1");
    // It removes all containers from containersToReuse running in node1
    reuseRequestor.containerFailedOnHost("node1");
    Assert.assertFalse("node1 should not present in reuse containers.",
        containersToReuse.containsValue("node1"));
    // There will not any change to containersToReuse when there are no
    // containers to reuse in that node
    reuseRequestor.containerFailedOnHost("node3");
    Assert.assertEquals(1, containersToReuse.size());
  }

  private void testNoOfTimesEachContainerCanReuseWithDefaultConfig(
      TaskType taskType, Priority priority) {
    // Verify that no of times each container can be reused

    // Add 10 container reqs to the requestor
    addContainerReqs(priority);
    Container container = newContainerInstance(
        "container_123456789_0001_01_000002", priority);
    for (int i = 0; i < 10; i++) {
      JobId jobId = MRBuilderUtils.newJobId(123456789, 1, 1);
      TaskId taskId = MRBuilderUtils.newTaskId(jobId, i + 1, taskType);
      TaskAttemptId taskAttemptId = MRBuilderUtils.newTaskAttemptId(taskId, 1);
      ContainerAvailableEvent event = new ContainerAvailableEvent(
          EventType.CONTAINER_AVAILABLE, taskAttemptId, container);
      reuseRequestor.handle(event);
      Map<Container, HostInfo> containersToReuse = reuseRequestor
          .getContainersToReuse();
      Assert.assertTrue("Container should be added for reuse.",
          containersToReuse.containsKey(container));
    }
  }

  private void testNoOfTimesEachContainerCanReuseWithConfigLimit(
      TaskType taskType, Priority priority, Configuration conf) {
    reuseRequestor.init(conf);
    // Add a container request
    ContainerRequest req1 = new ContainerRequest(null,
        Resource.newInstance(2048, 1), new String[0], new String[0], priority,
        null);
    reuseRequestor.addContainerReq(req1);
    // Add an another container request
    ContainerRequest req2 = new ContainerRequest(null,
        Resource.newInstance(2048, 1), new String[0], new String[0], priority,
        null);
    reuseRequestor.addContainerReq(req2);

    EventType eventType = EventType.CONTAINER_AVAILABLE;
    Container container = newContainerInstance(
        "container_123456789_0001_01_000002", priority);
    JobId jobId = MRBuilderUtils.newJobId(123456789, 1, 1);
    TaskId taskId1 = MRBuilderUtils.newTaskId(jobId, 1, taskType);
    TaskAttemptId taskAttemptId1 = MRBuilderUtils.newTaskAttemptId(taskId1, 1);

    TaskId taskId2 = MRBuilderUtils.newTaskId(jobId, 2, taskType);
    TaskAttemptId taskAttemptId2 = MRBuilderUtils.newTaskAttemptId(taskId2, 1);

    ContainerAvailableEvent event1 = new ContainerAvailableEvent(eventType,
        taskAttemptId1, container);
    reuseRequestor.handle(event1);
    Map<Container, HostInfo> containersToReuse = reuseRequestor
        .getContainersToReuse();
    // It is reusing the container
    Assert.assertTrue("Container should be added for reuse.",
        containersToReuse.containsKey(container));
    containersToReuse.clear();
    ContainerAvailableEvent event2 = new ContainerAvailableEvent(eventType,
        taskAttemptId2, container);
    reuseRequestor.handle(event2);
    // It should not be reused since it has already reused and limit value is 1.
    Assert.assertFalse("Container should not be added for reuse.",
        containersToReuse.containsKey(container));
  }

  private void testNoOfMaxContainersCanReuseWithDefaultConfig(TaskType taskType,
      Priority priority) {
    // It tests no of times each container can be reused

    // Add 10 container reqs to the requestor
    addContainerReqs(priority);
    for (int i = 0; i < 10; i++) {
      Container container = newContainerInstance(
          "container_123456789_0001_01_00000" + (i + 2), priority);
      JobId jobId = MRBuilderUtils.newJobId(123456789, 1, 1);
      TaskId taskId1 = MRBuilderUtils.newTaskId(jobId, i + 1, taskType);
      TaskAttemptId taskAttemptId1 = MRBuilderUtils.newTaskAttemptId(taskId1,
          1);
      ContainerAvailableEvent event1 = new ContainerAvailableEvent(
          EventType.CONTAINER_AVAILABLE, taskAttemptId1, container);
      reuseRequestor.handle(event1);
      Map<Container, HostInfo> containersToReuse = reuseRequestor
          .getContainersToReuse();
      Assert.assertTrue("Container should be added for reuse.",
          containersToReuse.containsKey(container));
    }
  }

  private void testNoOfMaxContainersCanReuseWithConfigLimit(TaskType taskType,
      Priority priority, Configuration conf) {
    reuseRequestor.init(conf);
    ContainerRequest req1 = new ContainerRequest(null,
        Resource.newInstance(2048, 1), new String[0], new String[0], priority,
        null);
    reuseRequestor.addContainerReq(req1);

    ContainerRequest req2 = new ContainerRequest(null,
        Resource.newInstance(2048, 1), new String[0], new String[0], priority,
        null);
    reuseRequestor.addContainerReq(req2);

    EventType eventType = EventType.CONTAINER_AVAILABLE;
    Container container1 = newContainerInstance(
        "container_123456789_0001_01_000002", priority);
    JobId jobId = MRBuilderUtils.newJobId(123456789, 1, 1);
    TaskId taskId1 = MRBuilderUtils.newTaskId(jobId, 1, taskType);
    TaskAttemptId taskAttemptId1 = MRBuilderUtils.newTaskAttemptId(taskId1, 1);

    TaskId taskId2 = MRBuilderUtils.newTaskId(jobId, 2, taskType);
    TaskAttemptId taskAttemptId2 = MRBuilderUtils.newTaskAttemptId(taskId2, 1);

    ContainerAvailableEvent event1 = new ContainerAvailableEvent(eventType,
        taskAttemptId1, container1);
    reuseRequestor.handle(event1);
    Map<Container, HostInfo> containersToReuse = reuseRequestor
        .getContainersToReuse();
    Assert.assertTrue("Container should be added for reuse.",
        containersToReuse.containsKey(container1));
    containersToReuse.clear();
    Container container2 = newContainerInstance(
        "container_123456789_0001_01_000003", priority);
    ContainerAvailableEvent event2 = new ContainerAvailableEvent(eventType,
        taskAttemptId2, container2);
    reuseRequestor.handle(event2);
    Assert.assertFalse("Container should not be added for reuse.",
        containersToReuse.containsKey(container2));
  }

  private void addContainerReqs(Priority priority) {
    Configuration conf = new Configuration();
    reuseRequestor.init(conf);
    for (int i = 0; i < 10; i++) {
      ContainerRequest req = new ContainerRequest(null,
          Resource.newInstance(2048, 1), new String[0], new String[0], priority,
          null);
      reuseRequestor.addContainerReq(req);
    }
  }

  private Container newContainerInstance(String containerId,
      Priority priority) {
    return Container.newInstance(ContainerId.fromString(containerId),
        NodeId.newInstance("node1", 8080), "", null, priority, null);
  }

  @After
  public void tearDown() {
    reuseRequestor.stop();
  }
}
