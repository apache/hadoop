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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.*;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.junit.Assert;
import org.junit.Test;

public class TestAppSchedulingInfo {

  @Test
  public void testBacklistChanged() {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appIdImpl, 1);

    FSLeafQueue queue = mock(FSLeafQueue.class);
    RMContext rmContext = mock(RMContext.class);
    doReturn("test").when(queue).getQueueName();
    doReturn(new YarnConfiguration()).when(rmContext).getYarnConfiguration();
    AppSchedulingInfo appSchedulingInfo = new AppSchedulingInfo(appAttemptId,
        "test", queue, null, 0, new ResourceUsage(),
        new HashMap<String, String>(), rmContext);

    appSchedulingInfo.updatePlacesBlacklistedByApp(new ArrayList<String>(),
        new ArrayList<String>());
    Assert.assertFalse(appSchedulingInfo.getAndResetBlacklistChanged());

    ArrayList<String> blacklistAdditions = new ArrayList<String>();
    blacklistAdditions.add("node1");
    blacklistAdditions.add("node2");
    appSchedulingInfo.updatePlacesBlacklistedByApp(blacklistAdditions,
        new ArrayList<String>());
    Assert.assertTrue(appSchedulingInfo.getAndResetBlacklistChanged());

    blacklistAdditions.clear();
    blacklistAdditions.add("node1");
    appSchedulingInfo.updatePlacesBlacklistedByApp(blacklistAdditions,
        new ArrayList<String>());
    Assert.assertFalse(appSchedulingInfo.getAndResetBlacklistChanged());

    ArrayList<String> blacklistRemovals = new ArrayList<String>();
    blacklistRemovals.add("node1");
    appSchedulingInfo.updatePlacesBlacklistedByApp(new ArrayList<String>(),
        blacklistRemovals);
    appSchedulingInfo.updatePlacesBlacklistedByApp(new ArrayList<String>(),
        blacklistRemovals);
    Assert.assertTrue(appSchedulingInfo.getAndResetBlacklistChanged());

    appSchedulingInfo.updatePlacesBlacklistedByApp(new ArrayList<String>(),
        blacklistRemovals);
    Assert.assertFalse(appSchedulingInfo.getAndResetBlacklistChanged());
  }

  @Test
  public void testSchedulerRequestKeyOrdering() {
    TreeSet<SchedulerRequestKey> ts = new TreeSet<>();
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(1), 1));
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(1), 2));
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(0), 4));
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(0), 3));
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(2), 5));
    ts.add(TestUtils.toSchedulerKey(Priority.newInstance(2), 6));
    Iterator<SchedulerRequestKey> iter = ts.iterator();
    SchedulerRequestKey sk = iter.next();
    Assert.assertEquals(0, sk.getPriority().getPriority());
    Assert.assertEquals(3, sk.getAllocationRequestId());
    sk = iter.next();
    Assert.assertEquals(0, sk.getPriority().getPriority());
    Assert.assertEquals(4, sk.getAllocationRequestId());
    sk = iter.next();
    Assert.assertEquals(1, sk.getPriority().getPriority());
    Assert.assertEquals(1, sk.getAllocationRequestId());
    sk = iter.next();
    Assert.assertEquals(1, sk.getPriority().getPriority());
    Assert.assertEquals(2, sk.getAllocationRequestId());
    sk = iter.next();
    Assert.assertEquals(2, sk.getPriority().getPriority());
    Assert.assertEquals(5, sk.getAllocationRequestId());
    sk = iter.next();
    Assert.assertEquals(2, sk.getPriority().getPriority());
    Assert.assertEquals(6, sk.getAllocationRequestId());
  }

  @Test
  public void testSchedulerKeyAccounting() {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appIdImpl, 1);

    Queue queue = mock(Queue.class);
    doReturn(mock(QueueMetrics.class)).when(queue).getMetrics();
    RMContext rmContext = mock(RMContext.class);
    doReturn(new YarnConfiguration()).when(rmContext).getYarnConfiguration();
    AppSchedulingInfo  info = new AppSchedulingInfo(
        appAttemptId, "test", queue, mock(ActiveUsersManager.class), 0,
        new ResourceUsage(), new HashMap<>(), rmContext);
    Assert.assertEquals(0, info.getSchedulerKeys().size());

    Priority pri1 = Priority.newInstance(1);
    ResourceRequest req1 = ResourceRequest.newInstance(pri1,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 1);
    Priority pri2 = Priority.newInstance(2);
    ResourceRequest req2 = ResourceRequest.newInstance(pri2,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 2);
    List<ResourceRequest> reqs = new ArrayList<>();
    reqs.add(req1);
    reqs.add(req2);
    info.updateResourceRequests(reqs, false);
    ArrayList<SchedulerRequestKey> keys =
        new ArrayList<>(info.getSchedulerKeys());
    Assert.assertEquals(2, keys.size());
    Assert.assertEquals(SchedulerRequestKey.create(req1), keys.get(0));
    Assert.assertEquals(SchedulerRequestKey.create(req2), keys.get(1));

    // iterate to verify no ConcurrentModificationException
    for (SchedulerRequestKey schedulerKey : info.getSchedulerKeys()) {
      info.allocate(NodeType.OFF_SWITCH, null, schedulerKey, null);
    }
    Assert.assertEquals(1, info.getSchedulerKeys().size());
    Assert.assertEquals(SchedulerRequestKey.create(req2),
        info.getSchedulerKeys().iterator().next());

    req2 = ResourceRequest.newInstance(pri2,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 1);
    reqs.clear();
    reqs.add(req2);
    info.updateResourceRequests(reqs, false);
    info.allocate(NodeType.OFF_SWITCH, null, SchedulerRequestKey.create(req2),
        null);
    Assert.assertEquals(0, info.getSchedulerKeys().size());

    req1 = ResourceRequest.newInstance(pri1,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 5);
    reqs.clear();
    reqs.add(req1);
    info.updateResourceRequests(reqs, false);
    Assert.assertEquals(1, info.getSchedulerKeys().size());
    Assert.assertEquals(SchedulerRequestKey.create(req1),
        info.getSchedulerKeys().iterator().next());
    req1 = ResourceRequest.newInstance(pri1,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 0);
    reqs.clear();
    reqs.add(req1);
    info.updateResourceRequests(reqs, false);
    Assert.assertEquals(0, info.getSchedulerKeys().size());
  }
}
