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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Assert;
import org.junit.Test;

public class TestAppSchedulingInfo {

  @Test
  public void testSchedulerKeyAccounting() {
    ApplicationId appIdImpl = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appIdImpl, 1);

    Queue queue = mock(Queue.class);
    doReturn(mock(QueueMetrics.class)).when(queue).getMetrics();
    AppSchedulingInfo  info = new AppSchedulingInfo(
        appAttemptId, "test", queue, mock(ActiveUsersManager.class), 0);
    Assert.assertEquals(0, info.getPriorities().size());

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
    ArrayList<Priority> priorities = new ArrayList<>(info.getPriorities());
    Assert.assertEquals(2, priorities.size());
    Assert.assertEquals(req1.getPriority(), priorities.get(0));
    Assert.assertEquals(req2.getPriority(), priorities.get(1));

    // iterate to verify no ConcurrentModificationException
    for (Priority priority: info.getPriorities()) {
      info.allocate(NodeType.OFF_SWITCH, null, priority, req1, null);
    }
    Assert.assertEquals(1, info.getPriorities().size());
    Assert.assertEquals(req2.getPriority(),
        info.getPriorities().iterator().next());

    req2 = ResourceRequest.newInstance(pri2,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 1);
    reqs.clear();
    reqs.add(req2);
    info.updateResourceRequests(reqs, false);
    info.allocate(NodeType.OFF_SWITCH, null, req2.getPriority(), req2, null);
    Assert.assertEquals(0, info.getPriorities().size());

    req1 = ResourceRequest.newInstance(pri1,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 5);
    reqs.clear();
    reqs.add(req1);
    info.updateResourceRequests(reqs, false);
    Assert.assertEquals(1, info.getPriorities().size());
    Assert.assertEquals(req1.getPriority(),
        info.getPriorities().iterator().next());
    req1 = ResourceRequest.newInstance(pri1,
        ResourceRequest.ANY, Resource.newInstance(1024, 1), 0);
    reqs.clear();
    reqs.add(req1);
    info.updateResourceRequests(reqs, false);
    Assert.assertEquals(0, info.getPriorities().size());
  }
}
