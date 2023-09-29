/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockContainer {
  ContainerSpecification containerSpec;
  public int containerId;
  private MockApplication mockApp;
  RMContainerImpl rmContainerMock;

  MockContainer(ContainerSpecification containerSpec,
      int containerId, MockApplication mockApp) {
    this.containerSpec = containerSpec;
    this.containerId = containerId;
    this.mockApp = mockApp;
    this.rmContainerMock = mock(RMContainerImpl.class);
    init();
  }

  private void init() {
    Container c = mock(Container.class);
    when(c.getResource()).thenReturn(containerSpec.resource);
    when(c.getPriority()).thenReturn(containerSpec.priority);
    SchedulerRequestKey sk = SchedulerRequestKey.extractFrom(c);
    when(rmContainerMock.getAllocatedSchedulerKey()).thenReturn(sk);
    when(rmContainerMock.getAllocatedNode()).thenReturn(containerSpec.nodeId);
    when(rmContainerMock.getNodeLabelExpression()).thenReturn(containerSpec.label);
    when(rmContainerMock.getAllocatedResource()).thenReturn(containerSpec.resource);
    when(rmContainerMock.getContainer()).thenReturn(c);
    when(rmContainerMock.getApplicationAttemptId()).thenReturn(mockApp.appAttemptId);
    when(rmContainerMock.getQueueName()).thenReturn(mockApp.queueName);
    final ContainerId cId = ContainerId.newContainerId(mockApp.appAttemptId,
        containerId);
    when(rmContainerMock.getContainerId()).thenReturn(cId);
    doAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) {
        return cId.compareTo(
            ((RMContainer) invocation.getArguments()[0]).getContainerId());
      }
    }).when(rmContainerMock).compareTo(any(RMContainer.class));

    if (containerId == 1) {
      when(rmContainerMock.isAMContainer()).thenReturn(true);
    }

    if (containerSpec.reserved) {
      when(rmContainerMock.getReservedResource()).thenReturn(containerSpec.resource);
    }
  }
}
