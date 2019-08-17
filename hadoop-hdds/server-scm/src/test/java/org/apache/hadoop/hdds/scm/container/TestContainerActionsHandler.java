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

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerActionsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerActionsFromDatanode;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests ContainerActionsHandler.
 */
public class TestContainerActionsHandler {

  @Test
  public void testCloseContainerAction() {
    EventQueue queue = new EventQueue();
    ContainerActionsHandler actionsHandler = new ContainerActionsHandler();
    CloseContainerEventHandler closeContainerEventHandler = Mockito.mock(
        CloseContainerEventHandler.class);
    queue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerEventHandler);
    queue.addHandler(SCMEvents.CONTAINER_ACTIONS, actionsHandler);

    ContainerAction action = ContainerAction.newBuilder()
        .setContainerID(1L)
        .setAction(ContainerAction.Action.CLOSE)
        .setReason(ContainerAction.Reason.CONTAINER_FULL)
        .build();

    ContainerActionsProto cap = ContainerActionsProto.newBuilder()
        .addContainerActions(action)
        .build();

    ContainerActionsFromDatanode containerActions =
        new ContainerActionsFromDatanode(
            TestUtils.randomDatanodeDetails(), cap);

    queue.fireEvent(SCMEvents.CONTAINER_ACTIONS, containerActions);
    queue.processAll(1000L);
    verify(closeContainerEventHandler, times(1))
        .onMessage(ContainerID.valueof(1L), queue);

  }

}