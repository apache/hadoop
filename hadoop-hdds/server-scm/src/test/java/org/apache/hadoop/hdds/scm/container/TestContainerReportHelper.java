/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Set;

/**
 * Helper methods for testing ContainerReportHandler and
 * IncrementalContainerReportHandler.
 */
public final class TestContainerReportHelper {

  private TestContainerReportHelper() {}

  static void addContainerToContainerManager(
      final ContainerManager containerManager, final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws ContainerNotFoundException {
    Mockito.when(containerManager.getContainer(container.containerID()))
        .thenReturn(container);
    Mockito.when(
        containerManager.getContainerReplicas(container.containerID()))
        .thenReturn(replicas);
  }

  static void mockUpdateContainerReplica(
      final ContainerManager containerManager,
      final ContainerInfo containerInfo, final Set<ContainerReplica> replicas)
      throws ContainerNotFoundException {
    Mockito.doAnswer((Answer<Void>) invocation -> {
      Object[] args = invocation.getArguments();
      if (args[0].equals(containerInfo.containerID())) {
        ContainerReplica replica = (ContainerReplica) args[1];
        replicas.remove(replica);
        replicas.add(replica);
      }
      return null;
    }).when(containerManager).updateContainerReplica(
        Mockito.any(ContainerID.class), Mockito.any(ContainerReplica.class));
  }

  static void mockUpdateContainerState(
      final ContainerManager containerManager,
      final ContainerInfo containerInfo,
      final LifeCycleEvent event, final LifeCycleState state)
      throws IOException {
    Mockito.doAnswer((Answer<LifeCycleState>) invocation -> {
      containerInfo.setState(state);
      return containerInfo.getState();
    }).when(containerManager).updateContainerState(
        containerInfo.containerID(), event);
  }

}
