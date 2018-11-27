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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashSet;
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

  public static ContainerInfo getContainer(final LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(RandomUtils.nextLong())
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setState(state)
        .build();
  }

  static Set<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final DatanodeDetails... datanodeDetails) {
    return getReplicas(containerId, state, 10000L, datanodeDetails);
  }

  static Set<ContainerReplica> getReplicas(
      final ContainerID containerId,
      final ContainerReplicaProto.State state,
      final long sequenceId,
      final DatanodeDetails... datanodeDetails) {
    Set<ContainerReplica> replicas = new HashSet<>();
    for (DatanodeDetails datanode : datanodeDetails) {
      replicas.add(ContainerReplica.newBuilder()
          .setContainerID(containerId)
          .setContainerState(state)
          .setDatanodeDetails(datanode)
          .setOriginNodeId(datanode.getUuid())
          .setSequenceId(sequenceId)
          .build());
    }
    return replicas;
  }
}
