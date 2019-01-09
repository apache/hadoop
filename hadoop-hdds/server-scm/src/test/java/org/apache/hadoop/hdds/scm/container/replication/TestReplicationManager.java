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
package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.replication
    .ReplicationManager.ReplicationRequestToRepeat;
import org.apache.hadoop.hdds.scm.container.replication
    .ReplicationManager.DeletionRequestToRepeat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;

import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .TRACK_DELETE_CONTAINER_COMMAND;
import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .TRACK_REPLICATE_COMMAND;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyObject;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

/**
 * Test behaviour of the TestReplication.
 */
public class TestReplicationManager {

  private EventQueue queue;

  private List<ReplicationRequestToRepeat> trackReplicationEvents;
  private List<DeletionRequestToRepeat> trackDeleteEvents;

  private List<CommandForDatanode<ReplicateContainerCommandProto>> copyEvents;

  private ContainerManager containerManager;

  private ContainerPlacementPolicy containerPlacementPolicy;
  private List<DatanodeDetails> listOfDatanodeDetails;
  private List<ContainerReplica> listOfContainerReplica;
  private LeaseManager<Long> leaseManager;
  private ReplicationManager replicationManager;

  @Before
  public void initReplicationManager() throws IOException {

    listOfDatanodeDetails = new ArrayList<>();
    listOfContainerReplica = new ArrayList<>();
    IntStream.range(1, 6).forEach(i -> {
      DatanodeDetails dd = TestUtils.randomDatanodeDetails();
      listOfDatanodeDetails.add(dd);
      listOfContainerReplica.add(ContainerReplica.newBuilder()
          .setContainerID(ContainerID.valueof(i))
          .setContainerState(ContainerReplicaProto.State.CLOSED)
          .setSequenceId(10000L)
          .setOriginNodeId(dd.getUuid())
          .setDatanodeDetails(dd).build());
    });

    containerPlacementPolicy =
        (excludedNodes, nodesRequired, sizeRequired) -> listOfDatanodeDetails
            .subList(2, 2 + nodesRequired);

    containerManager = Mockito.mock(ContainerManager.class);

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(LifeCycleState.CLOSED)
        .build();

    when(containerManager.getContainer(anyObject()))
        .thenReturn(containerInfo);

    when(containerManager.getContainerReplicas(new ContainerID(1L)))
        .thenReturn(new HashSet<>(Arrays.asList(
            listOfContainerReplica.get(0),
            listOfContainerReplica.get(1)
        )));


    when(containerManager.getContainerReplicas(new ContainerID(3L)))
        .thenReturn(new HashSet<>());

    queue = new EventQueue();

    trackReplicationEvents = new ArrayList<>();
    queue.addHandler(TRACK_REPLICATE_COMMAND,
        (event, publisher) -> trackReplicationEvents.add(event));

    trackDeleteEvents = new ArrayList<>();
    queue.addHandler(TRACK_DELETE_CONTAINER_COMMAND,
        (event, publisher) -> trackDeleteEvents.add(event));

    copyEvents = new ArrayList<>();
    queue.addHandler(SCMEvents.DATANODE_COMMAND,
        (event, publisher) -> copyEvents.add(event));

    leaseManager = new LeaseManager<>("Test", 100000L);

    replicationManager = new ReplicationManager(containerPlacementPolicy,
        containerManager, queue, leaseManager);

  }

  /**
   * Container should be replicated but no source replicas.
   */
  @Test()
  public void testNoExistingReplicas() throws InterruptedException {
    try {
      leaseManager.start();
      replicationManager.start();

      //WHEN
      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(3L, (short) 2, System.currentTimeMillis(),
              (short) 3));

      Thread.sleep(500L);
      queue.processAll(1000L);

      //THEN
      Assert.assertEquals(0, trackReplicationEvents.size());
      Assert.assertEquals(0, copyEvents.size());

    } finally {
      if (leaseManager != null) {
        leaseManager.shutdown();
      }
    }
  }

  @Test
  public void testOverReplication() throws ContainerNotFoundException,
      InterruptedException {
    try {
      leaseManager.start();
      replicationManager.start();

      final ContainerID containerID = ContainerID.valueof(5L);

      final ContainerReplica duplicateReplicaOne = ContainerReplica.newBuilder()
          .setContainerID(containerID)
          .setContainerState(ContainerReplicaProto.State.CLOSED)
          .setSequenceId(10000L)
          .setOriginNodeId(listOfDatanodeDetails.get(0).getUuid())
          .setDatanodeDetails(listOfDatanodeDetails.get(3))
          .build();

      final ContainerReplica duplicateReplicaTwo = ContainerReplica.newBuilder()
          .setContainerID(containerID)
          .setContainerState(ContainerReplicaProto.State.CLOSED)
          .setSequenceId(10000L)
          .setOriginNodeId(listOfDatanodeDetails.get(1).getUuid())
          .setDatanodeDetails(listOfDatanodeDetails.get(4))
          .build();

      when(containerManager.getContainerReplicas(new ContainerID(5L)))
          .thenReturn(new HashSet<>(Arrays.asList(
              listOfContainerReplica.get(0),
              listOfContainerReplica.get(1),
              listOfContainerReplica.get(2),
              duplicateReplicaOne,
              duplicateReplicaTwo
          )));

      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(5L, (short) 5, System.currentTimeMillis(),
              (short) 3));
      Thread.sleep(500L);
      queue.processAll(1000L);

      //THEN
      Assert.assertEquals(2, trackDeleteEvents.size());
      Assert.assertEquals(2, copyEvents.size());

    } finally {
      if (leaseManager != null) {
        leaseManager.shutdown();
      }
    }
  }

  @Test
  public void testEventSending() throws InterruptedException, IOException {

    //GIVEN
    try {
      leaseManager.start();

      replicationManager.start();

      //WHEN
      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(1L, (short) 2, System.currentTimeMillis(),
              (short) 3));

      Thread.sleep(500L);
      queue.processAll(1000L);

      //THEN
      Assert.assertEquals(1, trackReplicationEvents.size());
      Assert.assertEquals(1, copyEvents.size());
    } finally {
      if (leaseManager != null) {
        leaseManager.shutdown();
      }
    }
  }

  @Test
  public void testCommandWatcher() throws InterruptedException, IOException {
    LeaseManager<Long> rapidLeaseManager =
        new LeaseManager<>("Test", 1000L);

    replicationManager = new ReplicationManager(containerPlacementPolicy,
        containerManager, queue, rapidLeaseManager);

    try {
      leaseManager.start();
      rapidLeaseManager.start();
      replicationManager.start();

      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(1L, (short) 2, System.currentTimeMillis(),
              (short) 3));

      Thread.sleep(500L);

      queue.processAll(1000L);

      Assert.assertEquals(1, trackReplicationEvents.size());
      Assert.assertEquals(1, copyEvents.size());

      Assert.assertEquals(trackReplicationEvents.get(0).getId(),
          copyEvents.get(0).getCommand().getId());

      //event is timed out
      Thread.sleep(1500);

      queue.processAll(1000L);

      //original copy command + retry
      Assert.assertEquals(2, trackReplicationEvents.size());
      Assert.assertEquals(2, copyEvents.size());

    } finally {
      rapidLeaseManager.shutdown();
      if (leaseManager != null) {
        leaseManager.shutdown();
      }
    }
  }

}