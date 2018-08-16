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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineID;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager
    .ReplicationRequestToRepeat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.hdds.scm.events.SCMEvents
    .TRACK_REPLICATE_COMMAND;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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

  private List<CommandForDatanode<ReplicateContainerCommandProto>> copyEvents;

  private ContainerStateManager containerStateManager;

  private ContainerPlacementPolicy containerPlacementPolicy;
  private List<DatanodeDetails> listOfDatanodeDetails;

  @Before
  public void initReplicationManager() throws IOException {

    listOfDatanodeDetails = new ArrayList<>();
    listOfDatanodeDetails.add(TestUtils.randomDatanodeDetails());
    listOfDatanodeDetails.add(TestUtils.randomDatanodeDetails());
    listOfDatanodeDetails.add(TestUtils.randomDatanodeDetails());
    listOfDatanodeDetails.add(TestUtils.randomDatanodeDetails());
    listOfDatanodeDetails.add(TestUtils.randomDatanodeDetails());

    containerPlacementPolicy =
        (excludedNodes, nodesRequired, sizeRequired) -> listOfDatanodeDetails
            .subList(2, 2 + nodesRequired);

    containerStateManager = Mockito.mock(ContainerStateManager.class);

    //container with 2 replicas
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(LifeCycleState.CLOSED)
        .build();

    when(containerStateManager.getContainer(anyObject()))
        .thenReturn(containerInfo);

    queue = new EventQueue();

    trackReplicationEvents = new ArrayList<>();
    queue.addHandler(TRACK_REPLICATE_COMMAND,
        (event, publisher) -> trackReplicationEvents.add(event));

    copyEvents = new ArrayList<>();
    queue.addHandler(SCMEvents.DATANODE_COMMAND,
        (event, publisher) -> copyEvents.add(event));

  }

  @Test
  public void testEventSending() throws InterruptedException, IOException {


    //GIVEN

    LeaseManager<Long> leaseManager = new LeaseManager<>("Test", 100000L);
    try {
      leaseManager.start();

      ReplicationManager replicationManager =
          new ReplicationManager(containerPlacementPolicy,
              containerStateManager,
              queue, leaseManager) {
            @Override
            protected List<DatanodeDetails> getCurrentReplicas(
                ReplicationRequest request) throws IOException {
              return listOfDatanodeDetails.subList(0, 2);
            }
          };
      replicationManager.start();

      //WHEN

      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(1l, (short) 2, System.currentTimeMillis(),
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

    Logger.getRootLogger().setLevel(Level.DEBUG);
    LeaseManager<Long> leaseManager = new LeaseManager<>("Test", 1000L);

    try {
      leaseManager.start();

      ReplicationManager replicationManager =
          new ReplicationManager(containerPlacementPolicy, containerStateManager,


              queue, leaseManager) {
            @Override
            protected List<DatanodeDetails> getCurrentReplicas(
                ReplicationRequest request) throws IOException {
              return listOfDatanodeDetails.subList(0, 2);
            }
          };
      replicationManager.start();

      queue.fireEvent(SCMEvents.REPLICATE_CONTAINER,
          new ReplicationRequest(1l, (short) 2, System.currentTimeMillis(),
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
      if (leaseManager != null) {
        leaseManager.shutdown();
      }
    }
  }

  public static Pipeline createPipeline(Iterable<DatanodeDetails> ids)
      throws IOException {
    Objects.requireNonNull(ids, "ids == null");
    final Iterator<DatanodeDetails> i = ids.iterator();
    Preconditions.checkArgument(i.hasNext());
    final DatanodeDetails leader = i.next();
    final Pipeline pipeline =
        new Pipeline(leader.getUuidString(), LifeCycleState.OPEN,
            ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
            PipelineID.randomId());
    pipeline.addMember(leader);
    for (; i.hasNext(); ) {
      pipeline.addMember(i.next());
    }
    return pipeline;
  }

}