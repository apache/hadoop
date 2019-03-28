/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.container.placement.algorithms
    .ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hdds.scm.TestUtils.createDatanodeDetails;
import static org.apache.hadoop.hdds.scm.TestUtils.getContainer;
import static org.apache.hadoop.hdds.scm.TestUtils.getReplicas;
import static org.apache.hadoop.hdds.scm.TestUtils.randomDatanodeDetails;

/**
 * Test cases to verify the functionality of ReplicationManager.
 */
public class TestReplicationManager {

  private ReplicationManager replicationManager;
  private ContainerStateManager containerStateManager;
  private ContainerPlacementPolicy containerPlacementPolicy;
  private EventQueue eventQueue;
  private DatanodeCommandHandler datanodeCommandHandler;

  @Before
  public void setup() throws IOException, InterruptedException {
    final Configuration conf = new OzoneConfiguration();
    final ContainerManager containerManager =
        Mockito.mock(ContainerManager.class);
    eventQueue = new EventQueue();
    containerStateManager = new ContainerStateManager(conf);

    datanodeCommandHandler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, datanodeCommandHandler);

    Mockito.when(containerManager.getContainerIDs())
        .thenAnswer(invocation -> containerStateManager.getAllContainerIDs());

    Mockito.when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainer((ContainerID)invocation.getArguments()[0]));

    Mockito.when(containerManager.getContainerReplicas(
        Mockito.any(ContainerID.class)))
        .thenAnswer(invocation -> containerStateManager
            .getContainerReplicas((ContainerID)invocation.getArguments()[0]));

    containerPlacementPolicy = Mockito.mock(ContainerPlacementPolicy.class);

    Mockito.when(containerPlacementPolicy.chooseDatanodes(
        Mockito.anyListOf(DatanodeDetails.class),
        Mockito.anyInt(), Mockito.anyLong()))
        .thenAnswer(invocation -> {
          int count = (int) invocation.getArguments()[1];
          return IntStream.range(0, count)
              .mapToObj(i -> randomDatanodeDetails())
              .collect(Collectors.toList());
        });

    replicationManager = new ReplicationManager(
        conf, containerManager, containerPlacementPolicy, eventQueue);
    replicationManager.start();
    Thread.sleep(100L);
  }

  /**
   * Open containers are not handled by ReplicationManager.
   * This test-case makes sure that ReplicationManages doesn't take
   * any action on OPEN containers.
   */
  @Test
  public void testOpenContainer() throws SCMException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.OPEN);
    containerStateManager.loadContainer(container);
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

  }

  /**
   * If the container is in CLOSING state we resend close container command
   * to all the datanodes.
   */
  @Test
  public void testClosingContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSING);
    final ContainerID id = container.containerID();

    containerStateManager.loadContainer(container);

    // Two replicas in CLOSING state
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSING,
        randomDatanodeDetails(),
        randomDatanodeDetails());

    // One replica in OPEN state
    final DatanodeDetails datanode = randomDatanodeDetails();
    replicas.addAll(getReplicas(id, State.OPEN, datanode));

    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

    // Update the OPEN to CLOSING
    for (ContainerReplica replica : getReplicas(id, State.CLOSING, datanode)) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 6, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
  }


  /**
   * The container is QUASI_CLOSED but two of the replica is still in
   * open state. ReplicationManager should resend close command to those
   * datanodes.
   */
  @Test
  public void testQuasiClosedContainerWithTwoOpenReplica() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.OPEN, 1000L, originNodeId, randomDatanodeDetails());
    final DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final ContainerReplica replicaThree = getReplicas(
        id, State.OPEN, 1000L, datanodeDetails.getUuid(), datanodeDetails);

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);
    // Two of the replicas are in OPEN state
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentCloseCommandCount + 2, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaTwo.getDatanodeDetails()));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.closeContainerCommand,
        replicaThree.getDatanodeDetails()));
  }

  /**
   * When the container is in QUASI_CLOSED state and all the replicas are
   * also in QUASI_CLOSED state and doesn't have a quorum to force close
   * the container, ReplicationManager will not do anything.
   */
  @Test
  public void testHealthyQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  /**
   * When a container is QUASI_CLOSED and we don't have quorum to force close
   * the container, the container should have all the replicas in QUASI_CLOSED
   * state, else ReplicationManager will take action.
   *
   * In this test case we make one of the replica unhealthy, replication manager
   * will send delete container command to the datanode which has the unhealthy
   * replica.
   */
  @Test
  public void testQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException,
      ContainerReplicaNotFoundException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);
    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    // All the QUASI_CLOSED replicas have same originNodeId, so the
    // container will not be closed. ReplicationManager should take no action.
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());

    // Make the first replica unhealthy
    final ContainerReplica unhealthyReplica = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId,
        replicaOne.getDatanodeDetails());
    containerStateManager.updateContainerReplica(id, unhealthyReplica);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));

    // Now we will delete the unhealthy replica from in-memory.
    containerStateManager.removeContainerReplica(id, replicaOne);

    // The container is under replicated as unhealthy replica is removed
    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);

    // We should get replicate command
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is over replicated, ReplicationManager
   * deletes the excess replicas. While choosing the replica for deletion
   * ReplicationManager should prioritize unhealthy replica over QUASI_CLOSED
   * replica.
   */
  @Test
  public void testOverReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaThree = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaFour = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    containerStateManager.updateContainerReplica(id, replicaThree);
    containerStateManager.updateContainerReplica(id, replicaFour);

    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaOne.getDatanodeDetails()));
  }

  /**
   * ReplicationManager should replicate an QUASI_CLOSED replica if it is
   * under replicated.
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainer() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }

  /**
   * When a QUASI_CLOSED container is under replicated, ReplicationManager
   * should re-replicate it. If there are any unhealthy replica, it has to
   * be deleted.
   *
   * In this test case, the container is QUASI_CLOSED and is under replicated
   * and also has an unhealthy replica.
   *
   * In the first iteration of ReplicationManager, it should re-replicate
   * the container so that it has enough replicas.
   *
   * In the second iteration, ReplicationManager should delete the unhealthy
   * replica.
   *
   * In the third iteration, ReplicationManager will re-replicate as the
   * container has again become under replicated after the unhealthy
   * replica has been deleted.
   *
   */
  @Test
  public void testUnderReplicatedQuasiClosedContainerWithUnhealthyReplica()
      throws SCMException, ContainerNotFoundException, InterruptedException,
      ContainerReplicaNotFoundException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final UUID originNodeId = UUID.randomUUID();
    final ContainerReplica replicaOne = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, randomDatanodeDetails());
    final ContainerReplica replicaTwo = getReplicas(
        id, State.UNHEALTHY, 1000L, originNodeId, randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);

    final int currentReplicateCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.replicateContainerCommand);
    final int currentDeleteCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + 1,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));

    Optional<CommandForDatanode> replicateCommand = datanodeCommandHandler
        .getReceivedCommands().stream()
        .filter(c -> c.getCommand().getType()
            .equals(SCMCommandProto.Type.replicateContainerCommand))
        .findFirst();

    Assert.assertTrue(replicateCommand.isPresent());

    DatanodeDetails newNode = createDatanodeDetails(
        replicateCommand.get().getDatanodeId());
    ContainerReplica newReplica = getReplicas(
        id, State.QUASI_CLOSED, 1000L, originNodeId, newNode);
    containerStateManager.updateContainerReplica(id, newReplica);

    /*
     * We have report the replica to SCM, in the next ReplicationManager
     * iteration it should delete the unhealthy replica.
     */

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentDeleteCommandCount + 1, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.deleteContainerCommand));
    // ReplicaTwo should be deleted, that is the unhealthy one
    Assert.assertTrue(datanodeCommandHandler.received(
        SCMCommandProto.Type.deleteContainerCommand,
        replicaTwo.getDatanodeDetails()));

    containerStateManager.removeContainerReplica(id, replicaTwo);

    /*
     * We have now removed unhealthy replica, next iteration of
     * ReplicationManager should re-replicate the container as it
     * is under replicated now
     */

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(currentReplicateCommandCount + 2,
        datanodeCommandHandler.getInvocationCount(
            SCMCommandProto.Type.replicateContainerCommand));
  }


  /**
   * When a container is QUASI_CLOSED and it has >50% of its replica
   * in QUASI_CLOSED state with unique origin node id,
   * ReplicationManager should force close the replica(s) with
   * highest BCSID.
   */
  @Test
  public void testQuasiClosedToClosed() throws
      SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.QUASI_CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.QUASI_CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());
    containerStateManager.loadContainer(container);
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    final int currentCloseCommandCount = datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand);

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);

    // All the replicas have same BCSID, so all of them will be closed.
    Assert.assertEquals(currentCloseCommandCount + 3, datanodeCommandHandler
        .getInvocationCount(SCMCommandProto.Type.closeContainerCommand));

  }


  /**
   * ReplicationManager should not take any action if the container is
   * CLOSED and healthy.
   */
  @Test
  public void testHealthyClosedContainer()
      throws SCMException, ContainerNotFoundException, InterruptedException {
    final ContainerInfo container = getContainer(LifeCycleState.CLOSED);
    final ContainerID id = container.containerID();
    final Set<ContainerReplica> replicas = getReplicas(id, State.CLOSED,
        randomDatanodeDetails(),
        randomDatanodeDetails(),
        randomDatanodeDetails());

    containerStateManager.loadContainer(container);
    for (ContainerReplica replica : replicas) {
      containerStateManager.updateContainerReplica(id, replica);
    }

    replicationManager.processContainersNow();
    // Wait for EventQueue to call the event handler
    Thread.sleep(100L);
    Assert.assertEquals(0, datanodeCommandHandler.getInvocation());
  }

  @After
  public void teardown() throws IOException {
    containerStateManager.close();
    replicationManager.stop();
  }

  private class DatanodeCommandHandler implements
      EventHandler<CommandForDatanode> {

    private AtomicInteger invocation = new AtomicInteger(0);
    private Map<SCMCommandProto.Type, AtomicInteger> commandInvocation =
        new HashMap<>();
    private List<CommandForDatanode> commands = new ArrayList<>();

    @Override
    public void onMessage(final CommandForDatanode command,
                          final EventPublisher publisher) {
      final SCMCommandProto.Type type = command.getCommand().getType();
      commandInvocation.computeIfAbsent(type, k -> new AtomicInteger(0));
      commandInvocation.get(type).incrementAndGet();
      invocation.incrementAndGet();
      commands.add(command);
    }

    private int getInvocation() {
      return invocation.get();
    }

    private int getInvocationCount(SCMCommandProto.Type type) {
      return commandInvocation.containsKey(type) ?
          commandInvocation.get(type).get() : 0;
    }

    private List<CommandForDatanode> getReceivedCommands() {
      return commands;
    }

    /**
     * Returns true if the command handler has received the given
     * command type for the provided datanode.
     *
     * @param type Command Type
     * @param datanode DatanodeDetails
     * @return True if command was received, false otherwise
     */
    private boolean received(final SCMCommandProto.Type type,
                             final DatanodeDetails datanode) {
      return commands.stream().anyMatch(dc ->
          dc.getCommand().getType().equals(type) &&
              dc.getDatanodeId().equals(datanode.getUuid()));
    }
  }

}