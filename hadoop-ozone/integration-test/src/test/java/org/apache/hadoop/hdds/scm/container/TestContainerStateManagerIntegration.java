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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Tests for ContainerStateManager.
 */
public class TestContainerStateManagerIntegration {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerStateManagerIntegration.class);

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private XceiverClientManager xceiverClientManager;
  private StorageContainerManager scm;
  private ContainerManager containerManager;
  private ContainerStateManager containerStateManager;
  private String containerOwner = "OZONE";
  private int numContainerPerOwnerInPipeline;


  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    numContainerPerOwnerInPipeline =
        conf.getInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
            ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    xceiverClientManager = new XceiverClientManager(conf);
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    containerStateManager = ((SCMContainerManager)containerManager)
        .getContainerStateManager();
  }

  @After
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateContainer() throws IOException {
    // Allocate a container and verify the container info
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            container1.getPipeline());
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
    Assert.assertEquals(containerOwner, info.getOwner());
    Assert.assertEquals(xceiverClientManager.getType(),
        info.getReplicationType());
    Assert.assertEquals(xceiverClientManager.getFactor(),
        info.getReplicationFactor());
    Assert.assertEquals(HddsProtos.LifeCycleState.OPEN, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    int numContainers = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN).size();
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
        container2.getContainerInfo().getContainerID());
    Assert.assertEquals(3, numContainers);
  }

  @Test
  public void testContainerStateManagerRestart() throws IOException,
      TimeoutException, InterruptedException, AuthenticationException {
    // Allocate 5 containers in ALLOCATED state and 5 in CREATING state

    for (int i = 0; i < 10; i++) {

      ContainerWithPipeline container = scm.getClientProtocolServer()
          .allocateContainer(
              xceiverClientManager.getType(),
              xceiverClientManager.getFactor(), containerOwner);
      if (i >= 5) {
        scm.getContainerManager().updateContainerState(container
                .getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
      }
    }

    cluster.restartStorageContainerManager(true);

    List<ContainerInfo> result = cluster.getStorageContainerManager()
        .getContainerManager().listContainer(null, 100);

    long matchCount = result.stream()
        .filter(info ->
            info.getOwner().equals(containerOwner))
        .filter(info ->
            info.getReplicationType() == xceiverClientManager.getType())
        .filter(info ->
            info.getReplicationFactor() == xceiverClientManager.getFactor())
        .filter(info ->
            info.getState() == HddsProtos.LifeCycleState.OPEN)
        .count();
    Assert.assertEquals(5, matchCount);
    matchCount = result.stream()
        .filter(info ->
            info.getOwner().equals(containerOwner))
        .filter(info ->
            info.getReplicationType() == xceiverClientManager.getType())
        .filter(info ->
            info.getReplicationFactor() == xceiverClientManager.getFactor())
        .filter(info ->
            info.getState() == HddsProtos.LifeCycleState.CLOSING)
        .count();
    Assert.assertEquals(5, matchCount);
  }

  @Test
  public void testGetMatchingContainer() throws IOException {
    long cid;
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    cid = container1.getContainerInfo().getContainerID();

    // each getMatchingContainer call allocates a container in the
    // pipeline till the pipeline has numContainerPerOwnerInPipeline number of
    // containers.
    for (int i = 1; i < numContainerPerOwnerInPipeline; i++) {
      ContainerInfo info = containerManager
          .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
              container1.getPipeline());
      Assert.assertTrue(info.getContainerID() > cid);
      cid = info.getContainerID();
    }

    // At this point there are already three containers in the pipeline.
    // next container should be the same as first container
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            container1.getPipeline());
    Assert.assertEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
  }

  @Test
  public void testGetMatchingContainerWithExcludedList() throws IOException {
    long cid;
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    cid = container1.getContainerInfo().getContainerID();

    // each getMatchingContainer call allocates a container in the
    // pipeline till the pipeline has numContainerPerOwnerInPipeline number of
    // containers.
    for (int i = 1; i < numContainerPerOwnerInPipeline; i++) {
      ContainerInfo info = containerManager
          .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
              container1.getPipeline());
      Assert.assertTrue(info.getContainerID() > cid);
      cid = info.getContainerID();
    }

    // At this point there are already three containers in the pipeline.
    // next container should be the same as first container
    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            container1.getPipeline(), Collections.singletonList(new
                ContainerID(1)));
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
  }


  @Test
  public void testCreateContainerLogicWithExcludedList() throws IOException {
    long cid;
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    cid = container1.getContainerInfo().getContainerID();

    for (int i = 1; i < numContainerPerOwnerInPipeline; i++) {
      ContainerInfo info = containerManager
          .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
              container1.getPipeline());
      Assert.assertTrue(info.getContainerID() > cid);
      cid = info.getContainerID();
    }

    ContainerInfo info = containerManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            container1.getPipeline(), Arrays.asList(new ContainerID(1), new
                ContainerID(2), new ContainerID(3)));
    Assert.assertEquals(info.getContainerID(), 4);
  }

  @Test
  @Ignore("TODO:HDDS-1159")
  public void testGetMatchingContainerMultipleThreads()
      throws IOException, InterruptedException {
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    Map<Long, Long> container2MatchedCount = new ConcurrentHashMap<>();

    // allocate blocks using multiple threads
    int numBlockAllocates = 100000;
    for (int i = 0; i < numBlockAllocates; i++) {
      CompletableFuture.supplyAsync(() -> {
        ContainerInfo info = containerManager
            .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
                container1.getPipeline());
        container2MatchedCount
            .compute(info.getContainerID(), (k, v) -> v == null ? 1L : v + 1);
        return null;
      });
    }

    // make sure pipeline has has numContainerPerOwnerInPipeline number of
    // containers.
    Assert.assertEquals(scm.getPipelineManager()
            .getNumberOfContainers(container1.getPipeline().getId()),
        numContainerPerOwnerInPipeline);
    Thread.sleep(5000);
    long threshold = 2000;
    // check the way the block allocations are distributed in the different
    // containers.
    for (Long matchedCount : container2MatchedCount.values()) {
      // TODO: #CLUTIL Look at the division of block allocations in different
      // containers.
      LOG.error("Total allocated block = " + matchedCount);
      Assert.assertTrue(matchedCount <=
          numBlockAllocates / container2MatchedCount.size() + threshold
          && matchedCount >=
          numBlockAllocates / container2MatchedCount.size() - threshold);
    }
  }

  @Test
  public void testUpdateContainerState() throws IOException {
    NavigableSet<ContainerID> containerList = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN);
    int containers = containerList == null ? 0 : containerList.size();
    Assert.assertEquals(0, containers);

    // Allocate container1 and update its state from
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.OPEN).size();
    Assert.assertEquals(1, containers);

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSING).size();
    Assert.assertEquals(1, containers);

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.DELETE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    containerManager
        .updateContainerState(container1.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETED).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from
    // OPEN -> CLOSING -> CLOSED
    ContainerWithPipeline container3 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    containerManager
        .updateContainerState(container3.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(container3.getContainerInfo().containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);
  }


  @Test
  public void testReplicaMap() throws Exception {
    DatanodeDetails dn1 = DatanodeDetails.newBuilder().setHostName("host1")
        .setIpAddress("1.1.1.1")
        .setUuid(UUID.randomUUID().toString()).build();
    DatanodeDetails dn2 = DatanodeDetails.newBuilder().setHostName("host2")
        .setIpAddress("2.2.2.2")
        .setUuid(UUID.randomUUID().toString()).build();

    // Test 1: no replica's exist
    ContainerID containerID = ContainerID.valueof(RandomUtils.nextLong());
    Set<ContainerReplica> replicaSet;
    try {
      containerStateManager.getContainerReplicas(containerID);
      Assert.fail();
    } catch (ContainerNotFoundException ex) {
      // expected.
    }

    ContainerWithPipeline container = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);

    ContainerID id = container.getContainerInfo().containerID();

    // Test 2: Add replica nodes and then test
    ContainerReplica replicaOne = ContainerReplica.newBuilder()
        .setContainerID(id)
        .setContainerState(ContainerReplicaProto.State.OPEN)
        .setDatanodeDetails(dn1)
        .build();
    ContainerReplica replicaTwo = ContainerReplica.newBuilder()
        .setContainerID(id)
        .setContainerState(ContainerReplicaProto.State.OPEN)
        .setDatanodeDetails(dn2)
        .build();
    containerStateManager.updateContainerReplica(id, replicaOne);
    containerStateManager.updateContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Test 3: Remove one replica node and then test
    containerStateManager.removeContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Test 3: Remove second replica node and then test
    containerStateManager.removeContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(0, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(replicaOne));
    Assert.assertFalse(replicaSet.contains(replicaTwo));

    // Test 4: Re-insert dn1
    containerStateManager.updateContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertFalse(replicaSet.contains(replicaTwo));

    // Re-insert dn2
    containerStateManager.updateContainerReplica(id, replicaTwo);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));

    // Re-insert dn1
    containerStateManager.updateContainerReplica(id, replicaOne);
    replicaSet = containerStateManager.getContainerReplicas(id);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(replicaOne));
    Assert.assertTrue(replicaSet.contains(replicaTwo));
  }

}
