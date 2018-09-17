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

import com.google.common.primitives.Longs;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.states.ContainerStateMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import org.slf4j.event.Level;

/**
 * Tests for ContainerStateManager.
 */
public class TestContainerStateManagerIntegration {

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private XceiverClientManager xceiverClientManager;
  private StorageContainerManager scm;
  private ContainerMapping scmContainerMapping;
  private ContainerStateManager containerStateManager;
  private PipelineSelector selector;
  private String containerOwner = "OZONE";


  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    xceiverClientManager = new XceiverClientManager(conf);
    scm = cluster.getStorageContainerManager();
    scmContainerMapping = (ContainerMapping) scm.getScmContainerManager();
    containerStateManager = scmContainerMapping.getStateManager();
    selector = scmContainerMapping.getPipelineSelector();
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
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    ContainerInfo info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());
    Assert.assertEquals(OzoneConsts.GB * 3, info.getAllocatedBytes());
    Assert.assertEquals(containerOwner, info.getOwner());
    Assert.assertEquals(xceiverClientManager.getType(),
        info.getReplicationType());
    Assert.assertEquals(xceiverClientManager.getFactor(),
        info.getReplicationFactor());
    Assert.assertEquals(HddsProtos.LifeCycleState.ALLOCATED, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    int numContainers = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertNotEquals(container1.getContainerInfo().getContainerID(),
        container2.getContainerInfo().getContainerID());
    Assert.assertEquals(2, numContainers);
  }

  @Test
  public void testContainerStateManagerRestart() throws IOException {
    // Allocate 5 containers in ALLOCATED state and 5 in CREATING state

    List<ContainerInfo> containers = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ContainerWithPipeline container = scm.getClientProtocolServer()
          .allocateContainer(
              xceiverClientManager.getType(),
              xceiverClientManager.getFactor(), containerOwner);
      containers.add(container.getContainerInfo());
      if (i >= 5) {
        scm.getScmContainerManager().updateContainerState(container
                .getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
      }
    }

    // New instance of ContainerStateManager should load all the containers in
    // container store.
    ContainerStateManager stateManager =
        new ContainerStateManager(conf, scmContainerMapping, selector);
    int matchCount = stateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(5, matchCount);
    matchCount = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(5, matchCount);
  }

  @Test
  public void testGetMatchingContainer() throws IOException {
    ContainerWithPipeline container1 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATED);

    ContainerWithPipeline container2 = scm.getClientProtocolServer().
        allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);

    ContainerInfo info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container1.getContainerInfo().getContainerID(),
        info.getContainerID());

    info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container2.getContainerInfo().getContainerID(),
        info.getContainerID());

    scmContainerMapping
        .updateContainerState(container2.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container2.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATED);

    // space has already been allocated in container1, now container 2 should
    // be chosen.
    info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container2.getContainerInfo().getContainerID(),
        info.getContainerID());
  }

  @Test
  public void testUpdateContainerState() throws IOException {
    NavigableSet<ContainerID> containerList = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    int containers = containerList == null ? 0 : containerList.size();
    Assert.assertEquals(0, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATED);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.OPEN).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.DELETE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETED).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // DELETING
    ContainerWithPipeline container2 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    scmContainerMapping
        .updateContainerState(container2.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container2.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.TIMEOUT);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED
    ContainerWithPipeline container3 = scm.getClientProtocolServer()
        .allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    scmContainerMapping
        .updateContainerState(container3.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container3.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATED);
    scmContainerMapping
        .updateContainerState(container3.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerMapping
        .updateContainerState(container3.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);
  }

  @Test
  public void testUpdatingAllocatedBytes() throws Exception {
    ContainerWithPipeline container1 = scm.getClientProtocolServer()
        .allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerOwner);
    scmContainerMapping.updateContainerState(container1
            .getContainerInfo().getContainerID(),
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container1
            .getContainerInfo().getContainerID(),
        HddsProtos.LifeCycleEvent.CREATED);

    Random ran = new Random();
    long allocatedSize = 0;
    for (int i = 0; i<5; i++) {
      long size = Math.abs(ran.nextLong() % OzoneConsts.GB);
      allocatedSize += size;
      // trigger allocating bytes by calling getMatchingContainer
      ContainerInfo info = containerStateManager
          .getMatchingContainer(size, containerOwner,
              xceiverClientManager.getType(), xceiverClientManager.getFactor(),
              HddsProtos.LifeCycleState.OPEN);
      Assert.assertEquals(container1.getContainerInfo().getContainerID(),
          info.getContainerID());

      ContainerMapping containerMapping =
          (ContainerMapping) scmContainerMapping;
      // manually trigger a flush, this will persist the allocated bytes value
      // to disk
      containerMapping.flushContainerInfo();

      // the persisted value should always be equal to allocated size.
      byte[] containerBytes = containerMapping.getContainerStore().get(
          Longs.toByteArray(container1.getContainerInfo().getContainerID()));
      HddsProtos.SCMContainerInfo infoProto =
          HddsProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);
      ContainerInfo currentInfo = ContainerInfo.fromProtobuf(infoProto);
      Assert.assertEquals(allocatedSize, currentInfo.getAllocatedBytes());
    }
  }

  @Test
  public void testReplicaMap() throws Exception {
    GenericTestUtils.setLogLevel(ContainerStateMap.getLOG(), Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(ContainerStateMap.getLOG());
    DatanodeDetails dn1 = DatanodeDetails.newBuilder().setHostName("host1")
        .setIpAddress("1.1.1.1")
        .setUuid(UUID.randomUUID().toString()).build();
    DatanodeDetails dn2 = DatanodeDetails.newBuilder().setHostName("host2")
        .setIpAddress("2.2.2.2")
        .setUuid(UUID.randomUUID().toString()).build();

    // Test 1: no replica's exist
    ContainerID containerID = ContainerID.valueof(RandomUtils.nextLong());
    Set<DatanodeDetails> replicaSet;
    LambdaTestUtils.intercept(SCMException.class, "", () -> {
      containerStateManager.getContainerReplicas(containerID);
    });

    // Test 2: Add replica nodes and then test
    containerStateManager.addContainerReplica(containerID, dn1);
    containerStateManager.addContainerReplica(containerID, dn2);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(dn1));
    Assert.assertTrue(replicaSet.contains(dn2));

    // Test 3: Remove one replica node and then test
    containerStateManager.removeContainerReplica(containerID, dn1);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(dn1));
    Assert.assertTrue(replicaSet.contains(dn2));

    // Test 3: Remove second replica node and then test
    containerStateManager.removeContainerReplica(containerID, dn2);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(0, replicaSet.size());
    Assert.assertFalse(replicaSet.contains(dn1));
    Assert.assertFalse(replicaSet.contains(dn2));

    // Test 4: Re-insert dn1
    containerStateManager.addContainerReplica(containerID, dn1);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(1, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(dn1));
    Assert.assertFalse(replicaSet.contains(dn2));

    // Re-insert dn2
    containerStateManager.addContainerReplica(containerID, dn2);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(dn1));
    Assert.assertTrue(replicaSet.contains(dn2));

    Assert.assertFalse(logCapturer.getOutput().contains(
        "ReplicaMap already contains entry for container Id: " + containerID
            .toString() + ",DataNode: " + dn1.toString()));
    // Re-insert dn1
    containerStateManager.addContainerReplica(containerID, dn1);
    replicaSet = containerStateManager.getContainerReplicas(containerID);
    Assert.assertEquals(2, replicaSet.size());
    Assert.assertTrue(replicaSet.contains(dn1));
    Assert.assertTrue(replicaSet.contains(dn2));
    Assert.assertTrue(logCapturer.getOutput().contains(
        "ReplicaMap already contains entry for container Id: " + containerID
            .toString() + ",DataNode: " + dn1.toString()));
  }

}
