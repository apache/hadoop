/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.scm.container.ContainerStates;

import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedSet;
import java.util.UUID;

import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.ReplicationType.STAND_ALONE;

public class TestContainerStateMap {
  @Test
  public void testLifeCyleStates() throws IOException {
    ContainerStateMap stateMap = new ContainerStateMap();
    int currentCount = 1;
    Pipeline pipeline = ContainerTestHelper
        .createSingleNodePipeline(UUID.randomUUID().toString());
    for (int x = 1; x < 1001; x++) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.OPEN)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(x)
          .build();
      stateMap.addContainer(containerInfo);
      currentCount++;
    }

    SortedSet<ContainerID> openSet = stateMap.getMatchingContainerIDs(OPEN,
        "OZONE", ONE, STAND_ALONE);
    Assert.assertEquals(1000, openSet.size());

    int nextMax = currentCount + 1000;
    for (int y = currentCount; y < nextMax; y++) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.CLOSED)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(y)
          .build();
      stateMap.addContainer(containerInfo);
      currentCount++;
    }

    openSet = stateMap.getMatchingContainerIDs(OPEN, "OZONE",
        ONE, STAND_ALONE);
    SortedSet<ContainerID> closeSet = stateMap.getMatchingContainerIDs(CLOSED,
        "OZONE", ONE, STAND_ALONE);

    // Assert that open is still 1000 and we added 1000 more closed containers.
    Assert.assertEquals(1000, openSet.size());
    Assert.assertEquals(1000, closeSet.size());

    SortedSet<ContainerID> ownerSet = stateMap.getContainerIDsByOwner("OZONE");

    // Ozone owns 1000 open and 1000 closed containers.
    Assert.assertEquals(2000, ownerSet.size());
  }

  @Test
  public void testGetMatchingContainers() throws IOException {
    ContainerStateMap stateMap = new ContainerStateMap();
    Pipeline pipeline = ContainerTestHelper
        .createSingleNodePipeline(UUID.randomUUID().toString());

    int currentCount = 1;
    for (int x = 1; x < 1001; x++) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.OPEN)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(x)
          .build();
      stateMap.addContainer(containerInfo);
      currentCount++;
    }
    SortedSet<ContainerID> openSet = stateMap.getMatchingContainerIDs(OPEN,
        "OZONE", ONE, STAND_ALONE);
    Assert.assertEquals(1000, openSet.size());
    int nextMax = currentCount + 200;
    for (int y = currentCount; y < nextMax; y++) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.CLOSED)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(y)
          .build();
      stateMap.addContainer(containerInfo);
      currentCount++;
    }

    nextMax = currentCount + 30000;
    for (int z = currentCount; z < nextMax; z++) {
      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.OPEN)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(z)
          .build();
      stateMap.addContainer(containerInfo);
      currentCount++;
    }
    // At this point, if we get all Open Containers that belong to Ozone,
    // with one replica and standalone replica strategy -- we should get
    // 1000 + 30000.

    openSet = stateMap.getMatchingContainerIDs(OPEN,
        "OZONE", ONE, STAND_ALONE);
    Assert.assertEquals(1000 + 30000, openSet.size());


    // There is no such owner, so should be a set of zero size.
    SortedSet<ContainerID> zeroSet = stateMap.getMatchingContainerIDs(OPEN,
        "BILBO", ONE, STAND_ALONE);
    Assert.assertEquals(0, zeroSet.size());
    int nextId = currentCount++;
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerName(pipeline.getContainerName())
        .setState(HdslProtos.LifeCycleState.OPEN)
        .setPipeline(pipeline)
        .setAllocatedBytes(0)
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.monotonicNow())
        .setOwner("BILBO")
        .setContainerID(nextId)
        .build();

    stateMap.addContainer(containerInfo);
    zeroSet = stateMap.getMatchingContainerIDs(OPEN,
        "BILBO", ONE, STAND_ALONE);
    Assert.assertEquals(1, zeroSet.size());

    // Assert that the container we got back is the nextID itself.
    Assert.assertTrue(zeroSet.contains(new ContainerID(nextId)));
  }

  @Test
  public void testUpdateState() throws IOException {
    ContainerStateMap stateMap = new ContainerStateMap();
    Pipeline pipeline = ContainerTestHelper
        .createSingleNodePipeline(UUID.randomUUID().toString());

    ContainerInfo containerInfo = null;
    int currentCount = 1;
    for (int x = 1; x < 1001; x++) {
      containerInfo = new ContainerInfo.Builder()
          .setContainerName(pipeline.getContainerName())
          .setState(HdslProtos.LifeCycleState.OPEN)
          .setPipeline(pipeline)
          .setAllocatedBytes(0)
          .setUsedBytes(0)
          .setNumberOfKeys(0)
          .setStateEnterTime(Time.monotonicNow())
          .setOwner("OZONE")
          .setContainerID(x)
          .build();


      stateMap.addContainer(containerInfo);
      currentCount++;
    }

    stateMap.updateState(containerInfo, OPEN, CLOSED);
    SortedSet<ContainerID> closedSet = stateMap.getMatchingContainerIDs(CLOSED,
        "OZONE", ONE, STAND_ALONE);
    Assert.assertEquals(1, closedSet.size());
    Assert.assertTrue(closedSet.contains(containerInfo.containerID()));

    SortedSet<ContainerID> openSet = stateMap.getMatchingContainerIDs(OPEN,
        "OZONE", ONE, STAND_ALONE);
    Assert.assertEquals(999, openSet.size());
  }
}