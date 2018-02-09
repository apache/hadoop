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
package org.apache.hadoop.ozone.scm.container;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.ozone.scm.container.ContainerStates.ContainerID;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.NavigableSet;
import java.util.Random;

/**
 * Tests for ContainerStateManager.
 */
public class TestContainerStateManager {

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private XceiverClientManager xceiverClientManager;
  private StorageContainerManager scm;
  private Mapping scmContainerMapping;
  private ContainerStateManager stateManager;
  private String containerOwner = "OZONE";


  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    xceiverClientManager = new XceiverClientManager(conf);
    scm = cluster.getStorageContainerManager();
    scmContainerMapping = scm.getScmContainerManager();
    stateManager = scmContainerMapping.getStateManager();
  }

  @After
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
      cluster.close();
    }
  }

  @Test
  public void testAllocateContainer() throws IOException {
    // Allocate a container and verify the container info
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    ContainerInfo info = stateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container1, info.getContainerName());
    Assert.assertEquals(OzoneConsts.GB * 3, info.getAllocatedBytes());
    Assert.assertEquals(containerOwner, info.getOwner());
    Assert.assertEquals(xceiverClientManager.getType(),
        info.getPipeline().getType());
    Assert.assertEquals(xceiverClientManager.getFactor(),
        info.getPipeline().getFactor());
    Assert.assertEquals(OzoneProtos.LifeCycleState.ALLOCATED, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    String container2 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);
    int numContainers = stateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(2, numContainers);
  }

  @Test
  public void testContainerStateManagerRestart() throws IOException {
    // Allocate 5 containers in ALLOCATED state and 5 in CREATING state
    String cname = "container" + RandomStringUtils.randomNumeric(5);
    for (int i = 0; i < 10; i++) {
      scm.allocateContainer(xceiverClientManager.getType(),
          xceiverClientManager.getFactor(), cname + i, containerOwner);
      if (i >= 5) {
        scm.getScmContainerManager()
            .updateContainerState(cname + i, OzoneProtos.LifeCycleEvent.CREATE);
      }
    }

    // New instance of ContainerStateManager should load all the containers in
    // container store.
    ContainerStateManager stateManager =
        new ContainerStateManager(conf, scmContainerMapping
        );
    int containers = stateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(5, containers);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(5, containers);
  }

  @Test
  public void testGetMatchingContainer() throws IOException {
    String container1 = "container-01234";
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATED);

    String container2 = "container-56789";
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);

    ContainerInfo info = stateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container1, info.getContainerName());

    info = stateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container2, info.getContainerName());

    scmContainerMapping.updateContainerState(container2,
        OzoneProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container2,
        OzoneProtos.LifeCycleEvent.CREATED);

    // space has already been allocated in container1, now container 2 should
    // be chosen.
    info = stateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container2, info.getContainerName());
  }

  @Test
  public void testUpdateContainerState() throws IOException {
    NavigableSet<ContainerID> containerList = stateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.ALLOCATED);
    int containers = containerList == null ? 0 : containerList.size();
    Assert.assertEquals(0, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATE);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATED);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.OPEN).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, OzoneProtos.LifeCycleEvent.FINALIZE);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.CLOSING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, OzoneProtos.LifeCycleEvent.CLOSE);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, OzoneProtos.LifeCycleEvent.DELETE);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, OzoneProtos.LifeCycleEvent.CLEANUP);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.DELETED).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // DELETING
    String container2 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);
    scmContainerMapping.updateContainerState(container2,
        OzoneProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container2, OzoneProtos.LifeCycleEvent.TIMEOUT);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED
    String container3 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container3, containerOwner);
    scmContainerMapping.updateContainerState(container3,
        OzoneProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container3,
        OzoneProtos.LifeCycleEvent.CREATED);
    scmContainerMapping.updateContainerState(container3,
        OzoneProtos.LifeCycleEvent.FINALIZE);
    scmContainerMapping
        .updateContainerState(container3, OzoneProtos.LifeCycleEvent.CLOSE);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        OzoneProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);
  }

  @Test
  public void testUpdatingAllocatedBytes() throws Exception {
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container1,
        OzoneProtos.LifeCycleEvent.CREATED);

    Random ran = new Random();
    long allocatedSize = 0;
    for (int i = 0; i<5; i++) {
      long size = Math.abs(ran.nextLong() % OzoneConsts.GB);
      allocatedSize += size;
      // trigger allocating bytes by calling getMatchingContainer
      ContainerInfo info = stateManager
          .getMatchingContainer(size, containerOwner,
              xceiverClientManager.getType(), xceiverClientManager.getFactor(),
              OzoneProtos.LifeCycleState.OPEN);
      Assert.assertEquals(container1, info.getContainerName());

      ContainerMapping containerMapping =
          (ContainerMapping)scmContainerMapping;
      // manually trigger a flush, this will persist the allocated bytes value
      // to disk
      containerMapping.flushContainerInfo();

      Charset utf8 = Charset.forName("UTF-8");
      // the persisted value should always be equal to allocated size.
      byte[] containerBytes =
          containerMapping.getContainerStore().get(container1.getBytes(utf8));
      OzoneProtos.SCMContainerInfo infoProto =
          OzoneProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);
      ContainerInfo currentInfo = ContainerInfo.fromProtobuf(infoProto);
      Assert.assertEquals(allocatedSize, currentInfo.getAllocatedBytes());
    }
  }
}
