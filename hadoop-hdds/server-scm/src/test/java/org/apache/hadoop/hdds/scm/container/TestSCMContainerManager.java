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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleEvent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Container ContainerManager.
 */
public class TestSCMContainerManager {
  private static SCMContainerManager containerManager;
  private static MockNodeManager nodeManager;
  private static File testDir;
  private static XceiverClientManager xceiverClientManager;
  private static String containerOwner = "OZONE";
  private static Random random;

  private static final long TIMEOUT = 10000;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = SCMTestUtils.getConf();

    testDir = GenericTestUtils
        .getTestDir(TestSCMContainerManager.class.getSimpleName());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT,
        TIMEOUT,
        TimeUnit.MILLISECONDS);
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 10);
    containerManager = new SCMContainerManager(conf, nodeManager, 128,
        new EventQueue());
    xceiverClientManager = new XceiverClientManager(conf);
    random = new Random();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if(containerManager != null) {
      containerManager.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Before
  public void clearChillMode() {
    nodeManager.setChillmode(false);
  }

  @Test
  public void testallocateContainer() throws Exception {
    ContainerWithPipeline containerInfo = containerManager.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    Assert.assertNotNull(containerInfo);
  }

  @Test
  public void testallocateContainerDistributesAllocation() throws Exception {
    /* This is a lame test, we should really be testing something like
    z-score or make sure that we don't have 3sigma kind of events. Too lazy
    to write all that code. This test very lamely tests if we have more than
    5 separate nodes  from the list of 10 datanodes that got allocated a
    container.
     */
    Set<UUID> pipelineList = new TreeSet<>();
    for (int x = 0; x < 30; x++) {
      ContainerWithPipeline containerInfo = containerManager.allocateContainer(
          xceiverClientManager.getType(),
          xceiverClientManager.getFactor(),
          containerOwner);

      Assert.assertNotNull(containerInfo);
      Assert.assertNotNull(containerInfo.getPipeline());
      pipelineList.add(containerInfo.getPipeline().getLeader()
          .getUuid());
    }
    Assert.assertTrue(pipelineList.size() > 5);
  }

  @Test
  public void testGetContainer() throws IOException {
    ContainerWithPipeline containerInfo = containerManager.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    Pipeline pipeline  = containerInfo.getPipeline();
    Assert.assertNotNull(pipeline);
    Pipeline newPipeline = containerInfo.getPipeline();
    Assert.assertEquals(pipeline.getLeader().getUuid(),
        newPipeline.getLeader().getUuid());
  }

  @Test
  public void testGetContainerWithPipeline() throws Exception {
    ContainerWithPipeline containerWithPipeline = containerManager
        .allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    ContainerInfo contInfo = containerWithPipeline.getContainerInfo();
    // Add dummy replicas for container.
    DatanodeDetails dn1 = DatanodeDetails.newBuilder()
        .setHostName("host1")
        .setIpAddress("1.1.1.1")
        .setUuid(UUID.randomUUID().toString()).build();
    DatanodeDetails dn2 = DatanodeDetails.newBuilder()
        .setHostName("host2")
        .setIpAddress("2.2.2.2")
        .setUuid(UUID.randomUUID().toString()).build();
    containerManager
        .updateContainerState(contInfo.getContainerID(), LifeCycleEvent.CREATE);
    containerManager.updateContainerState(contInfo.getContainerID(),
        LifeCycleEvent.CREATED);
    containerManager.updateContainerState(contInfo.getContainerID(),
        LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(contInfo.getContainerID(), LifeCycleEvent.CLOSE);
    ContainerInfo finalContInfo = contInfo;
    LambdaTestUtils.intercept(SCMException.class, "No entry exist for "
        + "containerId:", () -> containerManager.getContainerWithPipeline(
        finalContInfo.getContainerID()));

    containerManager.getStateManager().getContainerStateMap()
        .addContainerReplica(contInfo.containerID(), dn1, dn2);

    contInfo = containerManager.getContainer(contInfo.getContainerID());
    Assert.assertEquals(contInfo.getState(), LifeCycleState.CLOSED);
    Pipeline pipeline = containerWithPipeline.getPipeline();
    containerManager.getPipelineSelector().finalizePipeline(pipeline);

    ContainerWithPipeline containerWithPipeline2 = containerManager
        .getContainerWithPipeline(contInfo.getContainerID());
    pipeline = containerWithPipeline2.getPipeline();
    Assert.assertNotEquals(containerWithPipeline, containerWithPipeline2);
    Assert.assertNotNull("Pipeline should not be null", pipeline);
    Assert.assertTrue(pipeline.getDatanodeHosts().contains(dn1.getHostName()));
    Assert.assertTrue(pipeline.getDatanodeHosts().contains(dn2.getHostName()));
  }

  @Test
  public void testgetNoneExistentContainer() throws IOException {
    thrown.expectMessage("Specified key does not exist.");
    containerManager.getContainer(random.nextLong());
  }

  @Test
  public void testContainerCreationLeaseTimeout() throws IOException,
      InterruptedException {
    nodeManager.setChillmode(false);
    ContainerWithPipeline containerInfo = containerManager.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    containerManager.updateContainerState(containerInfo.getContainerInfo()
            .getContainerID(), HddsProtos.LifeCycleEvent.CREATE);
    Thread.sleep(TIMEOUT + 1000);

    NavigableSet<ContainerID> deleteContainers = containerManager
        .getStateManager().getMatchingContainerIDs("OZONE",
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.DELETING);
    Assert.assertTrue(deleteContainers
        .contains(containerInfo.getContainerInfo().containerID()));

    thrown.expect(IOException.class);
    thrown.expectMessage("Lease Exception");
    containerManager
        .updateContainerState(containerInfo.getContainerInfo().getContainerID(),
            HddsProtos.LifeCycleEvent.CREATED);
  }

  @Test
  public void testFullContainerReport() throws Exception {
    ContainerInfo info = createContainer();
    DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();
    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
        .setSize(5368709120L)
        .setUsed(2000000000L)
        .setKeyCount(100000000L)
        .setReadCount(100000000L)
        .setWriteCount(100000000L)
        .setReadBytes(2000000000L)
        .setWriteBytes(2000000000L)
        .setContainerID(info.getContainerID())
        .setDeleteTransactionId(0);

    reports.add(ciBuilder.build());

    ContainerReportsProto.Builder crBuilder = ContainerReportsProto
        .newBuilder();
    crBuilder.addAllReports(reports);

    containerManager.processContainerReports(
        datanodeDetails, crBuilder.build(), false);

    ContainerInfo updatedContainer =
        containerManager.getContainer(info.getContainerID());
    Assert.assertEquals(100000000L,
        updatedContainer.getNumberOfKeys());
    Assert.assertEquals(2000000000L, updatedContainer.getUsedBytes());

    for (StorageContainerDatanodeProtocolProtos.ContainerInfo c : reports) {
      LambdaTestUtils.intercept(SCMException.class, "No entry "
          + "exist for containerId:", () -> containerManager.getStateManager()
          .getContainerReplicas(ContainerID.valueof(c.getContainerID())));
    }

    containerManager.processContainerReports(TestUtils.randomDatanodeDetails(),
        crBuilder.build(), true);
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo c : reports) {
      Assert.assertTrue(containerManager.getStateManager()
          .getContainerReplicas(
              ContainerID.valueof(c.getContainerID())).size() > 0);
    }
  }

  @Test
  public void testListContainerAfterReport() throws Exception {
    ContainerInfo info1 = createContainer();
    ContainerInfo info2 = createContainer();
    DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();
    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    long cID1 = info1.getContainerID();
    long cID2 = info2.getContainerID();
    ciBuilder.setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
        .setSize(1000000000L)
        .setUsed(987654321L)
        .setKeyCount(100000000L)
        .setReadBytes(1000000000L)
        .setWriteBytes(1000000000L)
        .setContainerID(cID1);
    reports.add(ciBuilder.build());

    ciBuilder.setFinalhash("e16cc9d6024365750ed8dbd194ea54a9")
        .setSize(1000000000L)
        .setUsed(123456789L)
        .setKeyCount(200000000L)
        .setReadBytes(3000000000L)
        .setWriteBytes(4000000000L)
        .setContainerID(cID2);
    reports.add(ciBuilder.build());

    ContainerReportsProto.Builder crBuilder = ContainerReportsProto
        .newBuilder();
    crBuilder.addAllReports(reports);

    containerManager.processContainerReports(
        datanodeDetails, crBuilder.build(), false);

    List<ContainerInfo> list = containerManager.listContainer(0, 50);
    Assert.assertEquals(2, list.stream().filter(
        x -> x.getContainerID() == cID1 || x.getContainerID() == cID2).count());
    Assert.assertEquals(300000000L, list.stream().filter(
        x -> x.getContainerID() == cID1 || x.getContainerID() == cID2)
        .mapToLong(x -> x.getNumberOfKeys()).sum());
    Assert.assertEquals(1111111110L, list.stream().filter(
        x -> x.getContainerID() == cID1 || x.getContainerID() == cID2)
        .mapToLong(x -> x.getUsedBytes()).sum());
  }

  @Test
  public void testCloseContainer() throws IOException {
    ContainerInfo info = createContainer();
    containerManager.updateContainerState(info.getContainerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    NavigableSet<ContainerID> pendingCloseContainers = containerManager
        .getStateManager().getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.CLOSING);
    Assert.assertTrue(pendingCloseContainers.contains(info.containerID()));
    containerManager.updateContainerState(info.getContainerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    NavigableSet<ContainerID> closeContainers = containerManager
        .getStateManager().getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.CLOSED);
    Assert.assertTrue(closeContainers.contains(info.containerID()));
  }

  /**
   * Creates a container with the given name in SCMContainerManager.
   * @throws IOException
   */
  private ContainerInfo createContainer()
      throws IOException {
    nodeManager.setChillmode(false);
    ContainerWithPipeline containerWithPipeline = containerManager
        .allocateContainer(xceiverClientManager.getType(),
            xceiverClientManager.getFactor(), containerOwner);
    ContainerInfo containerInfo = containerWithPipeline.getContainerInfo();
    containerManager.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATE);
    containerManager.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATED);
    return containerInfo;
  }

  @Test
  public void testFlushAllContainers() throws IOException {
    ContainerInfo info = createContainer();
    List<ContainerInfo> containers = containerManager.getStateManager()
        .getAllContainers();
    Assert.assertTrue(containers.size() > 0);
    containerManager.flushContainerInfo();
  }

}
