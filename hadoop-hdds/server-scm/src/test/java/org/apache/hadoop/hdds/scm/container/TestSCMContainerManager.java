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
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
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
  private static PipelineManager pipelineManager;
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
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager, new EventQueue());
    containerManager = new SCMContainerManager(conf, nodeManager,
        pipelineManager, new EventQueue());
    xceiverClientManager = new XceiverClientManager(conf);
    random = new Random();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if(containerManager != null) {
      containerManager.close();
    }
    if (pipelineManager != null) {
      pipelineManager.close();
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
      pipelineList.add(containerInfo.getPipeline().getFirstNode()
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
    Assert.assertEquals(pipeline.getFirstNode().getUuid(),
        newPipeline.getFirstNode().getUuid());
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
        .updateContainerState(contInfo.containerID(), LifeCycleEvent.CREATE);
    containerManager.updateContainerState(contInfo.containerID(),
        LifeCycleEvent.CREATED);
    containerManager.updateContainerState(contInfo.containerID(),
        LifeCycleEvent.FINALIZE);
    containerManager
        .updateContainerState(contInfo.containerID(), LifeCycleEvent.CLOSE);
    ContainerInfo finalContInfo = contInfo;
    Assert.assertEquals(0,
        containerManager.getContainerReplicas(
            finalContInfo.containerID()).size());

    containerManager.updateContainerReplica(contInfo.containerID(),
        ContainerReplica.newBuilder().setContainerID(contInfo.containerID())
        .setDatanodeDetails(dn1).build());
    containerManager.updateContainerReplica(contInfo.containerID(),
        ContainerReplica.newBuilder().setContainerID(contInfo.containerID())
        .setDatanodeDetails(dn2).build());

    Assert.assertEquals(2,
        containerManager.getContainerReplicas(
            finalContInfo.containerID()).size());

    contInfo = containerManager.getContainer(contInfo.containerID());
    Assert.assertEquals(contInfo.getState(), LifeCycleState.CLOSED);
    Pipeline pipeline = containerWithPipeline.getPipeline();
    pipelineManager.finalizePipeline(pipeline.getId());

    ContainerWithPipeline containerWithPipeline2 = containerManager
        .getContainerWithPipeline(contInfo.containerID());
    pipeline = containerWithPipeline2.getPipeline();
    Assert.assertNotEquals(containerWithPipeline, containerWithPipeline2);
    Assert.assertNotNull("Pipeline should not be null", pipeline);
    Assert.assertTrue(pipeline.getNodes().contains(dn1));
    Assert.assertTrue(pipeline.getNodes().contains(dn2));
  }

  @Test
  public void testgetNoneExistentContainer() {
    try {
      containerManager.getContainer(ContainerID.valueof(
          random.nextInt() & Integer.MAX_VALUE));
      Assert.fail();
    } catch (ContainerNotFoundException ex) {
      // Success!
    }
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
        .containerID(), HddsProtos.LifeCycleEvent.CREATE);
    Thread.sleep(TIMEOUT + 1000);

    thrown.expect(IOException.class);
    thrown.expectMessage("Lease Exception");
    containerManager
        .updateContainerState(containerInfo.getContainerInfo().containerID(),
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
        datanodeDetails, crBuilder.build());

    ContainerInfo updatedContainer =
        containerManager.getContainer(info.containerID());
    Assert.assertEquals(100000000L,
        updatedContainer.getNumberOfKeys());
    Assert.assertEquals(2000000000L, updatedContainer.getUsedBytes());

    for (StorageContainerDatanodeProtocolProtos.ContainerInfo c : reports) {
     Assert.assertEquals(containerManager.getContainerReplicas(
         ContainerID.valueof(c.getContainerID())).size(), 1);
    }

    containerManager.processContainerReports(TestUtils.randomDatanodeDetails(),
        crBuilder.build());
    for (StorageContainerDatanodeProtocolProtos.ContainerInfo c : reports) {
      Assert.assertEquals(containerManager.getContainerReplicas(
              ContainerID.valueof(c.getContainerID())).size(), 2);
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
        datanodeDetails, crBuilder.build());

    List<ContainerInfo> list = containerManager.listContainer(
        ContainerID.valueof(1), 50);
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
    ContainerID id = createContainer().containerID();
    containerManager.updateContainerState(id,
        HddsProtos.LifeCycleEvent.FINALIZE);
    containerManager.updateContainerState(id,
        HddsProtos.LifeCycleEvent.CLOSE);
   ContainerInfo closedContainer = containerManager.getContainer(id);
   Assert.assertEquals(LifeCycleState.CLOSED, closedContainer.getState());
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
    containerManager.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.CREATE);
    containerManager.updateContainerState(containerInfo.containerID(),
        HddsProtos.LifeCycleEvent.CREATED);
    return containerInfo;
  }

}
