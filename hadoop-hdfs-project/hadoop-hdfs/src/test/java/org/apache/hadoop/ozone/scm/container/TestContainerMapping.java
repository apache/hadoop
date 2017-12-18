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
package org.apache.hadoop.ozone.scm.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests for Container Mapping.
 */
public class TestContainerMapping {
  private static ContainerMapping mapping;
  private static MockNodeManager nodeManager;
  private static File testDir;
  private static XceiverClientManager xceiverClientManager;

  private static final long TIMEOUT = 10000;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = SCMTestUtils.getConf();

    testDir = GenericTestUtils
        .getTestDir(TestContainerMapping.class.getSimpleName());
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
    mapping = new ContainerMapping(conf, nodeManager, 128);
    xceiverClientManager = new XceiverClientManager(conf);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if(mapping != null) {
      mapping.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Before
  public void clearChillMode() {
    nodeManager.setChillmode(false);
  }

  @Test
  public void testallocateContainer() throws Exception {
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        UUID.randomUUID().toString(), OzoneProtos.Owner.OZONE);
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
    Set<String> pipelineList = new TreeSet<>();
    for (int x = 0; x < 30; x++) {
      ContainerInfo containerInfo = mapping.allocateContainer(
          xceiverClientManager.getType(),
          xceiverClientManager.getFactor(),
          UUID.randomUUID().toString(), OzoneProtos.Owner.OZONE);

      Assert.assertNotNull(containerInfo);
      Assert.assertNotNull(containerInfo.getPipeline());
      pipelineList.add(containerInfo.getPipeline().getLeader()
          .getDatanodeUuid());
    }
    Assert.assertTrue(pipelineList.size() > 5);
  }

  @Test
  public void testGetContainer() throws IOException {
    String containerName = UUID.randomUUID().toString();
    Pipeline pipeline = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerName,
        OzoneProtos.Owner.OZONE).getPipeline();
    Assert.assertNotNull(pipeline);
    Pipeline newPipeline = mapping.getContainer(containerName).getPipeline();
    Assert.assertEquals(pipeline.getLeader().getDatanodeUuid(),
        newPipeline.getLeader().getDatanodeUuid());
  }

  @Test
  public void testDuplicateAllocateContainerFails() throws IOException {
    String containerName = UUID.randomUUID().toString();
    Pipeline pipeline = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerName,
        OzoneProtos.Owner.OZONE).getPipeline();
    Assert.assertNotNull(pipeline);
    thrown.expectMessage("Specified container already exists.");
    mapping.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerName,
        OzoneProtos.Owner.OZONE);
  }

  @Test
  public void testgetNoneExistentContainer() throws IOException {
    String containerName = UUID.randomUUID().toString();
    thrown.expectMessage("Specified key does not exist.");
    mapping.getContainer(containerName);
  }

  @Test
  public void testChillModeAllocateContainerFails() throws IOException {
    String containerName = UUID.randomUUID().toString();
    nodeManager.setChillmode(true);
    thrown.expectMessage("Unable to create container while in chill mode");
    mapping.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerName,
        OzoneProtos.Owner.OZONE);
  }

  @Test
  public void testContainerCreationLeaseTimeout() throws IOException,
      InterruptedException {
    String containerName = UUID.randomUUID().toString();
    nodeManager.setChillmode(false);
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerName,
        OzoneProtos.Owner.OZONE);
    mapping.updateContainerState(containerInfo.getContainerName(),
        OzoneProtos.LifeCycleEvent.BEGIN_CREATE);
    Thread.sleep(TIMEOUT + 1000);

    List<ContainerInfo> deleteContainers = mapping.getStateManager()
        .getMatchingContainers(
            containerInfo.getOwner(),
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.DELETING);
    Assert.assertTrue(deleteContainers.stream().map(
        container -> container.getContainerName()).collect(
        Collectors.toList()).contains(containerName));

    thrown.expect(IOException.class);
    thrown.expectMessage("Lease Exception");
    mapping.updateContainerState(containerInfo.getContainerName(),
        OzoneProtos.LifeCycleEvent.COMPLETE_CREATE);
  }

  @Test
  public void testFullContainerReport() throws IOException,
      InterruptedException {
    String containerName = UUID.randomUUID().toString();
    createContainer(containerName);
    DatanodeID datanodeID = SCMTestUtils.getDatanodeID();
    ContainerReportsRequestProto.reportType reportType =
        ContainerReportsRequestProto.reportType.fullReport;
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();
    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setContainerName(containerName)
        //setting some random hash
        .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
        .setSize(5368709120L)
        .setUsed(2000000000L)
        .setKeyCount(100000000L)
        .setReadCount(100000000L)
        .setWriteCount(100000000L)
        .setReadBytes(2000000000L)
        .setWriteBytes(2000000000L);

    reports.add(ciBuilder.build());
    mapping.processContainerReports(datanodeID, reportType, reports);

    ContainerInfo updatedContainer = mapping.getContainer(containerName);
    Assert.assertEquals(100000000L, updatedContainer.getNumberOfKeys());
    Assert.assertEquals(2000000000L, updatedContainer.getUsedBytes());
  }

  @Test
  public void testContainerCloseWithContainerReport() throws IOException,
      InterruptedException {
    String containerName = UUID.randomUUID().toString();
    createContainer(containerName);
    DatanodeID datanodeID = SCMTestUtils.getDatanodeID();
    ContainerReportsRequestProto.reportType reportType =
        ContainerReportsRequestProto.reportType.fullReport;
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();

    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setContainerName(containerName)
        //setting some random hash
        .setFinalhash("7c45eb4d7ed5e0d2e89aaab7759de02e")
        .setSize(5368709120L)
        .setUsed(5368705120L)
        .setKeyCount(500000000L)
        .setReadCount(500000000L)
        .setWriteCount(500000000L)
        .setReadBytes(5368705120L)
        .setWriteBytes(5368705120L);

    reports.add(ciBuilder.build());

    mapping.processContainerReports(datanodeID, reportType, reports);

    ContainerInfo updatedContainer = mapping.getContainer(containerName);
    Assert.assertEquals(500000000L, updatedContainer.getNumberOfKeys());
    Assert.assertEquals(5368705120L, updatedContainer.getUsedBytes());
    List<ContainerInfo> closeContainers = mapping.getStateManager()
        .getMatchingContainers(
            OzoneProtos.Owner.OZONE,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            OzoneProtos.LifeCycleState.CLOSED);
    Assert.assertTrue(closeContainers.stream().map(
        container -> container.getContainerName()).collect(
        Collectors.toList()).contains(containerName));
  }

  /**
   * Creates a container with the given name in ContainerMapping.
   * @param containerName
   *          Name of the container
   * @throws IOException
   */
  private void createContainer(String containerName) throws IOException {
    nodeManager.setChillmode(false);
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerName,
        OzoneProtos.Owner.OZONE);
    mapping.updateContainerState(containerInfo.getContainerName(),
        OzoneProtos.LifeCycleEvent.BEGIN_CREATE);
    mapping.updateContainerState(containerInfo.getContainerName(),
        OzoneProtos.LifeCycleEvent.COMPLETE_CREATE);
  }

}
