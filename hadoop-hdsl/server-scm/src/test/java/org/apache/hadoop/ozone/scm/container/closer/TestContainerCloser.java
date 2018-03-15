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

package org.apache.hadoop.ozone.scm.container.closer;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.MockNodeManager;
import org.apache.hadoop.ozone.scm.container.TestContainerMapping;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleEvent.CREATE;
import static org.apache.hadoop.hdsl.protocol.proto.HdslProtos.LifeCycleEvent.CREATED;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB;

/**
 * Test class for Closing Container.
 */
public class TestContainerCloser {

  private static final long GIGABYTE = 1024L * 1024L * 1024L;
  private static Configuration configuration;
  private static MockNodeManager nodeManager;
  private static ContainerMapping mapping;
  private static long size;
  private static File testDir;

  @BeforeClass
  public static void setUp() throws Exception {
    configuration = SCMTestUtils.getConf();
    size = configuration.getLong(OZONE_SCM_CONTAINER_SIZE_GB,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT) * 1024 * 1024 * 1024;
    configuration.setTimeDuration(OZONE_CONTAINER_REPORT_INTERVAL,
        1, TimeUnit.SECONDS);
    testDir = GenericTestUtils
        .getTestDir(TestContainerMapping.class.getSimpleName());
    configuration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    nodeManager = new MockNodeManager(true, 10);
    mapping = new ContainerMapping(configuration, nodeManager, 128);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (mapping != null) {
      mapping.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testClose() throws IOException {
    String containerName = "container-" + RandomStringUtils.randomNumeric(5);

    ContainerInfo info = mapping.allocateContainer(
        HdslProtos.ReplicationType.STAND_ALONE,
        HdslProtos.ReplicationFactor.ONE, containerName, "ozone");

    //Execute these state transitions so that we can close the container.
    mapping.updateContainerState(containerName, CREATE);
    mapping.updateContainerState(containerName, CREATED);
    long currentCount = mapping.getCloser().getCloseCount();
    long runCount = mapping.getCloser().getThreadRunCount();

    DatanodeID datanodeID = info.getPipeline().getLeader();
    // Send a container report with used set to 1 GB. This should not close.
    sendContainerReport(info, 1 * GIGABYTE);

    // with only one container the  cleaner thread should not run.
    Assert.assertEquals(0, mapping.getCloser().getThreadRunCount());

    // With only 1 GB, the container should not be queued for closing.
    Assert.assertEquals(0, mapping.getCloser().getCloseCount());

    // Assert that the Close command was not queued for this Datanode.
    Assert.assertEquals(0, nodeManager.getCommandCount(datanodeID));

    long newUsed = (long) (size * 0.91f);
    sendContainerReport(info, newUsed);

    // with only one container the  cleaner thread should not run.
    Assert.assertEquals(runCount, mapping.getCloser().getThreadRunCount());

    // and close count will be one.
    Assert.assertEquals(1,
        mapping.getCloser().getCloseCount() - currentCount);

    // Assert that the Close command was Queued for this Datanode.
    Assert.assertEquals(1, nodeManager.getCommandCount(datanodeID));
  }

  @Test
  public void testRepeatedClose() throws IOException,
      InterruptedException {
    // This test asserts that if we queue more than one report then the
    // second report is discarded by the system if it lands in the 3 * report
    // frequency window.

    configuration.setTimeDuration(OZONE_CONTAINER_REPORT_INTERVAL, 1,
        TimeUnit.SECONDS);
    String containerName = "container-" + RandomStringUtils.randomNumeric(5);

    ContainerInfo info = mapping.allocateContainer(
        HdslProtos.ReplicationType.STAND_ALONE,
        HdslProtos.ReplicationFactor.ONE, containerName, "ozone");

    //Execute these state transitions so that we can close the container.
    mapping.updateContainerState(containerName, CREATE);

    long currentCount = mapping.getCloser().getCloseCount();
    long runCount = mapping.getCloser().getThreadRunCount();


    DatanodeID datanodeID = info.getPipeline().getLeader();

    // Send this command twice and assert we have only one command in the queue.
    sendContainerReport(info, 5 * GIGABYTE);
    sendContainerReport(info, 5 * GIGABYTE);

    // Assert that the Close command was Queued for this Datanode.
    Assert.assertEquals(1,
        nodeManager.getCommandCount(datanodeID));
    // And close count will be one.
    Assert.assertEquals(1,
        mapping.getCloser().getCloseCount() - currentCount);
    Thread.sleep(TimeUnit.SECONDS.toMillis(4));

    //send another close and the system will queue this to the command queue.
    sendContainerReport(info, 5 * GIGABYTE);
    Assert.assertEquals(2,
        nodeManager.getCommandCount(datanodeID));
    // but the close count will still be one, since from the point of view of
    // closer we are closing only one container even if we have send multiple
    // close commands to the datanode.
    Assert.assertEquals(1, mapping.getCloser().getCloseCount()
        - currentCount);
  }

  @Test
  public void testCleanupThreadRuns() throws IOException,
      InterruptedException {
    // This test asserts that clean up thread runs once we have closed a
    // number above cleanup water mark.

    long runCount = mapping.getCloser().getThreadRunCount();

    for (int x = 0; x < ContainerCloser.getCleanupWaterMark() + 10; x++) {
      String containerName = "container-" + RandomStringUtils.randomNumeric(7);
      ContainerInfo info = mapping.allocateContainer(
          HdslProtos.ReplicationType.STAND_ALONE,
          HdslProtos.ReplicationFactor.ONE, containerName, "ozone");
      mapping.updateContainerState(containerName, CREATE);
      mapping.updateContainerState(containerName, CREATED);
      sendContainerReport(info, 5 * GIGABYTE);
    }

    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Assert that cleanup thread ran at least once.
    Assert.assertTrue(mapping.getCloser().getThreadRunCount() - runCount > 0);
  }

  private void sendContainerReport(ContainerInfo info, long used) throws
      IOException {
    ContainerReportsRequestProto.Builder
        reports =  ContainerReportsRequestProto.newBuilder();
    reports.setType(ContainerReportsRequestProto.reportType.fullReport);

    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setContainerName(info.getContainerName())
        .setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
        .setSize(size)
        .setUsed(used)
        .setKeyCount(100000000L)
        .setReadCount(100000000L)
        .setWriteCount(100000000L)
        .setReadBytes(2000000000L)
        .setWriteBytes(2000000000L)
        .setContainerID(1L);
    reports.setDatanodeID(
        DFSTestUtil.getLocalDatanodeID().getProtoBufMessage());
    reports.addReports(ciBuilder);
    mapping.processContainerReports(reports.build());
  }
}