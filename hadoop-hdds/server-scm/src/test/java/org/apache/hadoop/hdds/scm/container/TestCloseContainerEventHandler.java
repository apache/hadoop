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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CLOSE_CONTAINER;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;

/**
 * Tests the closeContainerEventHandler class.
 */
public class TestCloseContainerEventHandler {

  private static Configuration configuration;
  private static MockNodeManager nodeManager;
  private static SCMPipelineManager pipelineManager;
  private static SCMContainerManager containerManager;
  private static long size;
  private static File testDir;
  private static EventQueue eventQueue;

  @BeforeClass
  public static void setUp() throws Exception {
    configuration = SCMTestUtils.getConf();
    size = (long)configuration.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    testDir = GenericTestUtils
        .getTestDir(TestCloseContainerEventHandler.class.getSimpleName());
    configuration
        .set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    nodeManager = new MockNodeManager(true, 10);
    pipelineManager =
        new SCMPipelineManager(configuration, nodeManager, eventQueue);
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), configuration);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    containerManager = new
        SCMContainerManager(configuration, nodeManager,
        pipelineManager, new EventQueue());
    eventQueue = new EventQueue();
    eventQueue.addHandler(CLOSE_CONTAINER,
        new CloseContainerEventHandler(pipelineManager, containerManager));
    eventQueue.addHandler(DATANODE_COMMAND, nodeManager);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (containerManager != null) {
      containerManager.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testIfCloseContainerEventHadnlerInvoked() {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    eventQueue.fireEvent(CLOSE_CONTAINER,
        new ContainerID(Math.abs(RandomUtils.nextInt())));
    eventQueue.processAll(1000);
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Close container Event triggered for container"));
  }

  @Test
  public void testCloseContainerEventWithInvalidContainer() {
    long id = Math.abs(RandomUtils.nextInt());
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    eventQueue.fireEvent(CLOSE_CONTAINER,
        new ContainerID(id));
    eventQueue.processAll(1000);
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Failed to close the container"));
  }

  @Test
  public void testCloseContainerEventWithValidContainers() throws IOException {

    ContainerInfo container = containerManager
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, "ozone");
    ContainerID id = container.containerID();
    DatanodeDetails datanode = pipelineManager
        .getPipeline(container.getPipelineID()).getFirstNode();
    int closeCount = nodeManager.getCommandCount(datanode);
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    Assert.assertEquals(closeCount + 1,
        nodeManager.getCommandCount(datanode));
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING,
        containerManager.getContainer(id).getState());
  }

  @Test
  public void testCloseContainerEventWithRatis() throws IOException {

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    ContainerInfo container = containerManager
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, "ozone");
    ContainerID id = container.containerID();
    int[] closeCount = new int[3];
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    int i = 0;
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      closeCount[i] = nodeManager.getCommandCount(details);
      i++;
    }
    i = 0;
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      Assert.assertEquals(closeCount[i], nodeManager.getCommandCount(details));
      i++;
    }
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    i = 0;
    // Make sure close is queued for each datanode on the pipeline
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      Assert.assertEquals(closeCount[i] + 1,
          nodeManager.getCommandCount(details));
      Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING,
          containerManager.getContainer(id).getState());
      i++;
    }
  }
}
