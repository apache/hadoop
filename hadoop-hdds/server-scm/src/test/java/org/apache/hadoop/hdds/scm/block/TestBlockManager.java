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

package org.apache.hadoop.hdds.scm.block;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;


/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager implements EventHandler<Boolean> {
  private StorageContainerManager scm;
  private SCMContainerManager mapping;
  private MockNodeManager nodeManager;
  private PipelineManager pipelineManager;
  private BlockManagerImpl blockManager;
  private File testDir;
  private final static long DEFAULT_BLOCK_SIZE = 128 * MB;
  private static HddsProtos.ReplicationFactor factor;
  private static HddsProtos.ReplicationType type;
  private static String containerOwner = "OZONE";
  private static EventQueue eventQueue;
  private int numContainerPerOwnerInPipeline;
  private OzoneConfiguration conf;
  private SafeModeStatus safeModeStatus = new SafeModeStatus(false);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    conf = SCMTestUtils.getConf();
    numContainerPerOwnerInPipeline = conf.getInt(
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);


    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.newFolder().toString());

    // Override the default Node Manager in SCM with this Mock Node Manager.
    nodeManager = new MockNodeManager(true, 10);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    scm = TestUtils.getScm(conf, configurator);

    // Initialize these fields so that the tests can pass.
    mapping = (SCMContainerManager) scm.getContainerManager();
    pipelineManager = scm.getPipelineManager();
    blockManager = (BlockManagerImpl) scm.getScmBlockManager();

    eventQueue = new EventQueue();
    eventQueue.addHandler(SCMEvents.SAFE_MODE_STATUS,
        scm.getSafeModeHandler());
    eventQueue.addHandler(SCMEvents.START_REPLICATION, this);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(pipelineManager, mapping);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    if(conf.getBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT)){
      factor = HddsProtos.ReplicationFactor.THREE;
      type = HddsProtos.ReplicationType.RATIS;
    } else {
      factor = HddsProtos.ReplicationFactor.ONE;
      type = HddsProtos.ReplicationType.STAND_ALONE;
    }
  }

  @After
  public void cleanup() throws IOException {
    scm.stop();
  }

  @Test
  public void testAllocateBlock() throws Exception {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInSafeMode();
    }, 10, 1000 * 5);
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner, new ExcludeList());
    Assert.assertNotNull(block);
  }

  @Test
  public void testAllocateOversizedBlock() throws Exception {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInSafeMode();
    }, 10, 1000 * 5);
    long size = 6 * GB;
    thrown.expectMessage("Unsupported block size");
    AllocatedBlock block = blockManager.allocateBlock(size,
        type, factor, containerOwner, new ExcludeList());
  }


  @Test
  public void testAllocateBlockFailureInSafeMode() throws Exception {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS,
        new SafeModeStatus(true));
    GenericTestUtils.waitFor(() -> {
      return blockManager.isScmInSafeMode();
    }, 10, 1000 * 5);
    // Test1: In safe mode expect an SCMException.
    thrown.expectMessage("SafeModePrecheck failed for "
        + "allocateBlock");
    blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner, new ExcludeList());
  }

  @Test
  public void testAllocateBlockSucInSafeMode() throws Exception {
    // Test2: Exit safe mode and then try allocateBock again.
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInSafeMode();
    }, 10, 1000 * 5);
    Assert.assertNotNull(blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner, new ExcludeList()));
  }

  @Test(timeout = 10000)
  public void testMultipleBlockAllocation()
      throws IOException, TimeoutException, InterruptedException {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils
        .waitFor(() -> !blockManager.isScmInSafeMode(), 10, 1000 * 5);

    pipelineManager.createPipeline(type, factor);
    pipelineManager.createPipeline(type, factor);

    AllocatedBlock allocatedBlock = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner,
            new ExcludeList());
    // block should be allocated in different pipelines
    GenericTestUtils.waitFor(() -> {
      try {
        AllocatedBlock block = blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner,
                new ExcludeList());
        return !block.getPipeline().getId()
            .equals(allocatedBlock.getPipeline().getId());
      } catch (IOException e) {
      }
      return false;
    }, 100, 1000);
  }

  private boolean verifyNumberOfContainersInPipelines(
      int numContainersPerPipeline) {
    try {
      for (Pipeline pipeline : pipelineManager.getPipelines(type, factor)) {
        if (pipelineManager.getNumberOfContainers(pipeline.getId())
            != numContainersPerPipeline) {
          return false;
        }
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  @Test(timeout = 10000)
  public void testMultipleBlockAllocationWithClosedContainer()
      throws IOException, TimeoutException, InterruptedException {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils
        .waitFor(() -> !blockManager.isScmInSafeMode(), 10, 1000 * 5);

    // create pipelines
    for (int i = 0;
         i < nodeManager.getNodes(HddsProtos.NodeState.HEALTHY).size(); i++) {
      pipelineManager.createPipeline(type, factor);
    }

    // wait till each pipeline has the configured number of containers.
    // After this each pipeline has numContainerPerOwnerInPipeline containers
    // for each owner
    GenericTestUtils.waitFor(() -> {
      try {
        blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          numContainerPerOwnerInPipeline);
    }, 10, 1000);

    // close all the containers in all the pipelines
    for (Pipeline pipeline : pipelineManager.getPipelines(type, factor)) {
      for (ContainerID cid : pipelineManager
          .getContainersInPipeline(pipeline.getId())) {
        eventQueue.fireEvent(SCMEvents.CLOSE_CONTAINER, cid);
      }
    }
    // wait till no containers are left in the pipelines
    GenericTestUtils
        .waitFor(() -> verifyNumberOfContainersInPipelines(0), 10, 5000);

    // allocate block so that each pipeline has the configured number of
    // containers.
    GenericTestUtils.waitFor(() -> {
      try {
        blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          numContainerPerOwnerInPipeline);
    }, 10, 1000);
  }

  @Test(timeout = 10000)
  public void testBlockAllocationWithNoAvailablePipelines()
      throws IOException, TimeoutException, InterruptedException {
    eventQueue.fireEvent(SCMEvents.SAFE_MODE_STATUS, safeModeStatus);
    GenericTestUtils
        .waitFor(() -> !blockManager.isScmInSafeMode(), 10, 1000 * 5);

    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    Assert.assertEquals(0, pipelineManager.getPipelines(type, factor).size());
    Assert.assertNotNull(blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, containerOwner,
            new ExcludeList()));
    Assert.assertEquals(1, pipelineManager.getPipelines(type, factor).size());
  }

  @Override
  public void onMessage(Boolean aBoolean, EventPublisher publisher) {
    System.out.println("test");
  }
}
