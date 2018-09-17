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

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMStorage;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Storage.StorageState;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;


/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager implements EventHandler<Boolean> {
  private static ContainerMapping mapping;
  private static MockNodeManager nodeManager;
  private static BlockManagerImpl blockManager;
  private static File testDir;
  private final static long DEFAULT_BLOCK_SIZE = 128 * MB;
  private static HddsProtos.ReplicationFactor factor;
  private static HddsProtos.ReplicationType type;
  private static String containerOwner = "OZONE";
  private static EventQueue eventQueue;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    Configuration conf = SCMTestUtils.getConf();

    String path = GenericTestUtils
        .getTempPath(TestBlockManager.class.getSimpleName());
    testDir = Paths.get(path).toFile();
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, path);
    eventQueue = new EventQueue();
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 10);
    mapping = new ContainerMapping(conf, nodeManager, 128, eventQueue);
    blockManager = new BlockManagerImpl(conf,
        nodeManager, mapping, eventQueue);
    eventQueue.addHandler(SCMEvents.CHILL_MODE_STATUS, blockManager);
    eventQueue.addHandler(SCMEvents.START_REPLICATION, this);
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
    blockManager.close();
    mapping.close();
    FileUtil.fullyDelete(testDir);
  }

  private static StorageContainerManager getScm(OzoneConfiguration conf)
      throws IOException {
    conf.setBoolean(OZONE_ENABLED, true);
    SCMStorage scmStore = new SCMStorage(conf);
    if(scmStore.getState() != StorageState.INITIALIZED) {
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      // writes the version file properties
      scmStore.initialize();
    }
    return StorageContainerManager.createSCM(null, conf);
  }

  @Test
  public void testAllocateBlock() throws Exception {
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInChillMode();
    }, 10, 1000 * 5);
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
    Assert.assertNotNull(block);
  }

  @Test
  public void testAllocateOversizedBlock() throws Exception {
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInChillMode();
    }, 10, 1000 * 5);
    long size = 6 * GB;
    thrown.expectMessage("Unsupported block size");
    AllocatedBlock block = blockManager.allocateBlock(size,
        type, factor, containerOwner);
  }


  @Test
  public void testAllocateBlockFailureInChillMode() throws Exception {
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, true);
    GenericTestUtils.waitFor(() -> {
      return blockManager.isScmInChillMode();
    }, 10, 1000 * 5);
    // Test1: In chill mode expect an SCMException.
    thrown.expectMessage("ChillModePrecheck failed for "
        + "allocateBlock");
    blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
  }

  @Test
  public void testAllocateBlockSucInChillMode() throws Exception {
    // Test2: Exit chill mode and then try allocateBock again.
    eventQueue.fireEvent(SCMEvents.CHILL_MODE_STATUS, false);
    GenericTestUtils.waitFor(() -> {
      return !blockManager.isScmInChillMode();
    }, 10, 1000 * 5);
    Assert.assertNotNull(blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner));
  }

  @Override
  public void onMessage(Boolean aBoolean, EventPublisher publisher) {
    System.out.println("test");
  }
}
