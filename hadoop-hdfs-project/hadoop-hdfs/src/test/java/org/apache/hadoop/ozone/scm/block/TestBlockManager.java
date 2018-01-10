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

package org.apache.hadoop.ozone.scm.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.container.ContainerMapping;
import org.apache.hadoop.ozone.scm.container.MockNodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
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
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;


/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager {
  private static ContainerMapping mapping;
  private static MockNodeManager nodeManager;
  private static BlockManagerImpl blockManager;
  private static File testDir;
  private final static long DEFAULT_BLOCK_SIZE = 128 * MB;
  private static OzoneProtos.ReplicationFactor factor;
  private static OzoneProtos.ReplicationType type;
  private static String containerOwner = "OZONE";

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = SCMTestUtils.getConf();

    String path = GenericTestUtils
        .getTempPath(TestBlockManager.class.getSimpleName());

    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, path);
    testDir = Paths.get(path).toFile();
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 10);
    mapping = new ContainerMapping(conf, nodeManager, 128);
    blockManager = new BlockManagerImpl(conf, nodeManager, mapping, 128);
    if(conf.getBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT)){
      factor = OzoneProtos.ReplicationFactor.THREE;
      type = OzoneProtos.ReplicationType.RATIS;
    } else {
      factor = OzoneProtos.ReplicationFactor.ONE;
      type = OzoneProtos.ReplicationType.STAND_ALONE;
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    blockManager.close();
    mapping.close();
    FileUtil.fullyDelete(testDir);
  }

  @Before
  public void clearChillMode() {
    nodeManager.setChillmode(false);
  }

  @Test
  public void testAllocateBlock() throws Exception {
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
    Assert.assertNotNull(block);
  }

  @Test
  public void testGetAllocatedBlock() throws IOException {
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
    Assert.assertNotNull(block);
    Pipeline pipeline = blockManager.getBlock(block.getKey());
    Assert.assertEquals(pipeline.getLeader().getDatanodeUuid(),
        block.getPipeline().getLeader().getDatanodeUuid());
  }

  @Test
  public void testDeleteBlock() throws Exception {
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
    Assert.assertNotNull(block);
    blockManager.deleteBlocks(Collections.singletonList(block.getKey()));

    // Deleted block can not be retrieved
    thrown.expectMessage("Specified block key does not exist.");
    blockManager.getBlock(block.getKey());

    // Tombstone of the deleted block can be retrieved if it has not been
    // cleaned yet.
    String deletedKeyName = blockManager.getDeletedKeyName(block.getKey());
    Pipeline pipeline = blockManager.getBlock(deletedKeyName);
    Assert.assertEquals(pipeline.getLeader().getDatanodeUuid(),
        block.getPipeline().getLeader().getDatanodeUuid());
  }

  @Test
  public void testAllocateOversizedBlock() throws IOException {
    long size = 6 * GB;
    thrown.expectMessage("Unsupported block size");
    AllocatedBlock block = blockManager.allocateBlock(size,
        type, factor, containerOwner);
  }

  @Test
  public void testGetNoneExistentContainer() throws IOException {
    String nonExistBlockKey = UUID.randomUUID().toString();
    thrown.expectMessage("Specified block key does not exist.");
    blockManager.getBlock(nonExistBlockKey);
  }

  @Test
  public void testChillModeAllocateBlockFails() throws IOException {
    nodeManager.setChillmode(true);
    thrown.expectMessage("Unable to create block while in chill mode");
    blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
  }
}
