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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerMapping;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import java.nio.file.Paths;
import java.util.Collections;

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
  private static HddsProtos.ReplicationFactor factor;
  private static HddsProtos.ReplicationType type;
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
    mapping =
        new ContainerMapping(conf, nodeManager, 128, new EventQueue());
    blockManager = new BlockManagerImpl(conf, nodeManager, mapping, null);
    if(conf.getBoolean(ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT)){
      factor = HddsProtos.ReplicationFactor.THREE;
      type = HddsProtos.ReplicationType.RATIS;
    } else {
      factor = HddsProtos.ReplicationFactor.ONE;
      type = HddsProtos.ReplicationType.STAND_ALONE;
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
  public void testAllocateOversizedBlock() throws IOException {
    long size = 6 * GB;
    thrown.expectMessage("Unsupported block size");
    AllocatedBlock block = blockManager.allocateBlock(size,
        type, factor, containerOwner);
  }


  @Test
  public void testChillModeAllocateBlockFails() throws IOException {
    nodeManager.setChillmode(true);
    thrown.expectMessage("Unable to create block while in chill mode");
    blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, containerOwner);
  }
}
