/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_METADATA_STORE_IMPL_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_METADATA_STORE_IMPL_ROCKSDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test KeyValue container block iterator.
 */
@RunWith(Parameterized.class)
public class TestKeyValueBlockIterator {

  private KeyValueContainer container;
  private KeyValueContainerData containerData;
  private VolumeSet volumeSet;
  private Configuration conf;
  private File testRoot;

  private final String storeImpl;

  public TestKeyValueBlockIterator(String metadataImpl) {
    this.storeImpl = metadataImpl;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {OZONE_METADATA_STORE_IMPL_LEVELDB},
        {OZONE_METADATA_STORE_IMPL_ROCKSDB}});
  }

  @Before
  public void setUp() throws Exception {
    testRoot = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(OZONE_METADATA_STORE_IMPL, storeImpl);
    volumeSet = new VolumeSet(UUID.randomUUID().toString(), conf);
  }


  @After
  public void tearDown() {
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  @Test
  public void testKeyValueBlockIteratorWithMixedBlocks() throws Exception {

    long containerID = 100L;
    int deletedBlocks = 5;
    int normalBlocks = 5;
    createContainerWithBlocks(containerID, normalBlocks, deletedBlocks);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath));

    int counter = 0;
    while(keyValueBlockIterator.hasNext()) {
      BlockData blockData = keyValueBlockIterator.nextBlock();
      assertEquals(blockData.getLocalID(), counter++);
    }

    assertFalse(keyValueBlockIterator.hasNext());

    keyValueBlockIterator.seekToFirst();
    counter = 0;
    while(keyValueBlockIterator.hasNext()) {
      BlockData blockData = keyValueBlockIterator.nextBlock();
      assertEquals(blockData.getLocalID(), counter++);
    }
    assertFalse(keyValueBlockIterator.hasNext());

    try {
      keyValueBlockIterator.nextBlock();
    } catch (NoSuchElementException ex) {
      GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
          "for ContainerID " + containerID, ex);
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithNextBlock() throws Exception {
    long containerID = 101L;
    createContainerWithBlocks(containerID, 2, 0);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath));
    long blockID = 0L;
    assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());
    assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

    try {
      keyValueBlockIterator.nextBlock();
    } catch (NoSuchElementException ex) {
      GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
          "for ContainerID " + containerID, ex);
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithHasNext() throws Exception {
    long containerID = 102L;
    createContainerWithBlocks(containerID, 2, 0);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath));
    long blockID = 0L;

    // Even calling multiple times hasNext() should not move entry forward.
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());

    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertTrue(keyValueBlockIterator.hasNext());
    assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

    keyValueBlockIterator.seekToLast();
    assertTrue(keyValueBlockIterator.hasNext());
    assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

    keyValueBlockIterator.seekToFirst();
    blockID = 0L;
    assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());
    assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

    try {
      keyValueBlockIterator.nextBlock();
    } catch (NoSuchElementException ex) {
      GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
          "for ContainerID " + containerID, ex);
    }


  }

  @Test
  public void testKeyValueBlockIteratorWithFilter() throws Exception {
    long containerId = 103L;
    int deletedBlocks = 5;
    int normalBlocks = 5;
    createContainerWithBlocks(containerId, normalBlocks, deletedBlocks);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerId, new File(containerPath), MetadataKeyFilters
        .getDeletingKeyFilter());

    int counter = 5;
    while(keyValueBlockIterator.hasNext()) {
      BlockData blockData = keyValueBlockIterator.nextBlock();
      assertEquals(blockData.getLocalID(), counter++);
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithOnlyDeletedBlocks() throws
      Exception {
    long containerId = 104L;
    createContainerWithBlocks(containerId, 0, 5);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerId, new File(containerPath));
    //As all blocks are deleted blocks, blocks does not match with normal key
    // filter.
    assertFalse(keyValueBlockIterator.hasNext());
  }

  /**
   * Creates a container with specified number of normal blocks and deleted
   * blocks. First it will insert normal blocks, and then it will insert
   * deleted blocks.
   * @param containerId
   * @param normalBlocks
   * @param deletedBlocks
   * @throws Exception
   */
  private void createContainerWithBlocks(long containerId, int
      normalBlocks, int deletedBlocks) throws
      Exception {
    containerData = new KeyValueContainerData(containerId,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), UUID
        .randomUUID().toString());
    MetadataStore metadataStore = BlockUtils.getDB(containerData, conf);

    List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
    ChunkInfo info = new ChunkInfo("chunkfile", 0, 1024);
    chunkList.add(info.getProtoBufMessage());

    for (int i=0; i<normalBlocks; i++) {
      BlockID blockID = new BlockID(containerId, i);
      BlockData blockData = new BlockData(blockID);
      blockData.setChunks(chunkList);
      metadataStore.put(Longs.toByteArray(blockID.getLocalID()), blockData
          .getProtoBufMessage().toByteArray());
    }

    for (int i=normalBlocks; i<deletedBlocks; i++) {
      BlockID blockID = new BlockID(containerId, i);
      BlockData blockData = new BlockData(blockID);
      blockData.setChunks(chunkList);
      metadataStore.put(DFSUtil.string2Bytes(OzoneConsts
          .DELETING_KEY_PREFIX + blockID.getLocalID()), blockData
          .getProtoBufMessage().toByteArray());
    }
  }

}
