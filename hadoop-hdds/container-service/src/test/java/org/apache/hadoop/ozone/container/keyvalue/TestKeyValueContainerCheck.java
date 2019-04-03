/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Basic sanity test for the KeyValueContainerCheck class.
 */
@RunWith(Parameterized.class) public class TestKeyValueContainerCheck {
  private final String storeImpl;
  private KeyValueContainer container;
  private KeyValueContainerData containerData;
  private ChunkManagerImpl chunkManager;
  private VolumeSet volumeSet;
  private Configuration conf;
  private File testRoot;

  public TestKeyValueContainerCheck(String metadataImpl) {
    this.storeImpl = metadataImpl;
  }

  @Parameterized.Parameters public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{OZONE_METADATA_STORE_IMPL_LEVELDB},
        {OZONE_METADATA_STORE_IMPL_ROCKSDB}});
  }

  @Before public void setUp() throws Exception {
    this.testRoot = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(OZONE_METADATA_STORE_IMPL, storeImpl);
    volumeSet = new VolumeSet(UUID.randomUUID().toString(), conf);
  }

  @After public void teardown() {
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  /**
   * Sanity test, when there are no corruptions induced.
   * @throws Exception
   */
  @Test public void testKeyValueContainerCheckNoCorruption() throws Exception {
    long containerID = 101;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    int chunksPerBlock = 4;
    boolean corruption = false;

    // test Closed Container
    createContainerWithBlocks(containerID, normalBlocks, deletedBlocks, 65536,
        chunksPerBlock);
    File chunksPath = new File(containerData.getChunksPath());
    assertTrue(chunksPath.listFiles().length
        == (deletedBlocks + normalBlocks) * chunksPerBlock);

    KeyValueContainerCheck kvCheck =
        new KeyValueContainerCheck(containerData.getMetadataPath(), conf,
            containerID);

    // first run checks on a Open Container
    corruption = kvCheck.fastCheck();
    assertFalse(corruption);

    container.close();

    // next run checks on a Closed Container
    corruption = kvCheck.fullCheck();
    assertFalse(corruption);
  }

  /**
   * Creates a container with normal and deleted blocks.
   * First it will insert normal blocks, and then it will insert
   * deleted blocks.
   * @param containerId
   * @param normalBlocks
   * @param deletedBlocks
   * @throws Exception
   */
  private void createContainerWithBlocks(long containerId, int normalBlocks,
      int deletedBlocks, long chunkLen, int chunksPerBlock) throws Exception {
    long chunkCount;
    String strBlock = "block";
    String strChunk = "-chunkFile";
    byte[] chunkData = new byte[(int) chunkLen];
    long totalBlks = normalBlocks + deletedBlocks;

    containerData = new KeyValueContainerData(containerId,
        (long) StorageUnit.BYTES.toBytes(
            chunksPerBlock * chunkLen * totalBlks),
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
    container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
        UUID.randomUUID().toString());
    MetadataStore metadataStore = BlockUtils.getDB(containerData, conf);
    chunkManager = new ChunkManagerImpl(true);

    assertTrue(containerData.getChunksPath() != null);
    File chunksPath = new File(containerData.getChunksPath());
    assertTrue(chunksPath.exists());
    // Initially chunks folder should be empty.
    assertTrue(chunksPath.listFiles().length == 0);

    List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
    for (int i = 0; i < (totalBlks); i++) {
      BlockID blockID = new BlockID(containerId, i);
      BlockData blockData = new BlockData(blockID);

      chunkList.clear();
      for (chunkCount = 0; chunkCount < chunksPerBlock; chunkCount++) {
        String chunkName = strBlock + i + strChunk + chunkCount;
        long offset = chunkCount * chunkLen;
        ChunkInfo info = new ChunkInfo(chunkName, offset, chunkLen);
        chunkList.add(info.getProtoBufMessage());
        chunkManager
            .writeChunk(container, blockID, info, ByteBuffer.wrap(chunkData),
                new DispatcherContext.Builder()
                    .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
                    .build());
        chunkManager
            .writeChunk(container, blockID, info, ByteBuffer.wrap(chunkData),
                new DispatcherContext.Builder()
                    .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
                    .build());
      }
      blockData.setChunks(chunkList);

      if (i >= normalBlocks) {
        // deleted key
        metadataStore.put(DFSUtil.string2Bytes(
            OzoneConsts.DELETING_KEY_PREFIX + blockID.getLocalID()),
            blockData.getProtoBufMessage().toByteArray());
      } else {
        // normal key
        metadataStore.put(Longs.toByteArray(blockID.getLocalID()),
            blockData.getProtoBufMessage().toByteArray());
      }
    }
  }
}