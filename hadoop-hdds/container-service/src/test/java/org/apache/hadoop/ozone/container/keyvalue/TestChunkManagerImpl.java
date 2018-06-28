/*
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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * This class is used to test ChunkManager operations.
 */
public class TestChunkManagerImpl {

  private OzoneConfiguration config;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private KeyData keyData;
  private BlockID blockID;
  private ChunkManagerImpl chunkManager;
  private ChunkInfo chunkInfo;
  private byte[] data;
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();

    HddsVolume hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(config).datanodeUuid(UUID.randomUUID()
        .toString()).build();

    volumeSet = mock(VolumeSet.class);

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L);

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    data = "testing write chunks".getBytes();
    // Creating KeyData
    blockID = new BlockID(1L, 1L);
    keyData = new KeyData(blockID);
    keyData.addMetadata("VOLUME", "ozone");
    keyData.addMetadata("OWNER", "hdfs");
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, data.length);
    chunkList.add(chunkInfo.getProtoBufMessage());
    keyData.setChunks(chunkList);

    // Create a ChunkManager object.
    chunkManager = new ChunkManagerImpl();

  }

  @Test
  public void testWriteChunkStageWriteAndCommit() throws Exception {
    //As in Setup, we try to create container, these paths should exist.
    assertTrue(keyValueContainerData.getChunksPath() != null);
    File chunksPath = new File(keyValueContainerData.getChunksPath());
    assertTrue(chunksPath.exists());
    // Initially chunks folder should be empty.
    assertTrue(chunksPath.listFiles().length == 0);
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        ContainerProtos.Stage.WRITE_DATA);
    // Now a chunk file is being written with Stage WRITE_DATA, so it should
    // create a temporary chunk file.
    assertTrue(chunksPath.listFiles().length == 1);
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        ContainerProtos.Stage.COMMIT_DATA);
    // Old temp file should have been renamed to chunk file.
    assertTrue(chunksPath.listFiles().length == 1);

  }

  @Test
  public void testWriteChunkIncorrectLength() throws Exception {
    try {
      long randomLength = 200L;
      chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, randomLength);
      List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
      chunkList.add(chunkInfo.getProtoBufMessage());
      keyData.setChunks(chunkList);
      chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
          ContainerProtos.Stage.WRITE_DATA);
      fail("testWriteChunkIncorrectLength failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("data array does not match " +
          "the length ", ex);
      assertEquals(ContainerProtos.Result.INVALID_WRITE_SIZE, ex.getResult());
    }
  }

  @Test
  public void testWriteChunkStageCombinedData() throws Exception {
    //As in Setup, we try to create container, these paths should exist.
    assertTrue(keyValueContainerData.getChunksPath() != null);
    File chunksPath = new File(keyValueContainerData.getChunksPath());
    assertTrue(chunksPath.exists());
    // Initially chunks folder should be empty.
    assertTrue(chunksPath.listFiles().length == 0);
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        ContainerProtos.Stage.COMBINED);
    // Now a chunk file is being written with Stage WRITE_DATA, so it should
    // create a temporary chunk file.
    assertTrue(chunksPath.listFiles().length == 1);
  }

  @Test
  public void testReadChunk() throws Exception {
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        ContainerProtos.Stage.COMBINED);
    byte[] expectedData = chunkManager.readChunk(keyValueContainer, blockID,
        chunkInfo);
    assertEquals(expectedData.length, data.length);
    assertTrue(Arrays.equals(expectedData, data));
  }

  @Test
  public void testDeleteChunk() throws Exception {
    File chunksPath = new File(keyValueContainerData.getChunksPath());
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        ContainerProtos.Stage.COMBINED);
    assertTrue(chunksPath.listFiles().length == 1);
    chunkManager.deleteChunk(keyValueContainer, blockID, chunkInfo);
    assertTrue(chunksPath.listFiles().length == 0);
  }

  @Test
  public void testDeleteChunkUnsupportedRequest() throws Exception {
    try {
      chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
          ContainerProtos.Stage.COMBINED);
      long randomLength = 200L;
      chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, randomLength);
      List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
      chunkList.add(chunkInfo.getProtoBufMessage());
      keyData.setChunks(chunkList);
      chunkManager.deleteChunk(keyValueContainer, blockID, chunkInfo);
      fail("testDeleteChunkUnsupportedRequest");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Not Supported Operation.", ex);
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex.getResult());
    }
  }

  @Test
  public void testWriteChunkChecksumMismatch() throws Exception {
    try {
      chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, data.length);
      //Setting checksum to some value.
      chunkInfo.setChecksum("some garbage");
      List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
      chunkList.add(chunkInfo.getProtoBufMessage());
      keyData.setChunks(chunkList);
      chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
          ContainerProtos.Stage.COMBINED);
      fail("testWriteChunkChecksumMismatch failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Checksum mismatch.", ex);
      assertEquals(ContainerProtos.Result.CHECKSUM_MISMATCH, ex.getResult());
    }
  }

  @Test
  public void testReadChunkFileNotExists() throws Exception {
    try {
      // trying to read a chunk, where chunk file does not exist
      byte[] expectedData = chunkManager.readChunk(keyValueContainer, blockID,
          chunkInfo);
      fail("testReadChunkFileNotExists failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Unable to find the chunk " +
          "file.", ex);
      assertEquals(ContainerProtos.Result.UNABLE_TO_FIND_CHUNK, ex.getResult());
    }
  }


}
