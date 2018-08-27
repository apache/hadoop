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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.impl.KeyManagerImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * This class is used to test key related operations on the container.
 */
public class TestKeyManagerImpl {

  private OzoneConfiguration config;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private KeyData keyData;
  private KeyManagerImpl keyManager;
  private BlockID blockID;

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

    keyValueContainerData = new KeyValueContainerData(1L,
        (long) StorageUnit.GB.toBytes(5));

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    // Creating KeyData
    blockID = new BlockID(1L, 1L);
    keyData = new KeyData(blockID);
    keyData.addMetadata("VOLUME", "ozone");
    keyData.addMetadata("OWNER", "hdfs");
    List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
    ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, 1024);
    chunkList.add(info.getProtoBufMessage());
    keyData.setChunks(chunkList);

    // Create KeyValueContainerManager
    keyManager = new KeyManagerImpl(config);

  }

  @Test
  public void testPutAndGetKey() throws Exception {
    assertEquals(0, keyValueContainer.getContainerData().getKeyCount());
    //Put Key
    keyManager.putKey(keyValueContainer, keyData);

    assertEquals(1, keyValueContainer.getContainerData().getKeyCount());
    //Get Key
    KeyData fromGetKeyData = keyManager.getKey(keyValueContainer,
        keyData.getBlockID());

    assertEquals(keyData.getContainerID(), fromGetKeyData.getContainerID());
    assertEquals(keyData.getLocalID(), fromGetKeyData.getLocalID());
    assertEquals(keyData.getChunks().size(), fromGetKeyData.getChunks().size());
    assertEquals(keyData.getMetadata().size(), fromGetKeyData.getMetadata()
        .size());

  }


  @Test
  public void testDeleteKey() throws Exception {
    try {
      assertEquals(0, keyValueContainer.getContainerData().getKeyCount());
      //Put Key
      keyManager.putKey(keyValueContainer, keyData);
      assertEquals(1, keyValueContainer.getContainerData().getKeyCount());
      //Delete Key
      keyManager.deleteKey(keyValueContainer, blockID);
      assertEquals(0, keyValueContainer.getContainerData().getKeyCount());
      try {
        keyManager.getKey(keyValueContainer, blockID);
        fail("testDeleteKey");
      } catch (StorageContainerException ex) {
        GenericTestUtils.assertExceptionContains("Unable to find the key", ex);
      }
    } catch (IOException ex) {
      fail("testDeleteKey failed");
    }
  }

  @Test
  public void testListKey() throws Exception {
    try {
      keyManager.putKey(keyValueContainer, keyData);
      List<KeyData> listKeyData = keyManager.listKey(
          keyValueContainer, 1, 10);
      assertNotNull(listKeyData);
      assertTrue(listKeyData.size() == 1);

      for (long i = 2; i <= 10; i++) {
        blockID = new BlockID(1L, i);
        keyData = new KeyData(blockID);
        keyData.addMetadata("VOLUME", "ozone");
        keyData.addMetadata("OWNER", "hdfs");
        List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
        ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
            .getLocalID(), 0), 0, 1024);
        chunkList.add(info.getProtoBufMessage());
        keyData.setChunks(chunkList);
        keyManager.putKey(keyValueContainer, keyData);
      }

      listKeyData = keyManager.listKey(
          keyValueContainer, 1, 10);
      assertNotNull(listKeyData);
      assertTrue(listKeyData.size() == 10);

    } catch (IOException ex) {
      fail("testListKey failed");
    }
  }

  @Test
  public void testGetNoSuchKey() throws Exception {
    try {
      keyData = new KeyData(new BlockID(1L, 2L));
      keyManager.getKey(keyValueContainer, new BlockID(1L, 2L));
      fail("testGetNoSuchKey failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Unable to find the key.", ex);
      assertEquals(ContainerProtos.Result.NO_SUCH_KEY, ex.getResult());
    }
  }
}
