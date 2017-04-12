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
package org.apache.hadoop.cblock;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.cblock.util.MockStorageClient;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the basics of CBlock server. Mainly about the four
 * operations on volumes: create, delete, info and list.
 */
public class TestCBlockServer {
  private static CBlockManager cBlockManager;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setup() throws Exception {
    ScmClient storageClient = new MockStorageClient();
    conf = new OzoneConfiguration();
    cBlockManager = new CBlockManager(conf, storageClient);
    cBlockManager.start();
  }

  @AfterClass
  public static void clean() {
    cBlockManager.stop();
    cBlockManager.join();
    cBlockManager.clean();
  }

  /**
   * Test create volume for different users.
   * @throws Exception
   */
  @Test
  public void testCreateVolume() throws Exception {
    String userName1 = "user" + RandomStringUtils.randomNumeric(5);
    String userName2 = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName1 = "volume" +  RandomStringUtils.randomNumeric(5);
    String volumeName2 = "volume" +   RandomStringUtils.randomNumeric(5);
    long volumeSize = 1L*1024*1024;
    int blockSize = 4096;
    cBlockManager.createVolume(userName1, volumeName1, volumeSize, blockSize);
    List<VolumeInfo> volumes = cBlockManager.listVolume(userName1);
    assertEquals(1, volumes.size());
    VolumeInfo existingVolume = volumes.get(0);
    assertEquals(userName1, existingVolume.getUserName());
    assertEquals(volumeName1, existingVolume.getVolumeName());
    assertEquals(volumeSize, existingVolume.getVolumeSize());
    assertEquals(blockSize, existingVolume.getBlockSize());

    cBlockManager.createVolume(userName1, volumeName2, volumeSize, blockSize);
    cBlockManager.createVolume(userName2, volumeName1, volumeSize, blockSize);
    volumes = cBlockManager.listVolume(userName1);
    assertEquals(2, volumes.size());
    volumes = cBlockManager.listVolume(userName2);
    assertEquals(1, volumes.size());
  }

  /**
   * Test delete volume.
   * @throws Exception
   */
  @Test
  public void testDeleteVolume() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName1 = "volume" +  RandomStringUtils.randomNumeric(5);
    String volumeName2 = "volume" +  RandomStringUtils.randomNumeric(5);
    long volumeSize = 1L*1024*1024;
    int blockSize = 4096;
    cBlockManager.createVolume(userName, volumeName1, volumeSize, blockSize);
    cBlockManager.createVolume(userName, volumeName2, volumeSize, blockSize);
    cBlockManager.deleteVolume(userName, volumeName1, true);
    List<VolumeInfo> volumes = cBlockManager.listVolume(userName);
    assertEquals(1, volumes.size());

    VolumeInfo existingVolume = volumes.get(0);
    assertEquals(userName, existingVolume.getUserName());
    assertEquals(volumeName2, existingVolume.getVolumeName());
    assertEquals(volumeSize, existingVolume.getVolumeSize());
    assertEquals(blockSize, existingVolume.getBlockSize());
  }

  /**
   * Test info volume.
   *
   * TODO : usage field is not being tested (as it is not implemented yet)
   * @throws Exception
   */
  @Test
  public void testInfoVolume() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" +  RandomStringUtils.randomNumeric(5);
    long volumeSize = 1L*1024*1024;
    int blockSize = 4096;
    cBlockManager.createVolume(userName, volumeName, volumeSize, blockSize);
    VolumeInfo info = cBlockManager.infoVolume(userName, volumeName);
    assertEquals(userName, info.getUserName());
    assertEquals(volumeName, info.getVolumeName());
    assertEquals(volumeSize, info.getVolumeSize());
    assertEquals(blockSize, info.getBlockSize());
  }

  /**
   * Test listing a number of volumes.
   * @throws Exception
   */
  @Test
  public void testListVolume() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName ="volume" +  RandomStringUtils.randomNumeric(5);
    long volumeSize = 1L*1024*1024;
    int blockSize = 4096;
    int volumeNum = 100;
    for (int i = 0; i<volumeNum; i++) {
      cBlockManager.createVolume(userName, volumeName + i,
          volumeSize, blockSize);
    }
    List<VolumeInfo> volumes = cBlockManager.listVolume(userName);
    assertEquals(volumeNum, volumes.size());
    HashSet<String> volumeIds = new HashSet<>();
    for (int i = 0; i<volumeNum; i++) {
      VolumeInfo volumeInfo = volumes.get(i);
      assertEquals(userName, volumeInfo.getUserName());
      assertFalse(volumeIds.contains(volumeName + i));
      volumeIds.add(volumeName + i);
      assertEquals(volumeSize, volumeInfo.getVolumeSize());
      assertEquals(blockSize, volumeInfo.getBlockSize());
    }
    for (int i = 0; i<volumeNum; i++) {
      assertTrue(volumeIds.contains(volumeName + i));
    }
  }
}
