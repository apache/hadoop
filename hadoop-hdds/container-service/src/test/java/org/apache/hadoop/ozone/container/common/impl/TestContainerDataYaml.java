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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests create/read .container files.
 */
public class TestContainerDataYaml {

  private static long testContainerID = 1234;

  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  private static final long MAXSIZE = (long) StorageUnit.GB.toBytes(5);

  /**
   * Creates a .container file. cleanup() should be called at the end of the
   * test when container file is created.
   */
  private File createContainerFile(long containerID) throws IOException {
    new File(testRoot).mkdirs();

    String containerPath = containerID + ".container";

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(
        containerID, MAXSIZE, UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    keyValueContainerData.setContainerDBType("RocksDB");
    keyValueContainerData.setMetadataPath(testRoot);
    keyValueContainerData.setChunksPath(testRoot);

    File containerFile = new File(testRoot, containerPath);

    // Create .container file with ContainerData
    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
        .KeyValueContainer, keyValueContainerData, containerFile);

    //Check .container file exists or not.
    assertTrue(containerFile.exists());

    return containerFile;
  }

  private void cleanup() {
    FileUtil.fullyDelete(new File(testRoot));
  }

  @Test
  public void testCreateContainerFile() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID);

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(containerID, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals("RocksDB", kvData.getContainerDBType());
    assertEquals(containerFile.getParent(), kvData.getMetadataPath());
    assertEquals(containerFile.getParent(), kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerDataProto.State.OPEN, kvData
        .getState());
    assertEquals(1, kvData.getLayOutVersion());
    assertEquals(0, kvData.getMetadata().size());
    assertEquals(MAXSIZE, kvData.getMaxSize());

    // Update ContainerData.
    kvData.addMetadata("VOLUME", "hdfs");
    kvData.addMetadata("OWNER", "ozone");
    kvData.setState(ContainerProtos.ContainerDataProto.State.CLOSED);


    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
            .KeyValueContainer, kvData, containerFile);

    // Reading newly updated data from .container file
    kvData =  (KeyValueContainerData) ContainerDataYaml.readContainerFile(
        containerFile);

    // verify data.
    assertEquals(containerID, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals("RocksDB", kvData.getContainerDBType());
    assertEquals(containerFile.getParent(), kvData.getMetadataPath());
    assertEquals(containerFile.getParent(), kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, kvData
        .getState());
    assertEquals(1, kvData.getLayOutVersion());
    assertEquals(2, kvData.getMetadata().size());
    assertEquals("hdfs", kvData.getMetadata().get("VOLUME"));
    assertEquals("ozone", kvData.getMetadata().get("OWNER"));
    assertEquals(MAXSIZE, kvData.getMaxSize());
  }

  @Test
  public void testIncorrectContainerFile() throws IOException{
    try {
      String containerFile = "incorrect.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(containerFile).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      fail("testIncorrectContainerFile failed");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("No enum constant", ex);
    }
  }


  @Test
  public void testCheckBackWardCompatibilityOfContainerFile() throws
      IOException {
    // This test is for if we upgrade, and then .container files added by new
    // server will have new fields added to .container file, after a while we
    // decided to rollback. Then older ozone can read .container files
    // created or not.

    try {
      String containerFile = "additionalfields.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(containerFile).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      ContainerUtils.verifyChecksum(kvData);

      //Checking the Container file data is consistent or not
      assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, kvData
          .getState());
      assertEquals("RocksDB", kvData.getContainerDBType());
      assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
          .getContainerType());
      assertEquals(9223372036854775807L, kvData.getContainerID());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerDir0/1", kvData
          .getChunksPath());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerDir0/1", kvData
          .getMetadataPath());
      assertEquals(1, kvData.getLayOutVersion());
      assertEquals(2, kvData.getMetadata().size());

    } catch (Exception ex) {
      ex.printStackTrace();
      fail("testCheckBackWardCompatibilityOfContainerFile failed");
    }
  }

  /**
   * Test to verify {@link ContainerUtils#verifyChecksum(ContainerData)}.
   */
  @Test
  public void testChecksumInContainerFile() throws IOException {
    long containerID = testContainerID++;

    File containerFile = createContainerFile(containerID);

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    ContainerUtils.verifyChecksum(kvData);

    cleanup();
  }

  /**
   * Test to verify incorrect checksum is detected.
   */
  @Test
  public void testIncorrectChecksum() {
    try {
      String containerFile = "incorrect.checksum.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(containerFile).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      ContainerUtils.verifyChecksum(kvData);
      fail("testIncorrectChecksum failed");
    } catch (Exception ex) {
      GenericTestUtils.assertExceptionContains("Container checksum error for " +
          "ContainerID:", ex);
    }
  }
}
