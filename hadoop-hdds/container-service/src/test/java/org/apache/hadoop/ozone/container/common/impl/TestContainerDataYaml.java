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

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests create/read .container files.
 */
public class TestContainerDataYaml {

  private static final int MAXSIZE = 5;
  @Test
  public void testCreateContainerFile() throws IOException {
    String path = new FileSystemTestHelper().getTestRootDir();
    String containerPath = "1.container";

    File filePath = new File(new FileSystemTestHelper().getTestRootDir());
    filePath.mkdirs();

    KeyValueContainerData keyValueContainerData = new KeyValueContainerData(
        Long.MAX_VALUE, MAXSIZE);
    keyValueContainerData.setContainerDBType("RocksDB");
    keyValueContainerData.setMetadataPath(path);
    keyValueContainerData.setChunksPath(path);

    File containerFile = new File(filePath, containerPath);

    // Create .container file with ContainerData
    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
            .KeyValueContainer, containerFile, keyValueContainerData);

    //Check .container file exists or not.
    assertTrue(containerFile.exists());

    // Read from .container file, and verify data.
    KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(Long.MAX_VALUE, kvData.getContainerId());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals("RocksDB", kvData.getContainerDBType());
    assertEquals(path, kvData.getMetadataPath());
    assertEquals(path, kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerLifeCycleState.OPEN, kvData
        .getState());
    assertEquals(1, kvData.getLayOutVersion());
    assertEquals(0, kvData.getMetadata().size());
    assertEquals(MAXSIZE, kvData.getMaxSizeGB());

    // Update ContainerData.
    kvData.addMetadata("VOLUME", "hdfs");
    kvData.addMetadata("OWNER", "ozone");
    kvData.setState(ContainerProtos.ContainerLifeCycleState.CLOSED);


    // Update .container file with new ContainerData.
    containerFile = new File(filePath, containerPath);
    ContainerDataYaml.createContainerFile(ContainerProtos.ContainerType
            .KeyValueContainer, containerFile, kvData);

    // Reading newly updated data from .container file
    kvData =  (KeyValueContainerData) ContainerDataYaml.readContainerFile(
        containerFile);

    // verify data.
    assertEquals(Long.MAX_VALUE, kvData.getContainerId());
    assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
        .getContainerType());
    assertEquals("RocksDB", kvData.getContainerDBType());
    assertEquals(path, kvData.getMetadataPath());
    assertEquals(path, kvData.getChunksPath());
    assertEquals(ContainerProtos.ContainerLifeCycleState.CLOSED, kvData
        .getState());
    assertEquals(1, kvData.getLayOutVersion());
    assertEquals(2, kvData.getMetadata().size());
    assertEquals("hdfs", kvData.getMetadata().get("VOLUME"));
    assertEquals("ozone", kvData.getMetadata().get("OWNER"));
    assertEquals(MAXSIZE, kvData.getMaxSizeGB());

    FileUtil.fullyDelete(filePath);


  }

  @Test
  public void testIncorrectContainerFile() throws IOException{
    try {
      String path = "incorrect.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(path).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);
      fail("testIncorrectContainerFile failed");
    } catch (IllegalStateException ex) {
      GenericTestUtils.assertExceptionContains("Unexpected " +
          "ContainerLifeCycleState", ex);
    }
  }


  @Test
  public void testCheckBackWardCompatabilityOfContainerFile() throws
      IOException {
    // This test is for if we upgrade, and then .container files added by new
    // server will have new fields added to .container file, after a while we
    // decided to rollback. Then older ozone can read .container files
    // created or not.

    try {
      String path = "additionalfields.container";
      //Get file from resources folder
      ClassLoader classLoader = getClass().getClassLoader();
      File file = new File(classLoader.getResource(path).getFile());
      KeyValueContainerData kvData = (KeyValueContainerData) ContainerDataYaml
          .readContainerFile(file);

      //Checking the Container file data is consistent or not
      assertEquals(ContainerProtos.ContainerLifeCycleState.CLOSED, kvData
          .getState());
      assertEquals("RocksDB", kvData.getContainerDBType());
      assertEquals(ContainerProtos.ContainerType.KeyValueContainer, kvData
          .getContainerType());
      assertEquals(9223372036854775807L, kvData.getContainerId());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerdir0/1", kvData
          .getChunksPath());
      assertEquals("/hdds/current/aed-fg4-hji-jkl/containerdir0/1", kvData
          .getMetadataPath());
      assertEquals(1, kvData.getLayOutVersion());
      assertEquals(2, kvData.getMetadata().size());

    } catch (Exception ex) {
      fail("testCheckBackWardCompatabilityOfContainerFile failed");
    }
  }


}
