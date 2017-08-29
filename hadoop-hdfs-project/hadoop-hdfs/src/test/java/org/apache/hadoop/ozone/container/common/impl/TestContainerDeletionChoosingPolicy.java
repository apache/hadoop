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
package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.ozone.container.ContainerTestHelper.createSingleNodePipeline;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The class for testing container deletion choosing policy.
 */
public class TestContainerDeletionChoosingPolicy {
  private static String path;
  private static ContainerManagerImpl containerManager;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void init() throws Throwable {
    conf = new OzoneConfiguration();
    path = GenericTestUtils
        .getTempPath(TestContainerDeletionChoosingPolicy.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testRandomChoosingPolicy() throws IOException {
    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }
    Assert.assertTrue(containerDir.mkdirs());

    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_DELETION_CHOOSING_POLICY,
        RandomContainerDeletionChoosingPolicy.class.getName());
    List<StorageLocation> pathLists = new LinkedList<>();
    pathLists.add(StorageLocation.parse(containerDir.getAbsolutePath()));
    containerManager = new ContainerManagerImpl();
    containerManager.init(conf, pathLists);

    int numContainers = 10;
    for (int i = 0; i < numContainers; i++) {
      String containerName = OzoneUtils.getRequestID();
      ContainerData data = new ContainerData(containerName);
      containerManager.createContainer(createSingleNodePipeline(containerName),
          data);
      Assert.assertTrue(
          containerManager.getContainerMap().containsKey(containerName));
    }

    List<ContainerData> result0 = containerManager
        .chooseContainerForBlockDeletion(5);
    Assert.assertEquals(5, result0.size());

    // test random choosing
    List<ContainerData> result1 = containerManager
        .chooseContainerForBlockDeletion(numContainers);
    List<ContainerData> result2 = containerManager
        .chooseContainerForBlockDeletion(numContainers);

    boolean hasShuffled = false;
    for (int i = 0; i < numContainers; i++) {
      if (!result1.get(i).getContainerName()
          .equals(result2.get(i).getContainerName())) {
        hasShuffled = true;
        break;
      }
    }
    Assert.assertTrue("Chosen container results were same", hasShuffled);
  }
}
