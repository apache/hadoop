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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.container.ContainerTestHelper
    .createSingleNodePipeline;
import static org.junit.Assert.fail;

/**
 * Simple tests to verify that container persistence works as expected.
 */
public class TestContainerPersistence {

  static String path;
  static ContainerManagerImpl containerManager;
  static OzoneConfiguration conf;
  static FsDatasetSpi fsDataSet;
  static MiniDFSCluster cluster;
  static List<Path> pathLists = new LinkedList<>();

  @BeforeClass
  public static void init() throws IOException {
    conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    path = p.getPath().concat(
        TestContainerPersistence.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");

    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }

    Assert.assertTrue(containerDir.mkdirs());

    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fsDataSet = cluster.getDataNodes().get(0).getFSDataset();
    containerManager = new ContainerManagerImpl();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    cluster.shutdown();
    FileUtils.deleteDirectory(new File(path));
  }

  @Before
  public void setupPaths() throws IOException {
    if (!new File(path).exists()) {
      new File(path).mkdirs();
    }
    pathLists.clear();
    containerManager.getContainerMap().clear();
    pathLists.add(Paths.get(path));
    containerManager.init(conf, pathLists, fsDataSet);
  }

  @After
  public void cleanupDir() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testCreateContainer() throws Exception {

    String containerName = OzoneUtils.getRequestID();
    ContainerData data = new ContainerData(containerName);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(), data);
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName));
    ContainerManagerImpl.ContainerStatus status = containerManager
        .getContainerMap().get(containerName);

    Assert.assertTrue(status.isActive());
    Assert.assertNotNull(status.getContainer().getContainerPath());
    Assert.assertNotNull(status.getContainer().getDBPath());


    Assert.assertTrue(new File(status.getContainer().getContainerPath())
        .exists());

    String containerPathString = ContainerUtils.getContainerNameFromFile(new
        File(status.getContainer().getContainerPath()));

    Path meta = Paths.get(containerPathString);

    String metadataFile = meta.toString() + OzoneConsts.CONTAINER_META;
    Assert.assertTrue(new File(metadataFile).exists());


    String dbPath = status.getContainer().getDBPath();

    LevelDBStore store = null;
    try {
      store = new LevelDBStore(new File(dbPath), false);
      Assert.assertNotNull(store.getDB());
    } finally {
      if (store != null) {
        store.close();
      }
    }
  }

  @Test
  public void testCreateDuplicateContainer() throws Exception {
    String containerName = OzoneUtils.getRequestID();

    ContainerData data = new ContainerData(containerName);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(), data);
    try {
      containerManager.createContainer(createSingleNodePipeline(), data);
      fail("Expected Exception not thrown.");
    } catch (IOException ex) {
      Assert.assertNotNull(ex);
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    String containerName1 = OzoneUtils.getRequestID();
    String containerName2 = OzoneUtils.getRequestID();


    ContainerData data = new ContainerData(containerName1);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(), data);

    data = new ContainerData(containerName2);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(), data);


    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

    containerManager.deleteContainer(createSingleNodePipeline(),
        containerName1);
    Assert.assertFalse(containerManager.getContainerMap()
        .containsKey(containerName1));

    // Let us make sure that we are able to re-use a container name after
    // delete.

    data = new ContainerData(containerName1);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner)", "bilbo");
    containerManager.createContainer(createSingleNodePipeline(), data);

    // Assert we still have both containers.
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName1));
    Assert.assertTrue(containerManager.getContainerMap()
        .containsKey(containerName2));

  }

  /**
   * This test creates 1000 containers and reads them back 5 containers at a
   * time and verifies that we did get back all containers.
   *
   * @throws IOException
   */
  @Test
  public void testListContainer() throws IOException {
    final int count = 1000;
    final int step = 5;

    Map<String, ContainerData> testMap = new HashMap<>();
    for (int x = 0; x < count; x++) {
      String containerName = OzoneUtils.getRequestID();

      ContainerData data = new ContainerData(containerName);
      data.addMetadata("VOLUME", "shire");
      data.addMetadata("owner)", "bilbo");
      containerManager.createContainer(createSingleNodePipeline(), data);
      testMap.put(containerName, data);
    }

    int counter = 0;
    String prevKey = "";
    List<ContainerData> results = new LinkedList<>();
    while (counter < count) {
      containerManager.listContainer(prevKey, step, results);
      for (int y = 0; y < results.size(); y++) {
        testMap.remove(results.get(y).getContainerName());
      }
      counter += step;
      String nextKey = results.get(results.size() - 1).getContainerName();

      //Assert that container is returning results in a sorted fashion.
      Assert.assertTrue(prevKey.compareTo(nextKey) < 0);
      prevKey = nextKey;
      results.clear();
    }
    // Assert that we listed all the keys that we had put into
    // container.
    Assert.assertTrue(testMap.isEmpty());

  }
}
