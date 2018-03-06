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

package org.apache.hadoop.ozone.container.common;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.TestUtils.BlockDeletingServiceTestImpl;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.impl.RandomContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.statemachine.background.BlockDeletingService;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.apache.hadoop.ozone.container
    .ContainerTestHelper.createSingleNodePipeline;

/**
 * Tests to test block deleting service.
 */
public class TestBlockDeletingService {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBlockDeletingService.class);

  private static File testRoot;
  private static File containersDir;
  private static File chunksDir;

  @BeforeClass
  public static void init() {
    testRoot = GenericTestUtils
        .getTestDir(TestBlockDeletingService.class.getSimpleName());
    chunksDir = new File(testRoot, "chunks");
    containersDir = new File(testRoot, "containers");
  }

  @Before
  public void setup() throws IOException {
    if (chunksDir.exists()) {
      FileUtils.deleteDirectory(chunksDir);
    }
  }

  @After
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(chunksDir);
    FileUtils.deleteDirectory(containersDir);
    FileUtils.deleteDirectory(testRoot);
  }

  private ContainerManager createContainerManager(Configuration conf)
      throws Exception {
    // use random container choosing policy for testing
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_DELETION_CHOOSING_POLICY,
        RandomContainerDeletionChoosingPolicy.class.getName());
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        containersDir.getAbsolutePath());
    if (containersDir.exists()) {
      FileUtils.deleteDirectory(containersDir);
    }
    ContainerManager containerManager = new ContainerManagerImpl();
    List<StorageLocation> pathLists = new LinkedList<>();
    pathLists.add(StorageLocation.parse(containersDir.getAbsolutePath()));
    containerManager.init(conf, pathLists, DFSTestUtil.getLocalDatanodeID());
    return containerManager;
  }

  /**
   * A helper method to create some blocks and put them under deletion
   * state for testing. This method directly updates container.db and
   * creates some fake chunk files for testing.
   */
  private void createToDeleteBlocks(ContainerManager mgr,
      Configuration conf, int numOfContainers, int numOfBlocksPerContainer,
      int numOfChunksPerBlock, File chunkDir) throws IOException {
    for (int x = 0; x < numOfContainers; x++) {
      String containerName = OzoneUtils.getRequestID();
      ContainerData data = new ContainerData(containerName, new Long(x), conf);
      mgr.createContainer(createSingleNodePipeline(containerName), data);
      data = mgr.readContainer(containerName);
      MetadataStore metadata = KeyUtils.getDB(data, conf);
      for (int j = 0; j<numOfBlocksPerContainer; j++) {
        String blockName = containerName + "b" + j;
        String deleteStateName = OzoneConsts.DELETING_KEY_PREFIX + blockName;
        KeyData kd = new KeyData(containerName, deleteStateName);
        List<ContainerProtos.ChunkInfo> chunks = Lists.newArrayList();
        for (int k = 0; k<numOfChunksPerBlock; k++) {
          // offset doesn't matter here
          String chunkName = blockName + "_chunk_" + k;
          File chunk = new File(chunkDir, chunkName);
          FileUtils.writeStringToFile(chunk, "a chunk");
          LOG.info("Creating file {}", chunk.getAbsolutePath());
          // make sure file exists
          Assert.assertTrue(chunk.isFile() && chunk.exists());
          ContainerProtos.ChunkInfo info =
              ContainerProtos.ChunkInfo.newBuilder()
                  .setChunkName(chunk.getAbsolutePath())
                  .setLen(0)
                  .setOffset(0)
                  .setChecksum("")
                  .build();
          chunks.add(info);
        }
        kd.setChunks(chunks);
        metadata.put(DFSUtil.string2Bytes(deleteStateName),
            kd.getProtoBufMessage().toByteArray());
      }
    }
  }

  /**
   *  Run service runDeletingTasks and wait for it's been processed.
   */
  private void deleteAndWait(BlockDeletingServiceTestImpl service,
      int timesOfProcessed) throws TimeoutException, InterruptedException {
    service.runDeletingTasks();
    GenericTestUtils.waitFor(()
        -> service.getTimesOfProcessed() == timesOfProcessed, 100, 3000);
  }

  /**
   * Get under deletion blocks count from DB,
   * note this info is parsed from container.db.
   */
  private int getUnderDeletionBlocksCount(MetadataStore meta)
      throws IOException {
    List<Map.Entry<byte[], byte[]>> underDeletionBlocks =
        meta.getRangeKVs(null, 100, new MetadataKeyFilters.KeyPrefixFilter(
            OzoneConsts.DELETING_KEY_PREFIX));
    return underDeletionBlocks.size();
  }

  @Test
  public void testBlockDeletion() throws Exception {
    Configuration conf = new OzoneConfiguration();
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 2);
    ContainerManager containerManager = createContainerManager(conf);
    createToDeleteBlocks(containerManager, conf, 1, 3, 1, chunksDir);

    BlockDeletingServiceTestImpl svc =
        new BlockDeletingServiceTestImpl(containerManager, 1000, conf);
    svc.start();
    GenericTestUtils.waitFor(() -> svc.isStarted(), 100, 3000);

    // Ensure 1 container was created
    List<ContainerData> containerData = Lists.newArrayList();
    containerManager.listContainer(null, 1, "", containerData);
    Assert.assertEquals(1, containerData.size());
    MetadataStore meta = KeyUtils.getDB(containerData.get(0), conf);

    // Ensure there is 100 blocks under deletion
    Assert.assertEquals(3, getUnderDeletionBlocksCount(meta));

    // An interval will delete 1 * 2 blocks
    deleteAndWait(svc, 1);
    Assert.assertEquals(1, getUnderDeletionBlocksCount(meta));

    deleteAndWait(svc, 2);
    Assert.assertEquals(0, getUnderDeletionBlocksCount(meta));

    deleteAndWait(svc, 3);
    Assert.assertEquals(0, getUnderDeletionBlocksCount(meta));

    svc.shutdown();
    shutdownContainerMangaer(containerManager);
  }

  @Test
  public void testShutdownService() throws Exception {
    Configuration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500,
        TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 10);
    ContainerManager containerManager = createContainerManager(conf);
    // Create 1 container with 100 blocks
    createToDeleteBlocks(containerManager, conf, 1, 100, 1, chunksDir);

    BlockDeletingServiceTestImpl service =
        new BlockDeletingServiceTestImpl(containerManager, 1000, conf);
    service.start();
    GenericTestUtils.waitFor(() -> service.isStarted(), 100, 3000);

    // Run some deleting tasks and verify there are threads running
    service.runDeletingTasks();
    GenericTestUtils.waitFor(() -> service.getThreadCount() > 0, 100, 1000);

    // Wait for 1 or 2 intervals
    Thread.sleep(1000);

    // Shutdown service and verify all threads are stopped
    service.shutdown();
    GenericTestUtils.waitFor(() -> service.getThreadCount() == 0, 100, 1000);
    shutdownContainerMangaer(containerManager);
  }

  @Test
  public void testBlockDeletionTimeout() throws Exception {
    Configuration conf = new OzoneConfiguration();
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 2);
    ContainerManager containerManager = createContainerManager(conf);
    createToDeleteBlocks(containerManager, conf, 1, 3, 1, chunksDir);

    // set timeout value as 1ms to trigger timeout behavior
    long timeout  = 1;
    BlockDeletingService svc =
        new BlockDeletingService(containerManager, 1000, timeout, conf);
    svc.start();

    LogCapturer log = LogCapturer.captureLogs(BackgroundService.LOG);
    GenericTestUtils.waitFor(() -> {
      if(log.getOutput().contains(
          "Background task executes timed out, retrying in next interval")) {
        log.stopCapturing();
        return true;
      }

      return false;
    }, 1000, 100000);

    log.stopCapturing();
    svc.shutdown();

    // test for normal case that doesn't have timeout limitation
    timeout  = 0;
    createToDeleteBlocks(containerManager, conf, 1, 3, 1, chunksDir);
    svc =  new BlockDeletingService(containerManager, 1000, timeout, conf);
    svc.start();

    // get container meta data
    List<ContainerData> containerData = Lists.newArrayList();
    containerManager.listContainer(null, 1, "", containerData);
    MetadataStore meta = KeyUtils.getDB(containerData.get(0), conf);

    LogCapturer newLog = LogCapturer.captureLogs(BackgroundService.LOG);
    GenericTestUtils.waitFor(() -> {
      try {
        if (getUnderDeletionBlocksCount(meta) == 0) {
          return true;
        }
      } catch (IOException ignored) {
      }
      return false;
    }, 1000, 100000);
    newLog.stopCapturing();

    // The block deleting successfully and shouldn't catch timed
    // out warning log.
    Assert.assertTrue(!newLog.getOutput().contains(
        "Background task executes timed out, retrying in next interval"));
    svc.shutdown();
    shutdownContainerMangaer(containerManager);
  }

  @Test(timeout = 30000)
  public void testContainerThrottle() throws Exception {
    // Properties :
    //  - Number of containers : 2
    //  - Number of blocks per container : 1
    //  - Number of chunks per block : 10
    //  - Container limit per interval : 1
    //  - Block limit per container : 1
    //
    // Each time only 1 container can be processed, so each time
    // 1 block from 1 container can be deleted.
    Configuration conf = new OzoneConfiguration();
    // Process 1 container per interval
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 1);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 1);
    ContainerManager containerManager = createContainerManager(conf);
    createToDeleteBlocks(containerManager, conf, 2, 1, 10, chunksDir);

    BlockDeletingServiceTestImpl service =
        new BlockDeletingServiceTestImpl(containerManager, 1000, conf);
    service.start();

    try {
      GenericTestUtils.waitFor(() -> service.isStarted(), 100, 3000);
      // 1st interval processes 1 container 1 block and 10 chunks
      deleteAndWait(service, 1);
      Assert.assertEquals(10, chunksDir.listFiles().length);
    } finally {
      service.shutdown();
      shutdownContainerMangaer(containerManager);
    }
  }


  @Test(timeout = 30000)
  public void testBlockThrottle() throws Exception {
    // Properties :
    //  - Number of containers : 5
    //  - Number of blocks per container : 3
    //  - Number of chunks per block : 1
    //  - Container limit per interval : 10
    //  - Block limit per container : 2
    //
    // Each time containers can be all scanned, but only 2 blocks
    // per container can be actually deleted. So it requires 2 waves
    // to cleanup all blocks.
    Configuration conf = new OzoneConfiguration();
    conf.setInt(OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL, 10);
    conf.setInt(OZONE_BLOCK_DELETING_LIMIT_PER_CONTAINER, 2);
    ContainerManager containerManager = createContainerManager(conf);
    createToDeleteBlocks(containerManager, conf, 5, 3, 1, chunksDir);

    // Make sure chunks are created
    Assert.assertEquals(15, chunksDir.listFiles().length);

    BlockDeletingServiceTestImpl service =
        new BlockDeletingServiceTestImpl(containerManager, 1000, conf);
    service.start();

    try {
      GenericTestUtils.waitFor(() -> service.isStarted(), 100, 3000);
      // Total blocks = 3 * 5 = 15
      // block per task = 2
      // number of containers = 5
      // each interval will at most runDeletingTasks 5 * 2 = 10 blocks
      deleteAndWait(service, 1);
      Assert.assertEquals(5, chunksDir.listFiles().length);

      // There is only 5 blocks left to runDeletingTasks
      deleteAndWait(service, 2);
      Assert.assertEquals(0, chunksDir.listFiles().length);
    } finally {
      service.shutdown();
      shutdownContainerMangaer(containerManager);
    }
  }

  private void shutdownContainerMangaer(ContainerManager mgr)
      throws IOException {
    mgr.writeLock();
    try {
      mgr.shutdown();
    } finally {
      mgr.writeUnlock();
    }
  }
}
