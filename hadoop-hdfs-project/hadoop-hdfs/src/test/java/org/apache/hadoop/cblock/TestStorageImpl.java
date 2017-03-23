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

import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.cblock.jscsiHelper.CBlockIStorageImpl;
import org.apache.hadoop.cblock.jscsiHelper.CBlockTargetMetrics;
import org.apache.hadoop.cblock.jscsiHelper.ContainerCacheFlusher;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_DISK_CACHE_PATH_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys.DFS_CBLOCK_TRACE_IO;

/**
 * This class tests the cblock storage layer.
 */
public class TestStorageImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStorageImpl.class);
  private final static long GB = 1024 * 1024 * 1024;
  private final static int KB = 1024;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration config;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;

  @BeforeClass
  public static void init() throws IOException {
    config = new OzoneConfiguration();
    URL p = config.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    config.set(DFS_CBLOCK_DISK_CACHE_PATH_KEY, path);
    config.setBoolean(DFS_CBLOCK_TRACE_IO, true);
    cluster = new MiniOzoneCluster.Builder(config)
        .numDataNodes(1).setHandlerType("distributed").build();
    storageContainerLocationClient = cluster
        .createStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(config);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

  /**
   * getContainerPipelines creates a set of containers and returns the
   * Pipelines that define those containers.
   *
   * @param count - Number of containers to create.
   * @return - List of Pipelines.
   * @throws IOException
   */
  private List<Pipeline> getContainerPipeline(int count) throws IOException {
    List<Pipeline> containerPipelines = new LinkedList<>();
    for (int x = 0; x < count; x++) {
      String traceID = "trace" + RandomStringUtils.randomNumeric(4);
      String containerName = "container" + RandomStringUtils.randomNumeric(10);
      Pipeline pipeline =
          storageContainerLocationClient.allocateContainer(containerName);
      XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
      ContainerProtocolCalls.createContainer(client, traceID);
      // This step is needed since we set private data on pipelines, when we
      // read the list from CBlockServer. So we mimic that action here.
      pipeline.setData(Longs.toByteArray(x));
      containerPipelines.add(pipeline);
    }
    return containerPipelines;
  }

  @Test
  public void testStorageImplBasicReadWrite() throws Exception {
    OzoneConfiguration oConfig = new OzoneConfiguration();
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    long volumeSize = 50L * (1024L * 1024L * 1024L);
    int blockSize = 4096;
    byte[] data =
        RandomStringUtils.randomAlphanumeric(10 * (1024 * 1024))
            .getBytes(StandardCharsets.UTF_8);
    String hash = DigestUtils.sha256Hex(data);
    CBlockTargetMetrics metrics = CBlockTargetMetrics.create();
    ContainerCacheFlusher flusher = new ContainerCacheFlusher(oConfig,
        xceiverClientManager, metrics);
    CBlockIStorageImpl ozoneStore = CBlockIStorageImpl.newBuilder()
        .setUserName(userName)
        .setVolumeName(volumeName)
        .setVolumeSize(volumeSize)
        .setBlockSize(blockSize)
        .setContainerList(getContainerPipeline(10))
        .setClientManager(xceiverClientManager)
        .setConf(oConfig)
        .setFlusher(flusher)
        .setCBlockTargetMetrics(metrics)
        .build();
    ozoneStore.write(data, 0);

    // Currently, local cache is a placeholder and does not actually handle
    // read and write. So the below write is guaranteed to fail. After
    // CBlockLocalCache is properly implemented, we should uncomment the
    // following lines
    // TODO uncomment the following.

    //byte[] newData = new byte[10 * 1024 * 1024];
    //ozoneStore.read(newData, 0);
    //String newHash = DigestUtils.sha256Hex(newData);
    //Assert.assertEquals("hashes don't match.", hash, newHash);
    GenericTestUtils.waitFor(() -> !ozoneStore.getCache().isDirtyCache(),
        100, 20 *
            1000);

    ozoneStore.close();
  }
}
