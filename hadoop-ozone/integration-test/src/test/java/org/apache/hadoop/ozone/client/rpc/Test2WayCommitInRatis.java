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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;

/**
 * This class tests the 2 way commit in Ratis.
 */
public class Test2WayCommitInRatis {

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String containerOwner = "OZONE";

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  private void startCluster(OzoneConfiguration conf) throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;

    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY,
        1, TimeUnit.SECONDS);

    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(7)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "watchforcommithandlingtest";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }


  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void test2WayCommitForRetryfailure() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_WATCH_REQUEST_TIMEOUT, 20,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 20);
    startCluster(conf);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(XceiverClientRatis.LOG);
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, containerOwner);
    XceiverClientSpi xceiverClient = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, xceiverClient.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        xceiverClient.getPipeline());
    Pipeline pipeline = xceiverClient.getPipeline();
    XceiverClientRatis ratisClient = (XceiverClientRatis) xceiverClient;
    XceiverClientReply reply = xceiverClient.sendCommandAsync(
        ContainerTestHelper.getCreateContainerRequest(
            container1.getContainerInfo().getContainerID(),
            xceiverClient.getPipeline()));
    reply.getResponse().get();
    Assert.assertEquals(3, ratisClient.getCommitInfoMap().size());
    cluster.shutdownHddsDatanode(pipeline.getNodes().get(0));
    reply = xceiverClient.sendCommandAsync(ContainerTestHelper
        .getCloseContainer(pipeline,
            container1.getContainerInfo().getContainerID()));
    reply.getResponse().get();
    xceiverClient.watchForCommit(reply.getLogIndex(), 20000);

    // commitInfo Map will be reduced to 2 here
    Assert.assertEquals(2, ratisClient.getCommitInfoMap().size());
    clientManager.releaseClient(xceiverClient, false);
    Assert.assertTrue(logCapturer.getOutput().contains("3 way commit failed"));
    Assert
        .assertTrue(logCapturer.getOutput().contains("Committed by majority"));
    logCapturer.stopCapturing();
    shutdown();
  }
}
