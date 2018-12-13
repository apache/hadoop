/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientMetrics;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the metrics of XceiverClient.
 */
public class TestXceiverClientMetrics {
  // only for testing
  private volatile boolean breakFlag;
  private CountDownLatch latch;

  private static OzoneConfiguration config;
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String containerOwner = "OZONE";

  @BeforeClass
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(config).build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @AfterClass
  public static void shutdown() {
    cluster.shutdown();
  }

  @Test
  public void testMetrics() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);

    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client = clientManager
        .acquireClient(container.getPipeline());

    ContainerCommandRequestProto request = ContainerTestHelper
        .getCreateContainerRequest(
            container.getContainerInfo().getContainerID(),
            container.getPipeline());
    client.sendCommand(request);

    MetricsRecordBuilder containerMetrics = getMetrics(
        XceiverClientMetrics.SOURCE_NAME);
    // Above request command is in a synchronous way, so there will be no
    // pending requests.
    assertCounter("PendingOps", 0L, containerMetrics);
    assertCounter("numPendingCreateContainer", 0L, containerMetrics);
    // the counter value of average latency metric should be increased
    assertCounter("CreateContainerLatencyNumOps", 1L, containerMetrics);

    breakFlag = false;
    latch = new CountDownLatch(1);

    int numRequest = 10;
    List<CompletableFuture<ContainerCommandResponseProto>> computeResults
        = new ArrayList<>();
    // start new thread to send async requests
    Thread sendThread = new Thread(() -> {
      while (!breakFlag) {
        try {
          // use async interface for testing pending metrics
          for (int i = 0; i < numRequest; i++) {
            BlockID blockID = ContainerTestHelper.
                getTestBlockID(container.getContainerInfo().getContainerID());
            ContainerProtos.ContainerCommandRequestProto smallFileRequest;

            smallFileRequest = ContainerTestHelper.getWriteSmallFileRequest(
                client.getPipeline(), blockID, 1024);
            CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
                response =
                client.sendCommandAsync(smallFileRequest).getResponse();
            computeResults.add(response);
          }

          Thread.sleep(1000);
        } catch (Exception ignored) {
        }
      }

      latch.countDown();
    });
    sendThread.start();

    GenericTestUtils.waitFor(() -> {
      // check if pending metric count is increased
      MetricsRecordBuilder metric =
          getMetrics(XceiverClientMetrics.SOURCE_NAME);
      long pendingOps = getLongCounter("PendingOps", metric);
      long pendingPutSmallFileOps =
          getLongCounter("numPendingPutSmallFile", metric);

      if (pendingOps > 0 && pendingPutSmallFileOps > 0) {
        // reset break flag
        breakFlag = true;
        return true;
      } else {
        return false;
      }
    }, 100, 60000);

    // blocking until we stop sending async requests
    latch.await();
    // Wait for all futures being done.
    GenericTestUtils.waitFor(() -> {
      for (CompletableFuture future : computeResults) {
        if (!future.isDone()) {
          return false;
        }
      }

      return true;
    }, 100, 60000);

    // the counter value of pending metrics should be decreased to 0
    containerMetrics = getMetrics(XceiverClientMetrics.SOURCE_NAME);
    assertCounter("PendingOps", 0L, containerMetrics);
    assertCounter("numPendingPutSmallFile", 0L, containerMetrics);

    clientManager.close();
  }
}
