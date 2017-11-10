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

package org.apache.hadoop.ozone.container.metrics;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.XceiverClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for metrics published by storage containers.
 */
public class TestContainerMetrics {

  @Test
  public void testContainerMetrics() throws Exception {
    XceiverServer server = null;
    XceiverClient client = null;
    String containerName = OzoneUtils.getRequestID();
    String keyName = OzoneUtils.getRequestID();

    try {
      final int interval = 1;
      Pipeline pipeline = ContainerTestHelper
          .createSingleNodePipeline(containerName);
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getLeader().getContainerPort());
      conf.setInt(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY,
          interval);

      // Since we are only testing for container metrics and we can just
      // mock the ContainerManager and ChunkManager instances instead of
      // starting the whole cluster.
      ContainerManager containerManager = mock(ContainerManager.class);
      ChunkManager chunkManager = mock(ChunkManager.class);
      Mockito.doNothing().when(chunkManager).writeChunk(
          Mockito.any(Pipeline.class), Mockito.anyString(),
          Mockito.any(ChunkInfo.class), Mockito.any(byte[].class));

      Mockito.doReturn(chunkManager).when(containerManager).getChunkManager();
      Mockito.doReturn(true).when(containerManager).isOpen(containerName);

      Dispatcher dispatcher = new Dispatcher(containerManager, conf);
      dispatcher.init();
      server = new XceiverServer(conf, dispatcher);
      client = new XceiverClient(pipeline, conf);

      server.start();
      client.connect();

      // Create container
      ContainerCommandRequestProto request = ContainerTestHelper
          .getCreateContainerRequest(containerName, pipeline);
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      // Write Chunk
      ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
          ContainerTestHelper.getWriteChunkRequest(
              pipeline, containerName, keyName, 1024);
      response = client.sendCommand(writeChunkRequest);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      MetricsRecordBuilder containerMetrics = getMetrics(
          "StorageContainerMetrics");
      assertCounter("NumOps", 2L, containerMetrics);
      assertCounter("numCreateContainer", 1L, containerMetrics);
      assertCounter("numWriteChunk", 1L, containerMetrics);
      assertCounter("bytesWriteChunk", 1024L, containerMetrics);
      assertCounter("LatencyWriteChunkNumOps", 1L, containerMetrics);

      String sec = interval + "s";
      Thread.sleep((interval + 1) * 1000);
      assertQuantileGauges("WriteChunkNanos" + sec, containerMetrics);
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
    }
  }
}