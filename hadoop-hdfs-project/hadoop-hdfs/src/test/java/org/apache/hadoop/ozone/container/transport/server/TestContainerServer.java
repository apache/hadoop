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

package org.apache.hadoop.ozone.container.transport.server;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.impl.Dispatcher;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerHandler;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class TestContainerServer {

  @Test
  public void testPipeline() throws IOException {
    EmbeddedChannel channel = null;
    try {
      channel = new EmbeddedChannel(new XceiverServerHandler(
          new TestContainerDispatcher()));
      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest();
      channel.writeInbound(request);
      Assert.assertTrue(channel.finish());
      ContainerCommandResponseProto response = channel.readOutbound();
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
    } finally {
      if (channel != null) {
        channel.close();
      }
    }
  }

  @Test
  public void testClientServer() throws Exception {
    XceiverServer server = null;
    XceiverClient client = null;

    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
          pipeline.getLeader().getContainerPort());

      server = new XceiverServer(conf, new TestContainerDispatcher());
      client = new XceiverClient(pipeline, conf);

      server.start();
      client.connect();

      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest();
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  public void testClientServerWithContainerDispatcher() throws Exception {
    XceiverServer server = null;
    XceiverClient client = null;

    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
          pipeline.getLeader().getContainerPort());

      server = new XceiverServer(conf, new Dispatcher(
          mock(ContainerManager.class)));
      client = new XceiverClient(pipeline, conf);

      server.start();
      client.connect();

      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest();
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
      Assert.assertEquals(response.getResult(), ContainerProtos.Result.SUCCESS);
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
    }
  }

  private class TestContainerDispatcher implements ContainerDispatcher {
    /**
     * Dispatches commands to container layer.
     *
     * @param msg - Command Request
     * @return Command Response
     * @throws IOException
     */
    @Override
    public ContainerCommandResponseProto
    dispatch(ContainerCommandRequestProto msg) throws IOException {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }
  }
}