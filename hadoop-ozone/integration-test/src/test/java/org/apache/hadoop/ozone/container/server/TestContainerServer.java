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

package org.apache.hadoop.ozone.container.server;

import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.ratis.shaded.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;

import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServer;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerHandler;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.hdds.scm.XceiverClient;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.util.CheckedBiConsumer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.apache.ratis.rpc.SupportedRpcType.NETTY;
import static org.mockito.Mockito.mock;

/**
 * Test Containers.
 */
@Ignore("Takes too long to run this test. Ignoring for time being.")
public class TestContainerServer {
  static final String TEST_DIR
      = GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator;

  @Test
  public void testPipeline() throws IOException {
    EmbeddedChannel channel = null;
    String containerName = OzoneUtils.getRequestID();
    try {
      channel = new EmbeddedChannel(new XceiverServerHandler(
          new TestContainerDispatcher()));
      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest(
              ContainerTestHelper.getTestContainerID(),
              ContainerTestHelper.createSingleNodePipeline());
      channel.writeInbound(request);
      Assert.assertTrue(channel.finish());

      Object responseObject = channel.readOutbound();
      Assert.assertTrue(responseObject instanceof
          ContainerCommandResponseProto);
      ContainerCommandResponseProto  response =
          (ContainerCommandResponseProto) responseObject;
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
    } finally {
      if (channel != null) {
        channel.close();
      }
    }
  }

  @Test
  public void testClientServer() throws Exception {
    DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
    runTestClientServer(1,
        (pipeline, conf) -> conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
            pipeline.getLeader()
                .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue()),
        XceiverClient::new,
        (dn, conf) -> new XceiverServer(datanodeDetails, conf,
            new TestContainerDispatcher()),
        (dn, p) -> {});
  }

  @FunctionalInterface
  interface CheckedBiFunction<LEFT, RIGHT, OUT, THROWABLE extends Throwable> {
    OUT apply(LEFT left, RIGHT right) throws THROWABLE;
  }

  @Test
  public void testClientServerRatisNetty() throws Exception {
    runTestClientServerRatis(NETTY, 1);
    runTestClientServerRatis(NETTY, 3);
  }

  @Test
  public void testClientServerRatisGrpc() throws Exception {
    runTestClientServerRatis(GRPC, 1);
    runTestClientServerRatis(GRPC, 3);
  }

  static XceiverServerRatis newXceiverServerRatis(
      DatanodeDetails dn, OzoneConfiguration conf) throws IOException {
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        dn.getPort(DatanodeDetails.Port.Name.RATIS).getValue());
    final String dir = TEST_DIR + dn.getUuid();
    conf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, dir);

    final ContainerDispatcher dispatcher = new TestContainerDispatcher();
    return XceiverServerRatis.newXceiverServerRatis(dn, conf, dispatcher);
  }

  static void initXceiverServerRatis(
      RpcType rpc, DatanodeDetails dd, Pipeline pipeline) throws IOException {
    final RaftPeer p = RatisHelper.toRaftPeer(dd);
    final RaftClient client = RatisHelper.newRaftClient(rpc, p);
    client.reinitialize(RatisHelper.newRaftGroup(pipeline), p.getId());
  }


  static void runTestClientServerRatis(RpcType rpc, int numNodes)
      throws Exception {
    runTestClientServer(numNodes,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(rpc, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestContainerServer::newXceiverServerRatis,
        (dn, p) -> initXceiverServerRatis(rpc, dn, p));
  }

  static void runTestClientServer(
      int numDatanodes,
      BiConsumer<Pipeline, OzoneConfiguration> initConf,
      CheckedBiFunction<Pipeline, OzoneConfiguration, XceiverClientSpi,
          IOException> createClient,
      CheckedBiFunction<DatanodeDetails, OzoneConfiguration, XceiverServerSpi,
          IOException> createServer,
      CheckedBiConsumer<DatanodeDetails, Pipeline, IOException> initServer)
      throws Exception {
    final List<XceiverServerSpi> servers = new ArrayList<>();
    XceiverClientSpi client = null;
    String containerName = OzoneUtils.getRequestID();
    try {
      final Pipeline pipeline = ContainerTestHelper.createPipeline(numDatanodes);
      final OzoneConfiguration conf = new OzoneConfiguration();
      initConf.accept(pipeline, conf);

      for(DatanodeDetails dn : pipeline.getMachines()) {
        final XceiverServerSpi s = createServer.apply(dn, conf);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, conf);
      client.connect();

      final ContainerCommandRequestProto request =
          ContainerTestHelper
              .getCreateContainerRequest(
                  ContainerTestHelper.getTestContainerID(), pipeline);
      Assert.assertNotNull(request.getTraceID());

      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertEquals(request.getTraceID(), response.getTraceID());
    } finally {
      if (client != null) {
        client.close();
      }
      servers.stream().forEach(XceiverServerSpi::stop);
    }
  }

  @Test
  public void testClientServerWithContainerDispatcher() throws Exception {
    XceiverServer server = null;
    XceiverClient client = null;

    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getLeader()
              .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue());

      HddsDispatcher dispatcher = new HddsDispatcher(
          conf, mock(ContainerSet.class), mock(VolumeSet.class), null);
      dispatcher.init();
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      server = new XceiverServer(datanodeDetails, conf, dispatcher);
      client = new XceiverClient(pipeline, conf);

      server.start();
      client.connect();

      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest(
              ContainerTestHelper.getTestContainerID(), pipeline);
      ContainerCommandResponseProto response = client.sendCommand(request);
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    } finally {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.stop();
      }
    }
  }

  private static class TestContainerDispatcher implements ContainerDispatcher {
    /**
     * Dispatches commands to container layer.
     *
     * @param msg - Command Request
     * @return Command Response
     */
    @Override
    public ContainerCommandResponseProto
        dispatch(ContainerCommandRequestProto msg)  {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void init() {
    }

    @Override
    public void shutdown() {
    }
    @Override
    public Handler getHandler(ContainerProtos.ContainerType containerType) {
      return null;
    }

    @Override
    public void setScmId(String scmId) {

    }
  }
}