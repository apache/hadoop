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

import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.GrpcReplicationService;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
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
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;

import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.apache.ratis.rpc.SupportedRpcType.NETTY;
import static org.mockito.Mockito.mock;

/**
 * Test Containers.
 */
@Ignore("Takes too long to run this test. Ignoring for time being.")
public class TestContainerServer {
  static final String TEST_DIR = GenericTestUtils.getTestDir("dfs")
      .getAbsolutePath() + File.separator;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static CertificateClient caClient;

  private GrpcReplicationService createReplicationService(
      ContainerController containerController) {
    return new GrpcReplicationService(
        new OnDemandContainerReplicationSource(containerController));
  }

  @BeforeClass
  static public void setup() {
    CONF.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, TEST_DIR);
    caClient = new DNCertificateClient(new SecurityConfig(CONF));
  }

  @Test
  public void testClientServer() throws Exception {
    DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
    ContainerSet containerSet = new ContainerSet();
    ContainerController controller = new ContainerController(
        containerSet, null);
    runTestClientServer(1, (pipeline, conf) -> conf
            .setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
                pipeline.getFirstNode()
                    .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue()),
        XceiverClientGrpc::new,
        (dn, conf) -> new XceiverServerGrpc(datanodeDetails, conf,
            new TestContainerDispatcher(), caClient,
            createReplicationService(controller)), (dn, p) -> {
        });
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
    return XceiverServerRatis
        .newXceiverServerRatis(dn, conf, dispatcher, null, caClient);
  }

  static void runTestClientServerRatis(RpcType rpc, int numNodes)
      throws Exception {
    runTestClientServer(numNodes,
        (pipeline, conf) -> RatisTestHelper.initRatisConf(rpc, conf),
        XceiverClientRatis::newXceiverClientRatis,
        TestContainerServer::newXceiverServerRatis,
        (dn, p) -> RatisTestHelper.initXceiverServerRatis(rpc, dn, p));
  }

  static void runTestClientServer(
      int numDatanodes,
      CheckedBiConsumer<Pipeline, OzoneConfiguration, IOException> initConf,
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
      final Pipeline pipeline =
          ContainerTestHelper.createPipeline(numDatanodes);
      initConf.accept(pipeline, CONF);

      for (DatanodeDetails dn : pipeline.getNodes()) {
        final XceiverServerSpi s = createServer.apply(dn, CONF);
        servers.add(s);
        s.start();
        initServer.accept(dn, pipeline);
      }

      client = createClient.apply(pipeline, CONF);
      client.connect();

      final ContainerCommandRequestProto request =
          ContainerTestHelper
              .getCreateContainerRequest(
                  ContainerTestHelper.getTestContainerID(), pipeline);
      Assert.assertNotNull(request.getTraceID());

      ContainerCommandResponseProto response = client.sendCommand(request);
    } finally {
      if (client != null) {
        client.close();
      }
      servers.stream().forEach(XceiverServerSpi::stop);
    }
  }

  @Test
  public void testClientServerWithContainerDispatcher() throws Exception {
    XceiverServerGrpc server = null;
    XceiverClientGrpc client = null;
    UUID scmId = UUID.randomUUID();
    try {
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline();
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getFirstNode()
              .getPort(DatanodeDetails.Port.Name.STANDALONE).getValue());

      ContainerSet containerSet = new ContainerSet();
      VolumeSet volumeSet = mock(VolumeSet.class);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      DatanodeStateMachine stateMachine = Mockito.mock(
          DatanodeStateMachine.class);
      StateContext context = Mockito.mock(StateContext.class);
      Mockito.when(stateMachine.getDatanodeDetails())
          .thenReturn(datanodeDetails);
      Mockito.when(context.getParent()).thenReturn(stateMachine);


      for (ContainerProtos.ContainerType containerType :
          ContainerProtos.ContainerType.values()) {
        handlers.put(containerType,
            Handler.getHandlerForContainerType(containerType, conf, context,
                containerSet, volumeSet, metrics));
      }
      HddsDispatcher dispatcher = new HddsDispatcher(
          conf, containerSet, volumeSet, handlers, context, metrics);
      dispatcher.setScmId(scmId.toString());
      dispatcher.init();

      server = new XceiverServerGrpc(datanodeDetails, conf, dispatcher,
          caClient, createReplicationService(
              new ContainerController(containerSet, null)));
      client = new XceiverClientGrpc(pipeline, conf);

      server.start();
      client.connect();

      ContainerCommandRequestProto request =
          ContainerTestHelper.getCreateContainerRequest(
              ContainerTestHelper.getTestContainerID(), pipeline);
      ContainerCommandResponseProto response = client.sendCommand(request);
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
    public ContainerCommandResponseProto dispatch(
        ContainerCommandRequestProto msg,
        DispatcherContext context) {
      return ContainerTestHelper.getCreateContainerResponse(msg);
    }

    @Override
    public void init() {
    }

    @Override
    public void validateContainerCommand(
        ContainerCommandRequestProto msg) throws StorageContainerException {
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

    @Override
    public void buildMissingContainerSet(Set<Long> createdContainerSet) {
    }
  }
}