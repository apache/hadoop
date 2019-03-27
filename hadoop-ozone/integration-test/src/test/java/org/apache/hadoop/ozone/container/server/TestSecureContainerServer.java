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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.RatisTestHelper;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerGrpc;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.GrpcReplicationService;
import org.apache.hadoop.ozone.container.replication.OnDemandContainerReplicationSource;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;

import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BLOCK_TOKEN_VERIFICATION_FAILED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.SUCCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCreateContainerRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getTestContainerID;
import static org.apache.ratis.rpc.SupportedRpcType.GRPC;
import static org.apache.ratis.rpc.SupportedRpcType.NETTY;
import static org.junit.Assert.assertEquals;

/**
 * Test Container servers when security is enabled.
 */
public class TestSecureContainerServer {
  static final String TEST_DIR
      = GenericTestUtils.getTestDir("dfs").getAbsolutePath() + File.separator;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static CertificateClientTestImpl caClient;

  private GrpcReplicationService createReplicationService(
      ContainerController containerController) {
    return new GrpcReplicationService(
        new OnDemandContainerReplicationSource(containerController));
  }

  @BeforeClass
  static public void setup() throws Exception {
    CONF.set(HddsConfigKeys.HDDS_METADATA_DIR_NAME, TEST_DIR);
    CONF.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    CONF.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    caClient = new CertificateClientTestImpl(CONF);
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
  public void testClientServerRatisGrpc() throws Exception {
    runTestClientServerRatis(GRPC, 1);
    runTestClientServerRatis(GRPC, 3);
  }

  @Test
  @Ignore
  public void testClientServerRatisNetty() throws Exception {
    runTestClientServerRatis(NETTY, 1);
    runTestClientServerRatis(NETTY, 3);
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
        TestSecureContainerServer::newXceiverServerRatis,
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

      // Test 1: Test failure in request without block token.
      final ContainerCommandRequestProto request =
          getCreateContainerRequest(
                  getTestContainerID(), pipeline);
      Assert.assertNotNull(request.getTraceID());

      XceiverClientSpi finalClient = client;
      // Validation is different for grpc and ratis client.
      if(client instanceof XceiverClientGrpc) {
        LambdaTestUtils.intercept(SCMSecurityException.class, "Failed to" +
                " authenticate with GRPC XceiverServer with Ozone block token",
            () -> finalClient.sendCommand(request));
      } else {
        ContainerCommandResponseProto response = finalClient.
            sendCommand(request);
        assertEquals(BLOCK_TOKEN_VERIFICATION_FAILED, response.getResult());
      }

      // Test 2: Test success in request with valid block token.
      long expiryTime = Time.monotonicNow() + 60 * 60 * 24;

      String omCertSerialId =
          caClient.getCertificate().getSerialNumber().toString();
      OzoneBlockTokenSecretManager secretManager =
          new OzoneBlockTokenSecretManager(new SecurityConfig(CONF),
          expiryTime, omCertSerialId);
      secretManager.start(caClient);
      Token<OzoneBlockTokenIdentifier> token = secretManager.generateToken("1",
          EnumSet.allOf(AccessModeProto.class), RandomUtils.nextLong());
      final ContainerCommandRequestProto request2 =
          ContainerTestHelper
              .getCreateContainerSecureRequest(
                  getTestContainerID(), pipeline,
                  token);
      Assert.assertNotNull(request2.getTraceID());
      XceiverClientSpi finalClient2 = createClient.apply(pipeline, CONF);
      if(finalClient2 instanceof XceiverClientGrpc) {
        finalClient2.connect(token.encodeToUrlString());
      } else {
        finalClient2.connect();
      }

      ContainerCommandRequestProto request3 = getCreateContainerRequest(
          getTestContainerID(), pipeline, token);
      ContainerCommandResponseProto resp = finalClient2.sendCommand(request3);
      assertEquals(SUCCESS, resp.getResult());
    } finally {
      if (client != null) {
        client.close();
      }
      servers.stream().forEach(XceiverServerSpi::stop);
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