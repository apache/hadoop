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
package org.apache.hadoop.hdfs.server.federation;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.BlockingService;


/**
 * Mock for the network interfaces (e.g., RPC and HTTP) of a Namenode. This is
 * used by the Routers in a mock cluster.
 */
public class MockNamenode {

  /** Mock implementation of the Namenode. */
  private final NamenodeProtocols mockNn;

  /** HA state of the Namenode. */
  private HAServiceState haState = HAServiceState.STANDBY;

  /** RPC server of the Namenode that redirects calls to the mock. */
  private Server rpcServer;
  /** HTTP server of the Namenode that redirects calls to the mock. */
  private HttpServer2 httpServer;


  public MockNamenode() throws Exception {
    Configuration conf = new Configuration();

    this.mockNn = mock(NamenodeProtocols.class);
    setupMock();
    setupRPCServer(conf);
    setupHTTPServer(conf);
  }

  /**
   * Setup the mock of the Namenode. It offers the basic functionality for
   * Routers to get the status.
   * @throws IOException If the mock cannot be setup.
   */
  protected void setupMock() throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo(1, "clusterId", "bpId", 1);
    when(mockNn.versionRequest()).thenReturn(nsInfo);

    when(mockNn.getServiceStatus()).thenAnswer(new Answer<HAServiceStatus>() {
      @Override
      public HAServiceStatus answer(InvocationOnMock invocation)
          throws Throwable {
        HAServiceStatus haStatus = new HAServiceStatus(getHAServiceState());
        haStatus.setNotReadyToBecomeActive("");
        return haStatus;
      }
    });
  }

  /**
   * Setup the RPC server of the Namenode that redirects calls to the mock.
   * @param conf Configuration of the server.
   * @throws IOException If the RPC server cannot be setup.
   */
  private void setupRPCServer(final Configuration conf) throws IOException {
    RPC.setProtocolEngine(
        conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);
    ClientNamenodeProtocolServerSideTranslatorPB
        clientNNProtoXlator =
            new ClientNamenodeProtocolServerSideTranslatorPB(mockNn);
    BlockingService clientNNPbService =
        ClientNamenodeProtocol.newReflectiveBlockingService(
            clientNNProtoXlator);

    rpcServer = new RPC.Builder(conf)
        .setProtocol(ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress("0.0.0.0")
        .setPort(0)
        .build();

    NamenodeProtocolServerSideTranslatorPB nnProtoXlator =
        new NamenodeProtocolServerSideTranslatorPB(mockNn);
    BlockingService nnProtoPbService =
        NamenodeProtocolService.newReflectiveBlockingService(
            nnProtoXlator);
    DFSUtil.addPBProtocol(
        conf, NamenodeProtocolPB.class, nnProtoPbService, rpcServer);

    DatanodeProtocolServerSideTranslatorPB dnProtoPbXlator =
        new DatanodeProtocolServerSideTranslatorPB(mockNn, 1000);
    BlockingService dnProtoPbService =
        DatanodeProtocolService.newReflectiveBlockingService(
            dnProtoPbXlator);
    DFSUtil.addPBProtocol(
        conf, DatanodeProtocolPB.class, dnProtoPbService, rpcServer);

    HAServiceProtocolServerSideTranslatorPB haServiceProtoXlator =
        new HAServiceProtocolServerSideTranslatorPB(mockNn);
    BlockingService haProtoPbService =
        HAServiceProtocolService.newReflectiveBlockingService(
            haServiceProtoXlator);
    DFSUtil.addPBProtocol(
        conf, HAServiceProtocolPB.class, haProtoPbService, rpcServer);

    rpcServer.start();
  }

  /**
   * Setup the HTTP server of the Namenode that redirects calls to the mock.
   * @param conf Configuration of the server.
   * @throws IOException If the HTTP server cannot be setup.
   */
  private void setupHTTPServer(Configuration conf) throws IOException {
    HttpServer2.Builder builder = new HttpServer2.Builder()
        .setName("hdfs")
        .setConf(conf)
        .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .addEndpoint(URI.create("http://0.0.0.0:0"));
    httpServer = builder.build();
    httpServer.start();
  }

  /**
   * Get the RPC port for the Mock Namenode.
   * @return RPC port.
   */
  public int getRPCPort() {
    return rpcServer.getListenerAddress().getPort();
  }

  /**
   * Get the HTTP port for the Mock Namenode.
   * @return HTTP port.
   */
  public int getHTTPPort() {
    return httpServer.getConnectorAddress(0).getPort();
  }

  /**
   * Get the Mock core. This is used to extend the mock.
   * @return Mock Namenode protocol to be extended.
   */
  public NamenodeProtocols getMock() {
    return mockNn;
  }

  /**
   * Get the HA state of the Mock Namenode.
   * @return HA state (ACTIVE or STANDBY).
   */
  public HAServiceState getHAServiceState() {
    return haState;
  }

  /**
   * Show the Mock Namenode as Active.
   */
  public void transitionToActive() {
    this.haState = HAServiceState.ACTIVE;
  }

  /**
   * Show the Mock Namenode as Standby.
   */
  public void transitionToStandby() {
    this.haState = HAServiceState.STANDBY;
  }

  /**
   * Stop the Mock Namenode. It stops all the servers.
   * @throws Exception If it cannot stop the Namenode.
   */
  public void stop() throws Exception {
    if (rpcServer != null) {
      rpcServer.stop();
    }
    if (httpServer != null) {
      httpServer.stop();
    }
  }
}