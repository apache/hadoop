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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageContainerDatanodeProtocolService;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolPB;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.protobuf.BlockingService;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;

/**
 * Test Endpoint class.
 */
public final class SCMTestUtils {
  /**
   * Never constructed.
   */
  private SCMTestUtils() {
  }

  /**
   * Starts an RPC server, if configured.
   *
   * @param conf configuration
   * @param addr configured address of RPC server
   * @param protocol RPC protocol provided by RPC server
   * @param instance RPC protocol implementation instance
   * @param handlerCount RPC server handler count
   * @return RPC server
   * @throws IOException if there is an I/O error while creating RPC server
   */
  private static RPC.Server startRpcServer(Configuration conf,
      InetSocketAddress addr, Class<?>
      protocol, BlockingService instance, int handlerCount)
      throws IOException {
    RPC.Server rpcServer = new RPC.Builder(conf)
        .setProtocol(protocol)
        .setInstance(instance)
        .setBindAddress(addr.getHostString())
        .setPort(addr.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(null)
        .build();

    DFSUtil.addPBProtocol(conf, protocol, instance, rpcServer);
    return rpcServer;
  }


  /**
   * Start Datanode RPC server.
   */
  public static RPC.Server startScmRpcServer(Configuration configuration,
      StorageContainerDatanodeProtocol server,
      InetSocketAddress rpcServerAddresss, int handlerCount) throws
      IOException {
    RPC.setProtocolEngine(configuration,
        StorageContainerDatanodeProtocolPB.class,
        ProtobufRpcEngine.class);

    BlockingService scmDatanodeService =
        StorageContainerDatanodeProtocolService.
            newReflectiveBlockingService(
                new StorageContainerDatanodeProtocolServerSideTranslatorPB(
                    server));

    RPC.Server scmServer = startRpcServer(configuration, rpcServerAddresss,
        StorageContainerDatanodeProtocolPB.class, scmDatanodeService,
        handlerCount);

    scmServer.start();
    return scmServer;
  }

  public static InetSocketAddress getReuseableAddress() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      String addr = InetAddress.getLoopbackAddress().getHostAddress();
      return new InetSocketAddress(addr, port);
    }
  }

  public static OzoneConfiguration getConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, GenericTestUtils
        .getRandomizedTempPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, GenericTestUtils
        .getRandomizedTempPath());
    return conf;
  }

  public static OzoneConfiguration getOzoneConf() {
    return new OzoneConfiguration();
  }

}
