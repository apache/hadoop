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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server
    .XceiverServerSpi;

import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Objects;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis implements XceiverServerSpi {
  static final Logger LOG = LoggerFactory.getLogger(XceiverServerRatis.class);
  private final int port;
  private final RaftServer server;

  private XceiverServerRatis(DatanodeID id, int port, String storageDir,
      ContainerDispatcher dispatcher, RpcType rpcType) throws IOException {
    Objects.requireNonNull(id, "id == null");
    this.port = port;

    this.server = RaftServer.newBuilder()
        .setServerId(RatisHelper.toRaftPeerId(id))
        .setPeers(Collections.emptyList())
        .setProperties(newRaftProperties(rpcType, port, storageDir))
        .setStateMachine(new ContainerStateMachine(dispatcher))
        .build();
  }

  static RaftProperties newRaftProperties(
      RpcType rpc, int port, String storageDir) {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, storageDir);
    RaftConfigKeys.Rpc.setType(properties, rpc);
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else {
      if (rpc == SupportedRpcType.NETTY) {
        NettyConfigKeys.Server.setPort(properties, port);
      }
    }
    return properties;
  }

  public static XceiverServerRatis newXceiverServerRatis(DatanodeID datanodeID,
      Configuration ozoneConf, ContainerDispatcher dispatcher)
      throws IOException {
    final String ratisDir = File.separator + "ratis";
    int localPort = ozoneConf.getInt(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT_DEFAULT);
    String storageDir = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ozoneConf.get(OzoneConfigKeys
          .OZONE_METADATA_DIRS);
      Preconditions.checkNotNull(storageDir, "ozone.metadata.dirs " +
          "cannot be null, Please check your configs.");
      storageDir = storageDir.concat(ratisDir);
      LOG.warn("Storage directory for Ratis is not configured. Mapping Ratis " +
              "storage under {}. It is a good idea to map this to an SSD disk.",
          storageDir);
    }
    final String rpcType = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);

    // Get an available port on current node and
    // use that as the container port
    if (ozoneConf.getBoolean(OzoneConfigKeys
            .DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT_DEFAULT)) {
      try (ServerSocket socket = new ServerSocket()) {
        socket.setReuseAddress(true);
        SocketAddress address = new InetSocketAddress(0);
        socket.bind(address);
        localPort = socket.getLocalPort();
        LOG.info("Found a free port for the server : {}", localPort);
        // If we have random local ports configured this means that it
        // probably running under MiniOzoneCluster. Ratis locks the storage
        // directories, so we need to pass different local directory for each
        // local instance. So we map ratis directories under datanode ID.
        storageDir =
            storageDir.concat(File.separator + datanodeID.getDatanodeUuid());
      } catch (IOException e) {
        LOG.error("Unable find a random free port for the server, "
            + "fallback to use default port {}", localPort, e);
      }
    }
    datanodeID.setRatisPort(localPort);
    return new XceiverServerRatis(datanodeID, localPort, storageDir,
        dispatcher, rpc);
  }

  @Override
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), getIPCPort());
    server.start();
  }

  @Override
  public void stop() {
    try {
      server.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getIPCPort() {
    return port;
  }

  /**
   * Returns the Replication type supported by this end-point.
   *
   * @return enum -- {Stand_Alone, Ratis, Chained}
   */
  @Override
  public OzoneProtos.ReplicationType getServerType() {
    return OzoneProtos.ReplicationType.RATIS;
  }
}
