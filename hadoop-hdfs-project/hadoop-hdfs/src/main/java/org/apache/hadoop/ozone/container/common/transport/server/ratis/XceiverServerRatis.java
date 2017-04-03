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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates a ratis server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServerRatis implements XceiverServerSpi {
  static RaftProperties newRaftProperties(
      RpcType rpc, int port, String storageDir) {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, storageDir);
    RaftConfigKeys.Rpc.setType(properties, rpc);
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }
    return properties;
  }

  public static XceiverServerRatis newXceiverServerRatis(
      Configuration ozoneConf, ContainerDispatcher dispatcher)
      throws IOException {
    final String id = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_ADDRESS);
    final Collection<String> servers = ozoneConf.getStringCollection(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_CONF);
    final String storageDir = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);
    final String rpcType = ozoneConf.get(
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        OzoneConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(rpcType);
    return new XceiverServerRatis(id, servers, storageDir, dispatcher, rpc);
  }

  private final int port;
  private final RaftServer server;

  private XceiverServerRatis(
      String id, Collection<String> servers, String storageDir,
      ContainerDispatcher dispatcher, RpcType rpcType) throws IOException {
    Preconditions.checkArgument(servers.contains(id),
        "%s is not one of %s specified in %s",
        id, servers, OzoneConfigKeys.DFS_CONTAINER_RATIS_CONF);

    final List<RaftPeer> peers = servers.stream()
        .map(addr -> new RaftPeer(new RaftPeerId(addr), addr))
        .collect(Collectors.toList());

    this.port = NetUtils.createSocketAddr(id).getPort();

    this.server = RaftServer.newBuilder()
        .setServerId(new RaftPeerId(id))
        .setPeers(peers)
        .setProperties(newRaftProperties(rpcType, port, storageDir))
        .setStateMachine(new ContainerStateMachine(dispatcher))
        .build();
  }

  @Override
  public void start() throws IOException {
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
}
