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

package org.apache.hadoop.ozone.container.transport.client;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A Client for the storageContainer protocol.
 */
public class XceiverClient {
  static final Logger LOG = LoggerFactory.getLogger(XceiverClient.class);
  private final Pipeline pipeline;
  private final OzoneConfiguration config;
  private ChannelFuture channelFuture;
  private Bootstrap b;
  private EventLoopGroup group;

  /**
   * Constructs a client that can communicate with the Container framework on
   * data nodes.
   * @param pipeline - Pipeline that defines the machines.
   * @param config -- Ozone Config
   */
  public XceiverClient(Pipeline pipeline, OzoneConfiguration config) {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkNotNull(config);
    this.pipeline = pipeline;
    this.config = config;
  }

  /**
   * Connects to the leader in the pipeline.
   */
  public void connect() throws Exception {
    if (channelFuture != null
        && channelFuture.channel() != null
        && channelFuture.channel().isActive()) {
      throw new IOException("This client is already connected to a host.");
    }

    group = new NioEventLoopGroup();
    b = new Bootstrap();
    b.group(group)
        .channel(NioSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .handler(new XceiverClientInitializer(this.pipeline));
    DatanodeID leader = this.pipeline.getLeader();

    // read port from the data node, on failure use default configured
    // port.
    int port = leader.getContainerPort();
    if (port == 0) {
      port = config.getInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
          OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT_DEFAULT);
    }
    LOG.debug("Connecting to server Port : " + port);
    channelFuture = b.connect(leader.getHostName(), port).sync();
  }

  /**
   * Close the client.
   */
  public void close() {
    if(group != null) {
      group.shutdownGracefully();
    }

    if (channelFuture != null) {
      channelFuture.channel().close();
    }
  }

  /**
   * Sends a given command to server and gets the reply back.
   * @param request Request
   * @return Response to the command
   * @throws IOException
   */
  public ContainerProtos.ContainerCommandResponseProto sendCommand(
      ContainerProtos.ContainerCommandRequestProto request)
      throws IOException {
    if((channelFuture == null) || (!channelFuture.channel().isActive())) {
      throw new IOException("This channel is not connected.");
    }
    XceiverClientHandler handler =
        channelFuture.channel().pipeline().get(XceiverClientHandler.class);

    return handler.sendCommand(request);
  }
}
