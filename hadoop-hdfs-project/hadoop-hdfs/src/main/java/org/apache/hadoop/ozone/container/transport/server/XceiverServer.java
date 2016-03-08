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

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.interfaces.ContainerDispatcher;

/**
 * Creates a netty server endpoint that acts as the communication layer for
 * Ozone containers.
 */
public final class XceiverServer {
  private final int port;
  private final ContainerDispatcher storageContainer;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel channel;

  /**
   * Constructs a netty server class.
   *
   * @param conf - Configuration
   */
  public XceiverServer(OzoneConfiguration conf,
                       ContainerDispatcher dispatcher) {
    Preconditions.checkNotNull(conf);
    this.port = conf.getInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
        OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT_DEFAULT);
    this.storageContainer = dispatcher;
  }

  /**
   * Starts running the server.
   *
   * @throws Exception
   */
  public void start() throws Exception {
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();
    channel = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new XceiverServerInitializer(storageContainer))
        .bind(port)
        .syncUninterruptibly()
        .channel();
  }

  /**
   * Stops a running server.
   *
   * @throws Exception
   */
  public void stop() throws Exception {
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    if (channel != null) {
      channel.close().awaitUninterruptibly();
    }
  }
}
