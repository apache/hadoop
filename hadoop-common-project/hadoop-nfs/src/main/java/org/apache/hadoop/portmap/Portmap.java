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
package org.apache.hadoop.portmap;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Portmap service for binding RPC protocols. See RFC 1833 for details.
 */
final class Portmap {
  private static final Logger LOG = LoggerFactory.getLogger(Portmap.class);
  private static final int DEFAULT_IDLE_TIME_MILLISECONDS = 5000;

  private Bootstrap udpServer;
  private ServerBootstrap tcpServer;
  private ChannelGroup allChannels = new DefaultChannelGroup(
      GlobalEventExecutor.INSTANCE);
  private Channel udpChannel;
  private Channel tcpChannel;

  EventLoopGroup bossGroup;
  EventLoopGroup workerGroup;
  EventLoopGroup udpGroup;

  private final RpcProgramPortmap handler = new RpcProgramPortmap(allChannels);

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(Portmap.class, args, LOG);

    final int port = RpcProgram.RPCB_PORT;
    Portmap pm = new Portmap();
    try {
      pm.start(DEFAULT_IDLE_TIME_MILLISECONDS,
          new InetSocketAddress(port), new InetSocketAddress(port));
    } catch (Throwable e) {
      LOG.error("Failed to start the server. Cause:", e);
      pm.shutdown();
      System.exit(-1);
    }
  }

  void shutdown() {
    allChannels.close().awaitUninterruptibly();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    udpGroup.shutdownGracefully();
  }

  @VisibleForTesting
  SocketAddress getTcpServerLocalAddress() {
    return tcpChannel.localAddress();
  }

  @VisibleForTesting
  SocketAddress getUdpServerLoAddress() {
    return udpChannel.localAddress();
  }

  @VisibleForTesting
  RpcProgramPortmap getHandler() {
    return handler;
  }

  void start(final int idleTimeMilliSeconds, final SocketAddress tcpAddress,
      final SocketAddress udpAddress) throws InterruptedException {

    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup(0, Executors.newCachedThreadPool());

    tcpServer = new ServerBootstrap();
    tcpServer.group(bossGroup, workerGroup)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();

            p.addLast(RpcUtil.constructRpcFrameDecoder(),
                RpcUtil.STAGE_RPC_MESSAGE_PARSER, new IdleStateHandler(0, 0,
                            idleTimeMilliSeconds, TimeUnit.MILLISECONDS), handler,
                RpcUtil.STAGE_RPC_TCP_RESPONSE);
          }});

    udpGroup = new NioEventLoopGroup(0, Executors.newCachedThreadPool());

    udpServer = new Bootstrap();
    udpServer.group(udpGroup)
        .channel(NioDatagramChannel.class)
        .handler(new ChannelInitializer<NioDatagramChannel>() {
          @Override protected void initChannel(NioDatagramChannel ch)
              throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(
                new LoggingHandler(LogLevel.DEBUG),
                RpcUtil.STAGE_RPC_MESSAGE_PARSER, handler, RpcUtil.STAGE_RPC_UDP_RESPONSE);
          }
        })
        .option(ChannelOption.SO_REUSEADDR, true);

    ChannelFuture tcpChannelFuture = null;
    tcpChannelFuture = tcpServer.bind(tcpAddress);
    ChannelFuture udpChannelFuture = udpServer.bind(udpAddress);
    tcpChannel = tcpChannelFuture.sync().channel();
    udpChannel = udpChannelFuture.sync().channel();

    allChannels.add(tcpChannel);
    allChannels.add(udpChannel);

    LOG.info("Portmap server started at tcp://" + tcpChannel.localAddress()
        + ", udp://" + udpChannel.localAddress());
  }
}
