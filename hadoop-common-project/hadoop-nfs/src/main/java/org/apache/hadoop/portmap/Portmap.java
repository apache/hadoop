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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.util.StringUtils;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;

import com.google.common.annotations.VisibleForTesting;

/**
 * Portmap service for binding RPC protocols. See RFC 1833 for details.
 */
final class Portmap {
  private static final Log LOG = LogFactory.getLog(Portmap.class);
  private static final int DEFAULT_IDLE_TIME_MILLISECONDS = 5000;

  private ConnectionlessBootstrap udpServer;
  private ServerBootstrap tcpServer;
  private ChannelGroup allChannels = new DefaultChannelGroup();
  private Channel udpChannel;
  private Channel tcpChannel;
  private final RpcProgramPortmap handler = new RpcProgramPortmap(allChannels);

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(Portmap.class, args, LOG);

    final int port = RpcProgram.RPCB_PORT;
    Portmap pm = new Portmap();
    try {
      pm.start(DEFAULT_IDLE_TIME_MILLISECONDS,
          new InetSocketAddress(port), new InetSocketAddress(port));
    } catch (Throwable e) {
      LOG.fatal("Failed to start the server. Cause:", e);
      pm.shutdown();
      System.exit(-1);
    }
  }

  void shutdown() {
    allChannels.close().awaitUninterruptibly();
    tcpServer.releaseExternalResources();
    udpServer.releaseExternalResources();
  }

  @VisibleForTesting
  SocketAddress getTcpServerLocalAddress() {
    return tcpChannel.getLocalAddress();
  }

  @VisibleForTesting
  SocketAddress getUdpServerLoAddress() {
    return udpChannel.getLocalAddress();
  }

  @VisibleForTesting
  RpcProgramPortmap getHandler() {
    return handler;
  }

  void start(final int idleTimeMilliSeconds, final SocketAddress tcpAddress,
      final SocketAddress udpAddress) {

    tcpServer = new ServerBootstrap(new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
    tcpServer.setPipelineFactory(new ChannelPipelineFactory() {
      private final HashedWheelTimer timer = new HashedWheelTimer();
      private final IdleStateHandler idleStateHandler = new IdleStateHandler(
          timer, 0, 0, idleTimeMilliSeconds, TimeUnit.MILLISECONDS);

      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(RpcUtil.constructRpcFrameDecoder(),
            RpcUtil.STAGE_RPC_MESSAGE_PARSER, idleStateHandler, handler,
            RpcUtil.STAGE_RPC_TCP_RESPONSE);
      }
    });

    udpServer = new ConnectionlessBootstrap(new NioDatagramChannelFactory(
        Executors.newCachedThreadPool()));

    udpServer.setPipeline(Channels.pipeline(RpcUtil.STAGE_RPC_MESSAGE_PARSER,
        handler, RpcUtil.STAGE_RPC_UDP_RESPONSE));

    tcpChannel = tcpServer.bind(tcpAddress);
    udpChannel = udpServer.bind(udpAddress);
    allChannels.add(tcpChannel);
    allChannels.add(udpChannel);

    LOG.info("Portmap server started at tcp://" + tcpChannel.getLocalAddress()
        + ", udp://" + udpChannel.getLocalAddress());
  }
}
