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
package org.apache.hadoop.oncrpc;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple UDP server implemented based on netty.
 */
public class SimpleUdpServer {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleUdpServer.class);
  private final int SEND_BUFFER_SIZE = 65536;
  private final int RECEIVE_BUFFER_SIZE = 65536;

  protected final int port;
  protected final ChannelInboundHandlerAdapter rpcProgram;
  protected final int workerCount;
  protected int boundPort = -1; // Will be set after server starts
  private Bootstrap server;
  private Channel ch;
  private EventLoopGroup workerGroup;

  public SimpleUdpServer(int port, ChannelInboundHandlerAdapter program,
      int workerCount) {
    this.port = port;
    this.rpcProgram = program;
    this.workerCount = workerCount;
  }

  public void run() throws InterruptedException {
    workerGroup = new NioEventLoopGroup(workerCount, Executors.newCachedThreadPool());

    server = new Bootstrap();
    server.group(workerGroup)
        .channel(NioDatagramChannel.class)
        .option(ChannelOption.SO_BROADCAST, true)
        .option(ChannelOption.SO_SNDBUF, SEND_BUFFER_SIZE)
        .option(ChannelOption.SO_RCVBUF, RECEIVE_BUFFER_SIZE)
        .option(ChannelOption.SO_REUSEADDR, true)
        .handler(new ChannelInitializer<NioDatagramChannel>() {
          @Override protected void initChannel(NioDatagramChannel ch)
              throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(
                RpcUtil.STAGE_RPC_MESSAGE_PARSER,
                rpcProgram,
                RpcUtil.STAGE_RPC_UDP_RESPONSE);
          }
        });

    // Listen to the UDP port
    ChannelFuture f = server.bind(new InetSocketAddress(port)).sync();
    ch = f.channel();
    InetSocketAddress socketAddr = (InetSocketAddress) ch.localAddress();
    boundPort = socketAddr.getPort();

    LOG.info("Started listening to UDP requests at port " + boundPort + " for "
        + rpcProgram + " with workerCount " + workerCount);
  }

  // boundPort will be set only after server starts
  public int getBoundPort() {
    return this.boundPort;
  }

  public void shutdown() {
    if (ch != null) {
      ch.close().awaitUninterruptibly();
      ch = null;
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
