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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple UDP server implemented using netty.
 */
public class SimpleTcpServer {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleTcpServer.class);
  protected final int port;
  protected int boundPort = -1; // Will be set after server starts
  protected final ChannelInboundHandlerAdapter rpcProgram;
  private ServerBootstrap server;
  private Channel ch;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  /** The maximum number of I/O worker threads */
  protected final int workerCount;

  /**
   * @param port TCP port where to start the server at
   * @param program RPC program corresponding to the server
   * @param workercount Number of worker threads
   */
  public SimpleTcpServer(int port, RpcProgram program, int workercount) {
    this.port = port;
    this.rpcProgram = program;
    this.workerCount = workercount;
  }

  public void run() throws InterruptedException {
    // Configure the Server.
    bossGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup(workerCount, Executors.newCachedThreadPool());

    server = new ServerBootstrap();

    server.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(RpcUtil.constructRpcFrameDecoder(),
            RpcUtil.STAGE_RPC_MESSAGE_PARSER, rpcProgram,
            RpcUtil.STAGE_RPC_TCP_RESPONSE);
      }})
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_REUSEADDR, true);

    // Listen to TCP port
    ChannelFuture f = server.bind(new InetSocketAddress(port)).sync();
    ch = f.channel();
    InetSocketAddress socketAddr = (InetSocketAddress) ch.localAddress();
    boundPort = socketAddr.getPort();

    LOG.info("Started listening to TCP requests at port " + boundPort + " for "
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

    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
      bossGroup = null;
    }
  }
}
