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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * A simple TCP based RPC client which just sends a request to a server.
 */
public class SimpleTcpClient {
  protected final String host;
  protected final int port;
  protected final XDR request;
  protected final boolean oneShot;
  private NioEventLoopGroup workerGroup;
  private ChannelFuture future;

  public SimpleTcpClient(String host, int port, XDR request) {
    this(host,port, request, true);
  }

  public SimpleTcpClient(String host, int port, XDR request, Boolean oneShot) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.oneShot = oneShot;
  }

  protected ChannelInitializer<SocketChannel> setChannelHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(
            RpcUtil.constructRpcFrameDecoder(),
            new SimpleTcpClientHandler(request)
        );
      }
    };
  }

  @VisibleForTesting
  public void run() {
    // Configure the client.
    workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap()
        .group(workerGroup)
        .channel(NioSocketChannel.class);

    try {
      future = bootstrap.handler(setChannelHandler())
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          .connect(new InetSocketAddress(host, port)).sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (oneShot) {
        stop();
      }
    }
  }

  public void stop() {
    try {
      if (future != null) {
        // Wait until the connection is closed or the connection attempt fails.
        future.channel().closeFuture().sync();
      }

    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      // Shut down thread pools to exit.
      workerGroup.shutdownGracefully();
    }
  }
}
