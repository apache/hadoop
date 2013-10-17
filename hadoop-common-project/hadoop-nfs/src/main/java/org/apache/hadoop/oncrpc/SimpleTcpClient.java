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

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * A simple TCP based RPC client which just sends a request to a server.
 */
public class SimpleTcpClient {
  protected final String host;
  protected final int port;
  protected final XDR request;
  protected ChannelPipelineFactory pipelineFactory;
  protected final boolean oneShot;
  
  public SimpleTcpClient(String host, int port, XDR request) {
    this(host,port, request, true);
  }
  
  public SimpleTcpClient(String host, int port, XDR request, Boolean oneShot) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.oneShot = oneShot;
  }
  
  protected ChannelPipelineFactory setPipelineFactory() {
    this.pipelineFactory = new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        return Channels.pipeline(
            RpcUtil.constructRpcFrameDecoder(),
            new SimpleTcpClientHandler(request));
      }
    };
    return this.pipelineFactory;
  }

  public void run() {
    // Configure the client.
    ChannelFactory factory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 1, 1);
    ClientBootstrap bootstrap = new ClientBootstrap(factory);

    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(setPipelineFactory());

    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);

    // Start the connection attempt.
    ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

    if (oneShot) {
      // Wait until the connection is closed or the connection attempt fails.
      future.getChannel().getCloseFuture().awaitUninterruptibly();

      // Shut down thread pools to exit.
      bootstrap.releaseExternalResources();
    }
  }
}
