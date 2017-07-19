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

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
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
  protected final SimpleChannelUpstreamHandler rpcProgram;
  protected final int workerCount;
  protected int boundPort = -1; // Will be set after server starts
  private ConnectionlessBootstrap server;
  private Channel ch;

  public SimpleUdpServer(int port, SimpleChannelUpstreamHandler program,
      int workerCount) {
    this.port = port;
    this.rpcProgram = program;
    this.workerCount = workerCount;
  }

  public void run() {
    // Configure the client.
    DatagramChannelFactory f = new NioDatagramChannelFactory(
        Executors.newCachedThreadPool(), workerCount);

    server = new ConnectionlessBootstrap(f);
    server.setPipeline(Channels.pipeline(RpcUtil.STAGE_RPC_MESSAGE_PARSER,
        rpcProgram, RpcUtil.STAGE_RPC_UDP_RESPONSE));

    server.setOption("broadcast", "false");
    server.setOption("sendBufferSize", SEND_BUFFER_SIZE);
    server.setOption("receiveBufferSize", RECEIVE_BUFFER_SIZE);
    server.setOption("reuseAddress", true);

    // Listen to the UDP port
    ch = server.bind(new InetSocketAddress(port));
    InetSocketAddress socketAddr = (InetSocketAddress) ch.getLocalAddress();
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
    }
    if (server != null) {
      server.releaseExternalResources();
    }
  }
}
