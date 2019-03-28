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

package org.apache.hadoop.yarn.csi.client;

import csi.v0.ControllerGrpc;
import csi.v0.IdentityGrpc;
import csi.v0.NodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A CSI gRPC client, it connects a CSI driver via a given unix domain socket.
 */
public final class CsiGrpcClient implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(CsiGrpcClient.class);

  private final ManagedChannel channel;

  private CsiGrpcClient(ManagedChannel channel) {
    this.channel = channel;
  }

  public static GrpcClientBuilder newBuilder() {
    return new GrpcClientBuilder();
  }

  /**
   * The Grpc Client builder.
   */
  public static class GrpcClientBuilder {

    private SocketAddress socket;

    public GrpcClientBuilder setDomainSocketAddress(SocketAddress address) {
      this.socket = address;
      return this;
    }

    private ManagedChannel getChannel(SocketAddress socketAddress)
        throws IOException {
      DefaultThreadFactory tf = new DefaultThreadFactory(
          "yarn-csi-client-", true);
      EpollEventLoopGroup loopGroup = new EpollEventLoopGroup(0, tf);
      if (socketAddress instanceof DomainSocketAddress) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(socketAddress)
            .channelType(EpollDomainSocketChannel.class)
            .eventLoopGroup(loopGroup)
            .usePlaintext()
            .build();
        return channel;
      } else {
        throw new IOException("Currently only unix domain socket is supported");
      }
    }

    public CsiGrpcClient build() throws IOException {
      ManagedChannel socketChannel = getChannel(socket);
      return new CsiGrpcClient(socketChannel);
    }
  }

  /**
   * Shutdown the communication channel gracefully,
   * wait for 5 seconds before it is enforced.
   */
  @Override
  public void close() {
    try {
      this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to gracefully shutdown"
          + " gRPC communication channel in 5 seconds", e);
    }
  }

  /**
   * Creates a blocking stub for CSI identity plugin on the given channel.
   * @return the blocking stub
   */
  public IdentityGrpc.IdentityBlockingStub createIdentityBlockingStub() {
    return IdentityGrpc.newBlockingStub(channel);
  }

  /**
   * Creates a blocking stub for CSI controller plugin on the given channel.
   * @return the blocking stub
   */
  public ControllerGrpc.ControllerBlockingStub createControllerBlockingStub(){
    return ControllerGrpc.newBlockingStub(channel);
  }

  /**
   * Creates a blocking stub for CSI node plugin on the given channel.
   * @return the blocking stub
   */
  public NodeGrpc.NodeBlockingStub createNodeBlockingStub() {
    return NodeGrpc.newBlockingStub(channel);
  }
}
