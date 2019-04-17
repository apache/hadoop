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

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import org.apache.hadoop.yarn.csi.utils.GrpcHelper;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A fake implementation of CSI driver.
 * This is for testing purpose only.
 */
public class FakeCsiDriver {

  private static final Logger LOG = LoggerFactory
      .getLogger(FakeCsiDriver.class.getName());

  private Server server;
  private String socketAddress;

  public FakeCsiDriver(String socketAddress) {
    this.socketAddress = socketAddress;
  }

  public void start() throws IOException {
    EpollEventLoopGroup group = new EpollEventLoopGroup();
    server = NettyServerBuilder
        .forAddress(GrpcHelper.getSocketAddress(socketAddress))
        .channelType(EpollServerDomainSocketChannel.class)
        .workerEventLoopGroup(group)
        .bossEventLoopGroup(group)
        .addService(new FakeCsiIdentityService())
        .build();
    server.start();
    LOG.info("Server started, listening on " + socketAddress);
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
      LOG.info("Server has been shutdown");
    }
  }
}
