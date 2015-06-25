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
package org.apache.hadoop.hdfs.web.http2;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import org.eclipse.jetty.http2.ErrorCode;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;

public abstract class AbstractTestHttp2Server {

  protected EventLoopGroup bossGroup = new NioEventLoopGroup(1);

  protected EventLoopGroup workerGroup = new NioEventLoopGroup();

  protected Channel server;

  protected HTTP2Client client = new HTTP2Client();

  protected Session session;

  protected abstract Channel initServer();

  protected final void start() throws Exception {
    server = initServer();
    client.start();
    int port = ((InetSocketAddress) server.localAddress()).getPort();
    FuturePromise<Session> sessionPromise = new FuturePromise<>();
    client.connect(new InetSocketAddress("127.0.0.1", port),
      new Session.Listener.Adapter(), sessionPromise);
    session = sessionPromise.get();
  }

  protected final void stop() throws Exception {
    if (session != null) {
      session.close(ErrorCode.NO_ERROR.code, "", new Callback.Adapter());
    }
    if (server != null) {
      server.close();
    }
    client.stop();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}