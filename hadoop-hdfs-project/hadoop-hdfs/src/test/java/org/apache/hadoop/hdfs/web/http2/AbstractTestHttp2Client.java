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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.util.ByteString;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;

public abstract class AbstractTestHttp2Client {

  protected EventLoopGroup workerGroup = new NioEventLoopGroup();

  protected Server server;

  protected final class EchoHandler extends AbstractHandler {

    @Override
    public void handle(String target, Request baseRequest,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
      byte[] msg = IOUtils.toByteArray(request.getInputStream());
      response.getOutputStream().write(msg);
      response.getOutputStream().flush();
    }

  }

  protected Channel channel;

  protected void start() throws Exception {
    server = new Server();
    ServerConnector connector =
        new ServerConnector(server, new HTTP2CServerConnectionFactory(
            new HttpConfiguration()));
    connector.setPort(0);
    server.addConnector(connector);
    setHandler(server);
    server.start();
    channel =
        new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(
                  ClientHttp2ConnectionHandler.create(ch, new Configuration()));
              }

            })
            .connect(
              new InetSocketAddress("127.0.0.1", connector.getLocalPort()))
            .sync().channel();
  }

  protected void stop() throws Exception {
    if (channel != null) {
      channel.close();
    }
    if (server != null) {
      server.stop();
    }
    workerGroup.shutdownGracefully();
  }

  protected Http2StreamChannel connect(boolean endStream)
      throws InterruptedException, ExecutionException {
    return new Http2StreamBootstrap()
        .channel(channel)
        .handler(new ChannelInitializer<Http2StreamChannel>() {

          @Override
          protected void initChannel(Http2StreamChannel ch) throws Exception {
            ch.pipeline().addLast(new Http2DataReceiver());
          }

        })
        .headers(
          new DefaultHttp2Headers()
              .method(
                new ByteString(HttpMethod.GET.name(), StandardCharsets.UTF_8))
              .path(new ByteString("/", StandardCharsets.UTF_8))
              .scheme(new ByteString("http", StandardCharsets.UTF_8))
              .authority(
                new ByteString("127.0.0.1:"
                    + ((InetSocketAddress) channel.remoteAddress()).getPort(),
                    StandardCharsets.UTF_8))).endStream(endStream).connect()
        .sync().get();
  }

  protected abstract void setHandler(Server server);
}