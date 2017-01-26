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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.commons.logging.Log;

import java.net.InetSocketAddress;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Dead simple session-layer HTTP proxy. It gets the HTTP responses
 * inside the context, assuming that the remote peer is reasonable fast and
 * the response is small. The upper layer should be filtering out malicious
 * inputs.
 */
class SimpleHttpProxyHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private String uri;
  private Channel proxiedChannel;
  private final InetSocketAddress host;
  static final Log LOG = DatanodeHttpServer.LOG;

  SimpleHttpProxyHandler(InetSocketAddress host) {
    this.host = host;
  }

  private static class Forwarder extends ChannelInboundHandlerAdapter {
    private final String uri;
    private final Channel client;

    private Forwarder(String uri, Channel client) {
      this.uri = uri;
      this.client = client;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      closeOnFlush(client);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      client.writeAndFlush(msg).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
          if (future.isSuccess()) {
            ctx.channel().read();
          } else {
            LOG.debug("Proxy failed. Cause: ", future.cause());
            future.channel().close();
          }
        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.debug("Proxy for " + uri + " failed. cause: ", cause);
      closeOnFlush(ctx.channel());
    }
  }

  @Override
  public void channelRead0
    (final ChannelHandlerContext ctx, final HttpRequest req) {
    uri = req.getUri();
    final Channel client = ctx.channel();
    Bootstrap proxiedServer = new Bootstrap()
      .group(client.eventLoop())
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new HttpRequestEncoder(), new Forwarder(uri, client));
        }
      });
    ChannelFuture f = proxiedServer.connect(host);
    proxiedChannel = f.channel();
    f.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          ctx.channel().pipeline().remove(HttpResponseEncoder.class);
          HttpRequest newReq = new DefaultFullHttpRequest(HTTP_1_1,
            req.getMethod(), req.getUri());
          newReq.headers().add(req.headers());
          newReq.headers().set(CONNECTION, Values.CLOSE);
          future.channel().writeAndFlush(newReq);
        } else {
          DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
            INTERNAL_SERVER_ERROR);
          resp.headers().set(CONNECTION, Values.CLOSE);
          LOG.info("Proxy " + uri + " failed. Cause: ", future.cause());
          ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
          client.close();
        }
      }
    });
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (proxiedChannel != null) {
      proxiedChannel.close();
      proxiedChannel = null;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.info("Proxy for " + uri + " failed. cause: ", cause);
    if (proxiedChannel != null) {
      proxiedChannel.close();
      proxiedChannel = null;
    }
    ctx.close();
  }

  private static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER)
        .addListener(ChannelFutureListener.CLOSE);
    }
  }
}
