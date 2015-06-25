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

import static org.junit.Assert.assertEquals;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.ErrorCode;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.PriorityFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.FuturePromise;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHttp2Server extends AbstractTestHttp2Server {

  private final AtomicInteger handlerClosedCount = new AtomicInteger(0);

  private final class HelloWorldHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
      if (msg instanceof Http2Headers) {
        ctx.writeAndFlush(new DefaultHttp2Headers()
            .status(HttpResponseStatus.OK.codeAsText()));
      } else {
        ctx.writeAndFlush(ReferenceCountUtil.retain(msg));
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      handlerClosedCount.incrementAndGet();
    }

  }

  @Override
  protected Channel initServer() {
    return new ServerBootstrap().group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(
              ServerHttp2ConnectionHandler.create(ch,
                new ChannelInitializer<Http2StreamChannel>() {

                  @Override
                  protected void initChannel(Http2StreamChannel ch)
                      throws Exception {
                    ch.pipeline().addLast(new HelloWorldHandler());
                  }
                }));
          }

        }).bind(0).syncUninterruptibly().channel();
  }

  @Before
  public void setUp() throws Exception {
    start();
  }

  @After
  public void tearDown() throws Exception {
    stop();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException,
      IOException {
    HttpFields fields = new HttpFields();
    fields.put(HttpHeader.C_METHOD, HttpMethod.GET.asString());
    fields.put(HttpHeader.C_PATH, "/");
    FuturePromise<Stream> streamPromise = new FuturePromise<>();
    StreamListener listener = new StreamListener();
    session.newStream(new HeadersFrame(1, new MetaData(
        org.eclipse.jetty.http.HttpVersion.HTTP_2, fields), new PriorityFrame(
        1, 0, 1, false), false), streamPromise, listener);
    Stream stream = streamPromise.get();
    stream.data(
      new DataFrame(stream.getId(), ByteBuffer.wrap("Hello World"
          .getBytes(StandardCharsets.UTF_8)), true), new Callback.Adapter());
    assertEquals("Hello World", new String(listener.getData(),
        StandardCharsets.UTF_8));

    streamPromise = new FuturePromise<>();
    listener = new StreamListener();
    session.newStream(new HeadersFrame(1, new MetaData(
        org.eclipse.jetty.http.HttpVersion.HTTP_2, fields), new PriorityFrame(
        1, 0, 1, false), false), streamPromise, listener);
    stream = streamPromise.get();
    stream.reset(new ResetFrame(stream.getId(), ErrorCode.NO_ERROR.code),
      new Callback.Adapter());
    Thread.sleep(1000);
    assertEquals(2, handlerClosedCount.get());
  }
}
