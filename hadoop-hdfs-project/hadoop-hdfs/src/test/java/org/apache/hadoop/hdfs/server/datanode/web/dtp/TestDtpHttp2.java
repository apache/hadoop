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
package org.apache.hadoop.hdfs.server.datanode.web.dtp;

import static org.junit.Assert.assertEquals;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DelegatingDecompressorFrameListener;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.timeout.TimeoutException;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDtpHttp2 {

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, TestDtpHttp2.class);

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  private static Http2ResponseHandler RESPONSE_HANDLER;

  @BeforeClass
  public static void setUp() throws IOException, URISyntaxException,
      TimeoutException {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    RESPONSE_HANDLER = new Http2ResponseHandler();
    Bootstrap bootstrap =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .remoteAddress("127.0.0.1",
              CLUSTER.getDataNodes().get(0).getInfoPort())
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                Http2Connection connection = new DefaultHttp2Connection(false);
                Http2ConnectionHandler connectionHandler =
                    new HttpToHttp2ConnectionHandler(connection, frameReader(),
                        frameWriter(), new DelegatingDecompressorFrameListener(
                            connection, new InboundHttp2ToHttpAdapter.Builder(
                                connection).maxContentLength(Integer.MAX_VALUE)
                                .propagateSettings(true).build()));
                ch.pipeline().addLast(connectionHandler, RESPONSE_HANDLER);
              }
            });
    CHANNEL = bootstrap.connect().syncUninterruptibly().channel();

  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (CHANNEL != null) {
      CHANNEL.close().syncUninterruptibly();
    }
    WORKER_GROUP.shutdownGracefully();
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  private static Http2FrameReader frameReader() {
    return new Http2InboundFrameLogger(new DefaultHttp2FrameReader(),
        FRAME_LOGGER);
  }

  private static Http2FrameWriter frameWriter() {
    return new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(),
        FRAME_LOGGER);
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    int streamId = 3;
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    request.headers().add(HttpUtil.ExtensionHeaderNames.STREAM_ID.text(),
      streamId);
    Promise<FullHttpResponse> promise = CHANNEL.eventLoop().newPromise();
    synchronized (RESPONSE_HANDLER) {
      CHANNEL.writeAndFlush(request);
      RESPONSE_HANDLER.put(streamId, promise);
    }
    assertEquals(HttpResponseStatus.OK, promise.get().status());
    ByteBuf content = promise.get().content();
    assertEquals("HTTP/2 DTP", content.toString(StandardCharsets.UTF_8));
  }
}
