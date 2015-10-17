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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.timeout.TimeoutException;
import io.netty.util.AsciiString;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.http2.ClientHttp2ConnectionHandler;
import org.apache.hadoop.hdfs.web.http2.Http2DataReceiver;
import org.apache.hadoop.hdfs.web.http2.Http2StreamBootstrap;
import org.apache.hadoop.hdfs.web.http2.Http2StreamChannel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.ByteStreams;

public class TestDtpHttp2 {

  private static final Configuration CONF = WebHdfsTestUtil.createConf();

  private static MiniDFSCluster CLUSTER;

  private static final EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

  private static Channel CHANNEL;

  private static Http2StreamChannel STREAM;

  @BeforeClass
  public static void setUp() throws IOException, URISyntaxException,
      TimeoutException, InterruptedException, ExecutionException {
    CLUSTER = new MiniDFSCluster.Builder(CONF).numDataNodes(1).build();
    CLUSTER.waitActive();

    CHANNEL =
        new Bootstrap()
            .group(WORKER_GROUP)
            .channel(NioSocketChannel.class)
            .remoteAddress("127.0.0.1",
              CLUSTER.getDataNodes().get(0).getInfoPort())
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(
                  ClientHttp2ConnectionHandler.create(ch, CONF));
              }
            }).connect().syncUninterruptibly().channel();
    STREAM =
        new Http2StreamBootstrap()
            .channel(CHANNEL)
            .headers(
              new DefaultHttp2Headers().method(
                new AsciiString(HttpMethod.GET.name())).path(
                new AsciiString("/"))).endStream(true)
            .handler(new ChannelInitializer<Channel>() {

              @Override
              protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(new Http2DataReceiver());
              }
            }).connect().syncUninterruptibly().get();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (STREAM != null) {
      STREAM.close();
    }
    if (CHANNEL != null) {
      CHANNEL.close().syncUninterruptibly();
    }
    WORKER_GROUP.shutdownGracefully();
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
  }

  @Test
  public void test() throws InterruptedException, ExecutionException,
      IOException {
    Http2DataReceiver receiver = STREAM.pipeline().get(Http2DataReceiver.class);
    assertEquals(HttpResponseStatus.OK.codeAsText(), receiver.waitForResponse()
        .status());
    assertEquals("HTTP/2 DTP",
      new String(ByteStreams.toByteArray(receiver.content()),
          StandardCharsets.UTF_8));
  }
}
