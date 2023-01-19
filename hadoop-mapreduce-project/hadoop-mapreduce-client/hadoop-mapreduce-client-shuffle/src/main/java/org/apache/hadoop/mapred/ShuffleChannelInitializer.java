/*
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

package org.apache.hadoop.mapred;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.hadoop.security.ssl.SSLFactory;

import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;
import static org.apache.hadoop.mapred.ShuffleHandler.LOG;

public class ShuffleChannelInitializer extends ChannelInitializer<SocketChannel> {

  public static final int MAX_CONTENT_LENGTH = 1 << 16;

  private final ShuffleChannelHandlerContext handlerContext;
  private final SSLFactory sslFactory;


  public ShuffleChannelInitializer(ShuffleChannelHandlerContext ctx, SSLFactory sslFactory) {
    this.handlerContext = ctx;
    this.sslFactory = sslFactory;
  }

  @Override
  public void initChannel(SocketChannel ch) throws GeneralSecurityException, IOException {
    LOG.debug("ShuffleChannelInitializer init; channel='{}'", ch.id());

    ChannelPipeline pipeline = ch.pipeline();
    if (sslFactory != null) {
      pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
    }
    pipeline.addLast("http", new HttpServerCodec());
    pipeline.addLast("aggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH));
    pipeline.addLast("chunking", new ChunkedWriteHandler());

    // An EventExecutorGroup could be specified to run in a
    // different thread than an I/O thread so that the I/O thread
    // is not blocked by a time-consuming task:
    // https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html
    pipeline.addLast("shuffle", new ShuffleChannelHandler(handlerContext));

    pipeline.addLast(TIMEOUT_HANDLER,
        new ShuffleHandler.TimeoutHandler(handlerContext.connectionKeepAliveTimeOut));
    // TODO factor security manager into pipeline
    // TODO factor out encode/decode to permit binary shuffle
    // TODO factor out decode of index to permit alt. models
  }
}
