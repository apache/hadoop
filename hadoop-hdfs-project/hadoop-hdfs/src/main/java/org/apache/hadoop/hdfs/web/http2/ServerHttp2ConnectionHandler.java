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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2InboundFrameLogger;
import io.netty.handler.codec.http2.Http2OutboundFrameLogger;
import io.netty.handler.logging.LogLevel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * An {@link Http2ConnectionHandler} used at server side.
 */
@InterfaceAudience.Private
public class ServerHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Log LOG = LogFactory
      .getLog(ServerHttp2ConnectionHandler.class);

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ServerHttp2ConnectionHandler.class);

  private ServerHttp2ConnectionHandler(Http2Connection connection,
      Http2FrameReader frameReader, Http2FrameWriter frameWriter,
      Http2FrameListener listener) {
    super(connection, frameReader, frameWriter, listener);
  }

  /**
   * Create and initialize an {@link ServerHttp2ConnectionHandler}.
   * @param channel
   * @param initializer
   * @param verbose whether to log inbound and outbound HTTP/2 messages
   * @return the initialized {@link ServerHttp2ConnectionHandler}
   */
  public static ServerHttp2ConnectionHandler create(Channel channel,
      ChannelInitializer<Http2StreamChannel> initializer) {
    Http2Connection conn = new DefaultHttp2Connection(true);
    ServerHttp2EventListener listener =
        new ServerHttp2EventListener(channel, conn, initializer);
    conn.addListener(listener);
    Http2FrameReader frameReader;
    Http2FrameWriter frameWriter;
    if (LOG.isDebugEnabled()) {
      frameReader =
          new Http2InboundFrameLogger(new DefaultHttp2FrameReader(),
              FRAME_LOGGER);
      frameWriter =
          new Http2OutboundFrameLogger(new DefaultHttp2FrameWriter(),
              FRAME_LOGGER);
    } else {
      frameReader = new DefaultHttp2FrameReader();
      frameWriter = new DefaultHttp2FrameWriter();
    }
    return new ServerHttp2ConnectionHandler(conn, frameReader, frameWriter,
        listener);
  }
}
