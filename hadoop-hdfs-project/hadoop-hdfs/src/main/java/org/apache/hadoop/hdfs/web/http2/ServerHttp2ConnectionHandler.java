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
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.logging.LogLevel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * An {@link Http2ConnectionHandler} used at server side.
 */
@InterfaceAudience.Private
public class ServerHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Log LOG = LogFactory
      .getLog(ServerHttp2ConnectionHandler.class);

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ServerHttp2ConnectionHandler.class);

  private ServerHttp2ConnectionHandler(Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder) {
    super(decoder, encoder);
  }

  private static final Http2Util.Http2ConnectionHandlerFactory<ServerHttp2ConnectionHandler> FACTORY =
      new Http2Util.Http2ConnectionHandlerFactory<ServerHttp2ConnectionHandler>() {

        @Override
        public ServerHttp2ConnectionHandler create(
            Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder) {
          return new ServerHttp2ConnectionHandler(decoder, encoder);
        }
      };

  /**
   * Create and initialize an {@link ServerHttp2ConnectionHandler}.
   * @param channel
   * @param initializer
   * @param conf
   * @return the initialized {@link ServerHttp2ConnectionHandler}
   * @throws Http2Exception
   */
  public static ServerHttp2ConnectionHandler create(Channel channel,
      ChannelInitializer<Http2StreamChannel> initializer, Configuration conf)
      throws Http2Exception {
    Http2Connection conn = new DefaultHttp2Connection(true);
    ServerHttp2EventListener listener =
        new ServerHttp2EventListener(channel, conn, initializer);
    return Http2Util.create(conf, conn, listener, FACTORY,
      LOG.isDebugEnabled() ? FRAME_LOGGER : null);
  }
}
