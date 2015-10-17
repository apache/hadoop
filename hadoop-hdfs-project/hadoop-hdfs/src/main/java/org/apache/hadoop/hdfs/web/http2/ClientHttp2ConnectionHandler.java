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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPromiseAggregator;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * An {@link Http2ConnectionHandler} used at client side.
 */
@InterfaceAudience.Private
public class ClientHttp2ConnectionHandler extends Http2ConnectionHandler {

  private static final Log LOG = LogFactory
      .getLog(ClientHttp2ConnectionHandler.class);

  private static final Http2FrameLogger FRAME_LOGGER = new Http2FrameLogger(
      LogLevel.INFO, ClientHttp2ConnectionHandler.class);

  private AtomicInteger nextStreamId = new AtomicInteger(3);

  private final ClientHttp2EventListener listener;

  private ClientHttp2ConnectionHandler(Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder) {
    super(decoder, encoder);
    this.listener = (ClientHttp2EventListener) decoder.listener();
  }

  private int nextStreamId() {
    return nextStreamId.getAndAdd(2);
  }

  private void writeHeaders(ChannelHandlerContext ctx,
      Http2ConnectionEncoder encoder, final int streamId, Http2Headers headers,
      boolean endStream, ChannelPromise promise,
      final Promise<Http2StreamChannel> callback) {
    encoder.writeHeaders(ctx, streamId, headers, 0, endStream, promise)
        .addListener(new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              callback
                  .setSuccess(connection().stream(streamId)
                      .<Http2StreamChannel> getProperty(
                        listener.subChannelPropKey));
            } else {
              callback.setFailure(future.cause());
            }
          }
        });
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof StartHttp2StreamRequest) {
      final StartHttp2StreamRequest request = (StartHttp2StreamRequest) msg;
      final int streamId = nextStreamId();
      Http2ConnectionEncoder encoder = encoder();
      if (request.data.isReadable()) {
        ChannelPromiseAggregator aggregator =
            new ChannelPromiseAggregator(promise);
        ChannelPromise headerPromise = ctx.newPromise();
        aggregator.add(headerPromise);
        writeHeaders(ctx, encoder, streamId, request.headers, false,
          headerPromise, request.promise);
        ChannelPromise dataPromise = ctx.newPromise();
        aggregator.add(dataPromise);
        encoder.writeData(ctx, streamId, request.data, 0, request.endStream,
          dataPromise);
      } else {
        writeHeaders(ctx, encoder, streamId, request.headers,
          request.endStream, promise, request.promise);
      }
    } else {
      throw new UnsupportedMessageTypeException(msg,
          StartHttp2StreamRequest.class);
    }
  }

  public int numActiveStreams() {
    return listener.numActiveStreams();
  }

  public int currentStreamId() {
    return nextStreamId.get();
  }

  private static final Http2Util.Http2ConnectionHandlerFactory<ClientHttp2ConnectionHandler> FACTORY =
      new Http2Util.Http2ConnectionHandlerFactory<ClientHttp2ConnectionHandler>() {

        @Override
        public ClientHttp2ConnectionHandler create(
            Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder) {
          return new ClientHttp2ConnectionHandler(decoder, encoder);
        }
      };

  public static ClientHttp2ConnectionHandler create(Channel channel,
      Configuration conf) throws Http2Exception {
    Http2Connection conn = new DefaultHttp2Connection(false);
    ClientHttp2EventListener listener =
        new ClientHttp2EventListener(channel, conn);
    return Http2Util.create(conf, conn, listener, FACTORY,
      LOG.isDebugEnabled() ? FRAME_LOGGER : null);
  }
}
