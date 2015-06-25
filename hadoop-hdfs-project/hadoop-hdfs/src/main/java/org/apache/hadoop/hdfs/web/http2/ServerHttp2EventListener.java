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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Connection.PropertyKey;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2EventAdapter;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * An HTTP/2 FrameListener and EventListener to manage
 * {@link Http2StreamChannel}s.
 * <p>
 * We do not handle onRstStreamRead here, a stream that being reset will also
 * call onStreamClosed. The upper layer should not rely on a reset event.
 */
@InterfaceAudience.Private
public class ServerHttp2EventListener extends Http2EventAdapter {

  private final Channel parentChannel;

  private final ChannelInitializer<Http2StreamChannel> subChannelInitializer;

  private final Http2Connection conn;

  private final PropertyKey subChannelPropKey;

  public ServerHttp2EventListener(Channel parentChannel, Http2Connection conn,
      ChannelInitializer<Http2StreamChannel> subChannelInitializer) {
    this.parentChannel = parentChannel;
    this.conn = conn;
    this.subChannelInitializer = subChannelInitializer;
    this.subChannelPropKey = conn.newKey();
  }

  @Override
  public void onStreamActive(final Http2Stream stream) {
    Http2StreamChannel subChannel =
        new Http2StreamChannel(parentChannel, stream);
    stream.setProperty(subChannelPropKey, subChannel);
    subChannel.pipeline().addFirst(subChannelInitializer);
    parentChannel.eventLoop().register(subChannel)
        .addListener(new FutureListener<Void>() {

          @Override
          public void operationComplete(Future<Void> future) throws Exception {
            if (!future.isSuccess()) {
              stream.removeProperty(subChannelPropKey);
            }
          }

        });

  }

  @Override
  public void onStreamClosed(Http2Stream stream) {
    Http2StreamChannel subChannel = stream.removeProperty(subChannelPropKey);
    if (subChannel != null) {
      subChannel.close();
    }
  }

  private Http2StreamChannel getSubChannel(int streamId) throws Http2Exception {
    Http2StreamChannel subChannel =
        conn.stream(streamId).getProperty(subChannelPropKey);
    if (subChannel == null) {
      throw Http2Exception.streamError(streamId, Http2Error.INTERNAL_ERROR,
        "No sub channel found");
    }
    return subChannel;
  }

  private void writeInbound(int streamId, Object msg, boolean endOfStream)
      throws Http2Exception {
    Http2StreamChannel subChannel = getSubChannel(streamId);
    subChannel.writeInbound(msg);
    if (endOfStream) {
      subChannel.writeInbound(LastHttp2Message.get());
    }
    if (subChannel.config().isAutoRead()) {
      subChannel.read();
    }

  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int padding, boolean endOfStream)
      throws Http2Exception {
    writeInbound(streamId, headers, endOfStream);
  }

  @Override
  public void onHeadersRead(ChannelHandlerContext ctx, int streamId,
      Http2Headers headers, int streamDependency, short weight,
      boolean exclusive, int padding, boolean endOfStream)
      throws Http2Exception {
    onHeadersRead(ctx, streamId, headers, padding, endOfStream);
  }

  @Override
  public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data,
      int padding, boolean endOfStream) throws Http2Exception {
    int pendingBytes = data.readableBytes() + padding;
    writeInbound(streamId, data.retain(), endOfStream);
    return pendingBytes;
  }
}
