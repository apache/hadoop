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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 * A {@link Http2StreamBootstrap} that makes it easy to bootstrap a
 * {@link Http2StreamChannel} to use for clients.
 * <p>
 * Call connect when you finish set up other things to establish an
 * {@link Http2StreamChannel}.
 */
@InterfaceAudience.Private
public class Http2StreamBootstrap {
  private Channel channel;

  private Http2Headers headers;

  private ByteBuf data = Unpooled.EMPTY_BUFFER;

  private boolean endStream;

  private ChannelHandler handler;

  /**
   * Set the {@link Channel} this HTTP/2 stream is running on.
   */
  public Http2StreamBootstrap channel(Channel channel) {
    this.channel = channel;
    return this;
  }

  /**
   * Set the request headers.
   */
  public Http2StreamBootstrap headers(Http2Headers headers) {
    this.headers = headers;
    return this;
  }

  /**
   * Set the request data.
   * <p>
   * This is used to avoid one context-switch if you only need to send a small
   * piece of data.
   */
  public Http2StreamBootstrap data(ByteBuf data) {
    this.data = data;
    return this;
  }

  /**
   * Set whether there is no data after the headers being sent.
   * <p>
   * Default is <tt>false </tt>which means you could still send data using the
   * returned {@link Http2StreamChannel}.
   */
  public Http2StreamBootstrap endStream(boolean endStream) {
    this.endStream = endStream;
    return this;
  }

  /**
   * The {@link ChannelHandler} which is used to serve request.
   * <p>
   * Typically, you should use a {@link ChannelInitializer} here.
   */
  public Http2StreamBootstrap handler(ChannelHandler handler) {
    this.handler = handler;
    return this;
  }

  /**
   * Establish the {@link Http2StreamChannel}. You can get it with the returned
   * {@link Future}.
   */
  public Future<Http2StreamChannel> connect() {
    Preconditions.checkNotNull(headers);
    Preconditions.checkNotNull(handler);
    final Promise<Http2StreamChannel> registeredPromise =
        channel.eventLoop().<Http2StreamChannel> newPromise();

    final StartHttp2StreamRequest request =
        new StartHttp2StreamRequest(headers, data, endStream, channel
            .eventLoop().<Http2StreamChannel> newPromise()
            .addListener(new FutureListener<Http2StreamChannel>() {

              @Override
              public void operationComplete(Future<Http2StreamChannel> future)
                  throws Exception {
                if (future.isSuccess()) {
                  final Http2StreamChannel subChannel = future.get();
                  subChannel.pipeline().addFirst(handler);
                  channel.eventLoop().register(subChannel)
                      .addListener(new ChannelFutureListener() {

                        @Override
                        public void operationComplete(ChannelFuture future)
                            throws Exception {
                          if (future.isSuccess()) {
                            subChannel.config().setAutoRead(true);
                            registeredPromise.setSuccess(subChannel);
                          } else {
                            registeredPromise.setFailure(future.cause());
                          }
                        }
                      });
                } else {
                  registeredPromise.setFailure(future.cause());
                }
              }

            }));
    EventLoop eventLoop = channel.eventLoop();
    if (eventLoop.inEventLoop()) {
      channel.writeAndFlush(request);
    } else {
      channel.eventLoop().execute(new Runnable() {

        @Override
        public void run() {
          channel.writeAndFlush(request);
        }
      });
    }

    return registeredPromise;
  }
}
