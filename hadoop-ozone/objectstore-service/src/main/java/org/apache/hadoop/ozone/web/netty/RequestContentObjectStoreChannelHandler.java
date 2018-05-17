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
package org.apache.hadoop.ozone.web.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;

/**
 * Object Store Netty channel pipeline handler that handles inbound
 * {@link HttpContent} fragments for the request body by sending the bytes into
 * the pipe so that the application dispatch thread can read it.
 * After receiving the {@link LastHttpContent}, this handler also flushes the
 * response.
 */
public final class RequestContentObjectStoreChannelHandler
    extends ObjectStoreChannelHandler<HttpContent> {

  private final HttpRequest nettyReq;
  private final Future<HttpResponse> nettyResp;
  private final OutputStream reqOut;
  private final InputStream respIn;
  private ObjectStoreJerseyContainer jerseyContainer;

  /**
   * Creates a new RequestContentObjectStoreChannelHandler.
   *
   * @param nettyReq HTTP request
   * @param nettyResp asynchronous HTTP response
   * @param reqOut output stream for writing request body
   * @param respIn input stream for reading response body
   * @param jerseyContainer jerseyContainer to handle the request
   */
  public RequestContentObjectStoreChannelHandler(HttpRequest nettyReq,
      Future<HttpResponse> nettyResp, OutputStream reqOut, InputStream respIn,
      ObjectStoreJerseyContainer jerseyContainer) {
    this.nettyReq = nettyReq;
    this.nettyResp = nettyResp;
    this.reqOut = reqOut;
    this.respIn = respIn;
    this.jerseyContainer = jerseyContainer;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpContent content)
      throws Exception {
    LOG.trace(
        "begin RequestContentObjectStoreChannelHandler channelRead0, " +
        "ctx = {}, content = {}", ctx, content);
    content.content().readBytes(this.reqOut, content.content().readableBytes());
    if (content instanceof LastHttpContent) {
      IOUtils.cleanupWithLogger(null, this.reqOut);
      ctx.write(this.nettyResp.get());
      ChannelFuture respFuture = ctx.writeAndFlush(new ChunkedStream(
          this.respIn));
      respFuture.addListener(new CloseableCleanupListener(this.respIn));
      if (!HttpHeaders.isKeepAlive(this.nettyReq)) {
        respFuture.addListener(ChannelFutureListener.CLOSE);
      } else {
        respFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            // Notify client this is the last content for current request.
            ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            // Reset the pipeline handler for next request to reuses the
            // same connection.
            RequestDispatchObjectStoreChannelHandler h =
                new RequestDispatchObjectStoreChannelHandler(jerseyContainer);
            ctx.pipeline().replace(ctx.pipeline().last(),
                RequestDispatchObjectStoreChannelHandler.class.getSimpleName(),
                h);
          }
        });
      }
    }
    LOG.trace(
        "end RequestContentObjectStoreChannelHandler channelRead0, " +
        "ctx = {}, content = {}", ctx, content);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    super.exceptionCaught(ctx, cause);
    IOUtils.cleanupWithLogger(null, this.reqOut, this.respIn);
  }
}
