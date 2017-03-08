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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Abstract base class for the multiple Netty channel handlers used in the
 * Object Store Netty channel pipeline.
 */
abstract class ObjectStoreChannelHandler<T>
    extends SimpleChannelInboundHandler<T> {

  /** Log usable in all subclasses. */
  protected static final Logger LOG =
      LoggerFactory.getLogger(ObjectStoreChannelHandler.class);

  /**
   * Handles uncaught exceptions in the channel pipeline by sending an internal
   * server error response if the channel is still active.
   *
   * @param ctx ChannelHandlerContext to receive response
   * @param cause Throwable that was unhandled in the channel pipeline
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Unexpected exception in Netty pipeline.", cause);
    if (ctx.channel().isActive()) {
      sendErrorResponse(ctx, INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Sends an error response.  This method is used when an unexpected error is
   * encountered within the channel pipeline, outside of the actual Object Store
   * application.  It always closes the connection, because we can't in general
   * know the state of the connection when these errors occur, so attempting to
   * keep the connection alive could be unpredictable.
   *
   * @param ctx ChannelHandlerContext to receive response
   * @param status HTTP response status
   */
  protected static void sendErrorResponse(ChannelHandlerContext ctx,
      HttpResponseStatus status) {
    HttpResponse nettyResp = new DefaultFullHttpResponse(HTTP_1_1, status);
    nettyResp.headers().set(CONTENT_LENGTH, 0);
    nettyResp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(nettyResp).addListener(ChannelFutureListener.CLOSE);
  }
}
