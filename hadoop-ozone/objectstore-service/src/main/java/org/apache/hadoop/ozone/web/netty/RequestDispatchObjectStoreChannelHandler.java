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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.apache.hadoop.io.IOUtils;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Future;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Object Store Netty channel pipeline handler that handles an inbound
 * {@link HttpRequest} by dispatching it to the Object Store Jersey container.
 * The handler establishes 2 sets of connected piped streams: one for inbound
 * request handling and another for outbound response handling.  The relevant
 * ends of these pipes are handed off to the Jersey application dispatch and the
 * next channel handler, which is responsible for streaming in the inbound
 * request body and flushing out the response body.
 */
public final class RequestDispatchObjectStoreChannelHandler
    extends ObjectStoreChannelHandler<HttpRequest> {

  private final ObjectStoreJerseyContainer jerseyContainer;

  private PipedInputStream reqIn;
  private PipedOutputStream reqOut;
  private PipedInputStream respIn;
  private PipedOutputStream respOut;

  /**
   * Creates a new RequestDispatchObjectStoreChannelHandler.
   *
   * @param jerseyContainer Object Store application Jersey container for
   * request dispatch
   */
  public RequestDispatchObjectStoreChannelHandler(
      ObjectStoreJerseyContainer jerseyContainer) {
    this.jerseyContainer = jerseyContainer;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest nettyReq)
      throws Exception {
    LOG.trace("begin RequestDispatchObjectStoreChannelHandler channelRead0, " +
        "ctx = {}, nettyReq = {}", ctx, nettyReq);
    if (!nettyReq.getDecoderResult().isSuccess()) {
      sendErrorResponse(ctx, BAD_REQUEST);
      return;
    }

    this.reqIn = new PipedInputStream();
    this.reqOut = new PipedOutputStream(reqIn);
    this.respIn = new PipedInputStream();
    this.respOut = new PipedOutputStream(respIn);

    if (HttpHeaders.is100ContinueExpected(nettyReq)) {
      LOG.trace("Sending continue response.");
      ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
    }

    Future<HttpResponse> nettyResp = this.jerseyContainer.dispatch(nettyReq,
        reqIn, respOut);

    ctx.pipeline().replace(this,
        RequestContentObjectStoreChannelHandler.class.getSimpleName(),
        new RequestContentObjectStoreChannelHandler(nettyReq, nettyResp,
            reqOut, respIn, jerseyContainer));

    LOG.trace("end RequestDispatchObjectStoreChannelHandler channelRead0, " +
        "ctx = {}, nettyReq = {}", ctx, nettyReq);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    super.exceptionCaught(ctx, cause);
    IOUtils.cleanupWithLogger(null, this.reqIn, this.reqOut, this.respIn,
        this.respOut);
  }
}
