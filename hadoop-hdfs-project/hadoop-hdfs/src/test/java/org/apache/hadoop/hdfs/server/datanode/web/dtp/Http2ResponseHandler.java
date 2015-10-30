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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http2.HttpUtil;
import io.netty.util.concurrent.Promise;

import java.util.HashMap;
import java.util.Map;

public class Http2ResponseHandler extends
    SimpleChannelInboundHandler<FullHttpResponse> {

  private Map<Integer, Promise<FullHttpResponse>> streamId2Promise =
      new HashMap<>();

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg)
      throws Exception {
    Integer streamId =
        msg.headers().getInt(HttpUtil.ExtensionHeaderNames.STREAM_ID.text());
    if (streamId == null) {
      System.err.println("HttpResponseHandler unexpected message received: "
          + msg);
      return;
    }
    if (streamId.intValue() == 1) {
      // this is the upgrade response message, just ignore it.
      return;
    }
    Promise<FullHttpResponse> promise;
    synchronized (this) {
      promise = streamId2Promise.get(streamId);
    }
    if (promise == null) {
      System.err.println("Message received for unknown stream id " + streamId);
    } else {
      // Do stuff with the message (for now just print it)
      promise.setSuccess(msg.retain());

    }
  }

  public void put(Integer streamId, Promise<FullHttpResponse> promise) {
    streamId2Promise.put(streamId, promise);
  }
}