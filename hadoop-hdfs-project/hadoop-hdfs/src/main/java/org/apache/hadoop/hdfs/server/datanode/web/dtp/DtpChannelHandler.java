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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.web.http2.LastHttp2Message;

/**
 * A dummy handler that just write back a string message.
 */
@InterfaceAudience.Private
public class DtpChannelHandler extends
    SimpleChannelInboundHandler<Http2Headers> {

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Http2Headers msg)
      throws Exception {
    ctx.write(new DefaultHttp2Headers().status(HttpResponseStatus.OK
        .codeAsText()));
    ctx.write(ctx.alloc().buffer()
        .writeBytes("HTTP/2 DTP".getBytes(StandardCharsets.UTF_8)));
    ctx.writeAndFlush(LastHttp2Message.get());
  }
}
