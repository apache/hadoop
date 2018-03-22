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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;

class HdfsWriter extends SimpleChannelInboundHandler<HttpContent> {
  private final DFSClient client;
  private final OutputStream out;
  private final DefaultHttpResponse response;
  private static final Log LOG = WebHdfsHandler.LOG;

  HdfsWriter(DFSClient client, OutputStream out, DefaultHttpResponse response) {
    this.client = client;
    this.out = out;
    this.response = response;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpContent chunk)
    throws IOException {
    chunk.content().readBytes(out, chunk.content().readableBytes());
    if (chunk instanceof LastHttpContent) {
      try {
        releaseDfsResourcesAndThrow();
        response.headers().set(CONNECTION, CLOSE);
        ctx.write(response).addListener(ChannelFutureListener.CLOSE);
      } catch (Exception cause) {
        exceptionCaught(ctx, cause);
      }
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    releaseDfsResources();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    releaseDfsResources();
    DefaultHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
    resp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    if (LOG != null && LOG.isDebugEnabled()) {
      LOG.debug("Exception in channel handler ", cause);
    }
  }

  private void releaseDfsResources() {
    IOUtils.cleanup(LOG, out);
    IOUtils.cleanup(LOG, client);
  }

  private void releaseDfsResourcesAndThrow() throws Exception {
    out.close();
    client.close();
  }
}
