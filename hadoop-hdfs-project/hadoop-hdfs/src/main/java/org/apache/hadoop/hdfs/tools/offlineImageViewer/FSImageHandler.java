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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import static io.netty.handler.codec.http.HttpVersion.*;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;


/**
 * Implement the read-only WebHDFS API for fsimage.
 */
class FSImageHandler extends SimpleChannelInboundHandler<HttpRequest> {
  public static final Log LOG = LogFactory.getLog(FSImageHandler.class);
  private final FSImageLoader image;
  private final ChannelGroup activeChannels;

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    activeChannels.add(ctx.channel());
  }

  FSImageHandler(FSImageLoader image, ChannelGroup activeChannels) throws IOException {
    this.image = image;
    this.activeChannels = activeChannels;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest request)
          throws Exception {
    if (request.getMethod() != HttpMethod.GET) {
      DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1,
        METHOD_NOT_ALLOWED);
      resp.headers().set("Connection", "close");
      ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
      return;
    }

    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    final String op = getOp(decoder);

    final String content;
    String path = getPath(decoder);
    if ("GETFILESTATUS".equals(op)) {
      content = image.getFileStatus(path);
    } else if ("LISTSTATUS".equals(op)) {
      content = image.listStatus(path);
    } else if ("GETACLSTATUS".equals(op)) {
      content = image.getAclStatus(path);
    } else {
      throw new IllegalArgumentException("Invalid value for webhdfs parameter" + " \"op\"");
    }

    LOG.info("op=" + op + " target=" + path);

    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(
            HTTP_1_1, HttpResponseStatus.OK,
            Unpooled.wrappedBuffer(content.getBytes()));
    resp.headers().set("Content-Type", "application/json");
    resp.headers().set("Content-Length", resp.content().readableBytes());
    resp.headers().set("Connection", "close");
    ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
          throws Exception {
    Exception e = cause instanceof Exception ? (Exception) cause : new
      Exception(cause);
    final String output = JsonUtil.toJsonString(e);
    ByteBuf content = Unpooled.wrappedBuffer(output.getBytes());
    final DefaultFullHttpResponse resp = new DefaultFullHttpResponse(
            HTTP_1_1, INTERNAL_SERVER_ERROR, content);

    resp.headers().set("Content-Type", "application/json");
    if (e instanceof IllegalArgumentException) {
      resp.setStatus(BAD_REQUEST);
    } else if (e instanceof FileNotFoundException) {
      resp.setStatus(NOT_FOUND);
    }

    resp.headers().set("Content-Length", resp.content().readableBytes());
    resp.headers().set("Connection", "close");
    ctx.write(resp).addListener(ChannelFutureListener.CLOSE);
  }

  private static String getOp(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.parameters();
    return parameters.containsKey("op")
            ? parameters.get("op").get(0).toUpperCase() : null;
  }

  private static String getPath(QueryStringDecoder decoder)
          throws FileNotFoundException {
    String path = decoder.path();
    if (path.startsWith("/webhdfs/v1/")) {
      return path.substring(11);
    } else {
      throw new FileNotFoundException("Path: " + path + " should " +
              "start with \"/webhdfs/v1/\"");
    }
  }
}
