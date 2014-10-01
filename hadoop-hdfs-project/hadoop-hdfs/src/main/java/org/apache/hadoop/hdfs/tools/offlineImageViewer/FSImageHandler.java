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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import javax.management.Query;

/**
 * Implement the read-only WebHDFS API for fsimage.
 */
class FSImageHandler extends SimpleChannelUpstreamHandler {
  public static final Log LOG = LogFactory.getLog(FSImageHandler.class);
  private final FSImageLoader image;

  FSImageHandler(FSImageLoader image) throws IOException {
    this.image = image;
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    ChannelFuture future = e.getFuture();
    try {
      future = handleOperation(e);
    } finally {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private ChannelFuture handleOperation(MessageEvent e)
      throws IOException {
    HttpRequest request = (HttpRequest) e.getMessage();
    HttpResponse response = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");

    if (request.getMethod() != HttpMethod.GET) {
      response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
      return e.getChannel().write(response);
    }

    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    final String op = getOp(decoder);

    String content;
    String path = null;
    try {
      path = getPath(decoder);
      if ("GETFILESTATUS".equals(op)) {
        content = image.getFileStatus(path);
      } else if ("LISTSTATUS".equals(op)) {
        content = image.listStatus(path);
      } else if ("GETACLSTATUS".equals(op)) {
        content = image.getAclStatus(path);
      } else {
        throw new IllegalArgumentException("Invalid value for webhdfs parameter" + " \"op\"");
      }
    } catch (IllegalArgumentException ex) {
      response.setStatus(HttpResponseStatus.BAD_REQUEST);
      content = JsonUtil.toJsonString(ex);
    } catch (FileNotFoundException ex) {
      response.setStatus(HttpResponseStatus.NOT_FOUND);
      content = JsonUtil.toJsonString(ex);
    } catch (Exception ex) {
      content = JsonUtil.toJsonString(ex);
    }

    HttpHeaders.setContentLength(response, content.length());
    e.getChannel().write(response);
    ChannelFuture future = e.getChannel().write(content);

    LOG.info(response.getStatus().getCode() + " method="
        + request.getMethod().getName() + " op=" + op + " target=" + path);

    return future;
  }

  private static String getOp(QueryStringDecoder decoder) {
    Map<String, List<String>> parameters = decoder.getParameters();
    return parameters.containsKey("op")
            ? parameters.get("op").get(0).toUpperCase() : null;
  }

  private static String getPath(QueryStringDecoder decoder)
          throws FileNotFoundException {
    String path = decoder.getPath();
    if (path.startsWith("/webhdfs/v1/")) {
      return path.substring(11);
    } else {
      throw new FileNotFoundException("Path: " + path + " should " +
              "start with \"/webhdfs/v1/\"");
    }
  }
}
