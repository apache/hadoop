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

/**
 * Implement the read-only WebHDFS API for fsimage.
 */
public class FSImageHandler extends SimpleChannelUpstreamHandler {
  public static final Log LOG = LogFactory.getLog(FSImageHandler.class);
  private final FSImageLoader loader;

  public FSImageHandler(FSImageLoader loader) throws IOException {
    this.loader = loader;
  }

  @Override
  public void messageReceived(
      ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    String op = getOp(e);
    try {
      String path = getPath(e);
      handleOperation(op, path, e);
    } catch (Exception ex) {
      notFoundResponse(e);
      LOG.warn(ex.getMessage());
    } finally {
      e.getFuture().addListener(ChannelFutureListener.CLOSE);
    }
  }

  /** return the op parameter in upper case */
  private String getOp(MessageEvent e) {
    Map<String, List<String>> parameters = getDecoder(e).getParameters();
    if (parameters.containsKey("op")) {
      return parameters.get("op").get(0).toUpperCase();
    } else {
      // return "" to avoid NPE
      return "";
    }
  }

  private String getPath(MessageEvent e) throws FileNotFoundException {
    String path = getDecoder(e).getPath();
    // trim "/webhdfs/v1" to keep compatibility with WebHDFS API
    if (path.startsWith("/webhdfs/v1/")) {
      return path.replaceFirst("/webhdfs/v1", "");
    } else {
      throw new FileNotFoundException("Path: " + path + " should " +
          "start with \"/webhdfs/v1/\"");
    }
  }

  private QueryStringDecoder getDecoder(MessageEvent e) {
    HttpRequest request = (HttpRequest) e.getMessage();
    return new QueryStringDecoder(request.getUri());
  }

  private void handleOperation(String op, String path, MessageEvent e)
      throws IOException {
    HttpRequest request = (HttpRequest) e.getMessage();
    HttpResponse response = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
        "application/json");
    String content = null;

    if (request.getMethod() == HttpMethod.GET){
      if (op.equals("GETFILESTATUS")) {
        content = loader.getFileStatus(path);
      } else if (op.equals("LISTSTATUS")) {
        content = loader.listStatus(path);
      } else if (op.equals("GETACLSTATUS")) {
        content = loader.getAclStatus(path);
      } else {
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
      }
    } else {
      // only HTTP GET is allowed since fsimage is read-only.
      response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    if (content != null) {
      HttpHeaders.setContentLength(response, content.length());
    }
    e.getChannel().write(response);

    if (content != null) {
      e.getChannel().write(content);
    }

    LOG.info(response.getStatus().getCode() + " method="
        + request.getMethod().getName() + " op=" + op + " target=" + path);
  }

  private void notFoundResponse(MessageEvent e) {
    HttpResponse response = new DefaultHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    e.getChannel().write(response);
  }
}
