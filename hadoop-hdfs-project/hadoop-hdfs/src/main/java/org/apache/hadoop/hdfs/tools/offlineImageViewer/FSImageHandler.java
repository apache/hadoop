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

import java.io.IOException;

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
    HttpRequest request = (HttpRequest) e.getMessage();
    if (request.getMethod() == HttpMethod.GET){
      String uri = request.getUri();
      QueryStringDecoder decoder = new QueryStringDecoder(uri);

      String op = "null";
      if (decoder.getParameters().containsKey("op")) {
        op = decoder.getParameters().get("op").get(0).toUpperCase();
      }
      HttpResponse response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      String json = null;

      if (op.equals("LISTSTATUS")) {
        try {
          json = loader.listStatus(decoder.getPath());
          response.setStatus(HttpResponseStatus.OK);
          response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
              "application/json");
          HttpHeaders.setContentLength(response, json.length());
        } catch (Exception ex) {
          LOG.warn(ex.getMessage());
          response.setStatus(HttpResponseStatus.NOT_FOUND);
        }
      } else {
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
      }

      e.getChannel().write(response);
      if (json != null) {
        e.getChannel().write(json);
      }
      LOG.info(response.getStatus().getCode() + " method=GET op=" + op
          + " target=" + decoder.getPath());
    } else {
      // only HTTP GET is allowed since fsimage is read-only.
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.METHOD_NOT_ALLOWED);
      e.getChannel().write(response);
      LOG.info(response.getStatus().getCode() + " method="
          + request.getMethod().getName());
    }
    e.getFuture().addListener(ChannelFutureListener.CLOSE);
  }
}
