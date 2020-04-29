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
package org.apache.hadoop.hdfs.server.datanode.web;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

import java.util.Map;

import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter.HttpInteraction;
import org.slf4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;


/**
 * Netty handler that integrates with the {@link RestCsrfPreventionFilter}.  If
 * the filter determines that the request is allowed, then this handler forwards
 * the request to the next handler in the Netty pipeline.  Otherwise, this
 * handler drops the request and immediately sends an HTTP 400 response.
 */
@InterfaceAudience.Private
@Sharable
final class RestCsrfPreventionFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest> {

  private static final Logger LOG = DatanodeHttpServer.LOG;

  private final RestCsrfPreventionFilter restCsrfPreventionFilter;

  /**
   * Creates a new RestCsrfPreventionFilterHandler.  There will be a new
   * instance created for each new Netty channel/pipeline serving a new request.
   * To prevent the cost of repeated initialization of the filter, this
   * constructor requires the caller to pass in a pre-built, fully initialized
   * filter instance.  The filter is stateless after initialization, so it can
   * be shared across multiple Netty channels/pipelines.
   *
   * @param restCsrfPreventionFilter initialized filter
   */
  RestCsrfPreventionFilterHandler(
      RestCsrfPreventionFilter restCsrfPreventionFilter) {
    if(restCsrfPreventionFilter == null) {
      LOG.warn("Got null for restCsrfPreventionFilter - will not do any filtering.");
    }
    this.restCsrfPreventionFilter = restCsrfPreventionFilter;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final HttpRequest req) throws Exception {
    if(restCsrfPreventionFilter != null) {
      restCsrfPreventionFilter.handleHttpInteraction(new NettyHttpInteraction(
          ctx, req));
    } else {
      // we do not have a valid filter simply pass requests
      new NettyHttpInteraction(ctx, req).proceed();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception in " + this.getClass().getSimpleName(), cause);
    sendResponseAndClose(ctx,
        new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
  }

  /**
   * Finish handling this pipeline by writing a response with the
   * "Connection: close" header, flushing, and scheduling a close of the
   * connection.
   *
   * @param ctx context to receive the response
   * @param resp response to send
   */
  private static void sendResponseAndClose(ChannelHandlerContext ctx,
      DefaultHttpResponse resp) {
    resp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * {@link HttpInteraction} implementation for use in a Netty pipeline.
   */
  private static final class NettyHttpInteraction implements HttpInteraction {

    private final ChannelHandlerContext ctx;
    private final HttpRequest req;

    /**
     * Creates a new NettyHttpInteraction.
     *
     * @param ctx context to receive the response
     * @param req request to process
     */
    NettyHttpInteraction(ChannelHandlerContext ctx, HttpRequest req) {
      this.ctx = ctx;
      this.req = req;
    }

    @Override
    public String getHeader(String header) {
      return req.headers().get(header);
    }

    @Override
    public String getMethod() {
      return req.getMethod().name();
    }

    @Override
    public void proceed() {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    }

    @Override
    public void sendError(int code, String message) {
      HttpResponseStatus status = new HttpResponseStatus(code, message);
      sendResponseAndClose(ctx, new DefaultHttpResponse(HTTP_1_1, status));
    }
  }

  /**
   * Creates a {@link RestCsrfPreventionFilter} for the {@DatanodeHttpServer}.
   * This method takes care of configuration and implementing just enough of the
   * servlet API and related interfaces so that the DataNode can get a fully
   * initialized instance of the filter.
   *
   * @param conf configuration to read
   * @return initialized filter, or null if CSRF protection not enabled
   */
  public static RestCsrfPreventionFilter initializeState(
      Configuration conf) {
    if (!conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
        DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      return null;
    }
    String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
    Map<String, String> restCsrfParams = RestCsrfPreventionFilter
        .getFilterParams(conf, "dfs.webhdfs.rest-csrf.");
    RestCsrfPreventionFilter filter = new RestCsrfPreventionFilter();
    try {
      filter.init(new DatanodeHttpServer
          .MapBasedFilterConfig(restCsrfClassName, restCsrfParams));
    } catch (ServletException e) {
      throw new IllegalStateException(
          "Failed to initialize RestCsrfPreventionFilter.", e);
    }
    return(filter);
  }
}
