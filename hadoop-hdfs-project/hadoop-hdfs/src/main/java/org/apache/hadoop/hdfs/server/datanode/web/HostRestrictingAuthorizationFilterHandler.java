/*
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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HostRestrictingAuthorizationFilter;
import org.apache.hadoop.hdfs.server.common.HostRestrictingAuthorizationFilter.HttpInteraction;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/*
 * Netty handler that integrates with the {@link
 * HostRestrictingAuthorizationFilter}.  If
 * the filter determines that the request is allowed, then this handler forwards
 * the request to the next handler in the Netty pipeline.  Otherwise, this
 * handler drops the request and sends an HTTP 403 response.
 */
@InterfaceAudience.Private
@Sharable
final class HostRestrictingAuthorizationFilterHandler
    extends SimpleChannelInboundHandler<HttpRequest> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HostRestrictingAuthorizationFilterHandler.class);
  private final
  HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter;

  /*
   * Creates a new HostRestrictingAuthorizationFilterHandler.  There will be
   * a new instance created for each new Netty channel/pipeline serving a new
   * request.
   *
   * To prevent the cost of repeated initialization of the filter, this
   * constructor requires the caller to pass in a pre-built, fully initialized
   * filter instance.  The filter is stateless after initialization, so it can
   * be shared across multiple Netty channels/pipelines.
   *
   * @param hostRestrictingAuthorizationFilter initialized filter
   */
  public HostRestrictingAuthorizationFilterHandler(
      HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter) {
    this.hostRestrictingAuthorizationFilter =
        hostRestrictingAuthorizationFilter;
  }

  /*
   * Creates a new HostRestrictingAuthorizationFilterHandler.  There will be
   * a new instance created for each new Netty channel/pipeline serving a new
   * request.
   * To prevent the cost of repeated initialization of the filter, this
   * constructor requires the caller to pass in a pre-built, fully initialized
   * filter instance.  The filter is stateless after initialization, so it can
   * be shared across multiple Netty channels/pipelines.
   */
  public HostRestrictingAuthorizationFilterHandler() {
    Configuration conf = new Configuration();
    this.hostRestrictingAuthorizationFilter = initializeState(conf);
  }

  /*
   * Creates a {@link HostRestrictingAuthorizationFilter} for the
   * {@DatanodeHttpServer}.
   * This method takes care of configuration and implementing just enough of the
   * servlet API and related interfaces so that the DataNode can get a fully
   * initialized
   * instance of the filter.
   *
   * @param conf configuration to read
   * @return initialized filter, or null if CSRF protection not enabled
   * @throws IllegalStateException if filter fails initialization
   */
  public static HostRestrictingAuthorizationFilter
  initializeState(Configuration conf) {
    String confName = HostRestrictingAuthorizationFilter.HDFS_CONFIG_PREFIX +
        HostRestrictingAuthorizationFilter.RESTRICTION_CONFIG;
    String confValue = conf.get(confName);
    // simply pass a blank value if we do not have one set
    confValue = (confValue == null ? "" : confValue);

    Map<String, String> confMap =
        ImmutableMap.of(HostRestrictingAuthorizationFilter.RESTRICTION_CONFIG
            , confValue);
    FilterConfig fc =
        new DatanodeHttpServer.MapBasedFilterConfig(
            HostRestrictingAuthorizationFilter.class.getName(), confMap);
    HostRestrictingAuthorizationFilter hostRestrictingAuthorizationFilter =
        new HostRestrictingAuthorizationFilter();
    try {
      hostRestrictingAuthorizationFilter.init(fc);
    } catch (ServletException e) {
      throw new IllegalStateException(
          "Failed to initialize HostRestrictingAuthorizationFilter.", e);
    }
    return hostRestrictingAuthorizationFilter;
  }

  /*
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

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx,
      final HttpRequest req) throws Exception {
    hostRestrictingAuthorizationFilter
        .handleInteraction(new NettyHttpInteraction(ctx, req));
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception in " + this.getClass().getSimpleName(), cause);
    sendResponseAndClose(ctx,
        new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
  }

  /*
   * {@link HttpInteraction} implementation for use in a Netty pipeline.
   */
  private static final class NettyHttpInteraction implements HttpInteraction {

    private final ChannelHandlerContext ctx;
    private final HttpRequest req;
    private boolean committed;

    /*
     * Creates a new NettyHttpInteraction.
     *
     * @param ctx context to receive the response
     * @param req request to process
     */
    public NettyHttpInteraction(ChannelHandlerContext ctx, HttpRequest req) {
      this.committed = false;
      this.ctx = ctx;
      this.req = req;
    }

    @Override
    public boolean isCommitted() {
      return committed;
    }

    @Override
    public String getRemoteAddr() {
      return ((InetSocketAddress) ctx.channel().remoteAddress()).
          getAddress().getHostAddress();
    }

    @Override
    public String getQueryString() {
      try {
        return (new URI(req.getUri()).getQuery());
      } catch (URISyntaxException e) {
        return null;
      }
    }

    @Override
    public String getRequestURI() {
      String uri = req.getUri();
      // Netty's getUri includes the query string, while Servlet's does not
      return (uri.substring(0, uri.indexOf("?") >= 0 ? uri.indexOf("?") :
          uri.length()));
    }

    @Override
    public String getRemoteUser() {
      QueryStringDecoder queryString = new QueryStringDecoder(req.getUri());
      List<String> p = queryString.parameters().get(UserParam.NAME);
      String user = (p == null ? null : p.get(0));
      return (new UserParam(user).getValue());
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
      this.committed = true;
    }
  }
}
