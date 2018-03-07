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

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.stream.ChunkedStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCEPT;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_MAX_AGE;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;

public class WebHdfsHandler extends SimpleChannelInboundHandler<HttpRequest> {
  static final Log LOG = LogFactory.getLog(WebHdfsHandler.class);
  static final Log REQLOG = LogFactory.getLog("datanode.webhdfs");
  public static final String WEBHDFS_PREFIX = WebHdfsFileSystem.PATH_PREFIX;
  public static final int WEBHDFS_PREFIX_LENGTH = WEBHDFS_PREFIX.length();
  public static final String APPLICATION_OCTET_STREAM =
    "application/octet-stream";
  public static final String APPLICATION_JSON_UTF8 =
      "application/json; charset=utf-8";

  public static final EnumSet<CreateFlag> EMPTY_CREATE_FLAG =
      EnumSet.noneOf(CreateFlag.class);

  private final Configuration conf;
  private final Configuration confForCreate;

  private String path;
  private ParameterParser params;
  private UserGroupInformation ugi;
  private DefaultHttpResponse resp = null;

  public WebHdfsHandler(Configuration conf, Configuration confForCreate)
    throws IOException {
    this.conf = conf;
    this.confForCreate = confForCreate;
    /** set user pattern based on configuration file */
    UserParam.setUserPattern(
        conf.get(HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));
    AclPermissionParam.setAclPermissionPattern(
        conf.get(HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx,
                           final HttpRequest req) throws Exception {
    Preconditions.checkArgument(req.getUri().startsWith(WEBHDFS_PREFIX));
    QueryStringDecoder queryString = new QueryStringDecoder(req.getUri());
    params = new ParameterParser(queryString, conf);
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    ugi = ugiProvider.ugi();
    path = URLDecoder.decode(params.path(), "UTF-8");

    injectToken();
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          handle(ctx, req);
        } finally {
          String host = null;
          try {
            host = ((InetSocketAddress)ctx.channel().remoteAddress()).
                getAddress().getHostAddress();
          } catch (Exception e) {
            LOG.warn("Error retrieving hostname: ", e);
            host = "unknown";
          }
          REQLOG.info(host + " " + req.getMethod() + " "  + req.getUri() + " " +
              getResponseCode());
        }
        return null;
      }
    });
  }

  int getResponseCode() {
    return (resp == null) ? INTERNAL_SERVER_ERROR.code() :
        resp.getStatus().code();
  }

  public void handle(ChannelHandlerContext ctx, HttpRequest req)
    throws IOException, URISyntaxException {
    String op = params.op();
    HttpMethod method = req.getMethod();
    if (PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
      && method == PUT) {
      onCreate(ctx);
    } else if (PostOpParam.Op.APPEND.name().equalsIgnoreCase(op)
      && method == POST) {
      onAppend(ctx);
    } else if (GetOpParam.Op.OPEN.name().equalsIgnoreCase(op)
      && method == GET) {
      onOpen(ctx);
    } else if(GetOpParam.Op.GETFILECHECKSUM.name().equalsIgnoreCase(op)
      && method == GET) {
      onGetFileChecksum(ctx);
    } else if(PutOpParam.Op.CREATE.name().equalsIgnoreCase(op)
        && method == OPTIONS) {
      allowCORSOnCreate(ctx);
    } else {
      throw new IllegalArgumentException("Invalid operation " + op);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.debug("Error ", cause);
    resp = ExceptionHandler.exceptionCaught(cause);
    resp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  private void onCreate(ChannelHandlerContext ctx)
    throws IOException, URISyntaxException {
    writeContinueHeader(ctx);

    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final short replication = params.replication();
    final long blockSize = params.blockSize();
    final FsPermission unmaskedPermission = params.unmaskedPermission();
    final FsPermission permission = unmaskedPermission == null ?
        params.permission() :
        FsCreateModes.create(params.permission(), unmaskedPermission);
    final boolean createParent = params.createParent();

    EnumSet<CreateFlag> flags = params.createFlag();
    if (flags.equals(EMPTY_CREATE_FLAG)) {
      flags = params.overwrite() ?
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
          : EnumSet.of(CreateFlag.CREATE);
    } else {
      if(params.overwrite()) {
        flags.add(CreateFlag.OVERWRITE);
      }
    }

    final DFSClient dfsClient = newDfsClient(nnId, confForCreate);
    OutputStream out = dfsClient.createWrappedOutputStream(dfsClient.create(
        path, permission, flags, createParent, replication, blockSize, null,
        bufferSize, null), null);

    resp = new DefaultHttpResponse(HTTP_1_1, CREATED);

    final URI uri = new URI(HDFS_URI_SCHEME, nnId, path, null, null);
    resp.headers().set(LOCATION, uri.toString());
    resp.headers().set(CONTENT_LENGTH, 0);
    resp.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");

    ctx.pipeline().replace(this, HdfsWriter.class.getSimpleName(),
      new HdfsWriter(dfsClient, out, resp));
  }

  private void onAppend(ChannelHandlerContext ctx) throws IOException {
    writeContinueHeader(ctx);
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();

    DFSClient dfsClient = newDfsClient(nnId, conf);
    OutputStream out = dfsClient.append(path, bufferSize,
        EnumSet.of(CreateFlag.APPEND), null, null);
    resp = new DefaultHttpResponse(HTTP_1_1, OK);
    resp.headers().set(CONTENT_LENGTH, 0);
    ctx.pipeline().replace(this, HdfsWriter.class.getSimpleName(),
      new HdfsWriter(dfsClient, out, resp));
  }

  private void onOpen(ChannelHandlerContext ctx) throws IOException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final long offset = params.offset();
    final long length = params.length();

    resp = new DefaultHttpResponse(HTTP_1_1, OK);
    HttpHeaders headers = resp.headers();
    // Allow the UI to access the file
    headers.set(ACCESS_CONTROL_ALLOW_METHODS, GET);
    headers.set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    headers.set(CONTENT_TYPE, APPLICATION_OCTET_STREAM);
    headers.set(CONNECTION, CLOSE);

    final DFSClient dfsclient = newDfsClient(nnId, conf);
    HdfsDataInputStream in = dfsclient.createWrappedInputStream(
      dfsclient.open(path, bufferSize, true));
    in.seek(offset);

    long contentLength = in.getVisibleLength() - offset;
    if (length >= 0) {
      contentLength = Math.min(contentLength, length);
    }
    final InputStream data;
    if (contentLength >= 0) {
      headers.set(CONTENT_LENGTH, contentLength);
      data = new LimitInputStream(in, contentLength);
    } else {
      data = in;
    }

    ctx.write(resp);
    ctx.writeAndFlush(new ChunkedStream(data) {
      @Override
      public void close() throws Exception {
        super.close();
        dfsclient.close();
      }
    }).addListener(ChannelFutureListener.CLOSE);
  }

  private void onGetFileChecksum(ChannelHandlerContext ctx) throws IOException {
    MD5MD5CRC32FileChecksum checksum = null;
    final String nnId = params.namenodeId();
    DFSClient dfsclient = newDfsClient(nnId, conf);
    try {
      checksum = dfsclient.getFileChecksum(path, Long.MAX_VALUE);
      dfsclient.close();
      dfsclient = null;
    } finally {
      IOUtils.cleanup(LOG, dfsclient);
    }
    final byte[] js =
        JsonUtil.toJsonString(checksum).getBytes(StandardCharsets.UTF_8);
    resp =
      new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(js));

    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, js.length);
    resp.headers().set(CONNECTION, CLOSE);
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  //Accept preflighted CORS requests
  private void allowCORSOnCreate(ChannelHandlerContext ctx)
    throws IOException, URISyntaxException {
    resp = new DefaultHttpResponse(HTTP_1_1, OK);
    HttpHeaders headers = resp.headers();
    headers.set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    headers.set(ACCESS_CONTROL_ALLOW_HEADERS, ACCEPT);
    headers.set(ACCESS_CONTROL_ALLOW_METHODS, PUT);
    headers.set(ACCESS_CONTROL_MAX_AGE, 1728000);
    headers.set(CONTENT_LENGTH, 0);
    headers.set(CONNECTION, KEEP_ALIVE);

    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
  }

  private static void writeContinueHeader(ChannelHandlerContext ctx) {
    DefaultHttpResponse r = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE,
      Unpooled.EMPTY_BUFFER);
    ctx.writeAndFlush(r);
  }

  private static DFSClient newDfsClient
    (String nnId, Configuration conf) throws IOException {
    URI uri = URI.create(HDFS_URI_SCHEME + "://" + nnId);
    return new DFSClient(uri, conf);
  }

  private void injectToken() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<DelegationTokenIdentifier> token = params.delegationToken();
      token.setKind(HDFS_DELEGATION_KIND);
      ugi.addToken(token);
    }
  }
}
