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
import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.LimitInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS;
import static io.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;

public class WebHdfsHandler extends SimpleChannelInboundHandler<HttpRequest> {
  static final Log LOG = LogFactory.getLog(WebHdfsHandler.class);
  public static final String WEBHDFS_PREFIX = WebHdfsFileSystem.PATH_PREFIX;
  public static final int WEBHDFS_PREFIX_LENGTH = WEBHDFS_PREFIX.length();
  public static final String APPLICATION_OCTET_STREAM =
    "application/octet-stream";
  public static final String APPLICATION_JSON_UTF8 =
      "application/json; charset=utf-8";

  private final Configuration conf;
  private final Configuration confForCreate;

  private String path;
  private ParameterParser params;
  private UserGroupInformation ugi;

  public WebHdfsHandler(Configuration conf, Configuration confForCreate)
    throws IOException {
    this.conf = conf;
    this.confForCreate = confForCreate;
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx,
                           final HttpRequest req) throws Exception {
    Preconditions.checkArgument(req.getUri().startsWith(WEBHDFS_PREFIX));
    QueryStringDecoder queryString = new QueryStringDecoder(req.getUri());
    params = new ParameterParser(queryString, conf);
    DataNodeUGIProvider ugiProvider = new DataNodeUGIProvider(params);
    ugi = ugiProvider.ugi();
    path = params.path();

    injectToken();
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        handle(ctx, req);
        return null;
      }
    });
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
    } else {
      throw new IllegalArgumentException("Invalid operation " + op);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.debug("Error ", cause);
    DefaultHttpResponse resp = ExceptionHandler.exceptionCaught(cause);
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
    final FsPermission permission = params.permission();

    EnumSet<CreateFlag> flags = params.overwrite() ?
      EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
        : EnumSet.of(CreateFlag.CREATE);

    final DFSClient dfsClient = newDfsClient(nnId, confForCreate);
    OutputStream out = dfsClient.createWrappedOutputStream(dfsClient.create(
      path, permission, flags, replication,
      blockSize, null, bufferSize, null), null);
    DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1, CREATED);

    final URI uri = new URI(HDFS_URI_SCHEME, nnId, path, null, null);
    resp.headers().set(LOCATION, uri.toString());
    resp.headers().set(CONTENT_LENGTH, 0);
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
    DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1, OK);
    resp.headers().set(CONTENT_LENGTH, 0);
    ctx.pipeline().replace(this, HdfsWriter.class.getSimpleName(),
      new HdfsWriter(dfsClient, out, resp));
  }

  private void onOpen(ChannelHandlerContext ctx) throws IOException {
    final String nnId = params.namenodeId();
    final int bufferSize = params.bufferSize();
    final long offset = params.offset();
    final long length = params.length();

    DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
    HttpHeaders headers = response.headers();
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

    ctx.write(response);
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
    final byte[] js = JsonUtil.toJsonString(checksum).getBytes(Charsets.UTF_8);
    DefaultFullHttpResponse resp =
      new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(js));

    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, js.length);
    resp.headers().set(CONNECTION, CLOSE);
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
