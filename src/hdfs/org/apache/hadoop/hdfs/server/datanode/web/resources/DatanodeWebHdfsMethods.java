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
package org.apache.hadoop.hdfs.server.datanode.web.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.sun.jersey.spi.container.ResourceFilters;

/** Web-hdfs DataNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class DatanodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(DatanodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  private @Context ServletContext context;
  private @Context HttpServletResponse response;

  private void init(final UserGroupInformation ugi, final DelegationParam delegation,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
    
    if (UserGroupInformation.isSecurityEnabled()) {
      //add a token for RPC.
      final DataNode datanode = (DataNode)context.getAttribute("datanode");
      final InetSocketAddress nnRpcAddr = NameNode.getAddress(datanode.getConf());
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegation.getValue());
      SecurityUtil.setTokenService(token, nnRpcAddr);
      token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
      ugi.addToken(token);
    }
  }

  /** Handle HTTP PUT request for the root. */
  @PUT
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response putRoot(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize
      ) throws IOException, InterruptedException {
    return put(in, ugi, delegation, ROOT, op, permission, overwrite, bufferSize,
        replication, blockSize);
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response put(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, path, op, permission, overwrite, bufferSize,
        replication, blockSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {

    final String fullpath = path.getAbsolutePath();
    final DataNode datanode = (DataNode)context.getAttribute("datanode");

    switch(op.getValue()) {
    case CREATE:
    {
      final Configuration conf = new Configuration(datanode.getConf());
      conf.set(FsPermission.UMASK_LABEL, "000");

      final int b = bufferSize.getValue(conf);
      DFSClient dfsclient = new DFSClient(conf);
      FSDataOutputStream out = null;
      try {
        out = new FSDataOutputStream(dfsclient.create(
            fullpath, permission.getFsPermission(), overwrite.getValue(),
            replication.getValue(conf), blockSize.getValue(conf), null, b), null);
        IOUtils.copyBytes(in, out, b);
        out.close();
        out = null;
        dfsclient.close();
        dfsclient = null;
      } finally {
        IOUtils.cleanup(LOG, out);
        IOUtils.cleanup(LOG, dfsclient);
      }
      final String nnAddr = NameNode.getInfoServer(conf);
      final URI uri = new URI(WebHdfsFileSystem.SCHEME + "://" + nnAddr + fullpath);
      return Response.created(uri).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
      }
    });
  }

  /** Handle HTTP POST request for the root for the root. */
  @POST
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response postRoot(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {
    return post(in, ugi, delegation, ROOT, op, bufferSize);
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(
      final InputStream in,
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, path, op, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {

    final String fullpath = path.getAbsolutePath();
    final DataNode datanode = (DataNode)context.getAttribute("datanode");

    switch(op.getValue()) {
    case APPEND:
    {
      final Configuration conf = new Configuration(datanode.getConf());
      final int b = bufferSize.getValue(conf);
      DFSClient dfsclient = new DFSClient(conf);
      FSDataOutputStream out = null;
      try {
        out = dfsclient.append(fullpath, b, null, null);
        IOUtils.copyBytes(in, out, b);
        out.close();
        out = null;
        dfsclient.close();
        dfsclient = null;
      } finally {
        IOUtils.cleanup(LOG, out);
        IOUtils.cleanup(LOG, dfsclient);
      }
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
      }
    });
  }

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response getRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {
    return get(ugi, delegation, ROOT, op, offset, length, bufferSize); 
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, path, op, offset, length, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {

    final String fullpath = path.getAbsolutePath();
    final DataNode datanode = (DataNode)context.getAttribute("datanode");
    final Configuration conf = new Configuration(datanode.getConf());
    final InetSocketAddress nnRpcAddr = NameNode.getAddress(conf);

    switch(op.getValue()) {
    case OPEN:
    {
      final int b = bufferSize.getValue(conf);
      final DFSClient dfsclient = new DFSClient(nnRpcAddr, conf);
      DFSDataInputStream in = null;
      try {
        in = new DFSClient.DFSDataInputStream(
        dfsclient.open(fullpath, b, true, null));
        in.seek(offset.getValue());
      } catch(IOException ioe) {
        IOUtils.cleanup(LOG, in);
        IOUtils.cleanup(LOG, dfsclient);
        throw ioe;
      }
      final DFSDataInputStream dis = in;
      final StreamingOutput streaming = new StreamingOutput() {
        @Override
        public void write(final OutputStream out) throws IOException {
          final Long n = length.getValue();
          DFSDataInputStream dfsin = dis;
          DFSClient client = dfsclient;
          try {
            if (n == null) {
              IOUtils.copyBytes(dfsin, out, b);
            } else {
              IOUtils.copyBytes(dfsin, out, n, b, false);
            }
            dfsin.close();
            dfsin = null;
            client.close();
            client = null;
          } finally {
            IOUtils.cleanup(LOG, dfsin);
            IOUtils.cleanup(LOG, client);
          }
        }
      };

      return Response.ok(streaming).type(
          MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case GETFILECHECKSUM:
    {
      MD5MD5CRC32FileChecksum checksum = null;
      DFSClient dfsclient = new DFSClient(nnRpcAddr, conf);
      try {
        checksum = dfsclient.getFileChecksum(fullpath);
        dfsclient.close();
        dfsclient = null;
      } finally {
        IOUtils.cleanup(LOG, dfsclient);
      }
      final String js = JsonUtil.toJsonString(checksum);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
      }
    });
  }
}
