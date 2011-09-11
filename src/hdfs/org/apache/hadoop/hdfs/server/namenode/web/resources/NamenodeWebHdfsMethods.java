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
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DstPathParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;

/** Web-hdfs NameNode implementation. */
@Path("")
public class NamenodeWebHdfsMethods {
  private static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private @Context ServletContext context;

  private static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op) throws IOException {
    if (op == PostOpParam.Op.APPEND) {
      final HdfsFileStatus status = namenode.getFileInfo(path);
      final long len = status.getLen();
      if (len > 0) {
        final LocatedBlocks locations = namenode.getBlockLocations(path, len-1, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return JspHelper.bestNode(locations.get(count - 1));
        }
      }
    } 

    return namenode.getNamesystem().getRandomDatanode();
  }

  private static URI redirectURI(final NameNode namenode,
      final String path, final HttpOpParam.Op op,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn = chooseDatanode(namenode, path, op);
    final String query = op.toQueryString() + Param.toSortedString("&", parameters);
    final String uripath = "/" + WebHdfsFileSystem.PATH_PREFIX + path;

    final URI uri = new URI("http", null, dn.getHostName(), dn.getInfoPort(),
        uripath, query, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response put(
      final InputStream in,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DstPathParam.NAME) @DefaultValue(DstPathParam.DEFAULT)
          final DstPathParam dstPath,
      @QueryParam(OwnerParam.NAME) @DefaultValue(OwnerParam.DEFAULT)
          final OwnerParam owner,
      @QueryParam(GroupParam.NAME) @DefaultValue(GroupParam.DEFAULT)
          final GroupParam group,
      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
          final PermissionParam permission,
      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT)
          final OverwriteParam overwrite,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
          final ReplicationParam replication,
      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT)
          final BlockSizeParam blockSize,
      @QueryParam(ModificationTimeParam.NAME) @DefaultValue(ModificationTimeParam.DEFAULT)
          final ModificationTimeParam modificationTime,
      @QueryParam(AccessTimeParam.NAME) @DefaultValue(AccessTimeParam.DEFAULT)
          final AccessTimeParam accessTime
      ) throws IOException, URISyntaxException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path
            + Param.toSortedString(", ", dstPath, owner, group, permission,
                overwrite, bufferSize, replication, blockSize));
    }

    final String fullpath = path.getAbsolutePath();
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case CREATE:
    {
      final URI uri = redirectURI(namenode, fullpath, op.getValue(),
          permission, overwrite, bufferSize, replication, blockSize);
      return Response.temporaryRedirect(uri).build();
    } 
    case MKDIRS:
    {
      final boolean b = namenode.mkdirs(fullpath, permission.getFsPermission());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.MKDIRS, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case RENAME:
    {
      final boolean b = namenode.rename(fullpath, dstPath.getValue());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.RENAME, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETREPLICATION:
    {
      final boolean b = namenode.setReplication(fullpath, replication.getValue());
      final String js = JsonUtil.toJsonString(PutOpParam.Op.SETREPLICATION, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      namenode.setOwner(fullpath, owner.getValue(), group.getValue());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    case SETPERMISSION:
    {
      namenode.setPermission(fullpath, permission.getFsPermission());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    case SETTIMES:
    {
      namenode.setTimes(fullpath, modificationTime.getValue(), accessTime.getValue());
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(
      final InputStream in,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ": " + path
            + Param.toSortedString(", ", bufferSize));
    }

    final String fullpath = path.getAbsolutePath();
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case APPEND:
    {
      final URI uri = redirectURI(namenode, fullpath, op.getValue(), bufferSize);
      return Response.temporaryRedirect(uri).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response root(
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op
      ) throws IOException {
    return get(ROOT, op);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op
      ) throws IOException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ", " + path
          + Param.toSortedString(", "));
    }

    switch(op.getValue()) {
    case GETFILESTATUS:
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final String fullpath = path.getAbsolutePath();
      final HdfsFileStatus status = namenode.getFileInfo(fullpath);
      final String js = JsonUtil.toJsonString(status);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();

    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }    
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{path:.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException {

    if (LOG.isTraceEnabled()) {
      LOG.trace(op + ", " + path
        + Param.toSortedString(", ", recursive));
    }

    switch(op.getValue()) {
    case DELETE:
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final String fullpath = path.getAbsolutePath();
      final boolean b = namenode.delete(fullpath, recursive.getValue());
      final String js = JsonUtil.toJsonString(DeleteOpParam.Op.DELETE, b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();

    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }    
  }
}
