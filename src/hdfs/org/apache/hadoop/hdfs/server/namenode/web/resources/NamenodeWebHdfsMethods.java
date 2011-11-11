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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RecursiveParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.sun.jersey.spi.container.ResourceFilters;

/** Web-hdfs NameNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class NamenodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<String>(); 

  /** @return the remote client address. */
  public static String getRemoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  private @Context ServletContext context;
  private @Context HttpServletRequest request;
  private @Context HttpServletResponse response;

  private void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
  }

  private static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset
      ) throws IOException {
    if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      final HdfsFileStatus status = namenode.getFileInfo(path);
      if (status == null) {
        throw new FileNotFoundException("File " + path + " not found.");
      }
      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN && (openOffset < 0L || openOffset >= len)) {
        throw new IOException("Offset=" + openOffset + " out of the range [0, "
          + len + "); " + op + ", path=" + path);
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = namenode.getBlockLocations(
            path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return JspHelper.bestNode(locations.get(0));
        }
      }
    } 

    return namenode.getNamesystem().getRandomDatanode();
  }

  private Token<? extends TokenIdentifier> generateDelegationToken(
      final NameNode namenode, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    final Credentials c = DelegationTokenSecretManager.createCredentials(
        namenode, ugi, renewer != null? renewer: ugi.getShortUserName());
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    t.setKind(WebHdfsFileSystem.TOKEN_KIND);
    SecurityUtil.setTokenService(t, namenode.getHttpAddress());
    return t;
  }

  private URI redirectURI(final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn = chooseDatanode(namenode, path, op, openOffset);

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      //security disabled
      delegationQuery = Param.toSortedString("&", doAsUser, username);
    } else if (delegation.getValue() != null) {
      //client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      //generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          namenode, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }
    final String query = op.toQueryString() + delegationQuery
        + Param.toSortedString("&", parameters);
    final String uripath = WebHdfsFileSystem.PATH_PREFIX + path;

    final URI uri = new URI("http", null, dn.getHostName(), dn.getInfoPort(),
        uripath, query, null);
    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }
  
  /** Handle HTTP PUT request for the root. */
  @PUT
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response putRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
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
          final AccessTimeParam accessTime,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument
      ) throws IOException, InterruptedException {
    return put(ugi, delegation, username, doAsUser, ROOT, op, destination,
        owner, group, permission, overwrite, bufferSize, replication,
        blockSize, modificationTime, accessTime, delegationTokenArgument);
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response put(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PutOpParam.NAME) @DefaultValue(PutOpParam.DEFAULT)
          final PutOpParam op,
      @QueryParam(DestinationParam.NAME) @DefaultValue(DestinationParam.DEFAULT)
          final DestinationParam destination,
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
          final AccessTimeParam accessTime,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, destination, owner,
        group, permission, overwrite, bufferSize, replication, blockSize,
        modificationTime, accessTime, delegationTokenArgument);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final String fullpath = path.getAbsolutePath();
    final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case CREATE:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L,
          permission, overwrite, bufferSize, replication, blockSize);
      return Response.temporaryRedirect(uri).build();
    } 
    case MKDIRS:
    {
      final boolean b = namenode.mkdirs(fullpath, permission.getFsPermission());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case RENAME:
    {
      final boolean b = namenode.rename(fullpath, destination.getValue());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETREPLICATION:
    {
      final boolean b = namenode.setReplication(fullpath, replication.getValue(conf));
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      if (owner.getValue() == null && group.getValue() == null) {
        throw new IllegalArgumentException("Both owner and group are empty.");
      }

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
    case RENEWDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      final long expiryTime = namenode.renewDelegationToken(token);
      final String js = JsonUtil.toJsonString("long", expiryTime);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CANCELDELEGATIONTOKEN:
    {
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      namenode.cancelDelegationToken(token);
      return Response.ok().type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  /** Handle HTTP POST request for the root. */
  @POST
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response postRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {
    return post(ugi, delegation, username, doAsUser, ROOT, op, bufferSize);
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(PostOpParam.NAME) @DefaultValue(PostOpParam.DEFAULT)
          final PostOpParam op,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final String fullpath = path.getAbsolutePath();
    final NameNode namenode = (NameNode)context.getAttribute("name.node");

    switch(op.getValue()) {
    case APPEND:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L, bufferSize);
      return Response.temporaryRedirect(uri).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }

        } finally {
          REMOTE_ADDRESS.set(null);
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
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, URISyntaxException, InterruptedException {
    return get(ugi, delegation, username, doAsUser, ROOT, op,
        offset, length, renewer, bufferSize);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(GetOpParam.NAME) @DefaultValue(GetOpParam.DEFAULT)
          final GetOpParam op,
      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT)
          final OffsetParam offset,
      @QueryParam(LengthParam.NAME) @DefaultValue(LengthParam.DEFAULT)
          final LengthParam length,
      @QueryParam(RenewerParam.NAME) @DefaultValue(RenewerParam.DEFAULT)
          final RenewerParam renewer,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op,
        offset, length, renewer, bufferSize);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final String fullpath = path.getAbsolutePath();

    switch(op.getValue()) {
    case OPEN:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), offset.getValue(), offset, length, bufferSize);
      return Response.temporaryRedirect(uri).build();
    }
    case GET_BLOCK_LOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = namenode.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: Long.MAX_VALUE);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = namenode.getFileInfo(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status, true);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(namenode, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = namenode.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILECHECKSUM:
    {
      final URI uri = redirectURI(namenode, ugi, delegation, username, doAsUser,
          fullpath, op.getValue(), -1L);
      return Response.temporaryRedirect(uri).build();
    }
    case GETDELEGATIONTOKEN:
    {
      if (delegation.getValue() != null) {
        throw new IllegalArgumentException(delegation.getName()
            + " parameter is not null.");
      }
      final Token<? extends TokenIdentifier> token = generateDelegationToken(
          namenode, ugi, renewer.getValue());
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY:
    {
      final String js = JsonUtil.toJsonString(
          org.apache.hadoop.fs.Path.class.getSimpleName(),
          WebHdfsFileSystem.getHomeDirectoryString(ugi));
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }    

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }

  private static DirectoryListing getDirectoryListing(final NameNode np,
      final String p, byte[] startAfter) throws IOException {
    final DirectoryListing listing = np.getListing(p, startAfter);
    if (listing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    return listing;
  }
  
  private static StreamingOutput getListingStream(final NameNode np, 
      final String p) throws IOException {
    final DirectoryListing first = getDirectoryListing(np, p,
        HdfsFileStatus.EMPTY_NAME);

    return new StreamingOutput() {
      @Override
      public void write(final OutputStream outstream) throws IOException {
        final PrintStream out = new PrintStream(outstream);
        out.println("{\"" + FileStatus.class.getSimpleName() + "es\":{\""
            + FileStatus.class.getSimpleName() + "\":[");

        final HdfsFileStatus[] partial = first.getPartialListing();
        if (partial.length > 0) {
          out.print(JsonUtil.toJsonString(partial[0], false));
        }
        for(int i = 1; i < partial.length; i++) {
          out.println(',');
          out.print(JsonUtil.toJsonString(partial[i], false));
        }

        for(DirectoryListing curr = first; curr.hasMore(); ) { 
          curr = getDirectoryListing(np, p, curr.getLastName());
          for(HdfsFileStatus s : curr.getPartialListing()) {
            out.println(',');
            out.print(JsonUtil.toJsonString(s, false));
          }
        }
        
        out.println();
        out.println("]}}");
      }
    };
  }

  /** Handle HTTP DELETE request for the root. */
  @DELETE
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteRoot(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException, InterruptedException {
    return delete(ugi, delegation, username, doAsUser, ROOT, op, recursive);
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(
      @Context final UserGroupInformation ugi,
      @QueryParam(DelegationParam.NAME) @DefaultValue(DelegationParam.DEFAULT)
          final DelegationParam delegation,
      @QueryParam(UserParam.NAME) @DefaultValue(UserParam.DEFAULT)
          final UserParam username,
      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT)
          final DoAsParam doAsUser,
      @PathParam(UriFsPathParam.NAME) final UriFsPathParam path,
      @QueryParam(DeleteOpParam.NAME) @DefaultValue(DeleteOpParam.DEFAULT)
          final DeleteOpParam op,
      @QueryParam(RecursiveParam.NAME) @DefaultValue(RecursiveParam.DEFAULT)
          final RecursiveParam recursive
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, recursive);

    return ugi.doAs(new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {
        REMOTE_ADDRESS.set(request.getRemoteAddr());
        try {

        final NameNode namenode = (NameNode)context.getAttribute("name.node");
        final String fullpath = path.getAbsolutePath();

        switch(op.getValue()) {
        case DELETE:
        {
          final boolean b = namenode.delete(fullpath, recursive.getValue());
          final String js = JsonUtil.toJsonString("boolean", b);
          return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        }
        default:
          throw new UnsupportedOperationException(op + " is not supported");
        }

        } finally {
          REMOTE_ADDRESS.set(null);
        }
      }
    });
  }
}
