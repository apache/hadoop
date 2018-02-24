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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.sun.jersey.spi.container.ResourceFilters;

/** Web-hdfs NameNode implementation. */
@Path("")
@ResourceFilters(ParamFilter.class)
public class NamenodeWebHdfsMethods {
  public static final Log LOG = LogFactory.getLog(NamenodeWebHdfsMethods.class);

  private static final UriFsPathParam ROOT = new UriFsPathParam("");

  private volatile Boolean useIpcCallq;
  private String scheme;
  private Principal userPrincipal;
  private String remoteAddr;

  private @Context ServletContext context;
  private @Context HttpServletResponse response;
  private boolean supportEZ;

  public NamenodeWebHdfsMethods(@Context HttpServletRequest request) {
    // the request object is a proxy to thread-locals so we have to extract
    // what we want from it since the external call will be processed in a
    // different thread.
    scheme = request.getScheme();
    userPrincipal = request.getUserPrincipal();
    // get the remote address, if coming in via a trusted proxy server then
    // the address with be that of the proxied client
    remoteAddr = JspHelper.getRemoteAddr(request);
    supportEZ =
        Boolean.valueOf(request.getHeader(WebHdfsFileSystem.EZ_HEADER));
  }

  private void init(final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final UriFsPathParam path, final HttpOpParam<?> op,
      final Param<?, ?>... parameters) {
    if (useIpcCallq == null) {
      Configuration conf =
          (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
      useIpcCallq = conf.getBoolean(
          DFSConfigKeys.DFS_WEBHDFS_USE_IPC_CALLQ,
          DFSConfigKeys.DFS_WEBHDFS_USE_IPC_CALLQ_DEFAULT);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("HTTP " + op.getValue().getType() + ": " + op + ", " + path
          + ", ugi=" + ugi + ", " + username + ", " + doAsUser
          + Param.toSortedString(", ", parameters));
    }

    //clear content type
    response.setContentType(null);
  }

  private static NamenodeProtocols getRPCServer(NameNode namenode)
      throws IOException {
     final NamenodeProtocols np = namenode.getRpcServer();
     if (np == null) {
       throw new RetriableException("Namenode is in startup mode");
     }
     return np;
  }

  protected ClientProtocol getRpcClientProtocol() throws IOException {
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final ClientProtocol cp = namenode.getRpcServer();
    if (cp == null) {
      throw new RetriableException("Namenode is in startup mode");
    }
    return cp;
  }

  private <T> T doAs(final UserGroupInformation ugi,
      final PrivilegedExceptionAction<T> action)
          throws IOException, InterruptedException {
    return useIpcCallq ? doAsExternalCall(ugi, action) : ugi.doAs(action);
  }

  private <T> T doAsExternalCall(final UserGroupInformation ugi,
      final PrivilegedExceptionAction<T> action)
          throws IOException, InterruptedException {
    // set the remote address, if coming in via a trust proxy server then
    // the address with be that of the proxied client
    ExternalCall<T> call = new ExternalCall<T>(action){
      @Override
      public UserGroupInformation getRemoteUser() {
        return ugi;
      }
      @Override
      public String getProtocol() {
        return "webhdfs";
      }
      @Override
      public String getHostAddress() {
        return remoteAddr;
      }
      @Override
      public InetAddress getHostInetAddress() {
        try {
          return InetAddress.getByName(getHostAddress());
        } catch (UnknownHostException e) {
          return null;
        }
      }
    };
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    namenode.queueExternalCall(call);
    T result = null;
    try {
      result = call.get();
    } catch (ExecutionException ee) {
      Throwable t = ee.getCause();
      if (t instanceof RuntimeException) {
        throw (RuntimeException)t;
      } else if (t instanceof IOException) {
        throw (IOException)t;
      } else {
        throw new IOException(t);
      }
    }
    return result;
  }

  @VisibleForTesting
  static DatanodeInfo chooseDatanode(final NameNode namenode,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes,
      final String remoteAddr, final HdfsFileStatus status) throws IOException {
    FSNamesystem fsn = namenode.getNamesystem();
    if (fsn == null) {
      throw new IOException("Namesystem has not been intialized yet.");
    }
    final BlockManager bm = fsn.getBlockManager();
    
    HashSet<Node> excludes = new HashSet<Node>();
    if (excludeDatanodes != null) {
      for (String host : StringUtils
          .getTrimmedStringCollection(excludeDatanodes)) {
        int idx = host.indexOf(":");
        if (idx != -1) {          
          excludes.add(bm.getDatanodeManager().getDatanodeByXferAddr(
              host.substring(0, idx), Integer.parseInt(host.substring(idx + 1))));
        } else {
          excludes.add(bm.getDatanodeManager().getDatanodeByHost(host));
        }
      }
    }

    if (op == PutOpParam.Op.CREATE) {
      //choose a datanode near to client 
      final DatanodeDescriptor clientNode = bm.getDatanodeManager(
          ).getDatanodeByHost(remoteAddr);
      if (clientNode != null) {
        final DatanodeStorageInfo[] storages = bm.chooseTarget4WebHDFS(
            path, clientNode, excludes, blocksize);
        if (storages.length > 0) {
          return storages[0].getDatanodeDescriptor();
        }
      }
    } else if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      //choose a datanode containing a replica 
      final NamenodeProtocols np = getRPCServer(namenode);
      if (status == null) {
        throw new FileNotFoundException("File " + path + " not found.");
      }
      final long len = status.getLen();
      if (op == GetOpParam.Op.OPEN) {
        if (openOffset < 0L || (openOffset >= len && len > 0)) {
          throw new IOException("Offset=" + openOffset
              + " out of the range [0, " + len + "); " + op + ", path=" + path);
        }
      }

      if (len > 0) {
        final long offset = op == GetOpParam.Op.OPEN? openOffset: len - 1;
        final LocatedBlocks locations = np.getBlockLocations(path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          return bestNode(locations.get(0).getLocations(), excludes);
        }
      }
    }

    return (DatanodeDescriptor)bm.getDatanodeManager().getNetworkTopology(
        ).chooseRandom(NodeBase.ROOT, excludes);
  }

  /**
   * Choose the datanode to redirect the request. Note that the nodes have been
   * sorted based on availability and network distances, thus it is sufficient
   * to return the first element of the node here.
   */
  private static DatanodeInfo bestNode(DatanodeInfo[] nodes,
      HashSet<Node> excludes) throws IOException {
    for (DatanodeInfo dn: nodes) {
      if (false == dn.isDecommissioned() && false == excludes.contains(dn)) {
        return dn;
      }
    }
    throw new IOException("No active nodes contain this block");
  }

  private Token<? extends TokenIdentifier> generateDelegationToken(
      final NameNode namenode, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    final Credentials c = DelegationTokenSecretManager.createCredentials(
        namenode, ugi, renewer != null? renewer: ugi.getShortUserName());
    if (c == null) {
      return null;
    }
    final Token<? extends TokenIdentifier> t = c.getAllTokens().iterator().next();
    Text kind = scheme.equals("http")
        ? WebHdfsConstants.WEBHDFS_TOKEN_KIND
        : WebHdfsConstants.SWEBHDFS_TOKEN_KIND;
    t.setKind(kind);
    return t;
  }

  private URI redirectURI(ResponseBuilder rb, final NameNode namenode,
      final UserGroupInformation ugi, final DelegationParam delegation,
      final UserParam username, final DoAsParam doAsUser,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final long blocksize, final String excludeDatanodes,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn;
    final NamenodeProtocols np = getRPCServer(namenode);
    HdfsFileStatus status = null;
    if (op == GetOpParam.Op.OPEN
        || op == GetOpParam.Op.GETFILECHECKSUM
        || op == PostOpParam.Op.APPEND) {
      status = np.getFileInfo(path);
    }
    dn = chooseDatanode(namenode, path, op, openOffset, blocksize,
        excludeDatanodes, remoteAddr, status);
    if (dn == null) {
      throw new IOException("Failed to find datanode, suggest to check cluster"
          + " health. excludeDatanodes=" + excludeDatanodes);
    }

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
          namenode, ugi, null);
      delegationQuery = "&" + new DelegationParam(t.encodeToUrlString());
    }

    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(op.toQueryString());
    queryBuilder.append(delegationQuery);
    queryBuilder.append("&").append(new NamenodeAddressParam(namenode));
    queryBuilder.append(Param.toSortedString("&", parameters));

    boolean prependReservedRawPath  = false;
    if (op == GetOpParam.Op.OPEN && supportEZ
        && status.getFileEncryptionInfo() != null) {
      prependReservedRawPath = true;
      rb.header(WebHdfsFileSystem.FEFINFO_HEADER,
          encodeFeInfo(status.getFileEncryptionInfo()));
    }
    final String uripath = WebHdfsFileSystem.PATH_PREFIX +
        (prependReservedRawPath ? "/.reserved/raw" + path : path);

    int port = "http".equals(scheme) ? dn.getInfoPort() : dn
        .getInfoSecurePort();
    final URI uri = new URI(scheme, null, dn.getHostName(), port, uripath,
        queryBuilder.toString(), null);

    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI=" + uri);
    }
    return uri;
  }

  /** Handle HTTP PUT request for the root. */
  @PUT
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
      @QueryParam(UnmaskedPermissionParam.NAME)
      @DefaultValue(UnmaskedPermissionParam.DEFAULT)
          final UnmaskedPermissionParam unmaskedPermission,
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
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument,
      @QueryParam(AclPermissionParam.NAME) @DefaultValue(AclPermissionParam.DEFAULT)
          final AclPermissionParam aclPermission,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT)
          final XAttrNameParam xattrName,
      @QueryParam(XAttrValueParam.NAME) @DefaultValue(XAttrValueParam.DEFAULT)
          final XAttrValueParam xattrValue,
      @QueryParam(XAttrSetFlagParam.NAME) @DefaultValue(XAttrSetFlagParam.DEFAULT)
          final XAttrSetFlagParam xattrSetFlag,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(CreateFlagParam.NAME) @DefaultValue(CreateFlagParam.DEFAULT)
          final CreateFlagParam createFlagParam,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect,
      @QueryParam(StoragePolicyParam.NAME) @DefaultValue(StoragePolicyParam
          .DEFAULT) final StoragePolicyParam policyName
      ) throws IOException, InterruptedException {
    return put(ugi, delegation, username, doAsUser, ROOT, op, destination,
        owner, group, permission, unmaskedPermission, overwrite, bufferSize,
        replication, blockSize, modificationTime, accessTime, renameOptions,
        createParent, delegationTokenArgument, aclPermission, xattrName,
        xattrValue, xattrSetFlag, snapshotName, oldSnapshotName,
        excludeDatanodes, createFlagParam, noredirect, policyName);
  }

  /** Validate all required params. */
  @SuppressWarnings("rawtypes")
  private void validateOpParams(HttpOpParam<?> op, Param... params) {
    for (Param param : params) {
      if (param.getValue() == null || param.getValueString() == null || param
          .getValueString().isEmpty()) {
        throw new IllegalArgumentException("Required param " + param.getName()
            + " for op: " + op.getValueString() + " is null or empty");
      }
    }
  }

  /** Handle HTTP PUT request. */
  @PUT
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
      @QueryParam(UnmaskedPermissionParam.NAME)
      @DefaultValue(UnmaskedPermissionParam.DEFAULT)
          final UnmaskedPermissionParam unmaskedPermission,
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
      @QueryParam(RenameOptionSetParam.NAME) @DefaultValue(RenameOptionSetParam.DEFAULT)
          final RenameOptionSetParam renameOptions,
      @QueryParam(CreateParentParam.NAME) @DefaultValue(CreateParentParam.DEFAULT)
          final CreateParentParam createParent,
      @QueryParam(TokenArgumentParam.NAME) @DefaultValue(TokenArgumentParam.DEFAULT)
          final TokenArgumentParam delegationTokenArgument,
      @QueryParam(AclPermissionParam.NAME) @DefaultValue(AclPermissionParam.DEFAULT) 
          final AclPermissionParam aclPermission,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final XAttrNameParam xattrName,
      @QueryParam(XAttrValueParam.NAME) @DefaultValue(XAttrValueParam.DEFAULT) 
          final XAttrValueParam xattrValue,
      @QueryParam(XAttrSetFlagParam.NAME) @DefaultValue(XAttrSetFlagParam.DEFAULT) 
          final XAttrSetFlagParam xattrSetFlag,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(CreateFlagParam.NAME) @DefaultValue(CreateFlagParam.DEFAULT)
          final CreateFlagParam createFlagParam,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect,
      @QueryParam(StoragePolicyParam.NAME) @DefaultValue(StoragePolicyParam
          .DEFAULT) final StoragePolicyParam policyName
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, destination, owner,
        group, permission, unmaskedPermission, overwrite, bufferSize,
        replication, blockSize, modificationTime, accessTime, renameOptions,
        delegationTokenArgument, aclPermission, xattrName, xattrValue,
        xattrSetFlag, snapshotName, oldSnapshotName, excludeDatanodes,
        createFlagParam, noredirect, policyName);

    return doAs(ugi, new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
          return put(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, destination, owner, group,
              permission, unmaskedPermission, overwrite, bufferSize,
              replication, blockSize, modificationTime, accessTime,
              renameOptions, createParent, delegationTokenArgument,
              aclPermission, xattrName, xattrValue, xattrSetFlag,
              snapshotName, oldSnapshotName, excludeDatanodes,
              createFlagParam, noredirect, policyName);
      }
    });
  }

  private Response put(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PutOpParam op,
      final DestinationParam destination,
      final OwnerParam owner,
      final GroupParam group,
      final PermissionParam permission,
      final UnmaskedPermissionParam unmaskedPermission,
      final OverwriteParam overwrite,
      final BufferSizeParam bufferSize,
      final ReplicationParam replication,
      final BlockSizeParam blockSize,
      final ModificationTimeParam modificationTime,
      final AccessTimeParam accessTime,
      final RenameOptionSetParam renameOptions,
      final CreateParentParam createParent,
      final TokenArgumentParam delegationTokenArgument,
      final AclPermissionParam aclPermission,
      final XAttrNameParam xattrName,
      final XAttrValueParam xattrValue,
      final XAttrSetFlagParam xattrSetFlag,
      final SnapshotNameParam snapshotName,
      final OldSnapshotNameParam oldSnapshotName,
      final ExcludeDatanodesParam exclDatanodes,
      final CreateFlagParam createFlagParam,
      final NoRedirectParam noredirectParam,
      final StoragePolicyParam policyName
      ) throws IOException, URISyntaxException {

    final Configuration conf = (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
    final NameNode namenode = (NameNode)context.getAttribute("name.node");
    final ClientProtocol cp = getRpcClientProtocol();

    switch(op.getValue()) {
    case CREATE:
    {
      final URI uri = redirectURI(null, namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L, blockSize.getValue(conf),
          exclDatanodes.getValue(), permission, unmaskedPermission,
          overwrite, bufferSize, replication, blockSize, createParent,
          createFlagParam);
      if(!noredirectParam.getValue()) {
        return Response.temporaryRedirect(uri)
          .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
    }
    case MKDIRS:
    {
      FsPermission masked = unmaskedPermission.getValue() == null ?
          permission.getDirFsPermission() :
          FsCreateModes.create(permission.getDirFsPermission(),
              unmaskedPermission.getDirFsPermission());
      final boolean b = cp.mkdirs(fullpath, masked, true);
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CREATESYMLINK:
    {
      validateOpParams(op, destination);
      cp.createSymlink(destination.getValue(), fullpath,
          PermissionParam.getDefaultSymLinkFsPermission(),
          createParent.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENAME:
    {
      validateOpParams(op, destination);
      final EnumSet<Options.Rename> s = renameOptions.getValue();
      if (s.isEmpty()) {
        final boolean b = cp.rename(fullpath, destination.getValue());
        final String js = JsonUtil.toJsonString("boolean", b);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      } else {
        cp.rename2(fullpath, destination.getValue(),
            s.toArray(new Options.Rename[s.size()]));
        return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
      }
    }
    case SETREPLICATION:
    {
      final boolean b = cp.setReplication(fullpath, replication.getValue(conf));
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case SETOWNER:
    {
      if (owner.getValue() == null && group.getValue() == null) {
        throw new IllegalArgumentException("Both owner and group are empty.");
      }

      cp.setOwner(fullpath, owner.getValue(), group.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETPERMISSION:
    {
      cp.setPermission(fullpath, permission.getDirFsPermission());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETTIMES:
    {
      cp.setTimes(fullpath, modificationTime.getValue(), accessTime.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case RENEWDELEGATIONTOKEN:
    {
      validateOpParams(op, delegationTokenArgument);
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      final long expiryTime = cp.renewDelegationToken(token);
      final String js = JsonUtil.toJsonString("long", expiryTime);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CANCELDELEGATIONTOKEN:
    {
      validateOpParams(op, delegationTokenArgument);
      final Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationTokenArgument.getValue());
      cp.cancelDelegationToken(token);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case MODIFYACLENTRIES: {
      validateOpParams(op, aclPermission);
      cp.modifyAclEntries(fullpath, aclPermission.getAclPermission(true));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEACLENTRIES: {
      validateOpParams(op, aclPermission);
      cp.removeAclEntries(fullpath, aclPermission.getAclPermission(false));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEDEFAULTACL: {
      cp.removeDefaultAcl(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEACL: {
      cp.removeAcl(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETACL: {
      validateOpParams(op, aclPermission);
      cp.setAcl(fullpath, aclPermission.getAclPermission(true));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETXATTR: {
      validateOpParams(op, xattrName, xattrSetFlag);
      cp.setXAttr(
          fullpath,
          XAttrHelper.buildXAttr(xattrName.getXAttrName(),
              xattrValue.getXAttrValue()), xattrSetFlag.getFlag());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case REMOVEXATTR: {
      validateOpParams(op, xattrName);
      cp.removeXAttr(fullpath,
          XAttrHelper.buildXAttr(xattrName.getXAttrName()));
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case ALLOWSNAPSHOT: {
      cp.allowSnapshot(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case CREATESNAPSHOT: {
      String snapshotPath =
          cp.createSnapshot(fullpath, snapshotName.getValue());
      final String js = JsonUtil.toJsonString(
          org.apache.hadoop.fs.Path.class.getSimpleName(), snapshotPath);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case RENAMESNAPSHOT: {
      validateOpParams(op, oldSnapshotName, snapshotName);
      cp.renameSnapshot(fullpath, oldSnapshotName.getValue(),
          snapshotName.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case DISALLOWSNAPSHOT: {
      cp.disallowSnapshot(fullpath);
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    case SETSTORAGEPOLICY: {
      if (policyName.getValue() == null) {
        throw new IllegalArgumentException("Storage policy name is empty.");
      }
      cp.setStoragePolicy(fullpath, policyName.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP POST request for the root. */
  @POST
  @Path("/")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(NewLengthParam.NAME) @DefaultValue(NewLengthParam.DEFAULT)
          final NewLengthParam newLength,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect
      ) throws IOException, InterruptedException {
    return post(ugi, delegation, username, doAsUser, ROOT, op, concatSrcs,
        bufferSize, excludeDatanodes, newLength, noredirect);
  }

  /** Handle HTTP POST request. */
  @POST
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
      @QueryParam(ConcatSourcesParam.NAME) @DefaultValue(ConcatSourcesParam.DEFAULT)
          final ConcatSourcesParam concatSrcs,
      @QueryParam(BufferSizeParam.NAME) @DefaultValue(BufferSizeParam.DEFAULT)
          final BufferSizeParam bufferSize,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(NewLengthParam.NAME) @DefaultValue(NewLengthParam.DEFAULT)
          final NewLengthParam newLength,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, concatSrcs, bufferSize,
        excludeDatanodes, newLength);

    return doAs(ugi, new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
          return post(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, concatSrcs, bufferSize,
              excludeDatanodes, newLength, noredirect);
      }
    });
  }

  private Response post(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final PostOpParam op,
      final ConcatSourcesParam concatSrcs,
      final BufferSizeParam bufferSize,
      final ExcludeDatanodesParam excludeDatanodes,
      final NewLengthParam newLength,
      final NoRedirectParam noredirectParam
      ) throws IOException, URISyntaxException {
    final ClientProtocol cp = getRpcClientProtocol();

    switch(op.getValue()) {
    case APPEND:
    {
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final URI uri = redirectURI(null, namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L, -1L,
          excludeDatanodes.getValue(), bufferSize);
      if(!noredirectParam.getValue()) {
        return Response.temporaryRedirect(uri)
          .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
    }
    case CONCAT:
    {
      validateOpParams(op, concatSrcs);
      cp.concat(fullpath, concatSrcs.getAbsolutePaths());
      return Response.ok().build();
    }
    case TRUNCATE:
    {
      validateOpParams(op, newLength);
      // We treat each rest request as a separate client.
      final boolean b = cp.truncate(fullpath, newLength.getValue(),
          "DFSClient_" + DFSUtil.getSecureRandom().nextLong());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case UNSETSTORAGEPOLICY: {
      cp.unsetStoragePolicy(fullpath);
      return Response.ok().build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  /** Handle HTTP GET request for the root. */
  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT)
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT)
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect,
      @QueryParam(StartAfterParam.NAME) @DefaultValue(StartAfterParam.DEFAULT)
          final StartAfterParam startAfter
      ) throws IOException, InterruptedException {
    return get(ugi, delegation, username, doAsUser, ROOT, op, offset, length,
        renewer, bufferSize, xattrNames, xattrEncoding, excludeDatanodes,
        fsAction, snapshotName, oldSnapshotName, tokenKind, tokenService,
        noredirect, startAfter);
  }

  /** Handle HTTP GET request. */
  @GET
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
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
          final BufferSizeParam bufferSize,
      @QueryParam(XAttrNameParam.NAME) @DefaultValue(XAttrNameParam.DEFAULT) 
          final List<XAttrNameParam> xattrNames,
      @QueryParam(XAttrEncodingParam.NAME) @DefaultValue(XAttrEncodingParam.DEFAULT) 
          final XAttrEncodingParam xattrEncoding,
      @QueryParam(ExcludeDatanodesParam.NAME) @DefaultValue(ExcludeDatanodesParam.DEFAULT)
          final ExcludeDatanodesParam excludeDatanodes,
      @QueryParam(FsActionParam.NAME) @DefaultValue(FsActionParam.DEFAULT)
          final FsActionParam fsAction,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName,
      @QueryParam(OldSnapshotNameParam.NAME) @DefaultValue(OldSnapshotNameParam.DEFAULT)
          final OldSnapshotNameParam oldSnapshotName,
      @QueryParam(TokenKindParam.NAME) @DefaultValue(TokenKindParam.DEFAULT)
          final TokenKindParam tokenKind,
      @QueryParam(TokenServiceParam.NAME) @DefaultValue(TokenServiceParam.DEFAULT)
          final TokenServiceParam tokenService,
      @QueryParam(NoRedirectParam.NAME) @DefaultValue(NoRedirectParam.DEFAULT)
          final NoRedirectParam noredirect,
      @QueryParam(StartAfterParam.NAME) @DefaultValue(StartAfterParam.DEFAULT)
          final StartAfterParam startAfter
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, offset, length,
        renewer, bufferSize, xattrEncoding, excludeDatanodes, fsAction,
        snapshotName, oldSnapshotName, tokenKind, tokenService, startAfter);

    return doAs(ugi, new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException, URISyntaxException {
        return get(ugi, delegation, username, doAsUser, path.getAbsolutePath(),
            op, offset, length, renewer, bufferSize, xattrNames, xattrEncoding,
            excludeDatanodes, fsAction, snapshotName, oldSnapshotName,
            tokenKind, tokenService, noredirect, startAfter);
      }
    });
  }

  private static String encodeFeInfo(FileEncryptionInfo feInfo) {
    Encoder encoder = Base64.getEncoder();
    String encodedValue = encoder
        .encodeToString(PBHelperClient.convert(feInfo).toByteArray());
    return encodedValue;
  }

  private Response get(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final GetOpParam op,
      final OffsetParam offset,
      final LengthParam length,
      final RenewerParam renewer,
      final BufferSizeParam bufferSize,
      final List<XAttrNameParam> xattrNames,
      final XAttrEncodingParam xattrEncoding,
      final ExcludeDatanodesParam excludeDatanodes,
      final FsActionParam fsAction,
      final SnapshotNameParam snapshotName,
      final OldSnapshotNameParam oldSnapshotName,
      final TokenKindParam tokenKind,
      final TokenServiceParam tokenService,
      final NoRedirectParam noredirectParam,
      final StartAfterParam startAfter
      ) throws IOException, URISyntaxException {
    final Configuration conf = (Configuration) context
        .getAttribute(JspHelper.CURRENT_CONF);
    final ClientProtocol cp = getRpcClientProtocol();

    switch(op.getValue()) {
    case OPEN:
    {
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      ResponseBuilder rb = Response.noContent();
      final URI uri = redirectURI(rb, namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), offset.getValue(), -1L,
          excludeDatanodes.getValue(), offset, length, bufferSize);
      if(!noredirectParam.getValue()) {
        return rb.status(Status.TEMPORARY_REDIRECT).location(uri)
            .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return rb.status(Status.OK).entity(js).type(MediaType.APPLICATION_JSON)
            .build();
      }
    }
    case GET_BLOCK_LOCATIONS:
    {
      final long offsetValue = offset.getValue();
      final Long lengthValue = length.getValue();
      final LocatedBlocks locatedblocks = cp.getBlockLocations(fullpath,
          offsetValue, lengthValue != null? lengthValue: Long.MAX_VALUE);
      final String js = JsonUtil.toJsonString(locatedblocks);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILESTATUS:
    {
      final HdfsFileStatus status = cp.getFileInfo(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status, true);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS:
    {
      final StreamingOutput streaming = getListingStream(cp, fullpath);
      return Response.ok(streaming).type(MediaType.APPLICATION_JSON).build();
    }
    case GETCONTENTSUMMARY:
    {
      final ContentSummary contentsummary = cp.getContentSummary(fullpath);
      final String js = JsonUtil.toJsonString(contentsummary);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETFILECHECKSUM:
    {
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final URI uri = redirectURI(null, namenode, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L, -1L, null);
      if(!noredirectParam.getValue()) {
        return Response.temporaryRedirect(uri)
          .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
    }
    case GETDELEGATIONTOKEN:
    {
      if (delegation.getValue() != null) {
        throw new IllegalArgumentException(delegation.getName()
            + " parameter is not null.");
      }
      final NameNode namenode = (NameNode)context.getAttribute("name.node");
      final Token<? extends TokenIdentifier> token = generateDelegationToken(
          namenode, ugi, renewer.getValue());

      final String setServiceName = tokenService.getValue();
      final String setKind = tokenKind.getValue();
      if (setServiceName != null) {
        token.setService(new Text(setServiceName));
      }
      if (setKind != null) {
        token.setKind(new Text(setKind));
      }
      final String js = JsonUtil.toJsonString(token);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETHOMEDIRECTORY: {
      final String js = JsonUtil.toJsonString("Path",
          FileSystem.get(conf != null ? conf : new Configuration())
              .getHomeDirectory().toUri().getPath());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETACLSTATUS: {
      AclStatus status = cp.getAclStatus(fullpath);
      if (status == null) {
        throw new FileNotFoundException("File does not exist: " + fullpath);
      }

      final String js = JsonUtil.toJsonString(status);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETXATTRS: {
      validateOpParams(op, xattrEncoding);
      List<String> names = null;
      if (xattrNames != null) {
        names = Lists.newArrayListWithCapacity(xattrNames.size());
        for (XAttrNameParam xattrName : xattrNames) {
          if (xattrName.getXAttrName() != null) {
            names.add(xattrName.getXAttrName());
          }
        }
      }
      List<XAttr> xAttrs = cp.getXAttrs(fullpath, (names != null &&
          !names.isEmpty()) ? XAttrHelper.buildXAttrs(names) : null);
      final String js = JsonUtil.toJsonString(xAttrs,
          xattrEncoding.getEncoding());
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTXATTRS: {
      final List<XAttr> xAttrs = cp.listXAttrs(fullpath);
      final String js = JsonUtil.toJsonString(xAttrs);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case CHECKACCESS: {
      validateOpParams(op, fsAction);
      cp.checkAccess(fullpath, FsAction.getFsAction(fsAction.getValue()));
      return Response.ok().build();
    }
    case GETTRASHROOT: {
      final String trashPath = getTrashRoot(fullpath, conf);
      final String jsonStr = JsonUtil.toJsonString("Path", trashPath);
      return Response.ok(jsonStr).type(MediaType.APPLICATION_JSON).build();
    }
    case LISTSTATUS_BATCH:
    {
      byte[] start = HdfsFileStatus.EMPTY_NAME;
      if (startAfter != null && startAfter.getValue() != null) {
        start = startAfter.getValue().getBytes(Charsets.UTF_8);
      }
      final DirectoryListing listing = getDirectoryListing(cp, fullpath, start);
      final String js = JsonUtil.toJsonString(listing);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETALLSTORAGEPOLICY: {
      BlockStoragePolicy[] storagePolicies = cp.getStoragePolicies();
      final String js = JsonUtil.toJsonString(storagePolicies);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETSTORAGEPOLICY: {
      BlockStoragePolicy storagePolicy = cp.getStoragePolicy(fullpath);
      final String js = JsonUtil.toJsonString(storagePolicy);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case GETSERVERDEFAULTS: {
      // Since none of the server defaults values are hot reloaded, we can
      // cache the output of serverDefaults.
      String serverDefaultsResponse =
          (String) context.getAttribute("serverDefaults");
      if (serverDefaultsResponse == null) {
        FsServerDefaults serverDefaults = cp.getServerDefaults();
        serverDefaultsResponse = JsonUtil.toJsonString(serverDefaults);
        context.setAttribute("serverDefaults", serverDefaultsResponse);
      }
      return Response.ok(serverDefaultsResponse)
          .type(MediaType.APPLICATION_JSON).build();
    }
    case GETSNAPSHOTDIFF: {
      SnapshotDiffReport diffReport =
          cp.getSnapshotDiffReport(fullpath, oldSnapshotName.getValue(),
              snapshotName.getValue());
      final String js = JsonUtil.toJsonString(diffReport);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  private static String getTrashRoot(String fullPath,
      Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf != null ? conf : new Configuration());
    return fs.getTrashRoot(
        new org.apache.hadoop.fs.Path(fullPath)).toUri().getPath();
  }

  private static DirectoryListing getDirectoryListing(final ClientProtocol cp,
      final String p, byte[] startAfter) throws IOException {
    final DirectoryListing listing = cp.getListing(p, startAfter, false);
    if (listing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    return listing;
  }
  
  private static StreamingOutput getListingStream(final ClientProtocol cp,
      final String p) throws IOException {
    // allows exceptions like FNF or ACE to prevent http response of 200 for
    // a failure since we can't (currently) return error responses in the
    // middle of a streaming operation
    final DirectoryListing firstDirList = getDirectoryListing(cp, p,
        HdfsFileStatus.EMPTY_NAME);

    // must save ugi because the streaming object will be executed outside
    // the remote user's ugi
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return new StreamingOutput() {
      @Override
      public void write(final OutputStream outstream) throws IOException {
        final PrintWriter out = new PrintWriter(new OutputStreamWriter(
            outstream, Charsets.UTF_8));
        out.println("{\"" + FileStatus.class.getSimpleName() + "es\":{\""
            + FileStatus.class.getSimpleName() + "\":[");

        try {
          // restore remote user's ugi
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
              long n = 0;
              for (DirectoryListing dirList = firstDirList; ;
                   dirList = getDirectoryListing(cp, p, dirList.getLastName())
              ) {
                // send each segment of the directory listing
                for (HdfsFileStatus s : dirList.getPartialListing()) {
                  if (n++ > 0) {
                    out.println(',');
                  }
                  out.print(JsonUtil.toJsonString(s, false));
                }
                // stop if last segment
                if (!dirList.hasMore()) {
                  break;
                }
              }
              return null;
            }
          });
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        
        out.println();
        out.println("]}}");
        out.flush();
      }
    };
  }

  /** Handle HTTP DELETE request for the root. */
  @DELETE
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
          final RecursiveParam recursive,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName
      ) throws IOException, InterruptedException {
    return delete(ugi, delegation, username, doAsUser, ROOT, op, recursive,
        snapshotName);
  }

  /** Handle HTTP DELETE request. */
  @DELETE
  @Path("{" + UriFsPathParam.NAME + ":.*}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
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
          final RecursiveParam recursive,
      @QueryParam(SnapshotNameParam.NAME) @DefaultValue(SnapshotNameParam.DEFAULT)
          final SnapshotNameParam snapshotName
      ) throws IOException, InterruptedException {

    init(ugi, delegation, username, doAsUser, path, op, recursive, snapshotName);

    return doAs(ugi, new PrivilegedExceptionAction<Response>() {
      @Override
      public Response run() throws IOException {
          return delete(ugi, delegation, username, doAsUser,
              path.getAbsolutePath(), op, recursive, snapshotName);
      }
    });
  }

  private Response delete(
      final UserGroupInformation ugi,
      final DelegationParam delegation,
      final UserParam username,
      final DoAsParam doAsUser,
      final String fullpath,
      final DeleteOpParam op,
      final RecursiveParam recursive,
      final SnapshotNameParam snapshotName
      ) throws IOException {
    final ClientProtocol cp = getRpcClientProtocol();

    switch(op.getValue()) {
    case DELETE: {
      final boolean b = cp.delete(fullpath, recursive.getValue());
      final String js = JsonUtil.toJsonString("boolean", b);
      return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
    case DELETESNAPSHOT: {
      validateOpParams(op, snapshotName);
      cp.deleteSnapshot(fullpath, snapshotName.getValue());
      return Response.ok().type(MediaType.APPLICATION_OCTET_STREAM).build();
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }
}
