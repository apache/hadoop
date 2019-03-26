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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.util.StringUtils.getTrimmedStringCollection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.ConcatSourcesParam;
import org.apache.hadoop.hdfs.web.resources.CreateFlagParam;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.ExcludeDatanodesParam;
import org.apache.hadoop.hdfs.web.resources.FsActionParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.GroupParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.LengthParam;
import org.apache.hadoop.hdfs.web.resources.ModificationTimeParam;
import org.apache.hadoop.hdfs.web.resources.NewLengthParam;
import org.apache.hadoop.hdfs.web.resources.NoRedirectParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OldSnapshotNameParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.OwnerParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.PostOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.RenameOptionSetParam;
import org.apache.hadoop.hdfs.web.resources.RenewerParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.SnapshotNameParam;
import org.apache.hadoop.hdfs.web.resources.StartAfterParam;
import org.apache.hadoop.hdfs.web.resources.StoragePolicyParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.TokenKindParam;
import org.apache.hadoop.hdfs.web.resources.TokenServiceParam;
import org.apache.hadoop.hdfs.web.resources.UnmaskedPermissionParam;
import org.apache.hadoop.hdfs.web.resources.UriFsPathParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.hdfs.web.resources.XAttrEncodingParam;
import org.apache.hadoop.hdfs.web.resources.XAttrNameParam;
import org.apache.hadoop.hdfs.web.resources.XAttrSetFlagParam;
import org.apache.hadoop.hdfs.web.resources.XAttrValueParam;
import org.apache.hadoop.ipc.ExternalCall;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * WebHDFS Router implementation. This is an extension of
 * {@link NamenodeWebHdfsMethods}, and tries to reuse as much as possible.
 */
@Path("")
@ResourceFilters(ParamFilter.class)
public class RouterWebHdfsMethods extends NamenodeWebHdfsMethods {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterWebHdfsMethods.class);

  private static final ThreadLocal<String> REMOTE_ADDRESS =
      new ThreadLocal<String>();

  private @Context HttpServletRequest request;
  private String method;
  private String query;
  private String reqPath;

  public RouterWebHdfsMethods(@Context HttpServletRequest request) {
    super(request);
    this.method = request.getMethod();
    this.query = request.getQueryString();
    this.reqPath = request.getServletPath();
    REMOTE_ADDRESS.set(JspHelper.getRemoteAddr(request));
  }

  @Override
  protected void init(final UserGroupInformation ugi,
                      final DelegationParam delegation,
                      final UserParam username, final DoAsParam doAsUser,
                      final UriFsPathParam path, final HttpOpParam<?> op,
                      final Param<?, ?>... parameters) {
    super.init(ugi, delegation, username, doAsUser, path, op, parameters);

    REMOTE_ADDRESS.set(JspHelper.getRemoteAddr(request));
  }

  @Override
  protected ClientProtocol getRpcClientProtocol() throws IOException {
    final Router router = getRouter();
    final RouterRpcServer routerRpcServer = router.getRpcServer();
    if (routerRpcServer == null) {
      throw new RetriableException("Router is in startup mode");
    }
    return routerRpcServer;
  }

  private void reset() {
    REMOTE_ADDRESS.set(null);
  }

  @Override
  protected String getRemoteAddr() {
    return REMOTE_ADDRESS.get();
  }

  @Override
  protected void queueExternalCall(ExternalCall call)
      throws IOException, InterruptedException {
    getRouter().getRpcServer().getServer().queueCall(call);
  }

  private Router getRouter() {
    return (Router)getContext().getAttribute("name.node");
  }

  private static RouterRpcServer getRPCServer(final Router router)
      throws IOException {
    final RouterRpcServer routerRpcServer = router.getRpcServer();
    if (routerRpcServer == null) {
      throw new RetriableException("Router is in startup mode");
    }
    return routerRpcServer;
  }

  @Override
  protected Response put(
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

    switch(op.getValue()) {
    case CREATE:
    {
      final Router router = getRouter();
      final URI uri = redirectURI(router, fullpath);
      if (!noredirectParam.getValue()) {
        return Response.temporaryRedirect(uri)
            .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
    }
    case MKDIRS:
    case CREATESYMLINK:
    case RENAME:
    case SETREPLICATION:
    case SETOWNER:
    case SETPERMISSION:
    case SETTIMES:
    case RENEWDELEGATIONTOKEN:
    case CANCELDELEGATIONTOKEN:
    case MODIFYACLENTRIES:
    case REMOVEACLENTRIES:
    case REMOVEDEFAULTACL:
    case REMOVEACL:
    case SETACL:
    case SETXATTR:
    case REMOVEXATTR:
    case ALLOWSNAPSHOT:
    case CREATESNAPSHOT:
    case RENAMESNAPSHOT:
    case DISALLOWSNAPSHOT:
    case SETSTORAGEPOLICY:
    {
      // Whitelist operations that can handled by NamenodeWebHdfsMethods
      return super.put(ugi, delegation, username, doAsUser, fullpath, op,
          destination, owner, group, permission, unmaskedPermission,
          overwrite, bufferSize, replication, blockSize, modificationTime,
          accessTime, renameOptions, createParent, delegationTokenArgument,
          aclPermission, xattrName, xattrValue, xattrSetFlag, snapshotName,
          oldSnapshotName, exclDatanodes, createFlagParam, noredirectParam,
          policyName);
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  @Override
  protected Response post(
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
      final NoRedirectParam noRedirectParam
  ) throws IOException, URISyntaxException {
    switch(op.getValue()) {
    case APPEND:
    {
      final Router router = getRouter();
      final URI uri = redirectURI(router, ugi, delegation, username,
          doAsUser, fullpath, op.getValue(), -1L,
          excludeDatanodes.getValue(), bufferSize);
      if (!noRedirectParam.getValue()) {
        return Response.temporaryRedirect(uri)
            .type(MediaType.APPLICATION_OCTET_STREAM).build();
      } else {
        final String js = JsonUtil.toJsonString("Location", uri);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      }
    }
    case CONCAT:
    case TRUNCATE:
    case UNSETSTORAGEPOLICY:
    {
      return super.post(ugi, delegation, username, doAsUser, fullpath, op,
          concatSrcs, bufferSize, excludeDatanodes, newLength,
          noRedirectParam);
    }
    default:
      throw new UnsupportedOperationException(op + " is not supported");
    }
  }

  @Override
  protected Response get(
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
    try {
      final Router router = getRouter();

      switch (op.getValue()) {
      case OPEN:
      {
        final URI uri = redirectURI(router, ugi, delegation, username,
            doAsUser, fullpath, op.getValue(), offset.getValue(),
            excludeDatanodes.getValue(), offset, length, bufferSize);
        if (!noredirectParam.getValue()) {
          return Response.temporaryRedirect(uri)
              .type(MediaType.APPLICATION_OCTET_STREAM).build();
        } else {
          final String js = JsonUtil.toJsonString("Location", uri);
          return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        }
      }
      case GETFILECHECKSUM:
      {
        final URI uri = redirectURI(router, ugi, delegation, username,
            doAsUser, fullpath, op.getValue(), -1L, null);
        if (!noredirectParam.getValue()) {
          return Response.temporaryRedirect(uri)
              .type(MediaType.APPLICATION_OCTET_STREAM).build();
        } else {
          final String js = JsonUtil.toJsonString("Location", uri);
          return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        }
      }
      case GET_BLOCK_LOCATIONS:
      case GETFILESTATUS:
      case LISTSTATUS:
      case GETCONTENTSUMMARY:
      case GETHOMEDIRECTORY:
      case GETACLSTATUS:
      case GETXATTRS:
      case LISTXATTRS:
      case CHECKACCESS:
      {
        return super.get(ugi, delegation, username, doAsUser, fullpath, op,
            offset, length, renewer, bufferSize, xattrNames, xattrEncoding,
            excludeDatanodes, fsAction, snapshotName, oldSnapshotName,
            tokenKind, tokenService, noredirectParam, startAfter);
      }
      default:
        throw new UnsupportedOperationException(op + " is not supported");
      }
    } finally {
      reset();
    }
  }

  /**
   * Get the redirect URI from the Namenode responsible for a path.
   * @param router Router to check.
   * @param path Path to get location for.
   * @return URI returned by the Namenode.
   * @throws IOException If it cannot get the redirect URI.
   */
  private URI redirectURI(final Router router, final String path)
      throws IOException {
    // Forward the request to the proper Namenode
    final HttpURLConnection conn = forwardRequest(router, path);
    try {
      conn.setInstanceFollowRedirects(false);
      conn.setDoOutput(true);
      conn.connect();

      // Read the reply from the Namenode
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpServletResponse.SC_TEMPORARY_REDIRECT) {
        LOG.info("We expected a redirection from the Namenode, not {}",
            responseCode);
        return null;
      }

      // Extract the redirect location and return it
      String redirectLocation = conn.getHeaderField("Location");
      try {
        // We modify the namenode location and the path
        redirectLocation = redirectLocation
            .replaceAll("(?<=[?&;])namenoderpcaddress=.*?(?=[&;])",
                "namenoderpcaddress=" + router.getRouterId())
            .replaceAll("(?<=[/])webhdfs/v1/.*?(?=[?])",
                "webhdfs/v1" + path);
        return new URI(redirectLocation);
      } catch (URISyntaxException e) {
        LOG.error("Cannot parse redirect location {}", redirectLocation);
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return null;
  }

  /**
   * Forwards a request to a subcluster.
   * @param router Router to check.
   * @param path Path in HDFS.
   * @return Reply from the subcluster.
   * @throws IOException
   */
  private HttpURLConnection forwardRequest(
      final Router router, final String path) throws IOException {
    final Configuration conf =
        (Configuration)getContext().getAttribute(JspHelper.CURRENT_CONF);
    URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);

    // Find the namespace responsible for a path
    final RouterRpcServer rpcServer = getRPCServer(router);
    RemoteLocation createLoc = rpcServer.getCreateLocation(path);
    String nsId = createLoc.getNameserviceId();
    String dest = createLoc.getDest();
    ActiveNamenodeResolver nnResolver = router.getNamenodeResolver();
    List<? extends FederationNamenodeContext> namenodes =
        nnResolver.getNamenodesForNameserviceId(nsId);

    // Go over the namenodes responsible for that namespace
    for (FederationNamenodeContext namenode : namenodes) {
      try {
        // Generate the request for the namenode
        String nnWebAddress = namenode.getWebAddress();
        String[] nnWebAddressSplit = nnWebAddress.split(":");
        String host = nnWebAddressSplit[0];
        int port = Integer.parseInt(nnWebAddressSplit[1]);

        // Avoid double-encoding here
        query = URLDecoder.decode(query, "UTF-8");
        URI uri = new URI(getScheme(), null, host, port,
            reqPath + dest, query, null);
        URL url = uri.toURL();

        // Send a request to the proper Namenode
        final HttpURLConnection conn =
            (HttpURLConnection)connectionFactory.openConnection(url);
        conn.setRequestMethod(method);

        connectionFactory.destroy();
        return conn;
      } catch (Exception e) {
        LOG.error("Cannot redirect request to {}", namenode, e);
      }
    }
    connectionFactory.destroy();
    return null;
  }

  /**
   * Get a URI to redirect an operation to.
   * @param router Router to check.
   * @param ugi User group information.
   * @param delegation Delegation token.
   * @param username User name.
   * @param doAsUser Do as user.
   * @param path Path to check.
   * @param op Operation to perform.
   * @param openOffset Offset for opening a file.
   * @param excludeDatanodes Blocks to excluded.
   * @param parameters Other parameters.
   * @return Redirection URI.
   * @throws URISyntaxException If it cannot parse the URI.
   * @throws IOException If it cannot create the URI.
   */
  private URI redirectURI(final Router router, final UserGroupInformation ugi,
      final DelegationParam delegation, final UserParam username,
      final DoAsParam doAsUser, final String path, final HttpOpParam.Op op,
      final long openOffset, final String excludeDatanodes,
      final Param<?, ?>... parameters) throws URISyntaxException, IOException {
    final DatanodeInfo dn =
        chooseDatanode(router, path, op, openOffset, excludeDatanodes);

    if (dn == null) {
      throw new IOException("Failed to find datanode, suggest to check cluster"
          + " health. excludeDatanodes=" + excludeDatanodes);
    }

    final String delegationQuery;
    if (!UserGroupInformation.isSecurityEnabled()) {
      // security disabled
      delegationQuery = Param.toSortedString("&", doAsUser, username);
    } else if (delegation.getValue() != null) {
      // client has provided a token
      delegationQuery = "&" + delegation;
    } else {
      // generate a token
      final Token<? extends TokenIdentifier> t = generateDelegationToken(
          router, ugi, request.getUserPrincipal().getName());
      delegationQuery = "&delegation=" + t.encodeToUrlString();
    }

    final String redirectQuery = op.toQueryString() + delegationQuery
        + "&namenoderpcaddress=" + router.getRouterId()
        + Param.toSortedString("&", parameters);
    final String uripath = WebHdfsFileSystem.PATH_PREFIX + path;

    int port = "http".equals(getScheme()) ? dn.getInfoPort() :
        dn.getInfoSecurePort();
    final URI uri = new URI(getScheme(), null, dn.getHostName(), port, uripath,
        redirectQuery, null);

    if (LOG.isTraceEnabled()) {
      LOG.trace("redirectURI={}", uri);
    }
    return uri;
  }

  private DatanodeInfo chooseDatanode(final Router router,
      final String path, final HttpOpParam.Op op, final long openOffset,
      final String excludeDatanodes) throws IOException {
    // We need to get the DNs as a privileged user
    final RouterRpcServer rpcServer = getRPCServer(router);
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

    DatanodeInfo[] dns = loginUser.doAs(
        new PrivilegedAction<DatanodeInfo[]>() {
          @Override
          public DatanodeInfo[] run() {
            try {
              return rpcServer.getDatanodeReport(DatanodeReportType.LIVE);
            } catch (IOException e) {
              LOG.error("Cannot get the datanodes from the RPC server", e);
              return null;
            }
          }
        });

    HashSet<Node> excludes = new HashSet<Node>();
    if (excludeDatanodes != null) {
      Collection<String> collection =
          getTrimmedStringCollection(excludeDatanodes);
      for (DatanodeInfo dn : dns) {
        if (collection.contains(dn.getName())) {
          excludes.add(dn);
        }
      }
    }

    if (op == GetOpParam.Op.OPEN ||
        op == PostOpParam.Op.APPEND ||
        op == GetOpParam.Op.GETFILECHECKSUM) {
      // Choose a datanode containing a replica
      final ClientProtocol cp = getRpcClientProtocol();
      final HdfsFileStatus status = cp.getFileInfo(path);
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
        final long offset = op == GetOpParam.Op.OPEN ? openOffset : len - 1;
        final LocatedBlocks locations = cp.getBlockLocations(path, offset, 1);
        final int count = locations.locatedBlockCount();
        if (count > 0) {
          LocatedBlock location0 = locations.get(0);
          return bestNode(location0.getLocations(), excludes);
        }
      }
    }

    return getRandomDatanode(dns, excludes);
  }

  /**
   * Get a random Datanode from a subcluster.
   * @param dns Nodes to be chosen from.
   * @param excludes Nodes to be excluded from.
   * @return Random datanode from a particular subluster.
   */
  private static DatanodeInfo getRandomDatanode(
      final DatanodeInfo[] dns, final HashSet<Node> excludes) {
    DatanodeInfo dn = null;

    if (dns == null) {
      return dn;
    }

    int numDNs = dns.length;
    int availableNodes = 0;
    if (excludes.isEmpty()) {
      availableNodes = numDNs;
    } else {
      for (DatanodeInfo di : dns) {
        if (!excludes.contains(di)) {
          availableNodes++;
        }
      }
    }

    // Return a random one from the list
    if (availableNodes > 0) {
      while (dn == null || excludes.contains(dn)) {
        Random rnd = new Random();
        int idx = rnd.nextInt(numDNs);
        dn = dns[idx];
      }
    }
    return dn;
  }

  /**
   * Generate the delegation tokens for this request.
   * @param router Router.
   * @param ugi User group information.
   * @param renewer Who is asking for the renewal.
   * @return The delegation tokens.
   * @throws IOException If it cannot create the tokens.
   */
  private Token<? extends TokenIdentifier> generateDelegationToken(
      final Router router, final UserGroupInformation ugi,
      final String renewer) throws IOException {
    throw new UnsupportedOperationException("TODO Generate token for ugi=" +
        ugi + " request=" + request);
  }
}
