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

package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSUtils;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.AccessTimeParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.AclPermissionParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.BlockSizeParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.DataParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.DestinationParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.ECPolicyParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.FilterParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.FsActionParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.GroupParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.LenParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.ModifiedTimeParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.NewLengthParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.NoRedirectParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.OffsetParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.OldSnapshotNameParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.OperationParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.OverwriteParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.OwnerParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.PermissionParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.PolicyNameParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.RecursiveParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.ReplicationParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.SourcesParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.UnmaskedPermissionParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.SnapshotNameParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.XAttrEncodingParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.XAttrNameParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.XAttrSetFlagParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.XAttrValueParam;
import org.apache.hadoop.fs.http.server.HttpFSParametersProvider.AllUsersParam;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.lib.service.FileSystemAccessException;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.servlet.FileSystemReleaseFilter;
import org.apache.hadoop.lib.wsrs.InputStreamEntity;
import org.apache.hadoop.lib.wsrs.Parameters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Main class of HttpFSServer server.
 * <p>
 * The <code>HttpFSServer</code> class uses Jersey JAX-RS to binds HTTP requests to the
 * different operations.
 */
@Path(HttpFSFileSystem.SERVICE_VERSION)
@InterfaceAudience.Private
public class HttpFSServer {

  enum AccessMode {
    READWRITE, WRITEONLY, READONLY;
  }
  private static Logger AUDIT_LOG = LoggerFactory.getLogger("httpfsaudit");
  private static final Logger LOG = LoggerFactory.getLogger(HttpFSServer.class);
  AccessMode accessMode = AccessMode.READWRITE;

  public HttpFSServer() {
    Configuration conf = HttpFSServerWebApp.get().getConfig();
    final String accessModeString =  conf.get("httpfs.access.mode", "read-write").toLowerCase();
    if(accessModeString.compareTo("write-only") == 0)
      accessMode = AccessMode.WRITEONLY;
    else if(accessModeString.compareTo("read-only") == 0)
      accessMode = AccessMode.READONLY;
    else
      accessMode = AccessMode.READWRITE;
  }


  // First try getting a user through HttpUserGroupInformation. This will return
  // if the built-in hadoop auth filter is not used.  Fall back to getting the
  // authenticated user from the request.
  private UserGroupInformation getHttpUGI(HttpServletRequest request) {
    UserGroupInformation user = HttpUserGroupInformation.get();
    if (user != null) {
      return user;
    }

    return UserGroupInformation.createRemoteUser(request.getUserPrincipal().getName());
  }

  private static final HttpFSParametersProvider PARAMETERS_PROVIDER =
      new HttpFSParametersProvider();

  private Parameters getParams(HttpServletRequest request) {
    return PARAMETERS_PROVIDER.get(request);
  }

  private static final Object[] NULL = new Object[0];

  /**
   * Executes a {@link FileSystemAccess.FileSystemExecutor} using a filesystem for the effective
   * user.
   *
   * @param ugi user making the request.
   * @param executor FileSystemExecutor to execute.
   *
   * @return FileSystemExecutor response
   *
   * @throws IOException thrown if an IO error occurs.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  private <T> T fsExecute(UserGroupInformation ugi, FileSystemAccess.FileSystemExecutor<T> executor)
    throws IOException, FileSystemAccessException {
    FileSystemAccess fsAccess = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class).getFileSystemConfiguration();
    return fsAccess.execute(ugi.getShortUserName(), conf, executor);
  }

  /**
   * Returns a filesystem instance. The filesystem instance is wired for release at the completion
   * of the current Servlet request via the {@link FileSystemReleaseFilter}.
   * <p>
   * If a do-as user is specified, the current user must be a valid proxyuser, otherwise an
   * <code>AccessControlException</code> will be thrown.
   *
   * @param ugi principal for whom the filesystem instance is.
   *
   * @return a filesystem for the specified user or do-as user.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  private FileSystem createFileSystem(UserGroupInformation ugi)
      throws IOException, FileSystemAccessException {
    String hadoopUser = ugi.getShortUserName();
    FileSystemAccess fsAccess = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class).getFileSystemConfiguration();
    FileSystem fs = fsAccess.createFileSystem(hadoopUser, conf);
    FileSystemReleaseFilter.setFileSystem(fs);
    return fs;
  }

  private void enforceRootPath(HttpFSFileSystem.Operation op, String path) {
    if (!path.equals("/")) {
      throw new UnsupportedOperationException(
        MessageFormat.format("Operation [{0}], invalid path [{1}], must be '/'",
                             op, path));
    }
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   *
   * @param uriInfo uri info of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getRoot(@Context UriInfo uriInfo,
                          @QueryParam(OperationParam.NAME) OperationParam op,
                          @Context HttpServletRequest request)
    throws IOException, FileSystemAccessException {
    return get("", uriInfo, op, request);
  }

  private String makeAbsolute(String path) {
    return "/" + ((path != null) ? path : "");
  }

  /**
   * Binding to handle GET requests, supported operations are
   *
   * @param path the path for operation.
   * @param uriInfo uri info of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @GET
  @Path("{path:.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response get(@PathParam("path") String path,
                      @Context UriInfo uriInfo,
                      @QueryParam(OperationParam.NAME) OperationParam op,
                      @Context HttpServletRequest request)
    throws IOException, FileSystemAccessException {
    // Restrict access to only GETFILESTATUS and LISTSTATUS in write-only mode
    if((op.value() != HttpFSFileSystem.Operation.GETFILESTATUS) &&
            (op.value() != HttpFSFileSystem.Operation.LISTSTATUS) &&
            accessMode == AccessMode.WRITEONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    Response response;
    path = makeAbsolute(path);
    final Parameters params = getParams(request);
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
    case OPEN: {
      Boolean noRedirect = params.get(
          NoRedirectParam.NAME, NoRedirectParam.class);
      if (noRedirect) {
        URI redirectURL = createOpenRedirectionURL(uriInfo);
        final String js = JsonUtil.toJsonString("Location", redirectURL);
        response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      } else {
        //Invoking the command directly using an unmanaged FileSystem that is
        // released by the FileSystemReleaseFilter
        final FSOperations.FSOpen command = new FSOperations.FSOpen(path);
        final FileSystem fs = createFileSystem(user);
        InputStream is = null;
        UserGroupInformation ugi = UserGroupInformation
            .createProxyUser(user.getShortUserName(),
                UserGroupInformation.getLoginUser());
        try {
          is = ugi.doAs(new PrivilegedExceptionAction<InputStream>() {
            @Override
            public InputStream run() throws Exception {
              return command.execute(fs);
            }
          });
        } catch (InterruptedException ie) {
          LOG.warn("Open interrupted.", ie);
          Thread.currentThread().interrupt();
        }
        Long offset = params.get(OffsetParam.NAME, OffsetParam.class);
        Long len = params.get(LenParam.NAME, LenParam.class);
        AUDIT_LOG.info("[{}] offset [{}] len [{}]",
            new Object[] { path, offset, len });
        InputStreamEntity entity = new InputStreamEntity(is, offset, len);
        response = Response.ok(entity).type(MediaType.APPLICATION_OCTET_STREAM)
            .build();
      }
      break;
    }
    case GETFILESTATUS: {
      FSOperations.FSFileStatus command = new FSOperations.FSFileStatus(path);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case LISTSTATUS: {
      String filter = params.get(FilterParam.NAME, FilterParam.class);
      FSOperations.FSListStatus command =
          new FSOperations.FSListStatus(path, filter);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("[{}] filter [{}]", path, (filter != null) ? filter : "-");
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETHOMEDIRECTORY: {
      enforceRootPath(op.value(), path);
      FSOperations.FSHomeDir command = new FSOperations.FSHomeDir();
      JSONObject json = fsExecute(user, command);
      AUDIT_LOG.info("Home Directory for [{}]", user);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case INSTRUMENTATION: {
      enforceRootPath(op.value(), path);
      Groups groups = HttpFSServerWebApp.get().get(Groups.class);
      Set<String> userGroups = groups.getGroupsSet(user.getShortUserName());
      if (!userGroups.contains(HttpFSServerWebApp.get().getAdminGroup())) {
        throw new AccessControlException(
            "User not in HttpFSServer admin group");
      }
      Instrumentation instrumentation =
          HttpFSServerWebApp.get().get(Instrumentation.class);
      Map snapshot = instrumentation.getSnapshot();
      response = Response.ok(snapshot).build();
      break;
    }
    case GETCONTENTSUMMARY: {
      FSOperations.FSContentSummary command =
          new FSOperations.FSContentSummary(path);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("Content summary for [{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETQUOTAUSAGE: {
      FSOperations.FSQuotaUsage command =
          new FSOperations.FSQuotaUsage(path);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("Quota Usage for [{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETFILECHECKSUM: {
      FSOperations.FSFileChecksum command =
          new FSOperations.FSFileChecksum(path);

      Boolean noRedirect = params.get(
          NoRedirectParam.NAME, NoRedirectParam.class);
      AUDIT_LOG.info("[{}]", path);
      if (noRedirect) {
        URI redirectURL = createOpenRedirectionURL(uriInfo);
        final String js = JsonUtil.toJsonString("Location", redirectURL);
        response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      } else {
        Map json = fsExecute(user, command);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      }
      break;
    }
    case GETFILEBLOCKLOCATIONS: {
      long offset = 0;
      long len = Long.MAX_VALUE;
      Long offsetParam = params.get(OffsetParam.NAME, OffsetParam.class);
      Long lenParam = params.get(LenParam.NAME, LenParam.class);
      AUDIT_LOG.info("[{}] offset [{}] len [{}]", path, offsetParam, lenParam);
      if (offsetParam != null && offsetParam > 0) {
        offset = offsetParam;
      }
      if (lenParam != null && lenParam > 0) {
        len = lenParam;
      }
      FSOperations.FSFileBlockLocations command =
          new FSOperations.FSFileBlockLocations(path, offset, len);
      @SuppressWarnings("rawtypes")
      Map locations = fsExecute(user, command);
      final String json = JsonUtil.toJsonString("BlockLocations", locations);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETACLSTATUS: {
      FSOperations.FSAclStatus command = new FSOperations.FSAclStatus(path);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("ACL status for [{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETXATTRS: {
      List<String> xattrNames =
          params.getValues(XAttrNameParam.NAME, XAttrNameParam.class);
      XAttrCodec encoding =
          params.get(XAttrEncodingParam.NAME, XAttrEncodingParam.class);
      FSOperations.FSGetXAttrs command =
          new FSOperations.FSGetXAttrs(path, xattrNames, encoding);
      @SuppressWarnings("rawtypes") Map json = fsExecute(user, command);
      AUDIT_LOG.info("XAttrs for [{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case LISTXATTRS: {
      FSOperations.FSListXAttrs command = new FSOperations.FSListXAttrs(path);
      @SuppressWarnings("rawtypes") Map json = fsExecute(user, command);
      AUDIT_LOG.info("XAttr names for [{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case LISTSTATUS_BATCH: {
      String startAfter = params.get(
          HttpFSParametersProvider.StartAfterParam.NAME,
          HttpFSParametersProvider.StartAfterParam.class);
      byte[] token = HttpFSUtils.EMPTY_BYTES;
      if (startAfter != null) {
        token = startAfter.getBytes(StandardCharsets.UTF_8);
      }
      FSOperations.FSListStatusBatch command = new FSOperations
          .FSListStatusBatch(path, token);
      @SuppressWarnings("rawtypes") Map json = fsExecute(user, command);
      AUDIT_LOG.info("[{}] token [{}]", path, token);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETTRASHROOT: {
      FSOperations.FSTrashRoot command = new FSOperations.FSTrashRoot(path);
      JSONObject json = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETALLSTORAGEPOLICY: {
      FSOperations.FSGetAllStoragePolicies command =
          new FSOperations.FSGetAllStoragePolicies();
      JSONObject json = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSTORAGEPOLICY: {
      FSOperations.FSGetStoragePolicy command =
          new FSOperations.FSGetStoragePolicy(path);
      JSONObject json = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSNAPSHOTDIFF: {
      String oldSnapshotName = params.get(OldSnapshotNameParam.NAME,
          OldSnapshotNameParam.class);
      String snapshotName = params.get(SnapshotNameParam.NAME,
          SnapshotNameParam.class);
      FSOperations.FSGetSnapshotDiff command =
          new FSOperations.FSGetSnapshotDiff(path, oldSnapshotName,
              snapshotName);
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSNAPSHOTDIFFLISTING: {
      String oldSnapshotName = params.get(OldSnapshotNameParam.NAME,
          OldSnapshotNameParam.class);
      String snapshotName = params.get(SnapshotNameParam.NAME,
          SnapshotNameParam.class);
      String snapshotDiffStartPath = params
          .get(HttpFSParametersProvider.SnapshotDiffStartPathParam.NAME,
              HttpFSParametersProvider.SnapshotDiffStartPathParam.class);
      Integer snapshotDiffIndex = params.get(HttpFSParametersProvider.SnapshotDiffIndexParam.NAME,
          HttpFSParametersProvider.SnapshotDiffIndexParam.class);
      FSOperations.FSGetSnapshotDiffListing command =
          new FSOperations.FSGetSnapshotDiffListing(path, oldSnapshotName,
              snapshotName, snapshotDiffStartPath, snapshotDiffIndex);
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSNAPSHOTTABLEDIRECTORYLIST: {
      FSOperations.FSGetSnapshottableDirListing command =
          new FSOperations.FSGetSnapshottableDirListing();
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", "/");
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSNAPSHOTLIST: {
      FSOperations.FSGetSnapshotListing command =
          new FSOperations.FSGetSnapshotListing(path);
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", "/");
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSERVERDEFAULTS: {
      FSOperations.FSGetServerDefaults command =
          new FSOperations.FSGetServerDefaults();
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", "/");
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case CHECKACCESS: {
      String mode = params.get(FsActionParam.NAME, FsActionParam.class);
      FsActionParam fsparam = new FsActionParam(mode);
      FSOperations.FSAccess command = new FSOperations.FSAccess(path,
          FsAction.getFsAction(fsparam.value()));
      fsExecute(user, command);
      AUDIT_LOG.info("[{}]", "/");
      response = Response.ok().build();
      break;
    }
    case GETECPOLICY: {
      FSOperations.FSGetErasureCodingPolicy command =
          new FSOperations.FSGetErasureCodingPolicy(path);
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETECPOLICIES: {
      FSOperations.FSGetErasureCodingPolicies command =
          new FSOperations.FSGetErasureCodingPolicies();
      String js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETECCODECS: {
      FSOperations.FSGetErasureCodingCodecs command =
          new FSOperations.FSGetErasureCodingCodecs();
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GET_BLOCK_LOCATIONS: {
      long offset = 0;
      long len = Long.MAX_VALUE;
      Long offsetParam = params.get(OffsetParam.NAME, OffsetParam.class);
      Long lenParam = params.get(LenParam.NAME, LenParam.class);
      AUDIT_LOG.info("[{}] offset [{}] len [{}]", path, offsetParam, lenParam);
      if (offsetParam != null && offsetParam > 0) {
        offset = offsetParam;
      }
      if (lenParam != null && lenParam > 0) {
        len = lenParam;
      }
      FSOperations.FSFileBlockLocationsLegacy command =
          new FSOperations.FSFileBlockLocationsLegacy(path, offset, len);
      @SuppressWarnings("rawtypes")
      Map locations = fsExecute(user, command);
      final String json = JsonUtil.toJsonString("LocatedBlocks", locations);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETFILELINKSTATUS: {
      FSOperations.FSFileLinkStatus command =
          new FSOperations.FSFileLinkStatus(path);
      @SuppressWarnings("rawtypes") Map js = fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETSTATUS: {
      FSOperations.FSStatus command = new FSOperations.FSStatus(path);
      @SuppressWarnings("rawtypes") Map js = fsExecute(user, command);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    case GETTRASHROOTS: {
      Boolean allUsers = params.get(AllUsersParam.NAME, AllUsersParam.class);
      FSOperations.FSGetTrashRoots command = new FSOperations.FSGetTrashRoots(allUsers);
      Map json = fsExecute(user, command);
      AUDIT_LOG.info("allUsers [{}]", allUsers);
      response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
      break;
    }
    default: {
      throw new IOException(
          MessageFormat.format("Invalid HTTP GET operation [{0}]", op.value()));
    }
    }
    return response;
  }

  /**
   * Create an open redirection URL from a request. It points to the same
   * HttpFS endpoint but removes the "redirect" parameter.
   * @param uriInfo uri info of the request.
   * @return URL for the redirected location.
   */
  private URI createOpenRedirectionURL(UriInfo uriInfo) {
    UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
    uriBuilder.replaceQueryParam(NoRedirectParam.NAME, (Object[])null);
    return uriBuilder.build((Object[])null);
  }

  /**
   * Binding to handle DELETE requests.
   *
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @DELETE
  @Path("{path:.*}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response delete(@PathParam("path") String path,
                         @QueryParam(OperationParam.NAME) OperationParam op,
                         @Context HttpServletRequest request)
    throws IOException, FileSystemAccessException {
    // Do not allow DELETE commands in read-only mode
    if(accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    Response response;
    path = makeAbsolute(path);
    final Parameters params = getParams(request);
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
      case DELETE: {
        Boolean recursive =
          params.get(RecursiveParam.NAME,  RecursiveParam.class);
        AUDIT_LOG.info("[{}] recursive [{}]", path, recursive);
        FSOperations.FSDelete command =
          new FSOperations.FSDelete(path, recursive);
        JSONObject json = fsExecute(user, command);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case DELETESNAPSHOT: {
        String snapshotName = params.get(SnapshotNameParam.NAME,
            SnapshotNameParam.class);
        FSOperations.FSDeleteSnapshot command =
                new FSOperations.FSDeleteSnapshot(path, snapshotName);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] deleted snapshot [{}]", path, snapshotName);
        response = Response.ok().build();
        break;
      }
      default: {
        throw new IOException(
          MessageFormat.format("Invalid HTTP DELETE operation [{0}]",
                               op.value()));
      }
    }
    return response;
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param op the HttpFS operation of the request.
   * @param request the HttpFS request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   *           handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related
   *           error occurred. Thrown exceptions are handled by
   *           {@link HttpFSExceptionProvider}.
   */
  @POST
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8 })
  public Response postRoot(InputStream is, @Context UriInfo uriInfo,
      @QueryParam(OperationParam.NAME) OperationParam op,
      @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    return post(is, uriInfo, "/", op, request);
  }

  /**
   * Binding to handle POST requests.
   *
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @POST
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response post(InputStream is,
                       @Context UriInfo uriInfo,
                       @PathParam("path") String path,
                       @QueryParam(OperationParam.NAME) OperationParam op,
                       @Context HttpServletRequest request)
    throws IOException, FileSystemAccessException {
    // Do not allow POST commands in read-only mode
    if(accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    Response response;
    path = makeAbsolute(path);
    final Parameters params = getParams(request);
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
      case APPEND: {
        Boolean hasData = params.get(DataParam.NAME, DataParam.class);
        URI redirectURL = createUploadRedirectionURL(uriInfo,
            HttpFSFileSystem.Operation.APPEND);
        Boolean noRedirect =
            params.get(NoRedirectParam.NAME, NoRedirectParam.class);
        if (noRedirect) {
            final String js = JsonUtil.toJsonString("Location", redirectURL);
            response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        } else if (hasData) {
          FSOperations.FSAppend command =
            new FSOperations.FSAppend(is, path);
          fsExecute(user, command);
          AUDIT_LOG.info("[{}]", path);
          response = Response.ok().type(MediaType.APPLICATION_JSON).build();
        } else {
          response = Response.temporaryRedirect(redirectURL).build();
        }
        break;
      }
      case CONCAT: {
        String sources = params.get(SourcesParam.NAME, SourcesParam.class);
        FSOperations.FSConcat command =
            new FSOperations.FSConcat(path, sources.split(","));
        fsExecute(user, command);
        AUDIT_LOG.info("[{}]", path);
        response = Response.ok().build();
        break;
      }
      case TRUNCATE: {
        Long newLength = params.get(NewLengthParam.NAME, NewLengthParam.class);
        FSOperations.FSTruncate command = 
            new FSOperations.FSTruncate(path, newLength);
        JSONObject json = fsExecute(user, command);
        AUDIT_LOG.info("Truncate [{}] to length [{}]", path, newLength);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case UNSETSTORAGEPOLICY: {
        FSOperations.FSUnsetStoragePolicy command =
             new FSOperations.FSUnsetStoragePolicy(path);
         fsExecute(user, command);
         AUDIT_LOG.info("Unset storage policy [{}]", path);
         response = Response.ok().build();
         break;
      }
      case UNSETECPOLICY: {
        FSOperations.FSUnSetErasureCodingPolicy command =
            new FSOperations.FSUnSetErasureCodingPolicy(path);
        fsExecute(user, command);
        AUDIT_LOG.info("Unset ec policy [{}]", path);
        response = Response.ok().build();
        break;
      }
      default: {
        throw new IOException(
          MessageFormat.format("Invalid HTTP POST operation [{0}]",
                               op.value()));
      }
    }
    return response;
  }

  /**
   * Creates the URL for an upload operation (create or append).
   *
   * @param uriInfo uri info of the request.
   * @param uploadOperation operation for the upload URL.
   *
   * @return the URI for uploading data.
   */
  protected URI createUploadRedirectionURL(UriInfo uriInfo, Enum<?> uploadOperation) {
    UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
    uriBuilder = uriBuilder.replaceQueryParam(OperationParam.NAME, uploadOperation)
        .queryParam(DataParam.NAME, Boolean.TRUE)
        .replaceQueryParam(NoRedirectParam.NAME, (Object[]) null);
    // Workaround: NPE occurs when using null in Jersey 2.29
    return uriBuilder.build(NULL);
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param op the HttpFS operation of the request.
   * @param request the HttpFS request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   *           handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related
   *           error occurred. Thrown exceptions are handled by
   *           {@link HttpFSExceptionProvider}.
   */
  @PUT
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8 })
  public Response putRoot(InputStream is, @Context UriInfo uriInfo,
      @QueryParam(OperationParam.NAME) OperationParam op,
      @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    return put(is, uriInfo, "/", op, request);
  }

  /**
   * Binding to handle PUT requests.
   *
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @PUT
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response put(InputStream is,
                       @Context UriInfo uriInfo,
                       @PathParam("path") String path,
                       @QueryParam(OperationParam.NAME) OperationParam op,
                       @Context HttpServletRequest request)
    throws IOException, FileSystemAccessException {
    // Do not allow PUT commands in read-only mode
    if(accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    Response response;
    path = makeAbsolute(path);
    final Parameters params = getParams(request);
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
      case CREATE: {
        Boolean hasData = params.get(DataParam.NAME, DataParam.class);
        URI redirectURL = createUploadRedirectionURL(uriInfo,
            HttpFSFileSystem.Operation.CREATE);
        Boolean noRedirect =
            params.get(NoRedirectParam.NAME, NoRedirectParam.class);
        if (noRedirect) {
            final String js = JsonUtil.toJsonString("Location", redirectURL);
            response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
        } else if (hasData) {
          Short permission = params.get(PermissionParam.NAME,
                                         PermissionParam.class);
          Short unmaskedPermission = params.get(UnmaskedPermissionParam.NAME,
              UnmaskedPermissionParam.class);
          Boolean override = params.get(OverwriteParam.NAME,
                                        OverwriteParam.class);
          Short replication = params.get(ReplicationParam.NAME,
                                         ReplicationParam.class);
          Long blockSize = params.get(BlockSizeParam.NAME,
                                      BlockSizeParam.class);
          FSOperations.FSCreate command =
            new FSOperations.FSCreate(is, path, permission, override,
                replication, blockSize, unmaskedPermission);
          fsExecute(user, command);
          AUDIT_LOG.info(
              "[{}] permission [{}] override [{}] "+
              "replication [{}] blockSize [{}] unmaskedpermission [{}]",
              new Object[]{path, permission,  override, replication, blockSize,
                  unmaskedPermission});
          final String js = JsonUtil.toJsonString(
              "Location", uriInfo.getAbsolutePath());
          response = Response.created(uriInfo.getAbsolutePath())
              .type(MediaType.APPLICATION_JSON).entity(js).build();
        } else {
          response = Response.temporaryRedirect(redirectURL).build();
        }
        break;
      }
      case ALLOWSNAPSHOT: {
        FSOperations.FSAllowSnapshot command =
            new FSOperations.FSAllowSnapshot(path);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] allowed snapshot", path);
        response = Response.ok().build();
        break;
      }
      case DISALLOWSNAPSHOT: {
        FSOperations.FSDisallowSnapshot command =
            new FSOperations.FSDisallowSnapshot(path);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] disallowed snapshot", path);
        response = Response.ok().build();
        break;
      }
      case CREATESNAPSHOT: {
        String snapshotName = params.get(SnapshotNameParam.NAME,
            SnapshotNameParam.class);
        FSOperations.FSCreateSnapshot command =
            new FSOperations.FSCreateSnapshot(path, snapshotName);
        String json = fsExecute(user, command);
        AUDIT_LOG.info("[{}] snapshot created as [{}]", path, snapshotName);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case SETXATTR: {
        String xattrName = params.get(XAttrNameParam.NAME, 
            XAttrNameParam.class);
        String xattrValue = params.get(XAttrValueParam.NAME, 
            XAttrValueParam.class);
        EnumSet<XAttrSetFlag> flag = params.get(XAttrSetFlagParam.NAME, 
            XAttrSetFlagParam.class);

        FSOperations.FSSetXAttr command = new FSOperations.FSSetXAttr(
            path, xattrName, xattrValue, flag);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to xAttr [{}]", path, xattrName);
        response = Response.ok().build();
        break;
      }
      case RENAMESNAPSHOT: {
        String oldSnapshotName = params.get(OldSnapshotNameParam.NAME,
            OldSnapshotNameParam.class);
        String snapshotName = params.get(SnapshotNameParam.NAME,
            SnapshotNameParam.class);
        FSOperations.FSRenameSnapshot command =
                new FSOperations.FSRenameSnapshot(path, oldSnapshotName,
                    snapshotName);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] renamed snapshot [{}] to [{}]", path,
            oldSnapshotName, snapshotName);
        response = Response.ok().build();
        break;
      }
      case REMOVEXATTR: {
        String xattrName = params.get(XAttrNameParam.NAME, XAttrNameParam.class);
        FSOperations.FSRemoveXAttr command = new FSOperations.FSRemoveXAttr(
            path, xattrName);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] removed xAttr [{}]", path, xattrName);
        response = Response.ok().build();
        break;
      }
      case MKDIRS: {
        Short permission = params.get(PermissionParam.NAME,
                                       PermissionParam.class);
        Short unmaskedPermission = params.get(UnmaskedPermissionParam.NAME,
            UnmaskedPermissionParam.class);
        FSOperations.FSMkdirs command =
            new FSOperations.FSMkdirs(path, permission, unmaskedPermission);
        JSONObject json = fsExecute(user, command);
        AUDIT_LOG.info("[{}] permission [{}] unmaskedpermission [{}]",
            path, permission, unmaskedPermission);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case RENAME: {
        String toPath = params.get(DestinationParam.NAME, DestinationParam.class);
        FSOperations.FSRename command =
          new FSOperations.FSRename(path, toPath);
        JSONObject json = fsExecute(user, command);
        AUDIT_LOG.info("[{}] to [{}]", path, toPath);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case SETOWNER: {
        String owner = params.get(OwnerParam.NAME, OwnerParam.class);
        String group = params.get(GroupParam.NAME, GroupParam.class);
        FSOperations.FSSetOwner command =
          new FSOperations.FSSetOwner(path, owner, group);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to (O/G)[{}]", path, owner + ":" + group);
        response = Response.ok().build();
        break;
      }
      case SETPERMISSION: {
        Short permission = params.get(PermissionParam.NAME,
                                      PermissionParam.class);
        FSOperations.FSSetPermission command =
          new FSOperations.FSSetPermission(path, permission);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to [{}]", path, permission);
        response = Response.ok().build();
        break;
      }
      case SETREPLICATION: {
        Short replication = params.get(ReplicationParam.NAME,
                                       ReplicationParam.class);
        FSOperations.FSSetReplication command =
          new FSOperations.FSSetReplication(path, replication);
        JSONObject json = fsExecute(user, command);
        AUDIT_LOG.info("[{}] to [{}]", path, replication);
        response = Response.ok(json).build();
        break;
      }
      case SETTIMES: {
        Long modifiedTime = params.get(ModifiedTimeParam.NAME,
                                       ModifiedTimeParam.class);
        Long accessTime = params.get(AccessTimeParam.NAME,
                                     AccessTimeParam.class);
        FSOperations.FSSetTimes command =
          new FSOperations.FSSetTimes(path, modifiedTime, accessTime);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to (M/A)[{}]", path,
                       modifiedTime + ":" + accessTime);
        response = Response.ok().build();
        break;
      }
      case SETACL: {
        String aclSpec = params.get(AclPermissionParam.NAME,
                AclPermissionParam.class);
        FSOperations.FSSetAcl command =
                new FSOperations.FSSetAcl(path, aclSpec);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to acl [{}]", path, aclSpec);
        response = Response.ok().build();
        break;
      }
      case REMOVEACL: {
        FSOperations.FSRemoveAcl command =
                new FSOperations.FSRemoveAcl(path);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] removed acl", path);
        response = Response.ok().build();
        break;
      }
      case MODIFYACLENTRIES: {
        String aclSpec = params.get(AclPermissionParam.NAME,
                AclPermissionParam.class);
        FSOperations.FSModifyAclEntries command =
                new FSOperations.FSModifyAclEntries(path, aclSpec);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] modify acl entry with [{}]", path, aclSpec);
        response = Response.ok().build();
        break;
      }
      case REMOVEACLENTRIES: {
        String aclSpec = params.get(AclPermissionParam.NAME,
                AclPermissionParam.class);
        FSOperations.FSRemoveAclEntries command =
                new FSOperations.FSRemoveAclEntries(path, aclSpec);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] remove acl entry [{}]", path, aclSpec);
        response = Response.ok().build();
        break;
      }
      case REMOVEDEFAULTACL: {
        FSOperations.FSRemoveDefaultAcl command =
                new FSOperations.FSRemoveDefaultAcl(path);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] remove default acl", path);
        response = Response.ok().build();
        break;
      }
      case SETSTORAGEPOLICY: {
        String policyName = params.get(PolicyNameParam.NAME,
            PolicyNameParam.class);
        FSOperations.FSSetStoragePolicy command =
            new FSOperations.FSSetStoragePolicy(path, policyName);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to policy [{}]", path, policyName);
        response = Response.ok().build();
        break;
      }
      case SETECPOLICY: {
        String policyName = params.get(ECPolicyParam.NAME, ECPolicyParam.class);
        FSOperations.FSSetErasureCodingPolicy command =
            new FSOperations.FSSetErasureCodingPolicy(path, policyName);
        fsExecute(user, command);
        AUDIT_LOG.info("[{}] to policy [{}]", path, policyName);
        response = Response.ok().build();
        break;
    }
    case SATISFYSTORAGEPOLICY: {
      FSOperations.FSSatisyStoragePolicy command =
          new FSOperations.FSSatisyStoragePolicy(path);
      fsExecute(user, command);
      AUDIT_LOG.info("satisfy storage policy for [{}]", path);
      response = Response.ok().build();
      break;
    }
      default: {
        throw new IOException(
          MessageFormat.format("Invalid HTTP PUT operation [{0}]",
                               op.value()));
      }
    }
    return response;
  }

}
