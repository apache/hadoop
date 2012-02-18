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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.server.HttpFSParams.AccessTimeParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.BlockSizeParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.DataParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.DeleteOpParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.DeleteRecursiveParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.DoAsParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.FilterParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.FsPathParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.GetOpParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.GroupParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.LenParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.ModifiedTimeParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.OffsetParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.OverwriteParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.OwnerParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.PermissionParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.PostOpParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.PutOpParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.ReplicationParam;
import org.apache.hadoop.fs.http.server.HttpFSParams.ToPathParam;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.lib.service.FileSystemAccessException;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.service.ProxyUser;
import org.apache.hadoop.lib.servlet.FileSystemReleaseFilter;
import org.apache.hadoop.lib.servlet.HostnameFilter;
import org.apache.hadoop.lib.wsrs.InputStreamEntity;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * Main class of HttpFSServer server.
 * <p/>
 * The <code>HttpFSServer</code> class uses Jersey JAX-RS to binds HTTP requests to the
 * different operations.
 */
@Path(HttpFSFileSystem.SERVICE_VERSION)
public class HttpFSServer {
  private static Logger AUDIT_LOG = LoggerFactory.getLogger("httpfsaudit");

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   *
   * @param user principal making the request.
   * @param op GET operation, default value is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#OPEN}.
   * @param filter Glob filter, default value is none. Used only if the
   * operation is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#LISTSTATUS}
   * @param doAs user being impersonated, defualt value is none. It can be used
   * only if the current user is a HttpFSServer proxyuser.
   *
   * @return the request response
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response root(@Context Principal user,
                       @QueryParam(GetOpParam.NAME) GetOpParam op,
                       @QueryParam(FilterParam.NAME) @DefaultValue(FilterParam.DEFAULT) FilterParam filter,
                       @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT) DoAsParam doAs)
    throws IOException, FileSystemAccessException {
    return get(user, new FsPathParam(""), op, new OffsetParam(OffsetParam.DEFAULT),
               new LenParam(LenParam.DEFAULT), filter, doAs,
               new OverwriteParam(OverwriteParam.DEFAULT),
               new BlockSizeParam(BlockSizeParam.DEFAULT),
               new PermissionParam(PermissionParam.DEFAULT),
               new ReplicationParam(ReplicationParam.DEFAULT));
  }

  /**
   * Resolves the effective user that will be used to request a FileSystemAccess filesystem.
   * <p/>
   * If the doAs-user is NULL or the same as the user, it returns the user.
   * <p/>
   * Otherwise it uses proxyuser rules (see {@link ProxyUser} to determine if the
   * current user can impersonate the doAs-user.
   * <p/>
   * If the current user cannot impersonate the doAs-user an
   * <code>AccessControlException</code> will be thrown.
   *
   * @param user principal for whom the filesystem instance is.
   * @param doAs do-as user, if any.
   *
   * @return the effective user.
   *
   * @throws IOException thrown if an IO error occurrs.
   * @throws AccessControlException thrown if the current user cannot impersonate
   * the doAs-user.
   */
  private String getEffectiveUser(Principal user, String doAs) throws IOException {
    String effectiveUser = user.getName();
    if (doAs != null && !doAs.equals(user.getName())) {
      ProxyUser proxyUser = HttpFSServerWebApp.get().get(ProxyUser.class);
      proxyUser.validate(user.getName(), HostnameFilter.get(), doAs);
      effectiveUser = doAs;
      AUDIT_LOG.info("Proxy user [{}] DoAs user [{}]", user.getName(), doAs);
    }
    return effectiveUser;
  }

  /**
   * Executes a {@link FileSystemAccess.FileSystemExecutor} using a filesystem for the effective
   * user.
   *
   * @param user principal making the request.
   * @param doAs do-as user, if any.
   * @param executor FileSystemExecutor to execute.
   *
   * @return FileSystemExecutor response
   *
   * @throws IOException thrown if an IO error occurrs.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  private <T> T fsExecute(Principal user, String doAs, FileSystemAccess.FileSystemExecutor<T> executor)
    throws IOException, FileSystemAccessException {
    String hadoopUser = getEffectiveUser(user, doAs);
    FileSystemAccess fsAccess = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class).getDefaultConfiguration();
    return fsAccess.execute(hadoopUser, conf, executor);
  }

  /**
   * Returns a filesystem instance. The fileystem instance is wired for release at the completion of
   * the current Servlet request via the {@link FileSystemReleaseFilter}.
   * <p/>
   * If a do-as user is specified, the current user must be a valid proxyuser, otherwise an
   * <code>AccessControlException</code> will be thrown.
   *
   * @param user principal for whom the filesystem instance is.
   * @param doAs do-as user, if any.
   *
   * @return a filesystem for the specified user or do-as user.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  private FileSystem createFileSystem(Principal user, String doAs) throws IOException, FileSystemAccessException {
    String hadoopUser = getEffectiveUser(user, doAs);
    FileSystemAccess fsAccess = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class).getDefaultConfiguration();
    FileSystem fs = fsAccess.createFileSystem(hadoopUser, conf);
    FileSystemReleaseFilter.setFileSystem(fs);
    return fs;
  }

  /**
   * Binding to handle all GET requests, supported operations are
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues}.
   * <p/>
   * The @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#INSTRUMENTATION} operation is available only
   * to users that are in HttpFSServer's admin group (see {@link HttpFSServer}. It returns
   * HttpFSServer instrumentation data. The specified path must be '/'.
   *
   * @param user principal making the request.
   * @param path path for the GET request.
   * @param op GET operation, default value is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#OPEN}.
   * @param offset of the  file being fetch, used only with
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#OPEN} operations.
   * @param len amounts of bytes, used only with @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#OPEN}
   * operations.
   * @param filter Glob filter, default value is none. Used only if the
   * operation is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.GetOpValues#LISTSTATUS}
   * @param doAs user being impersonated, defualt value is none. It can be used
   * only if the current user is a HttpFSServer proxyuser.
   * @param override default is true. Used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#CREATE} operations.
   * @param blockSize block size to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#CREATE} operations.
   * @param permission permission to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETPERMISSION}.
   * @param replication replication factor to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETREPLICATION}.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  @GET
  @Path("{path:.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
  public Response get(@Context Principal user,
                      @PathParam("path") @DefaultValue("") FsPathParam path,
                      @QueryParam(GetOpParam.NAME) GetOpParam op,
                      @QueryParam(OffsetParam.NAME) @DefaultValue(OffsetParam.DEFAULT) OffsetParam offset,
                      @QueryParam(LenParam.NAME) @DefaultValue(LenParam.DEFAULT) LenParam len,
                      @QueryParam(FilterParam.NAME) @DefaultValue(FilterParam.DEFAULT) FilterParam filter,
                      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT) DoAsParam doAs,

                      //these params are only for createHandle operation acceptance purposes
                      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT) OverwriteParam override,
                      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT) BlockSizeParam blockSize,
                      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
                      PermissionParam permission,
                      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
                      ReplicationParam replication
  )
    throws IOException, FileSystemAccessException {
    Response response = null;
    if (op == null) {
      throw new UnsupportedOperationException(MessageFormat.format("Missing [{0}] parameter", GetOpParam.NAME));
    } else {
      path.makeAbsolute();
      MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
      switch (op.value()) {
        case OPEN: {
          //Invoking the command directly using an unmanaged FileSystem that is released by the
          //FileSystemReleaseFilter
          FSOperations.FSOpen command = new FSOperations.FSOpen(path.value());
          FileSystem fs = createFileSystem(user, doAs.value());
          InputStream is = command.execute(fs);
          AUDIT_LOG.info("[{}] offset [{}] len [{}]", new Object[]{path, offset, len});
          InputStreamEntity entity = new InputStreamEntity(is, offset.value(), len.value());
          response = Response.ok(entity).type(MediaType.APPLICATION_OCTET_STREAM).build();
          break;
        }
        case GETFILESTATUS: {
          FSOperations.FSFileStatus command = new FSOperations.FSFileStatus(path.value());
          Map json = fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("[{}]", path);
          response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
          break;
        }
        case LISTSTATUS: {
          FSOperations.FSListStatus command = new FSOperations.FSListStatus(path.value(), filter.value());
          Map json = fsExecute(user, doAs.value(), command);
          if (filter.value() == null) {
            AUDIT_LOG.info("[{}]", path);
          } else {
            AUDIT_LOG.info("[{}] filter [{}]", path, filter.value());
          }
          response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
          break;
        }
        case GETHOMEDIR: {
          FSOperations.FSHomeDir command = new FSOperations.FSHomeDir();
          JSONObject json = fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("");
          response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
          break;
        }
        case INSTRUMENTATION: {
          if (!path.value().equals("/")) {
            throw new UnsupportedOperationException(
              MessageFormat.format("Invalid path for {0}={1}, must be '/'",
                                   GetOpParam.NAME, HttpFSFileSystem.GetOpValues.INSTRUMENTATION));
          }
          Groups groups = HttpFSServerWebApp.get().get(Groups.class);
          List<String> userGroups = groups.getGroups(user.getName());
          if (!userGroups.contains(HttpFSServerWebApp.get().getAdminGroup())) {
            throw new AccessControlException("User not in HttpFSServer admin group");
          }
          Instrumentation instrumentation = HttpFSServerWebApp.get().get(Instrumentation.class);
          Map snapshot = instrumentation.getSnapshot();
          response = Response.ok(snapshot).build();
          break;
        }
        case GETCONTENTSUMMARY: {
          FSOperations.FSContentSummary command = new FSOperations.FSContentSummary(path.value());
          Map json = fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("[{}]", path);
          response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
          break;
        }
        case GETFILECHECKSUM: {
          FSOperations.FSFileChecksum command = new FSOperations.FSFileChecksum(path.value());
          Map json = fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("[{}]", path);
          response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
          break;
        }
        case GETDELEGATIONTOKEN: {
          response = Response.status(Response.Status.BAD_REQUEST).build();
          break;
        }
        case GETFILEBLOCKLOCATIONS: {
          response = Response.status(Response.Status.BAD_REQUEST).build();
          break;
        }
      }
      return response;
    }
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
    uriBuilder = uriBuilder.replaceQueryParam(PutOpParam.NAME, uploadOperation).
      queryParam(DataParam.NAME, Boolean.TRUE);
    return uriBuilder.build(null);
  }

  /**
   * Binding to handle all DELETE requests.
   *
   * @param user principal making the request.
   * @param path path for the DELETE request.
   * @param op DELETE operation, default value is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.DeleteOpValues#DELETE}.
   * @param recursive indicates if the delete is recursive, default is <code>false</code>
   * @param doAs user being impersonated, defualt value is none. It can be used
   * only if the current user is a HttpFSServer proxyuser.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  @DELETE
  @Path("{path:.*}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(@Context Principal user,
                         @PathParam("path") FsPathParam path,
                         @QueryParam(DeleteOpParam.NAME) DeleteOpParam op,
                         @QueryParam(DeleteRecursiveParam.NAME) @DefaultValue(DeleteRecursiveParam.DEFAULT)
                         DeleteRecursiveParam recursive,
                         @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT) DoAsParam doAs)
    throws IOException, FileSystemAccessException {
    Response response = null;
    if (op == null) {
      throw new UnsupportedOperationException(MessageFormat.format("Missing [{0}] parameter", DeleteOpParam.NAME));
    }
    switch (op.value()) {
      case DELETE: {
        path.makeAbsolute();
        MDC.put(HttpFSFileSystem.OP_PARAM, "DELETE");
        AUDIT_LOG.info("[{}] recursive [{}]", path, recursive);
        FSOperations.FSDelete command = new FSOperations.FSDelete(path.value(), recursive.value());
        JSONObject json = fsExecute(user, doAs.value(), command);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
    }
    return response;
  }


  /**
   * Binding to handle all PUT requests, supported operations are
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues}.
   *
   * @param is request input stream, used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PostOpValues#APPEND} operations.
   * @param user principal making the request.
   * @param uriInfo the request uriInfo.
   * @param path path for the PUT request.
   * @param op PUT operation, no default value.
   * @param toPath new path, used only for
   * {@link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#RENAME} operations.
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETTIMES}.
   * @param owner owner to set, used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETOWNER} operations.
   * @param group group to set, used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETOWNER} operations.
   * @param override default is true. Used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#CREATE} operations.
   * @param blockSize block size to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#CREATE} operations.
   * @param permission permission to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETPERMISSION}.
   * @param replication replication factor to set, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETREPLICATION}.
   * @param modifiedTime modified time, in seconds since EPOC, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETTIMES}.
   * @param accessTime accessed time, in seconds since EPOC, used only by
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PutOpValues#SETTIMES}.
   * @param hasData indicates if the append request is uploading data or not
   * (just getting the handle).
   * @param doAs user being impersonated, defualt value is none. It can be used
   * only if the current user is a HttpFSServer proxyuser.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  @PUT
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response put(InputStream is,
                      @Context Principal user,
                      @Context UriInfo uriInfo,
                      @PathParam("path") FsPathParam path,
                      @QueryParam(PutOpParam.NAME) PutOpParam op,
                      @QueryParam(ToPathParam.NAME) @DefaultValue(ToPathParam.DEFAULT) ToPathParam toPath,
                      @QueryParam(OwnerParam.NAME) @DefaultValue(OwnerParam.DEFAULT) OwnerParam owner,
                      @QueryParam(GroupParam.NAME) @DefaultValue(GroupParam.DEFAULT) GroupParam group,
                      @QueryParam(OverwriteParam.NAME) @DefaultValue(OverwriteParam.DEFAULT) OverwriteParam override,
                      @QueryParam(BlockSizeParam.NAME) @DefaultValue(BlockSizeParam.DEFAULT) BlockSizeParam blockSize,
                      @QueryParam(PermissionParam.NAME) @DefaultValue(PermissionParam.DEFAULT)
                      PermissionParam permission,
                      @QueryParam(ReplicationParam.NAME) @DefaultValue(ReplicationParam.DEFAULT)
                      ReplicationParam replication,
                      @QueryParam(ModifiedTimeParam.NAME) @DefaultValue(ModifiedTimeParam.DEFAULT)
                      ModifiedTimeParam modifiedTime,
                      @QueryParam(AccessTimeParam.NAME) @DefaultValue(AccessTimeParam.DEFAULT)
                      AccessTimeParam accessTime,
                      @QueryParam(DataParam.NAME) @DefaultValue(DataParam.DEFAULT) DataParam hasData,
                      @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT) DoAsParam doAs)
    throws IOException, FileSystemAccessException {
    Response response = null;
    if (op == null) {
      throw new UnsupportedOperationException(MessageFormat.format("Missing [{0}] parameter", PutOpParam.NAME));
    }
    path.makeAbsolute();
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    switch (op.value()) {
      case CREATE: {
        if (!hasData.value()) {
          response = Response.temporaryRedirect(
            createUploadRedirectionURL(uriInfo, HttpFSFileSystem.PutOpValues.CREATE)).build();
        } else {
          FSOperations.FSCreate
            command = new FSOperations.FSCreate(is, path.value(), permission.value(), override.value(),
                                                replication.value(), blockSize.value());
          fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("[{}] permission [{}] override [{}] replication [{}] blockSize [{}]",
                         new Object[]{path, permission, override, replication, blockSize});
          response = Response.status(Response.Status.CREATED).build();
        }
        break;
      }
      case MKDIRS: {
        FSOperations.FSMkdirs command = new FSOperations.FSMkdirs(path.value(), permission.value());
        JSONObject json = fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] permission [{}]", path, permission.value());
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case RENAME: {
        FSOperations.FSRename command = new FSOperations.FSRename(path.value(), toPath.value());
        JSONObject json = fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] to [{}]", path, toPath);
        response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
        break;
      }
      case SETOWNER: {
        FSOperations.FSSetOwner command = new FSOperations.FSSetOwner(path.value(), owner.value(), group.value());
        fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] to (O/G)[{}]", path, owner.value() + ":" + group.value());
        response = Response.ok().build();
        break;
      }
      case SETPERMISSION: {
        FSOperations.FSSetPermission command = new FSOperations.FSSetPermission(path.value(), permission.value());
        fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] to [{}]", path, permission.value());
        response = Response.ok().build();
        break;
      }
      case SETREPLICATION: {
        FSOperations.FSSetReplication command = new FSOperations.FSSetReplication(path.value(), replication.value());
        JSONObject json = fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] to [{}]", path, replication.value());
        response = Response.ok(json).build();
        break;
      }
      case SETTIMES: {
        FSOperations.FSSetTimes
          command = new FSOperations.FSSetTimes(path.value(), modifiedTime.value(), accessTime.value());
        fsExecute(user, doAs.value(), command);
        AUDIT_LOG.info("[{}] to (M/A)[{}]", path, modifiedTime.value() + ":" + accessTime.value());
        response = Response.ok().build();
        break;
      }
      case RENEWDELEGATIONTOKEN: {
        response = Response.status(Response.Status.BAD_REQUEST).build();
        break;
      }
      case CANCELDELEGATIONTOKEN: {
        response = Response.status(Response.Status.BAD_REQUEST).build();
        break;
      }
    }
    return response;
  }

  /**
   * Binding to handle all OPST requests, supported operations are
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PostOpValues}.
   *
   * @param is request input stream, used only for
   * @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PostOpValues#APPEND} operations.
   * @param user principal making the request.
   * @param uriInfo the request uriInfo.
   * @param path path for the POST request.
   * @param op POST operation, default is @link org.apache.hadoop.fs.http.client.HttpFSFileSystem.PostOpValues#APPEND}.
   * @param hasData indicates if the append request is uploading data or not (just getting the handle).
   * @param doAs user being impersonated, defualt value is none. It can be used
   * only if the current user is a HttpFSServer proxyuser.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated error occurred. Thrown
   * exceptions are handled by {@link HttpFSExceptionProvider}.
   */
  @POST
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(InputStream is,
                       @Context Principal user,
                       @Context UriInfo uriInfo,
                       @PathParam("path") FsPathParam path,
                       @QueryParam(PostOpParam.NAME) PostOpParam op,
                       @QueryParam(DataParam.NAME) @DefaultValue(DataParam.DEFAULT) DataParam hasData,
                       @QueryParam(DoAsParam.NAME) @DefaultValue(DoAsParam.DEFAULT) DoAsParam doAs)
    throws IOException, FileSystemAccessException {
    Response response = null;
    if (op == null) {
      throw new UnsupportedOperationException(MessageFormat.format("Missing [{0}] parameter", PostOpParam.NAME));
    }
    path.makeAbsolute();
    MDC.put(HttpFSFileSystem.OP_PARAM, op.value().name());
    switch (op.value()) {
      case APPEND: {
        if (!hasData.value()) {
          response = Response.temporaryRedirect(
            createUploadRedirectionURL(uriInfo, HttpFSFileSystem.PostOpValues.APPEND)).build();
        } else {
          FSOperations.FSAppend command = new FSOperations.FSAppend(is, path.value());
          fsExecute(user, doAs.value(), command);
          AUDIT_LOG.info("[{}]", path);
          response = Response.ok().type(MediaType.APPLICATION_JSON).build();
        }
        break;
      }
    }
    return response;
  }

}
