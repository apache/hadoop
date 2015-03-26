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
package org.apache.hadoop.fs.http.client;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.lib.wsrs.EnumSetParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * HttpFSServer implementation of the FileSystemAccess FileSystem.
 * <p>
 * This implementation allows a user to access HDFS over HTTP via a HttpFSServer server.
 */
@InterfaceAudience.Private
public class HttpFSFileSystem extends FileSystem
  implements DelegationTokenRenewer.Renewable {

  public static final String SERVICE_NAME = HttpFSUtils.SERVICE_NAME;

  public static final String SERVICE_VERSION = HttpFSUtils.SERVICE_VERSION;

  public static final String SCHEME = "webhdfs";

  public static final String OP_PARAM = "op";
  public static final String DO_AS_PARAM = "doas";
  public static final String OVERWRITE_PARAM = "overwrite";
  public static final String REPLICATION_PARAM = "replication";
  public static final String BLOCKSIZE_PARAM = "blocksize";
  public static final String PERMISSION_PARAM = "permission";
  public static final String ACLSPEC_PARAM = "aclspec";
  public static final String DESTINATION_PARAM = "destination";
  public static final String RECURSIVE_PARAM = "recursive";
  public static final String SOURCES_PARAM = "sources";
  public static final String OWNER_PARAM = "owner";
  public static final String GROUP_PARAM = "group";
  public static final String MODIFICATION_TIME_PARAM = "modificationtime";
  public static final String ACCESS_TIME_PARAM = "accesstime";
  public static final String XATTR_NAME_PARAM = "xattr.name";
  public static final String XATTR_VALUE_PARAM = "xattr.value";
  public static final String XATTR_SET_FLAG_PARAM = "flag";
  public static final String XATTR_ENCODING_PARAM = "encoding";
  public static final String NEW_LENGTH_PARAM = "newlength";

  public static final Short DEFAULT_PERMISSION = 0755;
  public static final String ACLSPEC_DEFAULT = "";

  public static final String RENAME_JSON = "boolean";

  public static final String TRUNCATE_JSON = "boolean";

  public static final String DELETE_JSON = "boolean";

  public static final String MKDIRS_JSON = "boolean";

  public static final String HOME_DIR_JSON = "Path";

  public static final String SET_REPLICATION_JSON = "boolean";

  public static final String UPLOAD_CONTENT_TYPE= "application/octet-stream";

  public static enum FILE_TYPE {
    FILE, DIRECTORY, SYMLINK;

    public static FILE_TYPE getType(FileStatus fileStatus) {
      if (fileStatus.isFile()) {
        return FILE;
      }
      if (fileStatus.isDirectory()) {
        return DIRECTORY;
      }
      if (fileStatus.isSymlink()) {
        return SYMLINK;
      }
      throw new IllegalArgumentException("Could not determine filetype for: " +
                                         fileStatus.getPath());
    }
  }

  public static final String FILE_STATUSES_JSON = "FileStatuses";
  public static final String FILE_STATUS_JSON = "FileStatus";
  public static final String PATH_SUFFIX_JSON = "pathSuffix";
  public static final String TYPE_JSON = "type";
  public static final String LENGTH_JSON = "length";
  public static final String OWNER_JSON = "owner";
  public static final String GROUP_JSON = "group";
  public static final String PERMISSION_JSON = "permission";
  public static final String ACCESS_TIME_JSON = "accessTime";
  public static final String MODIFICATION_TIME_JSON = "modificationTime";
  public static final String BLOCK_SIZE_JSON = "blockSize";
  public static final String REPLICATION_JSON = "replication";
  public static final String XATTRS_JSON = "XAttrs";
  public static final String XATTR_NAME_JSON = "name";
  public static final String XATTR_VALUE_JSON = "value";
  public static final String XATTRNAMES_JSON = "XAttrNames";

  public static final String FILE_CHECKSUM_JSON = "FileChecksum";
  public static final String CHECKSUM_ALGORITHM_JSON = "algorithm";
  public static final String CHECKSUM_BYTES_JSON = "bytes";
  public static final String CHECKSUM_LENGTH_JSON = "length";

  public static final String CONTENT_SUMMARY_JSON = "ContentSummary";
  public static final String CONTENT_SUMMARY_DIRECTORY_COUNT_JSON = "directoryCount";
  public static final String CONTENT_SUMMARY_FILE_COUNT_JSON = "fileCount";
  public static final String CONTENT_SUMMARY_LENGTH_JSON = "length";
  public static final String CONTENT_SUMMARY_QUOTA_JSON = "quota";
  public static final String CONTENT_SUMMARY_SPACE_CONSUMED_JSON = "spaceConsumed";
  public static final String CONTENT_SUMMARY_SPACE_QUOTA_JSON = "spaceQuota";

  public static final String ACL_STATUS_JSON = "AclStatus";
  public static final String ACL_STICKY_BIT_JSON = "stickyBit";
  public static final String ACL_ENTRIES_JSON = "entries";
  public static final String ACL_BIT_JSON = "aclBit";

  public static final int HTTP_TEMPORARY_REDIRECT = 307;

  private static final String HTTP_GET = "GET";
  private static final String HTTP_PUT = "PUT";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_DELETE = "DELETE";

  @InterfaceAudience.Private
  public static enum Operation {
    OPEN(HTTP_GET), GETFILESTATUS(HTTP_GET), LISTSTATUS(HTTP_GET),
    GETHOMEDIRECTORY(HTTP_GET), GETCONTENTSUMMARY(HTTP_GET),
    GETFILECHECKSUM(HTTP_GET),  GETFILEBLOCKLOCATIONS(HTTP_GET),
    INSTRUMENTATION(HTTP_GET), GETACLSTATUS(HTTP_GET),
    APPEND(HTTP_POST), CONCAT(HTTP_POST), TRUNCATE(HTTP_POST),
    CREATE(HTTP_PUT), MKDIRS(HTTP_PUT), RENAME(HTTP_PUT), SETOWNER(HTTP_PUT),
    SETPERMISSION(HTTP_PUT), SETREPLICATION(HTTP_PUT), SETTIMES(HTTP_PUT),
    MODIFYACLENTRIES(HTTP_PUT), REMOVEACLENTRIES(HTTP_PUT),
    REMOVEDEFAULTACL(HTTP_PUT), REMOVEACL(HTTP_PUT), SETACL(HTTP_PUT),
    DELETE(HTTP_DELETE), SETXATTR(HTTP_PUT), GETXATTRS(HTTP_GET),
    REMOVEXATTR(HTTP_PUT), LISTXATTRS(HTTP_GET);

    private String httpMethod;

    Operation(String httpMethod) {
      this.httpMethod = httpMethod;
    }

    public String getMethod() {
      return httpMethod;
    }

  }

  private DelegationTokenAuthenticatedURL authURL;
  private DelegationTokenAuthenticatedURL.Token authToken =
      new DelegationTokenAuthenticatedURL.Token();
  private URI uri;
  private Path workingDir;
  private UserGroupInformation realUser;



  /**
   * Convenience method that creates a <code>HttpURLConnection</code> for the
   * HttpFSServer file system operations.
   * <p>
   * This methods performs and injects any needed authentication credentials
   * via the {@link #getConnection(URL, String)} method
   *
   * @param method the HTTP method.
   * @param params the query string parameters.
   * @param path the file path
   * @param makeQualified if the path should be 'makeQualified'
   *
   * @return a <code>HttpURLConnection</code> for the HttpFSServer server,
   *         authenticated and ready to use for the specified path and file system operation.
   *
   * @throws IOException thrown if an IO error occurrs.
   */
  private HttpURLConnection getConnection(final String method,
      Map<String, String> params, Path path, boolean makeQualified)
      throws IOException {
    return getConnection(method, params, null, path, makeQualified);
  }

  /**
   * Convenience method that creates a <code>HttpURLConnection</code> for the
   * HttpFSServer file system operations.
   * <p/>
   * This methods performs and injects any needed authentication credentials
   * via the {@link #getConnection(URL, String)} method
   *
   * @param method the HTTP method.
   * @param params the query string parameters.
   * @param multiValuedParams multi valued parameters of the query string
   * @param path the file path
   * @param makeQualified if the path should be 'makeQualified'
   *
   * @return HttpURLConnection a <code>HttpURLConnection</code> for the
   *         HttpFSServer server, authenticated and ready to use for the
   *         specified path and file system operation.
   *
   * @throws IOException thrown if an IO error occurrs.
   */
  private HttpURLConnection getConnection(final String method,
      Map<String, String> params, Map<String, List<String>> multiValuedParams,
      Path path, boolean makeQualified) throws IOException {
    if (makeQualified) {
      path = makeQualified(path);
    }
    final URL url = HttpFSUtils.createURL(path, params, multiValuedParams);
    try {
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<HttpURLConnection>() {
            @Override
            public HttpURLConnection run() throws Exception {
              return getConnection(url, method);
            }
          }
      );
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException) ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  /**
   * Convenience method that creates a <code>HttpURLConnection</code> for the specified URL.
   * <p>
   * This methods performs and injects any needed authentication credentials.
   *
   * @param url url to connect to.
   * @param method the HTTP method.
   *
   * @return a <code>HttpURLConnection</code> for the HttpFSServer server, authenticated and ready to use for
   *         the specified path and file system operation.
   *
   * @throws IOException thrown if an IO error occurrs.
   */
  private HttpURLConnection getConnection(URL url, String method) throws IOException {
    try {
      HttpURLConnection conn = authURL.openConnection(url, authToken);
      conn.setRequestMethod(method);
      if (method.equals(HTTP_POST) || method.equals(HTTP_PUT)) {
        conn.setDoOutput(true);
      }
      return conn;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param name a uri whose authority section names the host, port, etc. for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    //the real use is the one that has the Kerberos credentials needed for
    //SPNEGO to work
    realUser = ugi.getRealUser();
    if (realUser == null) {
      realUser = UserGroupInformation.getLoginUser();
    }
    super.initialize(name, conf);
    try {
      uri = new URI(name.getScheme() + "://" + name.getAuthority());
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }

    Class<? extends DelegationTokenAuthenticator> klass =
        getConf().getClass("httpfs.authenticator.class",
            KerberosDelegationTokenAuthenticator.class,
            DelegationTokenAuthenticator.class);
    DelegationTokenAuthenticator authenticator =
        ReflectionUtils.newInstance(klass, getConf());
    authURL = new DelegationTokenAuthenticatedURL(authenticator);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   *
   * @return the URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Get the default port for this file system.
   * @return the default port or 0 if there isn't one
   */
  @Override
  protected int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  /**
   * HttpFSServer subclass of the <code>FSDataInputStream</code>.
   * <p>
   * This implementation does not support the
   * <code>PositionReadable</code> and <code>Seekable</code> methods.
   */
  private static class HttpFSDataInputStream extends FilterInputStream implements Seekable, PositionedReadable {

    protected HttpFSDataInputStream(InputStream in, int bufferSize) {
      super(new BufferedInputStream(in, bufferSize));
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * <p>
   * IMPORTANT: the returned <code>FSDataInputStream</code> does not support the
   * <code>PositionReadable</code> and <code>Seekable</code> methods.
   *
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.OPEN.toString());
    HttpURLConnection conn = getConnection(Operation.OPEN.getMethod(), params,
                                           f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    return new FSDataInputStream(
      new HttpFSDataInputStream(conn.getInputStream(), bufferSize));
  }

  /**
   * HttpFSServer subclass of the <code>FSDataOutputStream</code>.
   * <p>
   * This implementation closes the underlying HTTP connection validating the Http connection status
   * at closing time.
   */
  private static class HttpFSDataOutputStream extends FSDataOutputStream {
    private HttpURLConnection conn;
    private int closeStatus;

    public HttpFSDataOutputStream(HttpURLConnection conn, OutputStream out, int closeStatus, Statistics stats)
      throws IOException {
      super(out, stats);
      this.conn = conn;
      this.closeStatus = closeStatus;
    }

    @Override
    public void close() throws IOException {
      try {
        super.close();
      } finally {
        HttpExceptionUtils.validateResponse(conn, closeStatus);
      }
    }

  }

  /**
   * Converts a <code>FsPermission</code> to a Unix octal representation.
   *
   * @param p the permission.
   *
   * @return the Unix string symbolic reprentation.
   */
  public static String permissionToString(FsPermission p) {
    return  Integer.toString((p == null) ? DEFAULT_PERMISSION : p.toShort(), 8);
  }

  /*
   * Common handling for uploading data for create and append operations.
   */
  private FSDataOutputStream uploadData(String method, Path f, Map<String, String> params,
                                        int bufferSize, int expectedStatus) throws IOException {
    HttpURLConnection conn = getConnection(method, params, f, true);
    conn.setInstanceFollowRedirects(false);
    boolean exceptionAlreadyHandled = false;
    try {
      if (conn.getResponseCode() == HTTP_TEMPORARY_REDIRECT) {
        exceptionAlreadyHandled = true;
        String location = conn.getHeaderField("Location");
        if (location != null) {
          conn = getConnection(new URL(location), method);
          conn.setRequestProperty("Content-Type", UPLOAD_CONTENT_TYPE);
          try {
            OutputStream os = new BufferedOutputStream(conn.getOutputStream(), bufferSize);
            return new HttpFSDataOutputStream(conn, os, expectedStatus, statistics);
          } catch (IOException ex) {
            HttpExceptionUtils.validateResponse(conn, expectedStatus);
            throw ex;
          }
        } else {
          HttpExceptionUtils.validateResponse(conn, HTTP_TEMPORARY_REDIRECT);
          throw new IOException("Missing HTTP 'Location' header for [" + conn.getURL() + "]");
        }
      } else {
        throw new IOException(
          MessageFormat.format("Expected HTTP status was [307], received [{0}]",
                               conn.getResponseCode()));
      }
    } catch (IOException ex) {
      if (exceptionAlreadyHandled) {
        throw ex;
      } else {
        HttpExceptionUtils.validateResponse(conn, HTTP_TEMPORARY_REDIRECT);
        throw ex;
      }
    }
  }


  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * <p>
   * IMPORTANT: The <code>Progressable</code> parameter is not used.
   *
   * @param f the file name to open.
   * @param permission file permission.
   * @param overwrite if a file with this name already exists, then if true,
   * the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize block size.
   * @param progress progressable.
   *
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progress) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.CREATE.toString());
    params.put(OVERWRITE_PARAM, Boolean.toString(overwrite));
    params.put(REPLICATION_PARAM, Short.toString(replication));
    params.put(BLOCKSIZE_PARAM, Long.toString(blockSize));
    params.put(PERMISSION_PARAM, permissionToString(permission));
    return uploadData(Operation.CREATE.getMethod(), f, params, bufferSize,
                      HttpURLConnection.HTTP_CREATED);
  }


  /**
   * Append to an existing file (optional operation).
   * <p>
   * IMPORTANT: The <code>Progressable</code> parameter is not used.
   *
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   *
   * @throws IOException
   */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
                                   Progressable progress) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.APPEND.toString());
    return uploadData(Operation.APPEND.getMethod(), f, params, bufferSize,
                      HttpURLConnection.HTTP_OK);
  }

  /**
   * Truncate a file.
   * 
   * @param f the file to be truncated.
   * @param newLength The size the file is to be truncated to.
   *
   * @throws IOException
   */
  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.TRUNCATE.toString());
    params.put(NEW_LENGTH_PARAM, Long.toString(newLength));
    HttpURLConnection conn = getConnection(Operation.TRUNCATE.getMethod(),
        params, f, true);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return (Boolean) json.get(TRUNCATE_JSON);
  }

  /**
   * Concat existing files together.
   * @param f the path to the target destination.
   * @param psrcs the paths to the sources to use for the concatenation.
   *
   * @throws IOException
   */
  @Override
  public void concat(Path f, Path[] psrcs) throws IOException {
    List<String> strPaths = new ArrayList<String>(psrcs.length);
    for(Path psrc : psrcs) {
      strPaths.add(psrc.toUri().getPath());
    }
    String srcs = StringUtils.join(",", strPaths);

    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.CONCAT.toString());
    params.put(SOURCES_PARAM, srcs);
    HttpURLConnection conn = getConnection(Operation.CONCAT.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.RENAME.toString());
    params.put(DESTINATION_PARAM, dst.toString());
    HttpURLConnection conn = getConnection(Operation.RENAME.getMethod(),
                                           params, src, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return (Boolean) json.get(RENAME_JSON);
  }

  /**
   * Delete a file.
   *
   * @deprecated Use delete(Path, boolean) instead
   */
  @Deprecated
  @Override
  public boolean delete(Path f) throws IOException {
    return delete(f, false);
  }

  /**
   * Delete a file.
   *
   * @param f the path to delete.
   * @param recursive if path is a directory and set to
   * true, the directory is deleted else throws an exception. In
   * case of a file the recursive can be set to either true or false.
   *
   * @return true if delete is successful else false.
   *
   * @throws IOException
   */
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.DELETE.toString());
    params.put(RECURSIVE_PARAM, Boolean.toString(recursive));
    HttpURLConnection conn = getConnection(Operation.DELETE.getMethod(),
                                           params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return (Boolean) json.get(DELETE_JSON);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   *
   * @return the statuses of the files/directories in the given patch
   *
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.LISTSTATUS.toString());
    HttpURLConnection conn = getConnection(Operation.LISTSTATUS.getMethod(),
                                           params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    json = (JSONObject) json.get(FILE_STATUSES_JSON);
    JSONArray jsonArray = (JSONArray) json.get(FILE_STATUS_JSON);
    FileStatus[] array = new FileStatus[jsonArray.size()];
    f = makeQualified(f);
    for (int i = 0; i < jsonArray.size(); i++) {
      array[i] = createFileStatus(f, (JSONObject) jsonArray.get(i));
    }
    return array;
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir new directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  /**
   * Get the current working directory for the given file system
   *
   * @return the directory pathname
   */
  @Override
  public Path getWorkingDirectory() {
    if (workingDir == null) {
      workingDir = getHomeDirectory();
    }
    return workingDir;
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.MKDIRS.toString());
    params.put(PERMISSION_PARAM, permissionToString(permission));
    HttpURLConnection conn = getConnection(Operation.MKDIRS.getMethod(),
                                           params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return (Boolean) json.get(MKDIRS_JSON);
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   *
   * @return a FileStatus object
   *
   * @throws FileNotFoundException when the path does not exist;
   * IOException see specific implementation
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETFILESTATUS.toString());
    HttpURLConnection conn = getConnection(Operation.GETFILESTATUS.getMethod(),
                                           params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    json = (JSONObject) json.get(FILE_STATUS_JSON);
    f = makeQualified(f);
    return createFileStatus(f, json);
  }

  /**
   * Return the current user's home directory in this filesystem.
   * The default implementation returns "/user/$USER/".
   */
  @Override
  public Path getHomeDirectory() {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETHOMEDIRECTORY.toString());
    try {
      HttpURLConnection conn =
        getConnection(Operation.GETHOMEDIRECTORY.getMethod(), params,
                      new Path(getUri().toString(), "/"), false);
      HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
      JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
      return new Path((String) json.get(HOME_DIR_JSON));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Set owner of a path (i.e. a file or a directory).
   * The parameters username and groupname cannot both be null.
   *
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  @Override
  public void setOwner(Path p, String username, String groupname)
    throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETOWNER.toString());
    params.put(OWNER_PARAM, username);
    params.put(GROUP_PARAM, groupname);
    HttpURLConnection conn = getConnection(Operation.SETOWNER.getMethod(),
                                           params, p, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set permission of a path.
   *
   * @param p path.
   * @param permission permission.
   */
  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETPERMISSION.toString());
    params.put(PERMISSION_PARAM, permissionToString(permission));
    HttpURLConnection conn = getConnection(Operation.SETPERMISSION.getMethod(), params, p, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set access time of a file
   *
   * @param p The path
   * @param mtime Set the modification time of this file.
   * The number of milliseconds since Jan 1, 1970.
   * A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   * The number of milliseconds since Jan 1, 1970.
   * A value of -1 means that this call should not set access time.
   */
  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETTIMES.toString());
    params.put(MODIFICATION_TIME_PARAM, Long.toString(mtime));
    params.put(ACCESS_TIME_PARAM, Long.toString(atime));
    HttpURLConnection conn = getConnection(Operation.SETTIMES.getMethod(),
                                           params, p, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set replication for an existing file.
   *
   * @param src file name
   * @param replication new replication
   *
   * @return true if successful;
   *         false if file does not exist or is a directory
   *
   * @throws IOException
   */
  @Override
  public boolean setReplication(Path src, short replication)
    throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETREPLICATION.toString());
    params.put(REPLICATION_PARAM, Short.toString(replication));
    HttpURLConnection conn =
      getConnection(Operation.SETREPLICATION.getMethod(), params, src, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return (Boolean) json.get(SET_REPLICATION_JSON);
  }

  /**
   * Modify the ACL entries for a file.
   *
   * @param path Path to modify
   * @param aclSpec describing modifications
   * @throws IOException
   */
  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
          throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.MODIFYACLENTRIES.toString());
    params.put(ACLSPEC_PARAM, AclEntry.aclSpecToString(aclSpec));
    HttpURLConnection conn = getConnection(
            Operation.MODIFYACLENTRIES.getMethod(), params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Remove the specified ACL entries from a file
   * @param path Path to modify
   * @param aclSpec describing entries to remove
   * @throws IOException
   */
  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
          throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.REMOVEACLENTRIES.toString());
    params.put(ACLSPEC_PARAM, AclEntry.aclSpecToString(aclSpec));
    HttpURLConnection conn = getConnection(
            Operation.REMOVEACLENTRIES.getMethod(), params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Removes the default ACL for the given file
   * @param path Path from which to remove the default ACL.
   * @throws IOException
   */
  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.REMOVEDEFAULTACL.toString());
    HttpURLConnection conn = getConnection(
            Operation.REMOVEDEFAULTACL.getMethod(), params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Remove all ACLs from a file
   * @param path Path from which to remove all ACLs
   * @throws IOException
   */
  @Override
  public void removeAcl(Path path) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.REMOVEACL.toString());
    HttpURLConnection conn = getConnection(Operation.REMOVEACL.getMethod(),
            params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Set the ACLs for the given file
   * @param path Path to modify
   * @param aclSpec describing modifications, must include
   *                entries for user, group, and others for compatibility
   *                with permission bits.
   * @throws IOException
   */
  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETACL.toString());
    params.put(ACLSPEC_PARAM, AclEntry.aclSpecToString(aclSpec));
    HttpURLConnection conn = getConnection(Operation.SETACL.getMethod(),
                                           params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  /**
   * Get the ACL information for a given file
   * @param path Path to acquire ACL info for
   * @return the ACL information in JSON format
   * @throws IOException
   */
  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETACLSTATUS.toString());
    HttpURLConnection conn = getConnection(Operation.GETACLSTATUS.getMethod(),
            params, path, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    json = (JSONObject) json.get(ACL_STATUS_JSON);
    return createAclStatus(json);
  }

  private FileStatus createFileStatus(Path parent, JSONObject json) {
    String pathSuffix = (String) json.get(PATH_SUFFIX_JSON);
    Path path = (pathSuffix.equals("")) ? parent : new Path(parent, pathSuffix);
    FILE_TYPE type = FILE_TYPE.valueOf((String) json.get(TYPE_JSON));
    long len = (Long) json.get(LENGTH_JSON);
    String owner = (String) json.get(OWNER_JSON);
    String group = (String) json.get(GROUP_JSON);
    FsPermission permission =
      new FsPermission(Short.parseShort((String) json.get(PERMISSION_JSON), 8));
    long aTime = (Long) json.get(ACCESS_TIME_JSON);
    long mTime = (Long) json.get(MODIFICATION_TIME_JSON);
    long blockSize = (Long) json.get(BLOCK_SIZE_JSON);
    short replication = ((Long) json.get(REPLICATION_JSON)).shortValue();
    FileStatus fileStatus = null;

    switch (type) {
      case FILE:
      case DIRECTORY:
        fileStatus = new FileStatus(len, (type == FILE_TYPE.DIRECTORY),
                                    replication, blockSize, mTime, aTime,
                                    permission, owner, group, path);
        break;
      case SYMLINK:
        Path symLink = null;
        fileStatus = new FileStatus(len, false,
                                    replication, blockSize, mTime, aTime,
                                    permission, owner, group, symLink,
                                    path);
    }
    return fileStatus;
  }

  /**
   * Convert the given JSON object into an AclStatus
   * @param json Input JSON representing the ACLs
   * @return Resulting AclStatus
   */
  private AclStatus createAclStatus(JSONObject json) {
    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder()
            .owner((String) json.get(OWNER_JSON))
            .group((String) json.get(GROUP_JSON))
            .stickyBit((Boolean) json.get(ACL_STICKY_BIT_JSON));
    JSONArray entries = (JSONArray) json.get(ACL_ENTRIES_JSON);
    for ( Object e : entries ) {
      aclStatusBuilder.addEntry(AclEntry.parseAclEntry(e.toString(), true));
    }
    return aclStatusBuilder.build();
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETCONTENTSUMMARY.toString());
    HttpURLConnection conn =
      getConnection(Operation.GETCONTENTSUMMARY.getMethod(), params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) ((JSONObject)
      HttpFSUtils.jsonParse(conn)).get(CONTENT_SUMMARY_JSON);
    return new ContentSummary.Builder().
        length((Long) json.get(CONTENT_SUMMARY_LENGTH_JSON)).
        fileCount((Long) json.get(CONTENT_SUMMARY_FILE_COUNT_JSON)).
        directoryCount((Long) json.get(CONTENT_SUMMARY_DIRECTORY_COUNT_JSON)).
        quota((Long) json.get(CONTENT_SUMMARY_QUOTA_JSON)).
        spaceConsumed((Long) json.get(CONTENT_SUMMARY_SPACE_CONSUMED_JSON)).
        spaceQuota((Long) json.get(CONTENT_SUMMARY_SPACE_QUOTA_JSON)).build();
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETFILECHECKSUM.toString());
    HttpURLConnection conn =
      getConnection(Operation.GETFILECHECKSUM.getMethod(), params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    final JSONObject json = (JSONObject) ((JSONObject)
      HttpFSUtils.jsonParse(conn)).get(FILE_CHECKSUM_JSON);
    return new FileChecksum() {
      @Override
      public String getAlgorithmName() {
        return (String) json.get(CHECKSUM_ALGORITHM_JSON);
      }

      @Override
      public int getLength() {
        return ((Long) json.get(CHECKSUM_LENGTH_JSON)).intValue();
      }

      @Override
      public byte[] getBytes() {
        return StringUtils.hexStringToByte((String) json.get(CHECKSUM_BYTES_JSON));
      }

      @Override
      public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }


  @Override
  public Token<?> getDelegationToken(final String renewer)
    throws IOException {
    try {
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Token<?>>() {
            @Override
            public Token<?> run() throws Exception {
              return authURL.getDelegationToken(uri.toURL(), authToken,
                  renewer);
            }
          }
      );
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException) ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  public long renewDelegationToken(final Token<?> token) throws IOException {
    try {
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Long>() {
            @Override
            public Long run() throws Exception {
              return authURL.renewDelegationToken(uri.toURL(), authToken);
            }
          }
      );
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException) ex;
      } else {
        throw new IOException(ex);
      }
    }
  }

  public void cancelDelegationToken(final Token<?> token) throws IOException {
    authURL.cancelDelegationToken(uri.toURL(), authToken);
  }

  @Override
  public Token<?> getRenewToken() {
    return null; //TODO : for renewer
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends TokenIdentifier> void setDelegationToken(Token<T> token) {
    //TODO : for renewer
  }

  @Override
  public void setXAttr(Path f, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.SETXATTR.toString());
    params.put(XATTR_NAME_PARAM, name);
    if (value != null) {
      params.put(XATTR_VALUE_PARAM, 
          XAttrCodec.encodeValue(value, XAttrCodec.HEX));
    }
    params.put(XATTR_SET_FLAG_PARAM, EnumSetParam.toString(flag));
    HttpURLConnection conn = getConnection(Operation.SETXATTR.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }

  @Override
  public byte[] getXAttr(Path f, String name) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETXATTRS.toString());
    params.put(XATTR_NAME_PARAM, name);
    HttpURLConnection conn = getConnection(Operation.GETXATTRS.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    Map<String, byte[]> xAttrs = createXAttrMap(
        (JSONArray) json.get(XATTRS_JSON));
    return xAttrs != null ? xAttrs.get(name) : null;
  }

  /** Convert xAttrs json to xAttrs map */
  private Map<String, byte[]> createXAttrMap(JSONArray jsonArray) 
      throws IOException {
    Map<String, byte[]> xAttrs = Maps.newHashMap();
    for (Object obj : jsonArray) {
      JSONObject jsonObj = (JSONObject) obj;
      final String name = (String)jsonObj.get(XATTR_NAME_JSON);
      final byte[] value = XAttrCodec.decodeValue(
          (String)jsonObj.get(XATTR_VALUE_JSON));
      xAttrs.put(name, value);
    }

    return xAttrs;
  }

  /** Convert xAttr names json to names list */
  private List<String> createXAttrNames(String xattrNamesStr) throws IOException {
    JSONParser parser = new JSONParser();
    JSONArray jsonArray;
    try {
      jsonArray = (JSONArray)parser.parse(xattrNamesStr);
      List<String> names = Lists.newArrayListWithCapacity(jsonArray.size());
      for (Object name : jsonArray) {
        names.add((String) name);
      }
      return names;
    } catch (ParseException e) {
      throw new IOException("JSON parser error, " + e.getMessage(), e);
    }
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETXATTRS.toString());
    HttpURLConnection conn = getConnection(Operation.GETXATTRS.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return createXAttrMap((JSONArray) json.get(XATTRS_JSON));
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path f, List<String> names)
      throws IOException {
    Preconditions.checkArgument(names != null && !names.isEmpty(), 
        "XAttr names cannot be null or empty.");
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.GETXATTRS.toString());
    Map<String, List<String>> multiValuedParams = Maps.newHashMap();
    multiValuedParams.put(XATTR_NAME_PARAM, names);
    HttpURLConnection conn = getConnection(Operation.GETXATTRS.getMethod(),
        params, multiValuedParams, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return createXAttrMap((JSONArray) json.get(XATTRS_JSON));
  }

  @Override
  public List<String> listXAttrs(Path f) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.LISTXATTRS.toString());
    HttpURLConnection conn = getConnection(Operation.LISTXATTRS.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
    JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
    return createXAttrNames((String) json.get(XATTRNAMES_JSON));
  }

  @Override
  public void removeXAttr(Path f, String name) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put(OP_PARAM, Operation.REMOVEXATTR.toString());
    params.put(XATTR_NAME_PARAM, name);
    HttpURLConnection conn = getConnection(Operation.REMOVEXATTR.getMethod(),
        params, f, true);
    HttpExceptionUtils.validateResponse(conn, HttpURLConnection.HTTP_OK);
  }
}
