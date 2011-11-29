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

package org.apache.hadoop.hdfs.web;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.ByteRangeInputStream;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenRenewer;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.web.resources.AccessTimeParam;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DeleteOpParam;
import org.apache.hadoop.hdfs.web.resources.DestinationParam;
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
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.apache.hadoop.util.Progressable;
import org.mortbay.util.ajax.JSON;

/** A FileSystem for HDFS over the web. */
public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable {
  public static final Log LOG = LogFactory.getLog(WebHdfsFileSystem.class);
  /** File System URI: {SCHEME}://namenode:port/path/to/file */
  public static final String SCHEME = "webhdfs";
  /** WebHdfs version. */
  public static final int VERSION = 1;
  /** Http URI: http://namenode:port/{PATH_PREFIX}/path/to/file */
  public static final String PATH_PREFIX = "/" + SCHEME + "/v" + VERSION;

  /** SPNEGO authenticator */
  private static final KerberosUgiAuthenticator AUTH = new KerberosUgiAuthenticator();
  /** Delegation token kind */
  public static final Text TOKEN_KIND = new Text("WEBHDFS delegation");
  /** Token selector */
  public static final AbstractDelegationTokenSelector<DelegationTokenIdentifier> DT_SELECTOR
      = new AbstractDelegationTokenSelector<DelegationTokenIdentifier>(TOKEN_KIND) {};

  private static DelegationTokenRenewer<WebHdfsFileSystem> DT_RENEWER = null;

  private static synchronized void addRenewAction(final WebHdfsFileSystem webhdfs) {
    if (DT_RENEWER == null) {
      DT_RENEWER = new DelegationTokenRenewer<WebHdfsFileSystem>(WebHdfsFileSystem.class);
      DT_RENEWER.start();
    }

    DT_RENEWER.addRenewAction(webhdfs);
  }

  /** Is WebHDFS enabled in conf? */
  public static boolean isEnabled(final Configuration conf, final Log log) {
    final boolean b = conf.getBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        DFSConfigKeys.DFS_WEBHDFS_ENABLED_DEFAULT);
    log.info(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY + " = " + b);
    return b;
  }

  private final UserGroupInformation ugi;
  private InetSocketAddress nnAddr;
  private Token<?> delegationToken;
  private final AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
  private Path workingDir;

  {
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf
      ) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.nnAddr = NetUtils.createSocketAddr(uri.getAuthority(), getDefaultPort());
    this.workingDir = getHomeDirectory();

    if (UserGroupInformation.isSecurityEnabled()) {
      initDelegationToken();
    }
  }

  protected void initDelegationToken() throws IOException {
    // look for webhdfs token, then try hdfs
    final Text serviceName = SecurityUtil.buildTokenService(nnAddr);
    Token<?> token = DT_SELECTOR.selectToken(serviceName, ugi.getTokens());      
    if (token == null) {
      token = DelegationTokenSelector.selectHdfsDelegationToken(
          nnAddr, ugi, getConf());
    }

    //since we don't already have a token, go get one
    boolean createdToken = false;
    if (token == null) {
      token = getDelegationToken(null);
      createdToken = (token != null);
    }

    // security might be disabled
    if (token != null) {
      setDelegationToken(token);
      if (createdToken) {
        addRenewAction(this);
        LOG.debug("Created new DT for " + token.getService());
      } else {
        LOG.debug("Found existing DT for " + token.getService());        
      }
    }
  }

  @Override
  protected int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  @Override
  public URI getUri() {
    try {
      return new URI(SCHEME, null, nnAddr.getHostName(), nnAddr.getPort(),
          null, null, null);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  /** @return the home directory. */
  public static String getHomeDirectoryString(final UserGroupInformation ugi) {
    return "/user/" + ugi.getShortUserName();
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(getHomeDirectoryString(ugi)));
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public synchronized void setWorkingDirectory(final Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path f) {
    return f.isAbsolute()? f: new Path(workingDir, f);
  }

  static Map<?, ?> jsonParse(final InputStream in) throws IOException {
    if (in == null) {
      throw new IOException("The input stream is null.");
    }
    return (Map<?, ?>)JSON.parse(new InputStreamReader(in));
  }

  private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
      final HttpURLConnection conn) throws IOException {
    final int code = conn.getResponseCode();
    if (code != op.getExpectedHttpResponseCode()) {
      final Map<?, ?> m;
      try {
        m = jsonParse(conn.getErrorStream());
      } catch(IOException e) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage(), e);
      }

      if (m.get(RemoteException.class.getSimpleName()) == null) {
        return m;
      }

      final RemoteException re = JsonUtil.toRemoteException(m);
      throw re.unwrapRemoteException(AccessControlException.class,
          InvalidToken.class,
          AuthenticationException.class,
          AuthorizationException.class,
          FileAlreadyExistsException.class,
          FileNotFoundException.class,
          SafeModeException.class,
          DSQuotaExceededException.class,
          NSQuotaExceededException.class);
    }
    return null;
  }

  /**
   * Return a URL pointing to given path on the namenode.
   *
   * @param path to obtain the URL for
   * @param query string to append to the path
   * @return namenode URL referring to the given path
   * @throws IOException on error constructing the URL
   */
  private URL getNamenodeURL(String path, String query) throws IOException {
    final URL url = new URL("http", nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }
  
  private String addDt2Query(String query) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      synchronized (this) {
        if (delegationToken != null) {
          final String encoded = delegationToken.encodeToUrlString();
          return query + JspHelper.getDelegationTokenUrlParam(encoded);
        } // else we are talking to an insecure cluster
      }
    }
    return query;
  }

  URL toUrl(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    //initialize URI path and query
    final String path = PATH_PREFIX
        + (fspath == null? "/": makeQualified(fspath).toUri().getPath());
    final String query = op.toQueryString()
        + '&' + new UserParam(ugi)
        + Param.toSortedString("&", parameters);
    final URL url;
    if (op.equals(PutOpParam.Op.RENEWDELEGATIONTOKEN)
        || op.equals(GetOpParam.Op.GETDELEGATIONTOKEN)) {
      // Skip adding delegation token for getting or renewing delegation token,
      // because these operations require kerberos authentication.
      url = getNamenodeURL(path, query);
    } else {
      url = getNamenodeURL(path, addDt2Query(query));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  private HttpURLConnection getHttpUrlConnection(URL url)
      throws IOException {
    final HttpURLConnection conn;
    try {
      if (ugi.hasKerberosCredentials()) { 
        conn = new AuthenticatedURL(AUTH).openConnection(url, authToken);
      } else {
        conn = (HttpURLConnection)url.openConnection();
      }
    } catch (AuthenticationException e) {
      throw new IOException("Authentication failed, url=" + url, e);
    }
    return conn;
  }
  
  private HttpURLConnection httpConnect(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    final URL url = toUrl(op, fspath, parameters);

    //connect and get response
    HttpURLConnection conn = getHttpUrlConnection(url);
    try {
      conn.setRequestMethod(op.getType().toString());
      if (op.getDoOutput()) {
        conn = twoStepWrite(conn, op);
      }
      conn.setDoOutput(op.getDoOutput());
      conn.connect();
      return conn;
    } catch (IOException e) {
      conn.disconnect();
      throw e;
    }
  }
  
  /**
   * Two-step Create/Append:
   * Step 1) Submit a Http request with neither auto-redirect nor data. 
   * Step 2) Submit another Http request with the URL from the Location header with data.
   * 
   * The reason of having two-step create/append is for preventing clients to
   * send out the data before the redirect. This issue is addressed by the
   * "Expect: 100-continue" header in HTTP/1.1; see RFC 2616, Section 8.2.3.
   * Unfortunately, there are software library bugs (e.g. Jetty 6 http server
   * and Java 6 http client), which do not correctly implement "Expect:
   * 100-continue". The two-step create/append is a temporary workaround for
   * the software library bugs.
   */
  static HttpURLConnection twoStepWrite(HttpURLConnection conn,
      final HttpOpParam.Op op) throws IOException {
    //Step 1) Submit a Http request with neither auto-redirect nor data. 
    conn.setInstanceFollowRedirects(false);
    conn.setDoOutput(false);
    conn.connect();
    validateResponse(HttpOpParam.TemporaryRedirectOp.valueOf(op), conn);
    final String redirect = conn.getHeaderField("Location");
    conn.disconnect();

    //Step 2) Submit another Http request with the URL from the Location header with data.
    conn = (HttpURLConnection)new URL(redirect).openConnection();
    conn.setRequestMethod(op.getType().toString());
    return conn;
  }

  /**
   * Run a http operation.
   * Connect to the http server, validate response, and obtain the JSON output.
   * 
   * @param op http operation
   * @param fspath file system path
   * @param parameters parameters for the operation
   * @return a JSON object, e.g. Object[], Map<?, ?>, etc.
   * @throws IOException
   */
  private Map<?, ?> run(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    final HttpURLConnection conn = httpConnect(op, fspath, parameters);
    try {
      final Map<?, ?> m = validateResponse(op, conn);
      return m != null? m: jsonParse(conn.getInputStream());
    } finally {
      conn.disconnect();
    }
  }

  private FsPermission applyUMask(FsPermission permission) {
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    return permission.applyUMask(FsPermission.getUMask(getConf()));
  }

  private HdfsFileStatus getHdfsFileStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETFILESTATUS;
    final Map<?, ?> json = run(op, f);
    final HdfsFileStatus status = JsonUtil.toFileStatus(json, true);
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    return makeQualified(getHdfsFileStatus(f), f);
  }

  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.getFullPath(parent).makeQualified(this)); // fully-qualify path
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MKDIRS;
    final Map<?, ?> json = run(op, f,
        new PermissionParam(applyUMask(permission)));
    return (Boolean)json.get("boolean");
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    final Map<?, ?> json = run(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath()));
    return (Boolean)json.get("boolean");
  }

  @Override
  public void setOwner(final Path p, final String owner, final String group
      ) throws IOException {
    if (owner == null && group == null) {
      throw new IOException("owner == null && group == null");
    }

    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETOWNER;
    run(op, p, new OwnerParam(owner), new GroupParam(group));
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
    run(op, p, new PermissionParam(permission));
  }

  @Override
  public boolean setReplication(final Path p, final short replication
     ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
    final Map<?, ?> json = run(op, p, new ReplicationParam(replication));
    return (Boolean)json.get("boolean");
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETTIMES;
    run(op, p, new ModificationTimeParam(mtime), new AccessTimeParam(atime));
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short)getConf().getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  FSDataOutputStream write(final HttpOpParam.Op op,
      final HttpURLConnection conn, final int bufferSize) throws IOException {
    return new FSDataOutputStream(new BufferedOutputStream(
        conn.getOutputStream(), bufferSize), statistics) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          try {
            validateResponse(op, conn);
          } finally {
            conn.disconnect();
          }
        }
      }
    };
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    final HttpURLConnection conn = httpConnect(op, f, 
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize));
    return write(op, conn, bufferSize);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.APPEND;
    final HttpURLConnection conn = httpConnect(op, f, 
        new BufferSizeParam(bufferSize));
    return write(op, conn, bufferSize);
  }

  @Override
  public boolean delete(final Path f) throws IOException {
    return delete(f, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETE;
    final Map<?, ?> json = run(op, f, new RecursiveParam(recursive));
    return (Boolean)json.get("boolean");
  }

  @Override
  public FSDataInputStream open(final Path f, final int buffersize
      ) throws IOException {
    statistics.incrementReadOps(1);
    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    final URL url = toUrl(op, f, new BufferSizeParam(buffersize));
    return new FSDataInputStream(new OffsetUrlInputStream(
        new OffsetUrlOpener(url), new OffsetUrlOpener(null)));
  }

  class OffsetUrlOpener extends ByteRangeInputStream.URLOpener {
    /** The url with offset parameter */
    private URL offsetUrl;
  
    OffsetUrlOpener(final URL url) {
      super(url);
    }

    /** Open connection with offset url. */
    @Override
    protected HttpURLConnection openConnection() throws IOException {
      return getHttpUrlConnection(offsetUrl);
    }

    /** Setup offset url before open connection. */
    @Override
    protected HttpURLConnection openConnection(final long offset) throws IOException {
      offsetUrl = offset == 0L? url: new URL(url + "&" + new OffsetParam(offset));
      final HttpURLConnection conn = openConnection();
      conn.setRequestMethod("GET");
      return conn;
    }  
  }

  private static final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";

  /** Remove offset parameter, if there is any, from the url */
  static URL removeOffsetParam(final URL url) throws MalformedURLException {
    String query = url.getQuery();
    if (query == null) {
      return url;
    }
    final String lower = query.toLowerCase();
    if (!lower.startsWith(OFFSET_PARAM_PREFIX)
        && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
      return url;
    }

    //rebuild query
    StringBuilder b = null;
    for(final StringTokenizer st = new StringTokenizer(query, "&");
        st.hasMoreTokens();) {
      final String token = st.nextToken();
      if (!token.toLowerCase().startsWith(OFFSET_PARAM_PREFIX)) {
        if (b == null) {
          b = new StringBuilder("?").append(token);
        } else {
          b.append('&').append(token);
        }
      }
    }
    query = b == null? "": b.toString();

    final String urlStr = url.toString();
    return new URL(urlStr.substring(0, urlStr.indexOf('?')) + query);
  }

  static class OffsetUrlInputStream extends ByteRangeInputStream {
    OffsetUrlInputStream(OffsetUrlOpener o, OffsetUrlOpener r) {
      super(o, r);
    }
    
    @Override
    protected void checkResponseCode(final HttpURLConnection connection
        ) throws IOException {
      validateResponse(GetOpParam.Op.OPEN, connection);
    }

    /** Remove offset parameter before returning the resolved url. */
    @Override
    protected URL getResolvedUrl(final HttpURLConnection connection
        ) throws MalformedURLException {
      return removeOffsetParam(connection.getURL());
    }
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.LISTSTATUS;
    final Map<?, ?> json  = run(op, f);
    final Map<?, ?> rootmap = (Map<?, ?>)json.get(FileStatus.class.getSimpleName() + "es");
    final Object[] array = (Object[])rootmap.get(FileStatus.class.getSimpleName());

    //convert FileStatus
    final FileStatus[] statuses = new FileStatus[array.length];
    for(int i = 0; i < array.length; i++) {
      final Map<?, ?> m = (Map<?, ?>)array[i];
      statuses[i] = makeQualified(JsonUtil.toFileStatus(m, false), f);
    }
    return statuses;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(final String renewer
      ) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETDELEGATIONTOKEN;
    final Map<?, ?> m = run(op, null, new RenewerParam(renewer));
    final Token<DelegationTokenIdentifier> token = JsonUtil.toDelegationToken(m); 
    SecurityUtil.setTokenService(token, nnAddr);
    return token;
  }

  @Override
  public Token<?> getRenewToken() {
    return delegationToken;
  }

  @Override
  public <T extends TokenIdentifier> void setDelegationToken(
      final Token<T> token) {
    synchronized(this) {
      delegationToken = token;
    }
  }

  private synchronized long renewDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.RENEWDELEGATIONTOKEN;
    TokenArgumentParam dtargParam = new TokenArgumentParam(
        token.encodeToUrlString());
    final Map<?, ?> m = run(op, null, dtargParam);
    return (Long) m.get("long");
  }

  private synchronized void cancelDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.CANCELDELEGATIONTOKEN;
    TokenArgumentParam dtargParam = new TokenArgumentParam(
        token.encodeToUrlString());
    run(op, null, dtargParam);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }
    statistics.incrementReadOps(1);

    final Path p = status.getPath();
    final HttpOpParam.Op op = GetOpParam.Op.GET_BLOCK_LOCATIONS;
    final Map<?, ?> m = run(op, p, new OffsetParam(offset),
        new LengthParam(length));
    return DFSUtil.locatedBlocks2Locations(JsonUtil.toLocatedBlocks(m));
  }

  @Override
  public ContentSummary getContentSummary(final Path p) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GETCONTENTSUMMARY;
    final Map<?, ?> m = run(op, p);
    return JsonUtil.toContentSummary(m);
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(final Path p
      ) throws IOException {
    statistics.incrementReadOps(1);
  
    final HttpOpParam.Op op = GetOpParam.Op.GETFILECHECKSUM;
    final Map<?, ?> m = run(op, p);
    return JsonUtil.toMD5MD5CRC32FileChecksum(m);
  }


  /** Delegation token renewer. */
  public static class DtRenewer extends TokenRenewer {
    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(TOKEN_KIND);
    }
  
    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    private static WebHdfsFileSystem getWebHdfs(
        final Token<?> token, final Configuration conf
        ) throws IOException, InterruptedException, URISyntaxException {
      
      final InetSocketAddress nnAddr = SecurityUtil.getTokenServiceAddr(token);
      final URI uri = DFSUtil.createUri(WebHdfsFileSystem.SCHEME, nnAddr);
      return (WebHdfsFileSystem)FileSystem.get(uri, conf);
    }

    @Override
    public long renew(final Token<?> token, final Configuration conf
        ) throws IOException, InterruptedException {
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      // update the kerberos credentials, if they are coming from a keytab
      ugi.checkTGTAndReloginFromKeytab();

      try {
        WebHdfsFileSystem webhdfs = getWebHdfs(token, conf);
        return webhdfs.renewDelegationToken(token);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
  
    @Override
    public void cancel(final Token<?> token, final Configuration conf
        ) throws IOException, InterruptedException {
      final UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      // update the kerberos credentials, if they are coming from a keytab
      ugi.checkTGTAndReloginFromKeytab();

      try {
        final WebHdfsFileSystem webhdfs = getWebHdfs(token, conf);
        webhdfs.cancelDelegationToken(token);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
  }
}
