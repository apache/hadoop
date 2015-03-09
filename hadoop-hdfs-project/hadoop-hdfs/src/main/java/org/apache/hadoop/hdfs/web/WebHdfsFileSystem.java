/**
res * Licensed to the Apache Software Foundation (ASF) under one
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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/** A FileSystem for HDFS over the web. */
public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator {
  public static final Log LOG = LogFactory.getLog(WebHdfsFileSystem.class);
  /** File System URI: {SCHEME}://namenode:port/path/to/file */
  public static final String SCHEME = "webhdfs";
  /** WebHdfs version. */
  public static final int VERSION = 1;
  /** Http URI: http://namenode:port/{PATH_PREFIX}/path/to/file */
  public static final String PATH_PREFIX = "/" + SCHEME + "/v" + VERSION;

  /** Default connection factory may be overridden in tests to use smaller timeout values */
  protected URLConnectionFactory connectionFactory;

  /** Delegation token kind */
  public static final Text TOKEN_KIND = new Text("WEBHDFS delegation");

  @VisibleForTesting
  public static final String CANT_FALLBACK_TO_INSECURE_MSG =
      "The client is configured to only allow connecting to secure cluster";

  private boolean canRefreshDelegationToken;

  private UserGroupInformation ugi;
  private URI uri;
  private Token<?> delegationToken;
  protected Text tokenServiceName;
  private RetryPolicy retryPolicy = null;
  private Path workingDir;
  private InetSocketAddress nnAddrs[];
  private int currentNNAddrIndex;
  private boolean disallowFallbackToInsecureCluster;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>webhdfs</code>
   */
  @Override
  public String getScheme() {
    return SCHEME;
  }

  /**
   * return the underlying transport protocol (http / https).
   */
  protected String getTransportScheme() {
    return "http";
  }

  protected Text getTokenKind() {
    return TOKEN_KIND;
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf
      ) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
    /** set user pattern based on configuration file */
    UserParam.setUserPattern(conf.get(
        DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
        DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));

    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);

    ugi = UserGroupInformation.getCurrentUser();
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.nnAddrs = resolveNNAddr();

    boolean isHA = HAUtil.isClientFailoverConfigured(conf, this.uri);
    boolean isLogicalUri = isHA && HAUtil.isLogicalUri(conf, this.uri);
    // In non-HA or non-logical URI case, the code needs to call
    // getCanonicalUri() in order to handle the case where no port is
    // specified in the URI
    this.tokenServiceName = isLogicalUri ?
        HAUtil.buildTokenServiceForLogicalUri(uri, getScheme())
        : SecurityUtil.buildTokenService(getCanonicalUri());

    if (!isHA) {
      this.retryPolicy =
          RetryUtils.getDefaultRetryPolicy(
              conf,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY,
              DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT,
              SafeModeException.class);
    } else {

      int maxFailoverAttempts = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT);
      int failoverSleepBaseMillis = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      int failoverSleepMaxMillis = conf.getInt(
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
          DFSConfigKeys.DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);

      this.retryPolicy = RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              maxFailoverAttempts, maxRetryAttempts, failoverSleepBaseMillis,
              failoverSleepMaxMillis);
    }

    this.workingDir = getHomeDirectory();
    this.canRefreshDelegationToken = UserGroupInformation.isSecurityEnabled();
    this.disallowFallbackToInsecureCluster = !conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.delegationToken = null;
  }

  @Override
  public URI getCanonicalUri() {
    return super.getCanonicalUri();
  }

  /** Is WebHDFS enabled in conf? */
  public static boolean isEnabled(final Configuration conf, final Log log) {
    final boolean b = conf.getBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        DFSConfigKeys.DFS_WEBHDFS_ENABLED_DEFAULT);
    return b;
  }

  TokenSelector<DelegationTokenIdentifier> tokenSelector =
      new AbstractDelegationTokenSelector<DelegationTokenIdentifier>(getTokenKind()){};

  // the first getAuthParams() for a non-token op will either get the
  // internal token from the ugi or lazy fetch one
  protected synchronized Token<?> getDelegationToken() throws IOException {
    if (canRefreshDelegationToken && delegationToken == null) {
      Token<?> token = tokenSelector.selectToken(
          new Text(getCanonicalServiceName()), ugi.getTokens());
      // ugi tokens are usually indicative of a task which can't
      // refetch tokens.  even if ugi has credentials, don't attempt
      // to get another token to match hdfs/rpc behavior
      if (token != null) {
        LOG.debug("Using UGI token: " + token);
        canRefreshDelegationToken = false; 
      } else {
        token = getDelegationToken(null);
        if (token != null) {
          LOG.debug("Fetched new token: " + token);
        } else { // security is disabled
          canRefreshDelegationToken = false;
        }
      }
      setDelegationToken(token);
    }
    return delegationToken;
  }

  @VisibleForTesting
  synchronized boolean replaceExpiredDelegationToken() throws IOException {
    boolean replaced = false;
    if (canRefreshDelegationToken) {
      Token<?> token = getDelegationToken(null);
      LOG.debug("Replaced expired token: " + token);
      setDelegationToken(token);
      replaced = (token != null);
    }
    return replaced;
  }

  @Override
  @VisibleForTesting
  public int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT);
  }

  @Override
  public URI getUri() {
    return this.uri;
  }
  
  @Override
  protected URI canonicalizeUri(URI uri) {
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
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

  static Map<?, ?> jsonParse(final HttpURLConnection c, final boolean useErrorStream
      ) throws IOException {
    if (c.getContentLength() == 0) {
      return null;
    }
    final InputStream in = useErrorStream? c.getErrorStream(): c.getInputStream();
    if (in == null) {
      throw new IOException("The " + (useErrorStream? "error": "input") + " stream is null.");
    }
    try {
      final String contentType = c.getContentType();
      if (contentType != null) {
        final MediaType parsed = MediaType.valueOf(contentType);
        if (!MediaType.APPLICATION_JSON_TYPE.isCompatible(parsed)) {
          throw new IOException("Content-Type \"" + contentType
              + "\" is incompatible with \"" + MediaType.APPLICATION_JSON
              + "\" (parsed=\"" + parsed + "\")");
        }
      }
      ObjectMapper mapper = new ObjectMapper();
      return mapper.reader(Map.class).readValue(in);
    } finally {
      in.close();
    }
  }

  private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
      final HttpURLConnection conn, boolean unwrapException) throws IOException {
    final int code = conn.getResponseCode();
    // server is demanding an authentication we don't support
    if (code == HttpURLConnection.HTTP_UNAUTHORIZED) {
      // match hdfs/rpc exception
      throw new AccessControlException(conn.getResponseMessage());
    }
    if (code != op.getExpectedHttpResponseCode()) {
      final Map<?, ?> m;
      try {
        m = jsonParse(conn, true);
      } catch(Exception e) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage(), e);
      }

      if (m == null) {
        throw new IOException("Unexpected HTTP response: code=" + code + " != "
            + op.getExpectedHttpResponseCode() + ", " + op.toQueryString()
            + ", message=" + conn.getResponseMessage());
      } else if (m.get(RemoteException.class.getSimpleName()) == null) {
        return m;
      }

      IOException re = JsonUtil.toRemoteException(m);
      // extract UGI-related exceptions and unwrap InvalidToken
      // the NN mangles these exceptions but the DN does not and may need
      // to re-fetch a token if either report the token is expired
      if (re.getMessage() != null && re.getMessage().startsWith(
          SecurityUtil.FAILED_TO_GET_UGI_MSG_HEADER)) {
        String[] parts = re.getMessage().split(":\\s+", 3);
        re = new RemoteException(parts[1], parts[2]);
        re = ((RemoteException)re).unwrapRemoteException(InvalidToken.class);
      }
      throw unwrapException? toIOException(re): re;
    }
    return null;
  }

  /**
   * Covert an exception to an IOException.
   * 
   * For a non-IOException, wrap it with IOException.
   * For a RemoteException, unwrap it.
   * For an IOException which is not a RemoteException, return it. 
   */
  private static IOException toIOException(Exception e) {
    if (!(e instanceof IOException)) {
      return new IOException(e);
    }

    final IOException ioe = (IOException)e;
    if (!(ioe instanceof RemoteException)) {
      return ioe;
    }

    return ((RemoteException)ioe).unwrapRemoteException();
  }

  private synchronized InetSocketAddress getCurrentNNAddr() {
    return nnAddrs[currentNNAddrIndex];
  }

  /**
   * Reset the appropriate state to gracefully fail over to another name node
   */
  private synchronized void resetStateToFailOver() {
    currentNNAddrIndex = (currentNNAddrIndex + 1) % nnAddrs.length;
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
    InetSocketAddress nnAddr = getCurrentNNAddr();
    final URL url = new URL(getTransportScheme(), nnAddr.getHostName(),
          nnAddr.getPort(), path + '?' + query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }
  
  Param<?,?>[] getAuthParameters(final HttpOpParam.Op op) throws IOException {
    List<Param<?,?>> authParams = Lists.newArrayList();    
    // Skip adding delegation token for token operations because these
    // operations require authentication.
    Token<?> token = null;
    if (!op.getRequireAuth()) {
      token = getDelegationToken();
    }
    if (token != null) {
      authParams.add(new DelegationParam(token.encodeToUrlString()));
    } else {
      UserGroupInformation userUgi = ugi;
      UserGroupInformation realUgi = userUgi.getRealUser();
      if (realUgi != null) { // proxy user
        authParams.add(new DoAsParam(userUgi.getShortUserName()));
        userUgi = realUgi;
      }
      authParams.add(new UserParam(userUgi.getShortUserName()));
    }
    return authParams.toArray(new Param<?,?>[0]);
  }

  URL toUrl(final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    //initialize URI path and query
    final String path = PATH_PREFIX
        + (fspath == null? "/": makeQualified(fspath).toUri().getRawPath());
    final String query = op.toQueryString()
        + Param.toSortedString("&", getAuthParameters(op))
        + Param.toSortedString("&", parameters);
    final URL url = getNamenodeURL(path, query);
    if (LOG.isTraceEnabled()) {
      LOG.trace("url=" + url);
    }
    return url;
  }

  /**
   * This class is for initialing a HTTP connection, connecting to server,
   * obtaining a response, and also handling retry on failures.
   */
  abstract class AbstractRunner<T> {
    abstract protected URL getUrl() throws IOException;

    protected final HttpOpParam.Op op;
    private final boolean redirected;
    protected ExcludeDatanodesParam excludeDatanodes = new ExcludeDatanodesParam("");

    private boolean checkRetry;

    protected AbstractRunner(final HttpOpParam.Op op, boolean redirected) {
      this.op = op;
      this.redirected = redirected;
    }

    T run() throws IOException {
      UserGroupInformation connectUgi = ugi.getRealUser();
      if (connectUgi == null) {
        connectUgi = ugi;
      }
      if (op.getRequireAuth()) {
        connectUgi.checkTGTAndReloginFromKeytab();
      }
      try {
        // the entire lifecycle of the connection must be run inside the
        // doAs to ensure authentication is performed correctly
        return connectUgi.doAs(
            new PrivilegedExceptionAction<T>() {
              @Override
              public T run() throws IOException {
                return runWithRetry();
              }
            });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    /**
     * Two-step requests redirected to a DN
     * 
     * Create/Append:
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
     * 
     * Open/Checksum
     * Also implements two-step connects for other operations redirected to
     * a DN such as open and checksum
     */
    private HttpURLConnection connect(URL url) throws IOException {
      //redirect hostname and port
      String redirectHost = null;

      
      // resolve redirects for a DN operation unless already resolved
      if (op.getRedirect() && !redirected) {
        final HttpOpParam.Op redirectOp =
            HttpOpParam.TemporaryRedirectOp.valueOf(op);
        final HttpURLConnection conn = connect(redirectOp, url);
        // application level proxy like httpfs might not issue a redirect
        if (conn.getResponseCode() == op.getExpectedHttpResponseCode()) {
          return conn;
        }
        try {
          validateResponse(redirectOp, conn, false);
          url = new URL(conn.getHeaderField("Location"));
          redirectHost = url.getHost() + ":" + url.getPort();
        } finally {
          conn.disconnect();
        }
      }
      try {
        return connect(op, url);
      } catch (IOException ioe) {
        if (redirectHost != null) {
          if (excludeDatanodes.getValue() != null) {
            excludeDatanodes = new ExcludeDatanodesParam(redirectHost + ","
                + excludeDatanodes.getValue());
          } else {
            excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
          }
        }
        throw ioe;
      }      
    }

    private HttpURLConnection connect(final HttpOpParam.Op op, final URL url)
        throws IOException {
      final HttpURLConnection conn =
          (HttpURLConnection)connectionFactory.openConnection(url);
      final boolean doOutput = op.getDoOutput();
      conn.setRequestMethod(op.getType().toString());
      conn.setInstanceFollowRedirects(false);
      switch (op.getType()) {
        // if not sending a message body for a POST or PUT operation, need
        // to ensure the server/proxy knows this 
        case POST:
        case PUT: {
          conn.setDoOutput(true);
          if (!doOutput) {
            // explicitly setting content-length to 0 won't do spnego!!
            // opening and closing the stream will send "Content-Length: 0"
            conn.getOutputStream().close();
          } else {
            conn.setRequestProperty("Content-Type",
                MediaType.APPLICATION_OCTET_STREAM);
            conn.setChunkedStreamingMode(32 << 10); //32kB-chunk
          }
          break;
        }
        default: {
          conn.setDoOutput(doOutput);
          break;
        }
      }
      conn.connect();
      return conn;
    }

    private T runWithRetry() throws IOException {
      /**
       * Do the real work.
       *
       * There are three cases that the code inside the loop can throw an
       * IOException:
       *
       * <ul>
       * <li>The connection has failed (e.g., ConnectException,
       * @see FailoverOnNetworkExceptionRetry for more details)</li>
       * <li>The namenode enters the standby state (i.e., StandbyException).</li>
       * <li>The server returns errors for the command (i.e., RemoteException)</li>
       * </ul>
       *
       * The call to shouldRetry() will conduct the retry policy. The policy
       * examines the exception and swallows it if it decides to rerun the work.
       */
      for(int retry = 0; ; retry++) {
        checkRetry = !redirected;
        final URL url = getUrl();
        try {
          final HttpURLConnection conn = connect(url);
          // output streams will validate on close
          if (!op.getDoOutput()) {
            validateResponse(op, conn, false);
          }
          return getResponse(conn);
        } catch (AccessControlException ace) {
          // no retries for auth failures
          throw ace;
        } catch (InvalidToken it) {
          // try to replace the expired token with a new one.  the attempt
          // to acquire a new token must be outside this operation's retry
          // so if it fails after its own retries, this operation fails too.
          if (op.getRequireAuth() || !replaceExpiredDelegationToken()) {
            throw it;
          }
        } catch (IOException ioe) {
          shouldRetry(ioe, retry);
        }
      }
    }

    private void shouldRetry(final IOException ioe, final int retry
        ) throws IOException {
      InetSocketAddress nnAddr = getCurrentNNAddr();
      if (checkRetry) {
        try {
          final RetryPolicy.RetryAction a = retryPolicy.shouldRetry(
              ioe, retry, 0, true);

          boolean isRetry = a.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
          boolean isFailoverAndRetry =
              a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

          if (isRetry || isFailoverAndRetry) {
            LOG.info("Retrying connect to namenode: " + nnAddr
                + ". Already tried " + retry + " time(s); retry policy is "
                + retryPolicy + ", delay " + a.delayMillis + "ms.");

            if (isFailoverAndRetry) {
              resetStateToFailOver();
            }

            Thread.sleep(a.delayMillis);
            return;
          }
        } catch(Exception e) {
          LOG.warn("Original exception is ", ioe);
          throw toIOException(e);
        }
      }
      throw toIOException(ioe);
    }

    abstract T getResponse(HttpURLConnection conn) throws IOException;
  }

  /**
   * Abstract base class to handle path-based operations with params
   */
  abstract class AbstractFsPathRunner<T> extends AbstractRunner<T> {
    private final Path fspath;
    private final Param<?,?>[] parameters;
    
    AbstractFsPathRunner(final HttpOpParam.Op op, final Path fspath,
        Param<?,?>... parameters) {
      super(op, false);
      this.fspath = fspath;
      this.parameters = parameters;
    }
    
    AbstractFsPathRunner(final HttpOpParam.Op op, Param<?,?>[] parameters,
        final Path fspath) {
      super(op, false);
      this.fspath = fspath;
      this.parameters = parameters;
    }
    
    @Override
    protected URL getUrl() throws IOException {
      if (excludeDatanodes.getValue() != null) {
        Param<?, ?>[] tmpParam = new Param<?, ?>[parameters.length + 1];
        System.arraycopy(parameters, 0, tmpParam, 0, parameters.length);
        tmpParam[parameters.length] = excludeDatanodes;
        return toUrl(op, fspath, tmpParam);
      } else {
        return toUrl(op, fspath, parameters);
      }
    }
  }

  /**
   * Default path-based implementation expects no json response
   */
  class FsPathRunner extends AbstractFsPathRunner<Void> {
    FsPathRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }
    
    @Override
    Void getResponse(HttpURLConnection conn) throws IOException {
      return null;
    }
  }

  /**
   * Handle path-based operations with a json response
   */
  abstract class FsPathResponseRunner<T> extends AbstractFsPathRunner<T> {
    FsPathResponseRunner(final HttpOpParam.Op op, final Path fspath,
        Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }
    
    FsPathResponseRunner(final HttpOpParam.Op op, Param<?,?>[] parameters,
        final Path fspath) {
      super(op, parameters, fspath);
    }
    
    @Override
    final T getResponse(HttpURLConnection conn) throws IOException {
      try {
        final Map<?,?> json = jsonParse(conn, false);
        if (json == null) {
          // match exception class thrown by parser
          throw new IllegalStateException("Missing response");
        }
        return decodeResponse(json);
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) { // catch json parser errors
        final IOException ioe =
            new IOException("Response decoding failure: "+e.toString(), e);
        if (LOG.isDebugEnabled()) {
          LOG.debug(ioe);
        }
        throw ioe;
      } finally {
        conn.disconnect();
      }
    }
    
    abstract T decodeResponse(Map<?,?> json) throws IOException;
  }

  /**
   * Handle path-based operations with json boolean response
   */
  class FsPathBooleanRunner extends FsPathResponseRunner<Boolean> {
    FsPathBooleanRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }
    
    @Override
    Boolean decodeResponse(Map<?,?> json) throws IOException {
      return (Boolean)json.get("boolean");
    }
  }

  /**
   * Handle create/append output streams
   */
  class FsPathOutputStreamRunner extends AbstractFsPathRunner<FSDataOutputStream> {
    private final int bufferSize;
    
    FsPathOutputStreamRunner(Op op, Path fspath, int bufferSize,
        Param<?,?>... parameters) {
      super(op, fspath, parameters);
      this.bufferSize = bufferSize;
    }
    
    @Override
    FSDataOutputStream getResponse(final HttpURLConnection conn)
        throws IOException {
      return new FSDataOutputStream(new BufferedOutputStream(
          conn.getOutputStream(), bufferSize), statistics) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            try {
              validateResponse(op, conn, true);
            } finally {
              conn.disconnect();
            }
          }
        }
      };
    }
  }

  class FsPathConnectionRunner extends AbstractFsPathRunner<HttpURLConnection> {
    FsPathConnectionRunner(Op op, Path fspath, Param<?,?>... parameters) {
      super(op, fspath, parameters);
    }
    @Override
    HttpURLConnection getResponse(final HttpURLConnection conn)
        throws IOException {
      return conn;
    }
  }
  
  /**
   * Used by open() which tracks the resolved url itself
   */
  final class URLRunner extends AbstractRunner<HttpURLConnection> {
    private final URL url;
    @Override
    protected URL getUrl() {
      return url;
    }

    protected URLRunner(final HttpOpParam.Op op, final URL url, boolean redirected) {
      super(op, redirected);
      this.url = url;
    }

    @Override
    HttpURLConnection getResponse(HttpURLConnection conn) throws IOException {
      return conn;
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
    HdfsFileStatus status = new FsPathResponseRunner<HdfsFileStatus>(op, f) {
      @Override
      HdfsFileStatus decodeResponse(Map<?,?> json) {
        return JsonUtil.toFileStatus(json, true);
      }
    }.run();
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
        f.isSymlink() ? new Path(f.getSymlink()) : null,
        f.getFullPath(parent).makeQualified(getUri(), getWorkingDirectory()));
  }

  @Override
  public AclStatus getAclStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETACLSTATUS;
    AclStatus status = new FsPathResponseRunner<AclStatus>(op, f) {
      @Override
      AclStatus decodeResponse(Map<?,?> json) {
        return JsonUtil.toAclStatus(json);
      }
    }.run();
    if (status == null) {
      throw new FileNotFoundException("File does not exist: " + f);
    }
    return status;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MKDIRS;
    return new FsPathBooleanRunner(op, f,
        new PermissionParam(applyUMask(permission))
    ).run();
  }

  /**
   * Create a symlink pointing to the destination path.
   * @see org.apache.hadoop.fs.Hdfs#createSymlink(Path, Path, boolean) 
   */
  public void createSymlink(Path destination, Path f, boolean createParent
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESYMLINK;
    new FsPathRunner(op, f,
        new DestinationParam(makeQualified(destination).toUri().getPath()),
        new CreateParentParam(createParent)
    ).run();
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    return new FsPathBooleanRunner(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath())
    ).run();
  }

  @SuppressWarnings("deprecation")
  @Override
  public void rename(final Path src, final Path dst,
      final Options.Rename... options) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAME;
    new FsPathRunner(op, src,
        new DestinationParam(makeQualified(dst).toUri().getPath()),
        new RenameOptionSetParam(options)
    ).run();
  }
  
  @Override
  public void setXAttr(Path p, String name, byte[] value, 
      EnumSet<XAttrSetFlag> flag) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETXATTR;
    if (value != null) {
      new FsPathRunner(op, p, new XAttrNameParam(name), new XAttrValueParam(
          XAttrCodec.encodeValue(value, XAttrCodec.HEX)), 
          new XAttrSetFlagParam(flag)).run();
    } else {
      new FsPathRunner(op, p, new XAttrNameParam(name), 
          new XAttrSetFlagParam(flag)).run();
    }
  }
  
  @Override
  public byte[] getXAttr(Path p, final String name) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<byte[]>(op, p, new XAttrNameParam(name), 
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      byte[] decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtil.getXAttr(json, name);
      }
    }.run();
  }
  
  @Override
  public Map<String, byte[]> getXAttrs(Path p) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<Map<String, byte[]>>(op, p, 
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      Map<String, byte[]> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtil.toXAttrs(json);
      }
    }.run();
  }
  
  @Override
  public Map<String, byte[]> getXAttrs(Path p, final List<String> names) 
      throws IOException {
    Preconditions.checkArgument(names != null && !names.isEmpty(), 
        "XAttr names cannot be null or empty.");
    Param<?,?>[] parameters = new Param<?,?>[names.size() + 1];
    for (int i = 0; i < parameters.length - 1; i++) {
      parameters[i] = new XAttrNameParam(names.get(i));
    }
    parameters[parameters.length - 1] = new XAttrEncodingParam(XAttrCodec.HEX);
    
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<Map<String, byte[]>>(op, parameters, p) {
      @Override
      Map<String, byte[]> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtil.toXAttrs(json);
      }
    }.run();
  }
  
  @Override
  public List<String> listXAttrs(Path p) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.LISTXATTRS;
    return new FsPathResponseRunner<List<String>>(op, p) {
      @Override
      List<String> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtil.toXAttrNames(json);
      }
    }.run();
  }

  @Override
  public void removeXAttr(Path p, String name) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEXATTR;
    new FsPathRunner(op, p, new XAttrNameParam(name)).run();
  }

  @Override
  public void setOwner(final Path p, final String owner, final String group
      ) throws IOException {
    if (owner == null && group == null) {
      throw new IOException("owner == null && group == null");
    }

    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETOWNER;
    new FsPathRunner(op, p,
        new OwnerParam(owner), new GroupParam(group)
    ).run();
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
    new FsPathRunner(op, p,new PermissionParam(permission)).run();
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.MODIFYACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEDEFAULTACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void setAcl(final Path p, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETACL;
    new FsPathRunner(op, p, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName) 
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESNAPSHOT;
    Path spath = new FsPathResponseRunner<Path>(op, path,
        new SnapshotNameParam(snapshotName)) {
      @Override
      Path decodeResponse(Map<?,?> json) {
        return new Path((String) json.get(Path.class.getSimpleName()));
      }
    }.run();
    return spath;
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETESNAPSHOT;
    new FsPathRunner(op, path, new SnapshotNameParam(snapshotName)).run();
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.RENAMESNAPSHOT;
    new FsPathRunner(op, path, new OldSnapshotNameParam(snapshotOldName),
        new SnapshotNameParam(snapshotNewName)).run();
  }

  @Override
  public boolean setReplication(final Path p, final short replication
     ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
    return new FsPathBooleanRunner(op, p,
        new ReplicationParam(replication)
    ).run();
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime
      ) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PutOpParam.Op.SETTIMES;
    new FsPathRunner(op, p,
        new ModificationTimeParam(mtime),
        new AccessTimeParam(atime)
    ).run();
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short)getConf().getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  @Override
  public void concat(final Path trg, final Path [] srcs) throws IOException {
    statistics.incrementWriteOps(1);
    final HttpOpParam.Op op = PostOpParam.Op.CONCAT;
    new FsPathRunner(op, trg, new ConcatSourcesParam(srcs)).run();
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new PermissionParam(applyUMask(permission)),
        new OverwriteParam(overwrite),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize)
    ).run();
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.APPEND;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new BufferSizeParam(bufferSize)
    ).run();
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    statistics.incrementWriteOps(1);

    final HttpOpParam.Op op = PostOpParam.Op.TRUNCATE;
    return new FsPathBooleanRunner(op, f, new NewLengthParam(newLength)).run();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETE;
    return new FsPathBooleanRunner(op, f,
        new RecursiveParam(recursive)
    ).run();
  }

  @Override
  public FSDataInputStream open(final Path f, final int buffersize
      ) throws IOException {
    statistics.incrementReadOps(1);
    final HttpOpParam.Op op = GetOpParam.Op.OPEN;
    // use a runner so the open can recover from an invalid token
    FsPathConnectionRunner runner =
        new FsPathConnectionRunner(op, f, new BufferSizeParam(buffersize));
    return new FSDataInputStream(new OffsetUrlInputStream(
        new UnresolvedUrlOpener(runner), new OffsetUrlOpener(null)));
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (canRefreshDelegationToken && delegationToken != null) {
        cancelDelegationToken(delegationToken);
      }
    } catch (IOException ioe) {
      LOG.debug("Token cancel failed: "+ioe);
    } finally {
      super.close();
    }
  }

  // use FsPathConnectionRunner to ensure retries for InvalidTokens
  class UnresolvedUrlOpener extends ByteRangeInputStream.URLOpener {
    private final FsPathConnectionRunner runner;
    UnresolvedUrlOpener(FsPathConnectionRunner runner) {
      super(null);
      this.runner = runner;
    }

    @Override
    protected HttpURLConnection connect(long offset, boolean resolved)
        throws IOException {
      assert offset == 0;
      HttpURLConnection conn = runner.run();
      setURL(conn.getURL());
      return conn;
    }
  }

  class OffsetUrlOpener extends ByteRangeInputStream.URLOpener {
    OffsetUrlOpener(final URL url) {
      super(url);
    }

    /** Setup offset url and connect. */
    @Override
    protected HttpURLConnection connect(final long offset,
        final boolean resolved) throws IOException {
      final URL offsetUrl = offset == 0L? url
          : new URL(url + "&" + new OffsetParam(offset));
      return new URLRunner(GetOpParam.Op.OPEN, offsetUrl, resolved).run();
    }  
  }

  private static final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";

  /** Remove offset parameter, if there is any, from the url */
  static URL removeOffsetParam(final URL url) throws MalformedURLException {
    String query = url.getQuery();
    if (query == null) {
      return url;
    }
    final String lower = StringUtils.toLowerCase(query);
    if (!lower.startsWith(OFFSET_PARAM_PREFIX)
        && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
      return url;
    }

    //rebuild query
    StringBuilder b = null;
    for(final StringTokenizer st = new StringTokenizer(query, "&");
        st.hasMoreTokens();) {
      final String token = st.nextToken();
      if (!StringUtils.toLowerCase(token).startsWith(OFFSET_PARAM_PREFIX)) {
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
    OffsetUrlInputStream(UnresolvedUrlOpener o, OffsetUrlOpener r)
        throws IOException {
      super(o, r);
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
    return new FsPathResponseRunner<FileStatus[]>(op, f) {
      @Override
      FileStatus[] decodeResponse(Map<?,?> json) {
        final Map<?, ?> rootmap = (Map<?, ?>)json.get(FileStatus.class.getSimpleName() + "es");
        final List<?> array = JsonUtil.getList(
            rootmap, FileStatus.class.getSimpleName());

        //convert FileStatus
        final FileStatus[] statuses = new FileStatus[array.size()];
        int i = 0;
        for (Object object : array) {
          final Map<?, ?> m = (Map<?, ?>) object;
          statuses[i++] = makeQualified(JsonUtil.toFileStatus(m, false), f);
        }
        return statuses;
      }
    }.run();
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETDELEGATIONTOKEN;
    Token<DelegationTokenIdentifier> token =
        new FsPathResponseRunner<Token<DelegationTokenIdentifier>>(
            op, null, new RenewerParam(renewer)) {
      @Override
      Token<DelegationTokenIdentifier> decodeResponse(Map<?,?> json)
          throws IOException {
        return JsonUtil.toDelegationToken(json);
      }
    }.run();
    if (token != null) {
      token.setService(tokenServiceName);
    } else {
      if (disallowFallbackToInsecureCluster) {
        throw new AccessControlException(CANT_FALLBACK_TO_INSECURE_MSG);
      }
    }
    return token;
  }

  @Override
  public synchronized Token<?> getRenewToken() {
    return delegationToken;
  }

  @Override
  public <T extends TokenIdentifier> void setDelegationToken(
      final Token<T> token) {
    synchronized (this) {
      delegationToken = token;
    }
  }

  @Override
  public synchronized long renewDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.RENEWDELEGATIONTOKEN;
    return new FsPathResponseRunner<Long>(op, null,
        new TokenArgumentParam(token.encodeToUrlString())) {
      @Override
      Long decodeResponse(Map<?,?> json) throws IOException {
        return ((Number) json.get("long")).longValue();
      }
    }.run();
  }

  @Override
  public synchronized void cancelDelegationToken(final Token<?> token
      ) throws IOException {
    final HttpOpParam.Op op = PutOpParam.Op.CANCELDELEGATIONTOKEN;
    new FsPathRunner(op, null,
        new TokenArgumentParam(token.encodeToUrlString())
    ).run();
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus status,
      final long offset, final long length) throws IOException {
    if (status == null) {
      return null;
    }
    return getFileBlockLocations(status.getPath(), offset, length);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(final Path p, 
      final long offset, final long length) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GET_BLOCK_LOCATIONS;
    return new FsPathResponseRunner<BlockLocation[]>(op, p,
        new OffsetParam(offset), new LengthParam(length)) {
      @Override
      BlockLocation[] decodeResponse(Map<?,?> json) throws IOException {
        return DFSUtil.locatedBlocks2Locations(
            JsonUtil.toLocatedBlocks(json));
      }
    }.run();
  }

  @Override
  public void access(final Path path, final FsAction mode) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.CHECKACCESS;
    new FsPathRunner(op, path, new FsActionParam(mode)).run();
  }

  @Override
  public ContentSummary getContentSummary(final Path p) throws IOException {
    statistics.incrementReadOps(1);

    final HttpOpParam.Op op = GetOpParam.Op.GETCONTENTSUMMARY;
    return new FsPathResponseRunner<ContentSummary>(op, p) {
      @Override
      ContentSummary decodeResponse(Map<?,?> json) {
        return JsonUtil.toContentSummary(json);        
      }
    }.run();
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(final Path p
      ) throws IOException {
    statistics.incrementReadOps(1);
  
    final HttpOpParam.Op op = GetOpParam.Op.GETFILECHECKSUM;
    return new FsPathResponseRunner<MD5MD5CRC32FileChecksum>(op, p) {
      @Override
      MD5MD5CRC32FileChecksum decodeResponse(Map<?,?> json) throws IOException {
        return JsonUtil.toMD5MD5CRC32FileChecksum(json);
      }
    }.run();
  }

  /**
   * Resolve an HDFS URL into real INetSocketAddress. It works like a DNS
   * resolver when the URL points to an non-HA cluster. When the URL points to
   * an HA cluster with its logical name, the resolver further resolves the
   * logical name(i.e., the authority in the URL) into real namenode addresses.
   */
  private InetSocketAddress[] resolveNNAddr() throws IOException {
    Configuration conf = getConf();
    final String scheme = uri.getScheme();

    ArrayList<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();

    if (!HAUtil.isLogicalUri(conf, uri)) {
      InetSocketAddress addr = NetUtils.createSocketAddr(uri.getAuthority(),
          getDefaultPort());
      ret.add(addr);

    } else {
      Map<String, Map<String, InetSocketAddress>> addresses = DFSUtil
          .getHaNnWebHdfsAddresses(conf, scheme);

      // Extract the entry corresponding to the logical name.
      Map<String, InetSocketAddress> addrs = addresses.get(uri.getHost());
      for (InetSocketAddress addr : addrs.values()) {
        ret.add(addr);
      }
    }

    InetSocketAddress[] r = new InetSocketAddress[ret.size()];
    return ret.toArray(r);
  }

  @Override
  public String getCanonicalServiceName() {
    return tokenServiceName == null ? super.getCanonicalServiceName()
        : tokenServiceName.toString();
  }

  @VisibleForTesting
  InetSocketAddress[] getResolvedNNAddr() {
    return nnAddrs;
  }
}
