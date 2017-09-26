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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_KEY;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.GlobalStorageStatistics.StorageStatisticsProvider;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/** A FileSystem for HDFS over the web. */
public class WebHdfsFileSystem extends FileSystem
    implements DelegationTokenRenewer.Renewable,
    TokenAspect.TokenManagementDelegator {
  public static final Logger LOG = LoggerFactory
      .getLogger(WebHdfsFileSystem.class);
  /** WebHdfs version. */
  public static final int VERSION = 1;
  /** Http URI: http://namenode:port/{PATH_PREFIX}/path/to/file */
  public static final String PATH_PREFIX = "/" + WebHdfsConstants.WEBHDFS_SCHEME
      + "/v" + VERSION;

  /**
   * Default connection factory may be overridden in tests to use smaller
   * timeout values
   */
  protected URLConnectionFactory connectionFactory;

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
  private Path cachedHomeDirectory;
  private InetSocketAddress nnAddrs[];
  private int currentNNAddrIndex;
  private boolean disallowFallbackToInsecureCluster;
  private String restCsrfCustomHeader;
  private Set<String> restCsrfMethodsToIgnore;
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(Map.class);

  private DFSOpsCountStatistics storageStatistics;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>webhdfs</code>
   */
  @Override
  public String getScheme() {
    return WebHdfsConstants.WEBHDFS_SCHEME;
  }

  /**
   * return the underlying transport protocol (http / https).
   */
  protected String getTransportScheme() {
    return "http";
  }

  protected Text getTokenKind() {
    return WebHdfsConstants.WEBHDFS_TOKEN_KIND;
  }

  @Override
  public synchronized void initialize(URI uri, Configuration conf
  ) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    // set user and acl patterns based on configuration file
    UserParam.setUserPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));
    AclPermissionParam.setAclPermissionPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

    boolean isOAuth = conf.getBoolean(
        HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT);

    if(isOAuth) {
      LOG.debug("Enabling OAuth2 in WebHDFS");
      connectionFactory = URLConnectionFactory
          .newOAuth2URLConnectionFactory(conf);
    } else {
      LOG.debug("Not enabling OAuth2 in WebHDFS");
      connectionFactory = URLConnectionFactory
          .newDefaultURLConnectionFactory(conf);
    }


    ugi = UserGroupInformation.getCurrentUser();
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.nnAddrs = resolveNNAddr();

    boolean isHA = HAUtilClient.isClientFailoverConfigured(conf, this.uri);
    boolean isLogicalUri = isHA && HAUtilClient.isLogicalUri(conf, this.uri);
    // In non-HA or non-logical URI case, the code needs to call
    // getCanonicalUri() in order to handle the case where no port is
    // specified in the URI
    this.tokenServiceName = isLogicalUri ?
        HAUtilClient.buildTokenServiceForLogicalUri(uri, getScheme())
        : SecurityUtil.buildTokenService(getCanonicalUri());

    if (!isHA) {
      this.retryPolicy =
          RetryUtils.getDefaultRetryPolicy(
              conf,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_KEY,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_ENABLED_DEFAULT,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_KEY,
              HdfsClientConfigKeys.HttpClient.RETRY_POLICY_SPEC_DEFAULT,
              HdfsConstants.SAFEMODE_EXCEPTION_CLASS_NAME);
    } else {

      int maxFailoverAttempts = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_MAX_ATTEMPTS_DEFAULT);
      int maxRetryAttempts = conf.getInt(
          HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_KEY,
          HdfsClientConfigKeys.HttpClient.RETRY_MAX_ATTEMPTS_DEFAULT);
      int failoverSleepBaseMillis = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_BASE_DEFAULT);
      int failoverSleepMaxMillis = conf.getInt(
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_KEY,
          HdfsClientConfigKeys.HttpClient.FAILOVER_SLEEPTIME_MAX_DEFAULT);

      this.retryPolicy = RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              maxFailoverAttempts, maxRetryAttempts, failoverSleepBaseMillis,
              failoverSleepMaxMillis);
    }

    this.workingDir = makeQualified(new Path(getHomeDirectoryString(ugi)));
    this.canRefreshDelegationToken = UserGroupInformation.isSecurityEnabled();
    this.disallowFallbackToInsecureCluster = !conf.getBoolean(
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.initializeRestCsrf(conf);
    this.delegationToken = null;

    storageStatistics = (DFSOpsCountStatistics) GlobalStorageStatistics.INSTANCE
        .put(DFSOpsCountStatistics.NAME,
            new StorageStatisticsProvider() {
              @Override
              public StorageStatistics provide() {
                return new DFSOpsCountStatistics();
              }
            });
  }

  /**
   * Initializes client-side handling of cross-site request forgery (CSRF)
   * protection by figuring out the custom HTTP headers that need to be sent in
   * requests and which HTTP methods are ignored because they do not require
   * CSRF protection.
   *
   * @param conf configuration to read
   */
  private void initializeRestCsrf(Configuration conf) {
    if (conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
        DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      this.restCsrfCustomHeader = conf.getTrimmed(
          DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_KEY,
          DFS_WEBHDFS_REST_CSRF_CUSTOM_HEADER_DEFAULT);
      this.restCsrfMethodsToIgnore = new HashSet<>();
      this.restCsrfMethodsToIgnore.addAll(getTrimmedStringList(conf,
          DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_KEY,
          DFS_WEBHDFS_REST_CSRF_METHODS_TO_IGNORE_DEFAULT));
    } else {
      this.restCsrfCustomHeader = null;
      this.restCsrfMethodsToIgnore = null;
    }
  }

  /**
   * Returns a list of strings from a comma-delimited configuration value.
   *
   * @param conf configuration to check
   * @param name configuration property name
   * @param defaultValue default value if no value found for name
   * @return list of strings from comma-delimited configuration value, or an
   *     empty list if not found
   */
  private static List<String> getTrimmedStringList(Configuration conf,
      String name, String defaultValue) {
    String valueString = conf.get(name, defaultValue);
    if (valueString == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(StringUtils.getTrimmedStringCollection(valueString));
  }

  @Override
  public URI getCanonicalUri() {
    return super.getCanonicalUri();
  }

  TokenSelector<DelegationTokenIdentifier> tokenSelector =
      new AbstractDelegationTokenSelector<DelegationTokenIdentifier>(getTokenKind()){};

  // the first getAuthParams() for a non-token op will either get the
  // internal token from the ugi or lazy fetch one
  protected synchronized Token<?> getDelegationToken() throws IOException {
    if (delegationToken == null) {
      Token<?> token = tokenSelector.selectToken(
          new Text(getCanonicalServiceName()), ugi.getTokens());
      // ugi tokens are usually indicative of a task which can't
      // refetch tokens.  even if ugi has credentials, don't attempt
      // to get another token to match hdfs/rpc behavior
      if (token != null) {
        LOG.debug("Using UGI token: {}", token);
        canRefreshDelegationToken = false;
      } else {
        if (canRefreshDelegationToken) {
          token = getDelegationToken(null);
          if (token != null) {
            LOG.debug("Fetched new token: {}", token);
          } else { // security is disabled
            canRefreshDelegationToken = false;
          }
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
      LOG.debug("Replaced expired token: {}", token);
      setDelegationToken(token);
      replaced = (token != null);
    }
    return replaced;
  }

  @Override
  protected int getDefaultPort() {
    return HdfsClientConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  protected URI canonicalizeUri(URI uri) {
    return NetUtils.getCanonicalUri(uri, getDefaultPort());
  }

  /** @return the home directory */
  @Deprecated
  public static String getHomeDirectoryString(final UserGroupInformation ugi) {
    return "/user/" + ugi.getShortUserName();
  }

  @Override
  public Path getHomeDirectory() {
    if (cachedHomeDirectory == null) {
      final HttpOpParam.Op op = GetOpParam.Op.GETHOMEDIRECTORY;
      try {
        String pathFromDelegatedFS = new FsPathResponseRunner<String>(op, null,
            new UserParam(ugi)) {
          @Override
          String decodeResponse(Map<?, ?> json) throws IOException {
            return JsonUtilClient.getPath(json);
          }
        }   .run();

        cachedHomeDirectory = new Path(pathFromDelegatedFS).makeQualified(
            this.getUri(), null);

      } catch (IOException e) {
        LOG.error("Unable to get HomeDirectory from original File System", e);
        cachedHomeDirectory = new Path("/user/" + ugi.getShortUserName())
            .makeQualified(this.getUri(), null);
      }
    }
    return cachedHomeDirectory;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public synchronized void setWorkingDirectory(final Path dir) {
    Path absolutePath = makeAbsolute(dir);
    String result = absolutePath.toUri().getPath();
    if (!DFSUtilClient.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " +
          result);
    }
    workingDir = absolutePath;
  }

  private Path makeAbsolute(Path f) {
    return f.isAbsolute()? f: new Path(workingDir, f);
  }

  static Map<?, ?> jsonParse(final HttpURLConnection c,
      final boolean useErrorStream) throws IOException {
    if (c.getContentLength() == 0) {
      return null;
    }
    final InputStream in = useErrorStream ?
        c.getErrorStream() : c.getInputStream();
    if (in == null) {
      throw new IOException("The " + (useErrorStream? "error": "input") +
          " stream is null.");
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
      return READER.readValue(in);
    } finally {
      in.close();
    }
  }

  private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
      final HttpURLConnection conn, boolean unwrapException)
      throws IOException {
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

      IOException re = JsonUtilClient.toRemoteException(m);

      //check if exception is due to communication with a Standby name node
      if (re.getMessage() != null && re.getMessage().endsWith(
          StandbyException.class.getSimpleName())) {
        LOG.trace("Detected StandbyException", re);
        throw new IOException(re);
      }
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
    LOG.trace("url={}", url);
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
    LOG.trace("url={}", url);
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
    protected ExcludeDatanodesParam excludeDatanodes =
        new ExcludeDatanodesParam("");

    private boolean checkRetry;
    private String redirectHost;

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
     * Step 2) Submit another Http request with the URL from the Location header
     * with data.
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
    protected HttpURLConnection connect(URL url) throws IOException {
      //redirect hostname and port
      redirectHost = null;


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
          // TODO: consider not calling conn.disconnect() to allow connection reuse
          // See http://tinyurl.com/java7-http-keepalive
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
      if (restCsrfCustomHeader != null &&
          !restCsrfMethodsToIgnore.contains(op.getType().name())) {
        // The value of the header is unimportant.  Only its presence matters.
        conn.setRequestProperty(restCsrfCustomHeader, "\"\"");
      }
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
      default:
        conn.setDoOutput(doOutput);
        break;
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
          // Attempt to include the redirected node in the exception. If the
          // attempt to recreate the exception fails, just use the original.
          String node = redirectHost;
          if (node == null) {
            node = url.getAuthority();
          }
          try {
            IOException newIoe = ioe.getClass().getConstructor(String.class)
                .newInstance(node + ": " + ioe.getMessage());
            newIoe.setStackTrace(ioe.getStackTrace());
            ioe = newIoe;
          } catch (NoSuchMethodException | SecurityException 
                   | InstantiationException | IllegalAccessException
                   | IllegalArgumentException | InvocationTargetException e) {
          }
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

          boolean isRetry =
              a.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
          boolean isFailoverAndRetry =
              a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

          if (isRetry || isFailoverAndRetry) {
            LOG.info("Retrying connect to namenode: {}. Already retried {}"
                    + " time(s); retry policy is {}, delay {}ms.",
                nnAddr, retry, retryPolicy, a.delayMillis);

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
    private Param<?,?>[] parameters;

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

    protected void updateURLParameters(Param<?, ?>... p) {
      this.parameters = p;
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
        LOG.debug("Response decoding failure.", e);
        throw ioe;
      } finally {
        // Don't call conn.disconnect() to allow connection reuse
        // See http://tinyurl.com/java7-http-keepalive
        conn.getInputStream().close();
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
  class FsPathOutputStreamRunner
      extends AbstractFsPathRunner<FSDataOutputStream> {
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
              // This is a connection to DataNode.  Let's disconnect since
              // there is little chance that the connection will be reused
              // any time soonl
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

    protected URLRunner(final HttpOpParam.Op op, final URL url,
        boolean redirected) {
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
    return FsCreateModes.applyUMask(permission,
        FsPermission.getUMask(getConf()));
  }

  private HdfsFileStatus getHdfsFileStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETFILESTATUS;
    HdfsFileStatus status = new FsPathResponseRunner<HdfsFileStatus>(op, f) {
      @Override
      HdfsFileStatus decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toFileStatus(json, true);
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
    storageStatistics.incrementOpCounter(OpType.GET_FILE_STATUS);
    return getHdfsFileStatus(f).makeQualified(getUri(), f);
  }

  @Override
  public AclStatus getAclStatus(Path f) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETACLSTATUS;
    AclStatus status = new FsPathResponseRunner<AclStatus>(op, f) {
      @Override
      AclStatus decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toAclStatus(json);
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
    storageStatistics.incrementOpCounter(OpType.MKDIRS);
    final HttpOpParam.Op op = PutOpParam.Op.MKDIRS;
    final FsPermission modes = applyUMask(permission);
    return new FsPathBooleanRunner(op, f,
        new PermissionParam(modes.getMasked()),
        new UnmaskedPermissionParam(modes.getUnmasked())
    ).run();
  }

  /**
   * Create a symlink pointing to the destination path.
   */
  public void createSymlink(Path destination, Path f, boolean createParent
  ) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_SYM_LINK);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESYMLINK;
    new FsPathRunner(op, f,
        new DestinationParam(makeQualified(destination).toUri().getPath()),
        new CreateParentParam(createParent)
    ).run();
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.RENAME);
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
    storageStatistics.incrementOpCounter(OpType.RENAME);
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
    storageStatistics.incrementOpCounter(OpType.SET_XATTR);
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
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_XATTR);
    final HttpOpParam.Op op = GetOpParam.Op.GETXATTRS;
    return new FsPathResponseRunner<byte[]>(op, p, new XAttrNameParam(name),
        new XAttrEncodingParam(XAttrCodec.HEX)) {
      @Override
      byte[] decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.getXAttr(json);
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
        return JsonUtilClient.toXAttrs(json);
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
        return JsonUtilClient.toXAttrs(json);
      }
    }.run();
  }

  @Override
  public List<String> listXAttrs(Path p) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.LISTXATTRS;
    return new FsPathResponseRunner<List<String>>(op, p) {
      @Override
      List<String> decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toXAttrNames(json);
      }
    }.run();
  }

  @Override
  public void removeXAttr(Path p, String name) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_XATTR);
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
    storageStatistics.incrementOpCounter(OpType.SET_OWNER);
    final HttpOpParam.Op op = PutOpParam.Op.SETOWNER;
    new FsPathRunner(op, p,
        new OwnerParam(owner), new GroupParam(group)
    ).run();
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission
  ) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_PERMISSION);
    final HttpOpParam.Op op = PutOpParam.Op.SETPERMISSION;
    new FsPathRunner(op, p,new PermissionParam(permission)).run();
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.MODIFY_ACL_ENTRIES);
    final HttpOpParam.Op op = PutOpParam.Op.MODIFYACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_ACL_ENTRIES);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACLENTRIES;
    new FsPathRunner(op, path, new AclPermissionParam(aclSpec)).run();
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_DEFAULT_ACL);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEDEFAULTACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.REMOVE_ACL);
    final HttpOpParam.Op op = PutOpParam.Op.REMOVEACL;
    new FsPathRunner(op, path).run();
  }

  @Override
  public void setAcl(final Path p, final List<AclEntry> aclSpec)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_ACL);
    final HttpOpParam.Op op = PutOpParam.Op.SETACL;
    new FsPathRunner(op, p, new AclPermissionParam(aclSpec)).run();
  }

  public void allowSnapshot(final Path p) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.ALLOW_SNAPSHOT);
    final HttpOpParam.Op op = PutOpParam.Op.ALLOWSNAPSHOT;
    new FsPathRunner(op, p).run();
  }

  @Override
  public Path createSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_SNAPSHOT);
    final HttpOpParam.Op op = PutOpParam.Op.CREATESNAPSHOT;
    return new FsPathResponseRunner<Path>(op, path,
        new SnapshotNameParam(snapshotName)) {
      @Override
      Path decodeResponse(Map<?,?> json) {
        return new Path((String) json.get(Path.class.getSimpleName()));
      }
    }.run();
  }

  public void disallowSnapshot(final Path p) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DISALLOW_SNAPSHOT);
    final HttpOpParam.Op op = PutOpParam.Op.DISALLOWSNAPSHOT;
    new FsPathRunner(op, p).run();
  }

  @Override
  public void deleteSnapshot(final Path path, final String snapshotName)
      throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DELETE_SNAPSHOT);
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETESNAPSHOT;
    new FsPathRunner(op, path, new SnapshotNameParam(snapshotName)).run();
  }

  @Override
  public void renameSnapshot(final Path path, final String snapshotOldName,
      final String snapshotNewName) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.RENAME_SNAPSHOT);
    final HttpOpParam.Op op = PutOpParam.Op.RENAMESNAPSHOT;
    new FsPathRunner(op, path, new OldSnapshotNameParam(snapshotOldName),
        new SnapshotNameParam(snapshotNewName)).run();
  }

  @Override
  public boolean setReplication(final Path p, final short replication
  ) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_REPLICATION);
    final HttpOpParam.Op op = PutOpParam.Op.SETREPLICATION;
    return new FsPathBooleanRunner(op, p,
        new ReplicationParam(replication)
    ).run();
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime
  ) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_TIMES);
    final HttpOpParam.Op op = PutOpParam.Op.SETTIMES;
    new FsPathRunner(op, p,
        new ModificationTimeParam(mtime),
        new AccessTimeParam(atime)
    ).run();
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLongBytes(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY,
        HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
  }

  @Override
  public short getDefaultReplication() {
    return (short)getConf().getInt(HdfsClientConfigKeys.DFS_REPLICATION_KEY,
        HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  @Override
  public void concat(final Path trg, final Path [] srcs) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CONCAT);
    final HttpOpParam.Op op = PostOpParam.Op.CONCAT;
    new FsPathRunner(op, trg, new ConcatSourcesParam(srcs)).run();
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE);

    final FsPermission modes = applyUMask(permission);
    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new PermissionParam(modes.getMasked()),
        new UnmaskedPermissionParam(modes.getUnmasked()),
        new OverwriteParam(overwrite),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize)
    ).run();
  }

  @Override
  public FSDataOutputStream createNonRecursive(final Path f,
      final FsPermission permission, final EnumSet<CreateFlag> flag,
      final int bufferSize, final short replication, final long blockSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.CREATE_NON_RECURSIVE);

    final FsPermission modes = applyUMask(permission);
    final HttpOpParam.Op op = PutOpParam.Op.CREATE;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new PermissionParam(modes.getMasked()),
        new UnmaskedPermissionParam(modes.getUnmasked()),
        new CreateFlagParam(flag),
        new CreateParentParam(false),
        new BufferSizeParam(bufferSize),
        new ReplicationParam(replication),
        new BlockSizeParam(blockSize)
    ).run();
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.APPEND);

    final HttpOpParam.Op op = PostOpParam.Op.APPEND;
    return new FsPathOutputStreamRunner(op, f, bufferSize,
        new BufferSizeParam(bufferSize)
    ).run();
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.TRUNCATE);

    final HttpOpParam.Op op = PostOpParam.Op.TRUNCATE;
    return new FsPathBooleanRunner(op, f, new NewLengthParam(newLength)).run();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.DELETE);
    final HttpOpParam.Op op = DeleteOpParam.Op.DELETE;
    return new FsPathBooleanRunner(op, f,
        new RecursiveParam(recursive)
    ).run();
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize
  ) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.OPEN);
    return new FSDataInputStream(new WebHdfsInputStream(f, bufferSize));
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (canRefreshDelegationToken && delegationToken != null) {
        cancelDelegationToken(delegationToken);
      }
    } catch (IOException ioe) {
      LOG.debug("Token cancel failed: ", ioe);
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
    storageStatistics.incrementOpCounter(OpType.LIST_STATUS);

    final URI fsUri = getUri();
    final HttpOpParam.Op op = GetOpParam.Op.LISTSTATUS;
    return new FsPathResponseRunner<FileStatus[]>(op, f) {
      @Override
      FileStatus[] decodeResponse(Map<?,?> json) {
        HdfsFileStatus[] hdfsStatuses =
            JsonUtilClient.toHdfsFileStatusArray(json);
        final FileStatus[] statuses = new FileStatus[hdfsStatuses.length];
        for (int i = 0; i < hdfsStatuses.length; i++) {
          statuses[i] = hdfsStatuses[i].makeQualified(fsUri, f);
        }

        return statuses;
      }
    }.run();
  }

  private static final byte[] EMPTY_ARRAY = new byte[] {};

  @Override
  public DirectoryEntries listStatusBatch(Path f, byte[] token) throws
      FileNotFoundException, IOException {
    byte[] prevKey = EMPTY_ARRAY;
    if (token != null) {
      prevKey = token;
    }
    DirectoryListing listing = new FsPathResponseRunner<DirectoryListing>(
        GetOpParam.Op.LISTSTATUS_BATCH,
        f, new StartAfterParam(new String(prevKey, Charsets.UTF_8))) {
      @Override
      DirectoryListing decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toDirectoryListing(json);
      }
    }.run();
    // Qualify the returned FileStatus array
    final URI fsUri = getUri();
    final HdfsFileStatus[] statuses = listing.getPartialListing();
    FileStatus[] qualified = new FileStatus[statuses.length];
    for (int i = 0; i < statuses.length; i++) {
      qualified[i] = statuses[i].makeQualified(fsUri, f);
    }
    return new DirectoryEntries(qualified, listing.getLastName(),
        listing.hasMore());
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
            return JsonUtilClient.toDelegationToken(json);
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
    storageStatistics.incrementOpCounter(OpType.GET_FILE_BLOCK_LOCATIONS);

    final HttpOpParam.Op op = GetOpParam.Op.GET_BLOCK_LOCATIONS;
    return new FsPathResponseRunner<BlockLocation[]>(op, p,
        new OffsetParam(offset), new LengthParam(length)) {
      @Override
      BlockLocation[] decodeResponse(Map<?,?> json) throws IOException {
        return DFSUtilClient.locatedBlocks2Locations(
            JsonUtilClient.toLocatedBlocks(json));
      }
    }.run();
  }

  @Override
  public Path getTrashRoot(Path path) {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_TRASH_ROOT);

    final HttpOpParam.Op op = GetOpParam.Op.GETTRASHROOT;
    try {
      String strTrashPath = new FsPathResponseRunner<String>(op, path) {
        @Override
        String decodeResponse(Map<?, ?> json) throws IOException {
          return JsonUtilClient.getPath(json);
        }
      }.run();
      return new Path(strTrashPath).makeQualified(getUri(), null);
    } catch(IOException e) {
      LOG.warn("Cannot find trash root of " + path, e);
      // keep the same behavior with dfs
      return super.getTrashRoot(path).makeQualified(getUri(), null);
    }
  }

  @Override
  public void access(final Path path, final FsAction mode) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.CHECKACCESS;
    new FsPathRunner(op, path, new FsActionParam(mode)).run();
  }

  @Override
  public ContentSummary getContentSummary(final Path p) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_CONTENT_SUMMARY);

    final HttpOpParam.Op op = GetOpParam.Op.GETCONTENTSUMMARY;
    return new FsPathResponseRunner<ContentSummary>(op, p) {
      @Override
      ContentSummary decodeResponse(Map<?,?> json) {
        return JsonUtilClient.toContentSummary(json);
      }
    }.run();
  }

  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(final Path p
  ) throws IOException {
    statistics.incrementReadOps(1);
    storageStatistics.incrementOpCounter(OpType.GET_FILE_CHECKSUM);

    final HttpOpParam.Op op = GetOpParam.Op.GETFILECHECKSUM;
    return new FsPathResponseRunner<MD5MD5CRC32FileChecksum>(op, p) {
      @Override
      MD5MD5CRC32FileChecksum decodeResponse(Map<?,?> json) throws IOException {
        return JsonUtilClient.toMD5MD5CRC32FileChecksum(json);
      }
    }.run();
  }

  /**
   * Resolve an HDFS URL into real INetSocketAddress. It works like a DNS
   * resolver when the URL points to an non-HA cluster. When the URL points to
   * an HA cluster with its logical name, the resolver further resolves the
   * logical name(i.e., the authority in the URL) into real namenode addresses.
   */
  private InetSocketAddress[] resolveNNAddr() {
    Configuration conf = getConf();
    final String scheme = uri.getScheme();

    ArrayList<InetSocketAddress> ret = new ArrayList<>();

    if (!HAUtilClient.isLogicalUri(conf, uri)) {
      InetSocketAddress addr = NetUtils.createSocketAddr(uri.getAuthority(),
          getDefaultPort());
      ret.add(addr);

    } else {
      Map<String, Map<String, InetSocketAddress>> addresses = DFSUtilClient
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

  @Override
  public void setStoragePolicy(Path p, String policyName) throws IOException {
    if (policyName == null) {
      throw new IOException("policyName == null");
    }
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.SET_STORAGE_POLICY);
    final HttpOpParam.Op op = PutOpParam.Op.SETSTORAGEPOLICY;
    new FsPathRunner(op, p, new StoragePolicyParam(policyName)).run();
  }

  @Override
  public Collection<BlockStoragePolicy> getAllStoragePolicies()
      throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETALLSTORAGEPOLICY;
    return new FsPathResponseRunner<Collection<BlockStoragePolicy>>(op, null) {
      @Override
      Collection<BlockStoragePolicy> decodeResponse(Map<?, ?> json)
          throws IOException {
        return JsonUtilClient.getStoragePolicies(json);
      }
    }.run();
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(Path src) throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETSTORAGEPOLICY;
    return new FsPathResponseRunner<BlockStoragePolicy>(op, src) {
      @Override
      BlockStoragePolicy decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toBlockStoragePolicy((Map<?, ?>) json
            .get(BlockStoragePolicy.class.getSimpleName()));
      }
    }.run();
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    statistics.incrementWriteOps(1);
    storageStatistics.incrementOpCounter(OpType.UNSET_STORAGE_POLICY);
    final HttpOpParam.Op op = PostOpParam.Op.UNSETSTORAGEPOLICY;
    new FsPathRunner(op, src).run();
  }

  /*
   * Caller of this method should handle UnsupportedOperationException in case
   * when new client is talking to old namenode that don't support
   * FsServerDefaults call.
   */
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    final HttpOpParam.Op op = GetOpParam.Op.GETSERVERDEFAULTS;
    return new FsPathResponseRunner<FsServerDefaults>(op, null) {
      @Override
      FsServerDefaults decodeResponse(Map<?, ?> json) throws IOException {
        return JsonUtilClient.toFsServerDefaults(json);
      }
    }.run();
  }

  @VisibleForTesting
  InetSocketAddress[] getResolvedNNAddr() {
    return nnAddrs;
  }

  @VisibleForTesting
  public void setRetryPolicy(RetryPolicy rp) {
    this.retryPolicy = rp;
  }

  /**
   * This class is used for opening, reading, and seeking files while using the
   * WebHdfsFileSystem. This class will invoke the retry policy when performing
   * any of these actions.
   */
  @VisibleForTesting
  public class WebHdfsInputStream extends FSInputStream {
    private ReadRunner readRunner = null;

    WebHdfsInputStream(Path path, int buffersize) throws IOException {
      // Only create the ReadRunner once. Each read's byte array and position
      // will be updated within the ReadRunner object before every read.
      readRunner = new ReadRunner(path, buffersize);
    }

    @Override
    public int read() throws IOException {
      final byte[] b = new byte[1];
      return (read(b, 0, 1) == -1) ? -1 : (b[0] & 0xff);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
      return readRunner.read(b, off, len);
    }

    @Override
    public void seek(long newPos) throws IOException {
      readRunner.seek(newPos);
    }

    @Override
    public long getPos() throws IOException {
      return readRunner.getPos();
    }

    protected int getBufferSize() throws IOException {
      return readRunner.getBufferSize();
    }

    protected Path getPath() throws IOException {
      return readRunner.getPath();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public void close() throws IOException {
      readRunner.close();
    }

    public void setFileLength(long len) {
      readRunner.setFileLength(len);
    }

    public long getFileLength() {
      return readRunner.getFileLength();
    }

    @VisibleForTesting
    ReadRunner getReadRunner() {
      return readRunner;
    }

    @VisibleForTesting
    void setReadRunner(ReadRunner rr) {
      this.readRunner = rr;
    }
  }

  enum RunnerState {
    DISCONNECTED, // Connection is closed programmatically by ReadRunner
    OPEN,         // Connection has been established by ReadRunner
    SEEK,         // Calling code has explicitly called seek()
    CLOSED        // Calling code has explicitly called close()
    }

  /**
   * This class will allow retries to occur for both open and read operations.
   * The first WebHdfsFileSystem#open creates a new WebHdfsInputStream object,
   * which creates a new ReadRunner object that will be used to open a
   * connection and read or seek into the input stream.
   *
   * ReadRunner is a subclass of the AbstractRunner class, which will run the
   * ReadRunner#getUrl(), ReadRunner#connect(URL), and ReadRunner#getResponse
   * methods within a retry loop, based on the configured retry policy.
   * ReadRunner#connect will create a connection if one has not already been
   * created. Otherwise, it will return the previously created connection
   * object. This is necessary because a new connection should not be created
   * for every read.
   * Likewise, ReadRunner#getUrl will construct a new URL object only if the
   * connection has not previously been established. Otherwise, it will return
   * the previously created URL object.
   * ReadRunner#getResponse will initialize the input stream if it has not
   * already been initialized and read the requested data from the specified
   * input stream.
   */
  @VisibleForTesting
  protected class ReadRunner extends AbstractFsPathRunner<Integer> {
    private InputStream in = null;
    private HttpURLConnection cachedConnection = null;
    private byte[] readBuffer;
    private int readOffset;
    private int readLength;
    private RunnerState runnerState = RunnerState.DISCONNECTED;
    private URL originalUrl = null;
    private URL resolvedUrl = null;

    private final Path path;
    private final int bufferSize;
    private long pos = 0;
    private long fileLength = 0;

    /* The following methods are WebHdfsInputStream helpers. */

    ReadRunner(Path p, int bs) throws IOException {
      super(GetOpParam.Op.OPEN, p, new BufferSizeParam(bs));
      this.path = p;
      this.bufferSize = bs;
    }

    int read(byte[] b, int off, int len) throws IOException {
      if (runnerState == RunnerState.CLOSED) {
        throw new IOException("Stream closed");
      }
      if (len == 0) {
        return 0;
      }

      // Before the first read, pos and fileLength will be 0 and readBuffer
      // will all be null. They will be initialized once the first connection
      // is made. Only after that it makes sense to compare pos and fileLength.
      if (pos >= fileLength && readBuffer != null) {
        return -1;
      }

      // If a seek is occurring, the input stream will have been closed, so it
      // needs to be reopened. Use the URLRunner to call AbstractRunner#connect
      // with the previously-cached resolved URL and with the 'redirected' flag
      // set to 'true'. The resolved URL contains the URL of the previously
      // opened DN as opposed to the NN. It is preferable to use the resolved
      // URL when creating a connection because it does not hit the NN or every
      // seek, nor does it open a connection to a new DN after every seek.
      // The redirect flag is needed so that AbstractRunner#connect knows the
      // URL is already resolved.
      // Note that when the redirected flag is set, retries are not attempted.
      // So, if the connection fails using URLRunner, clear out the connection
      // and fall through to establish the connection using ReadRunner.
      if (runnerState == RunnerState.SEEK) {
        try {
          final URL rurl = new URL(resolvedUrl + "&" + new OffsetParam(pos));
          cachedConnection = new URLRunner(GetOpParam.Op.OPEN, rurl, true).run();
        } catch (IOException ioe) {
          closeInputStream(RunnerState.DISCONNECTED);
        }
      }

      readBuffer = b;
      readOffset = off;
      readLength = len;

      int count = -1;
      count = this.run();
      if (count >= 0) {
        statistics.incrementBytesRead(count);
        pos += count;
      } else if (pos < fileLength) {
        throw new EOFException(
                  "Premature EOF: pos=" + pos + " < filelength=" + fileLength);
      }
      return count;
    }

    void seek(long newPos) throws IOException {
      if (pos != newPos) {
        pos = newPos;
        closeInputStream(RunnerState.SEEK);
      }
    }

    public void close() throws IOException {
      closeInputStream(RunnerState.CLOSED);
    }

    /* The following methods are overriding AbstractRunner methods,
     * to be called within the retry policy context by runWithRetry.
     */

    @Override
    protected URL getUrl() throws IOException {
      // This method is called every time either a read is executed.
      // The check for connection == null is to ensure that a new URL is only
      // created upon a new connection and not for every read.
      if (cachedConnection == null) {
        // Update URL with current offset. BufferSize doesn't change, but it
        // still must be included when creating the new URL.
        updateURLParameters(new BufferSizeParam(bufferSize),
            new OffsetParam(pos));
        originalUrl = super.getUrl();
      }
      return originalUrl;
    }

    /* Only make the connection if it is not already open. Don't cache the
     * connection here. After this method is called, runWithRetry will call
     * validateResponse, and then call the below ReadRunner#getResponse. If
     * the code path makes it that far, then we can cache the connection.
     */
    @Override
    protected HttpURLConnection connect(URL url) throws IOException {
      HttpURLConnection conn = cachedConnection;
      if (conn == null) {
        try {
          conn = super.connect(url);
        } catch (IOException e) {
          closeInputStream(RunnerState.DISCONNECTED);
          throw e;
        }
      }
      return conn;
    }

    /*
     * This method is used to perform reads within the retry policy context.
     * This code is relying on runWithRetry to always call the above connect
     * method and the verifyResponse method prior to calling getResponse.
     */
    @Override
    Integer getResponse(final HttpURLConnection conn)
        throws IOException {
      try {
        // In the "open-then-read" use case, runWithRetry will have executed
        // ReadRunner#connect to make the connection and then executed
        // validateResponse to validate the response code. Only then do we want
        // to cache the connection.
        // In the "read-after-seek" use case, the connection is made and the
        // response is validated by the URLRunner. ReadRunner#read then caches
        // the connection and the ReadRunner#connect will pass on the cached
        // connection
        // In either case, stream initialization is done here if necessary.
        cachedConnection = conn;
        if (in == null) {
          in = initializeInputStream(conn);
        }

        int count = in.read(readBuffer, readOffset, readLength);
        if (count < 0 && pos < fileLength) {
          throw new EOFException(
                  "Premature EOF: pos=" + pos + " < filelength=" + fileLength);
        }
        return Integer.valueOf(count);
      } catch (IOException e) {
        String redirectHost = resolvedUrl.getAuthority();
        if (excludeDatanodes.getValue() != null) {
          excludeDatanodes = new ExcludeDatanodesParam(redirectHost + ","
              + excludeDatanodes.getValue());
        } else {
          excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
        }

        // If an exception occurs, close the input stream and null it out so
        // that if the abstract runner decides to retry, it will reconnect.
        closeInputStream(RunnerState.DISCONNECTED);
        throw e;
      }
    }

    @VisibleForTesting
    InputStream initializeInputStream(HttpURLConnection conn)
        throws IOException {
      // Cache the resolved URL so that it can be used in the event of
      // a future seek operation.
      resolvedUrl = removeOffsetParam(conn.getURL());
      final String cl = conn.getHeaderField(HttpHeaders.CONTENT_LENGTH);
      InputStream inStream = conn.getInputStream();
      if (LOG.isDebugEnabled()) {
        LOG.debug("open file: " + conn.getURL());
      }
      if (cl != null) {
        long streamLength = Long.parseLong(cl);
        fileLength = pos + streamLength;
        // Java has a bug with >2GB request streams.  It won't bounds check
        // the reads so the transfer blocks until the server times out
        inStream = new BoundedInputStream(inStream, streamLength);
      } else {
        fileLength = getHdfsFileStatus(path).getLen();
      }
      // Wrapping in BufferedInputStream because it is more performant than
      // BoundedInputStream by itself.
      runnerState = RunnerState.OPEN;
      return new BufferedInputStream(inStream, bufferSize);
    }

    // Close both the InputStream and the connection.
    @VisibleForTesting
    void closeInputStream(RunnerState rs) throws IOException {
      if (in != null) {
        IOUtils.close(cachedConnection);
        in = null;
      }
      cachedConnection = null;
      runnerState = rs;
    }

    /* Getters and Setters */

    @VisibleForTesting
    protected InputStream getInputStream() {
      return in;
    }

    @VisibleForTesting
    protected void setInputStream(InputStream inStream) {
      in = inStream;
    }

    Path getPath() {
      return path;
    }

    int getBufferSize() {
      return bufferSize;
    }

    long getFileLength() {
      return fileLength;
    }

    void setFileLength(long len) {
      fileLength = len;
    }

    long getPos() {
      return pos;
    }
  }
}
