/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.azurebfs.utils.NamespaceUtil;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.Permissions;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.RENAME_DESTINATION_PARENT_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * AbfsClient.
 */
public class AbfsClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final String HUNDRED_CONTINUE_USER_AGENT = SINGLE_WHITE_SPACE + HUNDRED_CONTINUE + SEMICOLON;

  private final URL baseUrl;
  private final SharedKeyCredentials sharedKeyCredentials;
  private ApiVersion xMsVersion = ApiVersion.getCurrentVersion();
  private final ExponentialRetryPolicy exponentialRetryPolicy;
  private final StaticRetryPolicy staticRetryPolicy;
  private final String filesystem;
  private final AbfsConfiguration abfsConfiguration;
  private final String userAgent;
  private final AbfsPerfTracker abfsPerfTracker;
  private String clientProvidedEncryptionKey = null;
  private String clientProvidedEncryptionKeySHA = null;

  private final String accountName;
  private final AuthType authType;
  private AccessTokenProvider tokenProvider;
  private SASTokenProvider sasTokenProvider;
  private final AbfsCounters abfsCounters;
  private final Timer timer;
  private final String abfsMetricUrl;
  private boolean isMetricCollectionEnabled = false;
  private final MetricFormat metricFormat;
  private final AtomicBoolean isMetricCollectionStopped;
  private final int metricAnalysisPeriod;
  private final int metricIdlePeriod;
  private EncryptionContextProvider encryptionContextProvider = null;
  private EncryptionType encryptionType = EncryptionType.NONE;
  private final AbfsThrottlingIntercept intercept;

  private final ListeningScheduledExecutorService executorService;
  private Boolean isNamespaceEnabled;

  private boolean renameResilience;
  private TimerTask runningTimerTask;
  private boolean isSendMetricCall;
  private SharedKeyCredentials metricSharedkeyCredentials = null;

  /**
   * logging the rename failure if metadata is in an incomplete state.
   */
  private static final LogExactlyOnce ABFS_METADATA_INCOMPLETE_RENAME_FAILURE = new LogExactlyOnce(LOG);

  private AbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.baseUrl = baseUrl;
    this.sharedKeyCredentials = sharedKeyCredentials;
    String baseUrlString = baseUrl.toString();
    this.filesystem = baseUrlString.substring(baseUrlString.lastIndexOf(FORWARD_SLASH) + 1);
    this.abfsConfiguration = abfsConfiguration;
    this.exponentialRetryPolicy = abfsClientContext.getExponentialRetryPolicy();
    this.staticRetryPolicy = abfsClientContext.getStaticRetryPolicy();
    this.accountName = abfsConfiguration.getAccountName().substring(0, abfsConfiguration.getAccountName().indexOf(AbfsHttpConstants.DOT));
    this.authType = abfsConfiguration.getAuthType(accountName);
    this.intercept = AbfsThrottlingInterceptFactory.getInstance(accountName, abfsConfiguration);
    this.renameResilience = abfsConfiguration.getRenameResilience();

    if (encryptionContextProvider != null) {
      this.encryptionContextProvider = encryptionContextProvider;
      xMsVersion = ApiVersion.APR_10_2021; // will be default once server change deployed
      encryptionType = EncryptionType.ENCRYPTION_CONTEXT;
    } else if (abfsConfiguration.getEncodedClientProvidedEncryptionKey() != null) {
      clientProvidedEncryptionKey =
          abfsConfiguration.getEncodedClientProvidedEncryptionKey();
      this.clientProvidedEncryptionKeySHA =
          abfsConfiguration.getEncodedClientProvidedEncryptionKeySHA();
      encryptionType = EncryptionType.GLOBAL_KEY;
    }

    String sslProviderName = null;

    if (this.baseUrl.toString().startsWith(HTTPS_SCHEME)) {
      try {
        LOG.trace("Initializing DelegatingSSLSocketFactory with {} SSL "
                + "Channel Mode", this.abfsConfiguration.getPreferredSSLFactoryOption());
        DelegatingSSLSocketFactory.initializeDefaultFactory(this.abfsConfiguration.getPreferredSSLFactoryOption());
        sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory().getProviderName();
      } catch (IOException e) {
        // Suppress exception, failure to init DelegatingSSLSocketFactory would have only performance impact.
        LOG.trace("NonCritFailure: DelegatingSSLSocketFactory Init failed : "
            + "{}", e.getMessage());
      }
    }

    this.userAgent = initializeUserAgent(abfsConfiguration, sslProviderName);
    this.abfsPerfTracker = abfsClientContext.getAbfsPerfTracker();
    this.abfsCounters = abfsClientContext.getAbfsCounters();

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("AbfsClient Lease Ops").setDaemon(true).build();
    this.executorService = MoreExecutors.listeningDecorator(
        HadoopExecutors.newScheduledThreadPool(this.abfsConfiguration.getNumLeaseThreads(), tf));
    this.metricFormat = abfsConfiguration.getMetricFormat();
    this.isMetricCollectionStopped = new AtomicBoolean(false);
    this.metricAnalysisPeriod = abfsConfiguration.getMetricAnalysisTimeout();
    this.metricIdlePeriod = abfsConfiguration.getMetricIdleTimeout();
    if (!metricFormat.toString().equals(EMPTY_STRING)) {
      String metricAccountName = abfsConfiguration.getMetricAccount();
      String metricAccountKey = abfsConfiguration.getMetricAccountKey();
      if (!metricAccountName.equals(EMPTY_STRING) && !metricAccountKey.equals(EMPTY_STRING)) {
        isMetricCollectionEnabled = true;
        abfsCounters.initializeMetrics(metricFormat);
        int dotIndex = metricAccountName.indexOf(AbfsHttpConstants.DOT);
        if (dotIndex <= 0) {
          throw new InvalidUriException(
              metricAccountName + " - account name is not fully qualified.");
        }
        try {
          metricSharedkeyCredentials = new SharedKeyCredentials(
              metricAccountName.substring(0, dotIndex),
              metricAccountKey);
        } catch (IllegalArgumentException e) {
          throw new IOException(
              "Exception while initializing metric credentials " + e);
        }
      }
    }
    this.timer = new Timer(
        "abfs-timer-client", true);
    if (isMetricCollectionEnabled) {
      timer.schedule(new TimerTaskImpl(),
          metricIdlePeriod,
          metricIdlePeriod);
    }
    this.abfsMetricUrl = abfsConfiguration.getMetricUri();
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final AccessTokenProvider tokenProvider,
                    final EncryptionContextProvider encryptionContextProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration,
        encryptionContextProvider, abfsClientContext);
    this.tokenProvider = tokenProvider;
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration,
        encryptionContextProvider, abfsClientContext);
    this.sasTokenProvider = sasTokenProvider;
  }

  @Override
  public void close() throws IOException {
    if (runningTimerTask != null) {
      runningTimerTask.cancel();
      timer.purge();
    }
    if (tokenProvider instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG,
          (Closeable) tokenProvider);
    }
    HadoopExecutors.shutdown(executorService, LOG, 0, TimeUnit.SECONDS);
  }

  public String getFileSystem() {
    return filesystem;
  }

  protected AbfsPerfTracker getAbfsPerfTracker() {
    return abfsPerfTracker;
  }

  ExponentialRetryPolicy getExponentialRetryPolicy() {
    return exponentialRetryPolicy;
  }

  StaticRetryPolicy getStaticRetryPolicy() {
    return staticRetryPolicy;
  }

  /**
   * Returns the retry policy to be used for Abfs Rest Operation Failure.
   * @param failureReason helps to decide which type of retryPolicy to be used.
   * @return retry policy to be used.
   */
  public AbfsRetryPolicy getRetryPolicy(final String failureReason) {
    return CONNECTION_TIMEOUT_ABBREVIATION.equals(failureReason)
        && getAbfsConfiguration().getStaticRetryForConnectionTimeoutEnabled()
        ? getStaticRetryPolicy()
        : getExponentialRetryPolicy();
  }

  SharedKeyCredentials getSharedKeyCredentials() {
    return sharedKeyCredentials;
  }

  SharedKeyCredentials getMetricSharedkeyCredentials() {
    return metricSharedkeyCredentials;
  }

  public void setEncryptionType(EncryptionType encryptionType) {
    this.encryptionType = encryptionType;
  }

  public EncryptionType getEncryptionType() {
    return encryptionType;
  }

  AbfsThrottlingIntercept getIntercept() {
    return intercept;
  }

  /**
   * Create request headers for Rest Operation using the current API version.
   * @return default request headers
   */
  @VisibleForTesting
  protected List<AbfsHttpHeader> createDefaultHeaders() {
    return createDefaultHeaders(this.xMsVersion);
  }

  /**
   * Create request headers for Rest Operation using the specified API version.
   * @param xMsVersion
   * @return default request headers
   */
  private List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion) {
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion.toString()));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
        + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT_CHARSET,
        UTF_8));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, EMPTY_STRING));
    requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgent));
    return requestHeaders;
  }

  /**
   * This method adds following headers:
   * <ol>
   *   <li>X_MS_ENCRYPTION_KEY</li>
   *   <li>X_MS_ENCRYPTION_KEY_SHA256</li>
   *   <li>X_MS_ENCRYPTION_ALGORITHM</li>
   * </ol>
   * Above headers have to be added in following operations:
   * <ol>
   *   <li>createPath</li>
   *   <li>append</li>
   *   <li>flush</li>
   *   <li>setPathProperties</li>
   *   <li>getPathStatus for fs.setXAttr and fs.getXAttr</li>
   *   <li>read</li>
   * </ol>
   */
  private void addEncryptionKeyRequestHeaders(String path,
      List<AbfsHttpHeader> requestHeaders, boolean isCreateFileRequest,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      return;
    }
    String encodedKey, encodedKeySHA256;
    switch (encryptionType) {
    case GLOBAL_KEY:
      encodedKey = clientProvidedEncryptionKey;
      encodedKeySHA256 = clientProvidedEncryptionKeySHA;
      break;

    case ENCRYPTION_CONTEXT:
      if (isCreateFileRequest) {
        // get new context for create file request
        requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_CONTEXT,
            contextEncryptionAdapter.getEncodedContext()));
      }
      // else use cached encryption keys from input/output streams
      encodedKey = contextEncryptionAdapter.getEncodedKey();
      encodedKeySHA256 = contextEncryptionAdapter.getEncodedKeySHA();
      break;

    default: return; // no client-provided encryption keys
    }

    requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_KEY, encodedKey));
    requestHeaders.add(
        new AbfsHttpHeader(X_MS_ENCRYPTION_KEY_SHA256, encodedKeySHA256));
    requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_ALGORITHM));
  }

  AbfsUriQueryBuilder createDefaultUriQueryBuilder() {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_TIMEOUT, DEFAULT_TIMEOUT);
    return abfsUriQueryBuilder;
  }

  public AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CreateFileSystem,
        HTTP_METHOD_PUT, url, requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setFilesystemProperties(final String properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to work around the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES,
            properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.SetFileSystemProperties,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation listPath(final String relativePath, final boolean recursive, final int listMaxResults,
                                    final String continuation, TracingContext tracingContext)
      throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DIRECTORY, getDirectoryQueryParameter(relativePath));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULTS, String.valueOf(listMaxResults));
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(relativePath, SASTokenProvider.LIST_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.ListPaths,
            HTTP_METHOD_GET,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getFilesystemProperties(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.GetFileSystemProperties,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.DeleteFileSystem,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Method for calling createPath API to the backend. Method can be called from:
   * <ol>
   *   <li>create new file</li>
   *   <li>overwrite file</li>
   *   <li>create new directory</li>
   * </ol>
   *
   * @param path: path of the file / directory to be created / overwritten.
   * @param isFile: defines if file or directory has to be created / overwritten.
   * @param overwrite: defines if the file / directory to be overwritten.
   * @param permissions: contains permission and umask
   * @param isAppendBlob: defines if directory in the path is enabled for appendBlob
   * @param eTag: required in case of overwrite of file / directory. Path would be
   * overwritten only if the provided eTag is equal to the one present in backend for
   * the path.
   * @param contextEncryptionAdapter: object that contains the encryptionContext and
   * encryptionKey created from the developer provided implementation of
   * {@link org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider}
   * @param tracingContext: Object of {@link org.apache.hadoop.fs.azurebfs.utils.TracingContext}
   * correlating to the current fs.create() request.
   * @return object of {@link AbfsRestOperation} which contain all the information
   * about the communication with the server. The information is in
   * {@link AbfsRestOperation#getResult()}
   * @throws AzureBlobFileSystemException throws back the exception it receives from the
   * {@link AbfsRestOperation#execute(TracingContext)} method call.
   */
  public AbfsRestOperation createPath(final String path,
      final boolean isFile,
      final boolean overwrite,
      final Permissions permissions,
      final boolean isAppendBlob,
      final String eTag,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (isFile) {
      addEncryptionKeyRequestHeaders(path, requestHeaders, true,
          contextEncryptionAdapter, tracingContext);
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }

    if (permissions.hasPermission()) {
      requestHeaders.add(
          new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS,
              permissions.getPermission()));
    }

    if (permissions.hasUmask()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_UMASK,
          permissions.getUmask()));
    }

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, isFile ? FILE : DIRECTORY);
    if (isAppendBlob) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOBTYPE, APPEND_BLOB_TYPE);
    }

    String operation = isFile
        ? SASTokenProvider.CREATE_FILE_OPERATION
        : SASTokenProvider.CREATE_DIRECTORY_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.CreatePath,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        String existingResource =
            op.getResult().getResponseHeader(X_MS_EXISTING_RESOURCE_TYPE);
        if (existingResource != null && existingResource.equals(DIRECTORY)) {
          return op; //don't throw ex on mkdirs for existing directory
        }
      }
      throw ex;
    }
    return op;
  }

  public AbfsRestOperation acquireLease(final String path, int duration, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation renewLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RENEW_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation releaseLease(final String path,
      final String leaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD, DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Rename a file or directory.
   * If a source etag is passed in, the operation will attempt to recover
   * from a missing source file by probing the destination for
   * existence and comparing etags.
   * The second value in the result will be true to indicate that this
   * took place.
   * As rename recovery is only attempted if the source etag is non-empty,
   * in normal rename operations rename recovery will never happen.
   *
   * @param source                    path to source file
   * @param destination               destination of rename.
   * @param continuation              continuation.
   * @param tracingContext            trace context
   * @param sourceEtag                etag of source file. may be null or empty
   * @param isMetadataIncompleteState was there a rename failure due to
   *                                  incomplete metadata state?
   * @param isNamespaceEnabled        whether namespace enabled account or not
   * @return AbfsClientRenameResult result of rename operation indicating the
   * AbfsRest operation, rename recovery and incomplete metadata state failure.
   * @throws AzureBlobFileSystemException failure, excluding any recovery from overload failures.
   */
  public AbfsClientRenameResult renamePath(
          final String source,
          final String destination,
          final String continuation,
          final TracingContext tracingContext,
          String sourceEtag,
          boolean isMetadataIncompleteState,
          boolean isNamespaceEnabled)
      throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final boolean hasEtag = !isEmpty(sourceEtag);

    boolean shouldAttemptRecovery = renameResilience && isNamespaceEnabled;
    if (!hasEtag && shouldAttemptRecovery) {
      // in case eTag is already not supplied to the API
      // and rename resilience is expected and it is an HNS enabled account
      // fetch the source etag to be used later in recovery
      try {
        final AbfsRestOperation srcStatusOp = getPathStatus(source,
                false, tracingContext, null);
        if (srcStatusOp.hasResult()) {
          final AbfsHttpOperation result = srcStatusOp.getResult();
          sourceEtag = extractEtagHeader(result);
          // and update the directory status.
          boolean isDir = checkIsDir(result);
          shouldAttemptRecovery = !isDir;
          LOG.debug("Retrieved etag of source for rename recovery: {}; isDir={}", sourceEtag, isDir);
        }
      } catch (AbfsRestOperationException e) {
        throw new AbfsRestOperationException(e.getStatusCode(), SOURCE_PATH_NOT_FOUND.getErrorCode(),
                e.getMessage(), e);
      }

     }

    String encodedRenameSource = urlEncode(FORWARD_SLASH + this.getFileSystem() + source);
    if (authType == AuthType.SAS) {
      final AbfsUriQueryBuilder srcQueryBuilder = new AbfsUriQueryBuilder();
      appendSASTokenToQuery(source, SASTokenProvider.RENAME_SOURCE_OPERATION, srcQueryBuilder);
      encodedRenameSource += srcQueryBuilder.toString();
    }

    LOG.trace("Rename source queryparam added {}", encodedRenameSource);
    requestHeaders.add(new AbfsHttpHeader(X_MS_RENAME_SOURCE, encodedRenameSource));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    appendSASTokenToQuery(destination, SASTokenProvider.RENAME_DESTINATION_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(destination, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = createRenameRestOperation(url, requestHeaders);
    try {
      incrementAbfsRenamePath();
      op.execute(tracingContext);
      // AbfsClientResult contains the AbfsOperation, If recovery happened or
      // not, and the incompleteMetaDataState is true or false.
      // If we successfully rename a path and isMetadataIncompleteState was
      // true, then rename was recovered, else it didn't, this is why
      // isMetadataIncompleteState is used for renameRecovery(as the 2nd param).
      return new AbfsClientRenameResult(op, isMetadataIncompleteState, isMetadataIncompleteState);
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }

      // ref: HADOOP-18242. Rename failure occurring due to a rare case of
      // tracking metadata being in incomplete state.
      if (op.getResult().getStorageErrorCode()
              .equals(RENAME_DESTINATION_PARENT_PATH_NOT_FOUND.getErrorCode())
              && !isMetadataIncompleteState) {
        //Logging
        ABFS_METADATA_INCOMPLETE_RENAME_FAILURE
                .info("Rename Failure attempting to resolve tracking metadata state and retrying.");
        // rename recovery should be attempted in this case also
        shouldAttemptRecovery = true;
        isMetadataIncompleteState = true;
        String sourceEtagAfterFailure = sourceEtag;
        if (isEmpty(sourceEtagAfterFailure)) {
          // Doing a HEAD call resolves the incomplete metadata state and
          // then we can retry the rename operation.
          AbfsRestOperation sourceStatusOp = getPathStatus(source, false,
              tracingContext, null);
          isMetadataIncompleteState = true;
          // Extract the sourceEtag, using the status Op, and set it
          // for future rename recovery.
          AbfsHttpOperation sourceStatusResult = sourceStatusOp.getResult();
          sourceEtagAfterFailure = extractEtagHeader(sourceStatusResult);
        }
        renamePath(source, destination, continuation, tracingContext,
                sourceEtagAfterFailure, isMetadataIncompleteState, isNamespaceEnabled);
      }
      // if we get out of the condition without a successful rename, then
      // it isn't metadata incomplete state issue.
      isMetadataIncompleteState = false;

      // setting default rename recovery success to false
      boolean etagCheckSucceeded = false;
      if (shouldAttemptRecovery) {
        etagCheckSucceeded = renameIdempotencyCheckOp(
                source,
                sourceEtag, op, destination, tracingContext);
      }
      if (!etagCheckSucceeded) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      }
      return new AbfsClientRenameResult(op, true, isMetadataIncompleteState);
    }
  }

  private boolean checkIsDir(AbfsHttpOperation result) {
    String resourceType = result.getResponseHeader(
            HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
    return resourceType != null
            && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

  @VisibleForTesting
  AbfsRestOperation createRenameRestOperation(URL url, List<AbfsHttpHeader> requestHeaders) {
    AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.RenamePath,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    return op;
  }

  private void incrementAbfsRenamePath() {
    abfsCounters.incrementCounter(RENAME_PATH_ATTEMPTS, 1);
  }

  /**
   * Check if the rename request failure is post a retry and if earlier rename
   * request might have succeeded at back-end.
   *
   * If a source etag was passed in, and the error was 404, get the
   * etag of any file at the destination.
   * If it matches the source etag, then the rename is considered
   * a success.
   * Exceptions raised in the probe of the destination are swallowed,
   * so that they do not interfere with the original rename failures.
   * @param source source path
   * @param op Rename request REST operation response with non-null HTTP response
   * @param destination rename destination path
   * @param sourceEtag etag of source file. may be null or empty
   * @param tracingContext Tracks identifiers for request header
   * @return true if the file was successfully copied
   */
  public boolean renameIdempotencyCheckOp(
      final String source,
      final String sourceEtag,
      final AbfsRestOperation op,
      final String destination,
      TracingContext tracingContext) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");

    // removing isDir from debug logs as it can be misleading
    LOG.debug("rename({}, {}) failure {}; retry={} etag {}",
              source, destination, op.getResult().getStatusCode(), op.isARetriedRequest(), sourceEtag);
    if (!(op.isARetriedRequest()
            && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND))) {
      // only attempt recovery if the failure was a 404 on a retried rename request.
      return false;
    }

    if (isNotEmpty(sourceEtag)) {
      // Server has returned HTTP 404, we have an etag, so see
      // if the rename has actually taken place,
      LOG.info("rename {} to {} failed, checking etag of destination",
              source, destination);
      try {
        final AbfsRestOperation destStatusOp = getPathStatus(destination,
            false, tracingContext, null);
        final AbfsHttpOperation result = destStatusOp.getResult();

        final boolean recovered = result.getStatusCode() == HttpURLConnection.HTTP_OK
                && sourceEtag.equals(extractEtagHeader(result));
        LOG.info("File rename has taken place: recovery {}",
                recovered ? "succeeded" : "failed");
        return recovered;

      } catch (AzureBlobFileSystemException ex) {
        // GetFileStatus on the destination failed, the rename did not take place
        // or some other failure. log and swallow.
        LOG.debug("Failed to get status of path {}", destination, ex);
      }
    } else {
      LOG.debug("No source etag; unable to probe for the operation's success");
    }
      return false;
  }

  @VisibleForTesting
  boolean isSourceDestEtagEqual(String sourceEtag, AbfsHttpOperation result) {
    return sourceEtag.equals(extractEtagHeader(result));
  }

  public AbfsRestOperation append(final String path, final byte[] buffer,
      AppendRequestParameters reqParams, final String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(reqParams.getPosition()));

    if ((reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_MODE) || (
        reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE)) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_FLUSH, TRUE);
      if (reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) {
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, TRUE);
      }
    }

    // Check if the retry is with "Expect: 100-continue" header being present in the previous request.
    if (reqParams.isRetryDueToExpect()) {
      String userAgentRetry = userAgent;
      // Remove the specific marker related to "Expect: 100-continue" from the User-Agent string.
      userAgentRetry = userAgentRetry.replace(HUNDRED_CONTINUE_USER_AGENT, EMPTY_STRING);
      requestHeaders.removeIf(header -> header.getName().equalsIgnoreCase(USER_AGENT));
      requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgentRetry));
    }

    // Add MD5 Hash of request content as request header if feature is enabled
    if (isChecksumValidationEnabled()) {
      addCheckSumHeaderForWrite(requestHeaders, reqParams, buffer);
    }

    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.Append,
            HTTP_METHOD_PUT,
            url,
            requestHeaders,
            buffer,
            reqParams.getoffset(),
            reqParams.getLength(),
            sasTokenForReuse);
    try {
      op.execute(tracingContext);
    } catch (AbfsRestOperationException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = e.getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        reqParams.setRetryDueToExpect(true);
        return this.append(path, buffer, reqParams, cachedSasToken,
            contextEncryptionAdapter, tracingContext);
      }
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }

      if (isMd5ChecksumError(e)) {
        throw new AbfsInvalidChecksumException(e);
      }

      if (reqParams.isAppendBlob()
          && appendSuccessCheckOp(op, path,
          (reqParams.getPosition() + reqParams.getLength()), tracingContext)) {
        final AbfsRestOperation successOp = getAbfsRestOperation(
                AbfsRestOperationType.Append,
                HTTP_METHOD_PUT,
                url,
                requestHeaders,
                buffer,
                reqParams.getoffset(),
                reqParams.getLength(),
                sasTokenForReuse);
        successOp.hardSetResult(HttpURLConnection.HTTP_OK);
        return successOp;
      }
      throw e;
    }

    catch (AzureBlobFileSystemException e) {
      // Any server side issue will be returned as AbfsRestOperationException and will be handled above.
      LOG.debug("Append request failed with non server issues for path: {}, offset: {}, position: {}",
          path, reqParams.getoffset(), reqParams.getPosition());
      throw e;
    }

    return op;
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  private boolean checkUserError(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
        && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR);
  }

  /**
   * To check if the failure exception returned by server is due to MD5 Mismatch
   * @param e Exception returned by AbfsRestOperation
   * @return boolean whether exception is due to MD5Mismatch or not
   */
  private boolean isMd5ChecksumError(final AbfsRestOperationException e) {
    AzureServiceErrorCode storageErrorCode = e.getErrorCode();
    return storageErrorCode == AzureServiceErrorCode.MD5_MISMATCH;
  }

  // For AppendBlob its possible that the append succeeded in the backend but the request failed.
  // However a retry would fail with an InvalidQueryParameterValue
  // (as the current offset would be unacceptable).
  // Hence, we pass/succeed the appendblob append call
  // in case we are doing a retry after checking the length of the file
  public boolean appendSuccessCheckOp(AbfsRestOperation op, final String path,
                                       final long length, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_BAD_REQUEST)) {
      final AbfsRestOperation destStatusOp = getPathStatus(path, false, tracingContext, null);
      if (destStatusOp.getResult().getStatusCode() == HttpURLConnection.HTTP_OK) {
        String fileLength = destStatusOp.getResult().getResponseHeader(
            HttpHeaderConfigurations.CONTENT_LENGTH);
        if (length <= Long.parseLong(fileLength)) {
          LOG.debug("Returning success response from append blob idempotency code");
          return true;
        }
      }
    }
    return false;
  }

  public AbfsRestOperation flush(final String path, final long position,
      boolean retainUncommittedData, boolean isClose,
      final String cachedSasToken, final String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, FLUSH_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RETAIN_UNCOMMITTED_DATA, String.valueOf(retainUncommittedData));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.Flush,
            HTTP_METHOD_PUT,
            url,
            requestHeaders, sasTokenForReuse);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setPathProperties(final String path, final String properties,
                                             final TracingContext tracingContext, final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.SetPathProperties,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getPathStatus(final String path,
      final boolean includeProperties, final TracingContext tracingContext,
      final ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.GET_PROPERTIES_OPERATION;
    if (!includeProperties) {
      // The default action (operation) is implicitly to get properties and this action requires read permission
      // because it reads user defined properties.  If the action is getStatus or getAclStatus, then
      // only traversal (execute) permission is required.
      abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_STATUS);
      operation = SASTokenProvider.GET_STATUS_OPERATION;
    } else {
      addEncryptionKeyRequestHeaders(path, requestHeaders, false,
          contextEncryptionAdapter,
          tracingContext);
    }
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.GetPathStatus,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation read(final String path,
      final long position,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String eTag,
      String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addEncryptionKeyRequestHeaders(path, requestHeaders, false,
        contextEncryptionAdapter, tracingContext);
    AbfsHttpHeader rangeHeader = new AbfsHttpHeader(RANGE,
        String.format("bytes=%d-%d", position, position + bufferLength - 1));
    requestHeaders.add(rangeHeader);
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    // Add request header to fetch MD5 Hash of data returned by server.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_RANGE_GET_CONTENT_MD5, TRUE));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.ReadFile,
            HTTP_METHOD_GET,
            url,
            requestHeaders,
            buffer,
            bufferOffset,
            bufferLength, sasTokenForReuse);
    op.execute(tracingContext);

    // Verify the MD5 hash returned by server holds valid on the data received.
    if (isChecksumValidationEnabled(requestHeaders, rangeHeader, bufferLength)) {
      verifyCheckSumForRead(buffer, op.getResult(), bufferOffset);
    }

    return op;
  }

  public AbfsRestOperation deletePath(final String path, final boolean recursive,
                                      final String continuation,
                                      TracingContext tracingContext,
                                      final boolean isNamespaceEnabled)
          throws AzureBlobFileSystemException {
    /*
     * If Pagination is enabled and current API version is old,
     * use the minimum required version for pagination.
     * If Pagination is enabled and current API version is later than minimum required
     * version for pagination, use current version only as azure service is backward compatible.
     * If pagination is disabled, use the current API version only.
     */
    final List<AbfsHttpHeader> requestHeaders = (isPaginatedDelete(recursive,
        isNamespaceEnabled) && xMsVersion.compareTo(ApiVersion.AUG_03_2023) < 0)
        ? createDefaultHeaders(ApiVersion.AUG_03_2023)
        : createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    if (isPaginatedDelete(recursive, isNamespaceEnabled)) {
      // Add paginated query parameter
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_PAGINATED, TRUE);
    }

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    String operation = recursive ? SASTokenProvider.DELETE_RECURSIVE_OPERATION : SASTokenProvider.DELETE_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.DeletePath,
            this,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    try {
    op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }
      final AbfsRestOperation idempotencyOp = deleteIdempotencyCheckOp(op);
      if (idempotencyOp.getResult().getStatusCode()
          == op.getResult().getStatusCode()) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      } else {
        return idempotencyOp;
      }
    }

    return op;
  }

  /**
   * Check if the delete request failure is post a retry and if delete failure
   * qualifies to be a success response assuming idempotency.
   *
   * There are below scenarios where delete could be incorrectly deducted as
   * success post request retry:
   * 1. Target was originally not existing and initial delete request had to be
   * re-tried.
   * 2. Parallel delete issued from any other store interface rather than
   * delete issued from this filesystem instance.
   * These are few corner cases and usually returning a success at this stage
   * should help the job to continue.
   * @param op Delete request REST operation response with non-null HTTP response
   * @return REST operation response post idempotency check
   */
  public AbfsRestOperation deleteIdempotencyCheckOp(final AbfsRestOperation op) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND)
        && DEFAULT_DELETE_CONSIDERED_IDEMPOTENT) {
      // Server has returned HTTP 404, which means path no longer
      // exists. Assuming delete result to be idempotent, return success.
      final AbfsRestOperation successOp = getAbfsRestOperation(
          AbfsRestOperationType.DeletePath,
          HTTP_METHOD_DELETE,
          op.getUrl(),
          op.getRequestHeaders());
      successOp.hardSetResult(HttpURLConnection.HTTP_OK);
      LOG.debug("Returning success response from delete idempotency logic");
      return successOp;
    }

    return op;
  }

  public AbfsRestOperation setOwner(final String path, final String owner, final String group,
                                    TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    if (owner != null && !owner.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_OWNER, owner));
    }
    if (group != null && !group.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_GROUP, group));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_OWNER_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setPermission(final String path, final String permission,
                                         TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PERMISSION_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString,
                                  TracingContext tracingContext) throws AzureBlobFileSystemException {
    return setAcl(path, aclSpecString, AbfsHttpConstants.EMPTY_STRING, tracingContext);
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString, final String eTag,
                                  TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ACL, aclSpecString));

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getAclStatus(final String path, TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    return getAclStatus(path, abfsConfiguration.isUpnUsed(), tracingContext);
  }

  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
                                        TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(useUPN));
    appendSASTokenToQuery(path, SASTokenProvider.GET_ACL_OPERATION, abfsUriQueryBuilder);

    final URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Talks to the server to check whether the permission specified in
   * the rwx parameter is present for the path specified in the path parameter.
   *
   * @param path  Path for which access check needs to be performed
   * @param rwx   The permission to be checked on the path
   * @param tracingContext Tracks identifiers for request header
   * @return      The {@link AbfsRestOperation} object for the operation
   * @throws AzureBlobFileSystemException in case of bad requests
   */
  public AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, CHECK_ACCESS);
    abfsUriQueryBuilder.addQuery(QUERY_FS_ACTION, rwx);
    appendSASTokenToQuery(path, SASTokenProvider.CHECK_ACCESS_OPERATION, abfsUriQueryBuilder);
    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    AbfsRestOperation op = getAbfsRestOperation(
        AbfsRestOperationType.CheckAccess,
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        createDefaultHeaders());
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get the directory query parameter used by the List Paths REST API and used
   * as the path in the continuation token.  If the input path is null or the
   * root path "/", empty string is returned. If the input path begins with '/',
   * the return value is the substring beginning at offset 1.  Otherwise, the
   * input path is returned.
   * @param path the path to be listed.
   * @return the value of the directory query parameter
   */
  public static String getDirectoryQueryParameter(final String path) {
    String directory = path;
    if (Strings.isNullOrEmpty(directory)) {
      directory = AbfsHttpConstants.EMPTY_STRING;
    } else if (directory.charAt(0) == '/') {
      directory = directory.substring(1);
    }
    return directory;
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path
   * @param operation
   * @param queryBuilder
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  private String appendSASTokenToQuery(String path, String operation, AbfsUriQueryBuilder queryBuilder) throws SASTokenProviderException {
    return appendSASTokenToQuery(path, operation, queryBuilder, null);
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path
   * @param operation
   * @param queryBuilder
   * @param cachedSasToken - previously acquired SAS token to be reused.
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  private String appendSASTokenToQuery(String path,
                                       String operation,
                                       AbfsUriQueryBuilder queryBuilder,
                                       String cachedSasToken)
      throws SASTokenProviderException {
    String sasToken = null;
    if (this.authType == AuthType.SAS) {
      try {
        LOG.trace("Fetch SAS token for {} on {}", operation, path);
        if (cachedSasToken == null) {
          sasToken = sasTokenProvider.getSASToken(this.accountName,
              this.filesystem, path, operation);
          if ((sasToken == null) || sasToken.isEmpty()) {
            throw new UnsupportedOperationException("SASToken received is empty or null");
          }
        } else {
          sasToken = cachedSasToken;
          LOG.trace("Using cached SAS token.");
        }
        // if SAS Token contains a prefix of ?, it should be removed
        if (sasToken.charAt(0) == '?') {
          sasToken = sasToken.substring(1);
        }
        queryBuilder.setSASToken(sasToken);
        LOG.trace("SAS token fetch complete for {} on {}", operation, path);
      } catch (Exception ex) {
        throw new SASTokenProviderException(String.format("Failed to acquire a SAS token for %s on %s due to %s",
            operation,
            path,
            ex.toString()));
      }
    }
    return sasToken;
  }

  @VisibleForTesting
  private URL createRequestUrl(final String query) throws AzureBlobFileSystemException {
    return createRequestUrl(EMPTY_STRING, query);
  }

  @VisibleForTesting
  protected URL createRequestUrl(final String path, final String query)
          throws AzureBlobFileSystemException {
    return createRequestUrl(baseUrl, path, query);
  }

  @VisibleForTesting
  protected URL createRequestUrl(final URL baseUrl, final String path, final String query)
          throws AzureBlobFileSystemException {
    String encodedPath = path;
    try {
      encodedPath = urlEncode(path);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Unexpected error.", ex);
      throw new InvalidUriException(path);
    }

    final StringBuilder sb = new StringBuilder();
    if (baseUrl == null) {
      throw new InvalidUriException("URL provided is null");
    }
    sb.append(baseUrl.toString());
    sb.append(encodedPath);
    sb.append(query);

    final URL url;
    try {
      url = new URL(sb.toString());
    } catch (MalformedURLException ex) {
      throw new InvalidUriException("URL is malformed" + sb.toString());
    }
    return url;
  }

  public static String urlEncode(final String value) throws AzureBlobFileSystemException {
    String encodedString;
    try {
      encodedString =  URLEncoder.encode(value, UTF_8)
          .replace(PLUS, PLUS_ENCODE)
          .replace(FORWARD_SLASH_ENCODE, FORWARD_SLASH);
    } catch (UnsupportedEncodingException ex) {
        throw new InvalidUriException(value);
    }

    return encodedString;
  }

  public synchronized String getAccessToken() throws IOException {
    if (tokenProvider != null) {
      return "Bearer " + tokenProvider.getToken().getAccessToken();
    } else {
      return null;
    }
  }

  private synchronized Boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (isNamespaceEnabled == null) {
      setIsNamespaceEnabled(NamespaceUtil.isNamespaceEnabled(this,
          tracingContext));
    }
    return isNamespaceEnabled;
  }

  protected Boolean getIsPaginatedDeleteEnabled() {
    return abfsConfiguration.isPaginatedDeleteEnabled();
  }

  private Boolean isPaginatedDelete(boolean isRecursiveDelete, boolean isNamespaceEnabled) {
    return getIsPaginatedDeleteEnabled() && isNamespaceEnabled && isRecursiveDelete;
  }

  public AuthType getAuthType() {
    return authType;
  }

  public EncryptionContextProvider getEncryptionContextProvider() {
    return encryptionContextProvider;
  }

  @VisibleForTesting
  String initializeUserAgent(final AbfsConfiguration abfsConfiguration,
      final String sslProviderName) {

    StringBuilder sb = new StringBuilder();

    sb.append(APN_VERSION);
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(CLIENT_VERSION);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append("(");

    sb.append(System.getProperty(JAVA_VENDOR)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append("JavaJRE");
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(JAVA_VERSION));
    sb.append(SEMICOLON);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append(System.getProperty(OS_NAME)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(OS_VERSION));
    sb.append(FORWARD_SLASH);
    sb.append(System.getProperty(OS_ARCH));
    sb.append(SEMICOLON);

    appendIfNotEmpty(sb, sslProviderName, true);
    appendIfNotEmpty(sb,
        ExtensionHelper.getUserAgentSuffix(tokenProvider, EMPTY_STRING), true);

    if (abfsConfiguration.isExpectHeaderEnabled()) {
      sb.append(SINGLE_WHITE_SPACE);
      sb.append(HUNDRED_CONTINUE);
      sb.append(SEMICOLON);
    }

    sb.append(SINGLE_WHITE_SPACE);
    sb.append(abfsConfiguration.getClusterName());
    sb.append(FORWARD_SLASH);
    sb.append(abfsConfiguration.getClusterType());

    sb.append(")");

    appendIfNotEmpty(sb, abfsConfiguration.getCustomUserAgentPrefix(), false);

    return String.format(Locale.ROOT, sb.toString());
  }

  private void appendIfNotEmpty(StringBuilder sb, String regEx,
      boolean shouldAppendSemiColon) {
    if (regEx == null || regEx.trim().isEmpty()) {
      return;
    }
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(regEx);
    if (shouldAppendSemiColon) {
      sb.append(SEMICOLON);
    }
  }

  /**
   * Add MD5 hash as request header to the append request.
   * @param requestHeaders to be updated with checksum header
   * @param reqParams for getting offset and length
   * @param buffer for getting input data for MD5 computation
   * @throws AbfsRestOperationException if Md5 computation fails
   */
  private void addCheckSumHeaderForWrite(List<AbfsHttpHeader> requestHeaders,
      final AppendRequestParameters reqParams, final byte[] buffer)
      throws AbfsRestOperationException {
    String md5Hash = computeMD5Hash(buffer, reqParams.getoffset(),
        reqParams.getLength());
    requestHeaders.add(new AbfsHttpHeader(CONTENT_MD5, md5Hash));
  }

  /**
   * To verify the checksum information received from server for the data read.
   * @param buffer stores the data received from server.
   * @param result HTTP Operation Result.
   * @param bufferOffset Position where data returned by server is saved in buffer.
   * @throws AbfsRestOperationException if Md5Mismatch.
   */
  private void verifyCheckSumForRead(final byte[] buffer,
      final AbfsHttpOperation result, final int bufferOffset)
      throws AbfsRestOperationException {
    // Number of bytes returned by server could be less than or equal to what
    // caller requests. In case it is less, extra bytes will be initialized to 0
    // Server returned MD5 Hash will be computed on what server returned.
    // We need to get exact data that server returned and compute its md5 hash
    // Computed hash should be equal to what server returned.
    int numberOfBytesRead = (int) result.getBytesReceived();
    if (numberOfBytesRead == 0) {
      return;
    }
    String md5HashComputed = computeMD5Hash(buffer, bufferOffset,
        numberOfBytesRead);
    String md5HashActual = result.getResponseHeader(CONTENT_MD5);
    if (!md5HashComputed.equals(md5HashActual)) {
      LOG.debug("Md5 Mismatch Error in Read Operation. Server returned Md5: {}, Client computed Md5: {}", md5HashActual, md5HashComputed);
      throw new AbfsInvalidChecksumException(result.getRequestId());
    }
  }

  /**
   * Conditions check for allowing checksum support for read operation.
   * Sending MD5 Hash in request headers. For more details see
   * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read">
   *     Path - Read Azure Storage Rest API</a>.
   * 1. Range header must be present as one of the request headers.
   * 2. buffer length must be less than or equal to 4 MB.
   * @param requestHeaders to be checked for range header.
   * @param rangeHeader must be present.
   * @param bufferLength must be less than or equal to 4 MB.
   * @return true if all conditions are met.
   */
  private boolean isChecksumValidationEnabled(List<AbfsHttpHeader> requestHeaders,
      final AbfsHttpHeader rangeHeader, final int bufferLength) {
    return getAbfsConfiguration().getIsChecksumValidationEnabled()
        && requestHeaders.contains(rangeHeader) && bufferLength <= 4 * ONE_MB;
  }

  /**
   * Conditions check for allowing checksum support for write operation.
   * Server will support this if client sends the MD5 Hash as a request header.
   * For azure stoage service documentation see
   * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">
   *     Path - Update Azure Rest API</a>.
   * @return true if checksum validation enabled.
   */
  private boolean isChecksumValidationEnabled() {
    return getAbfsConfiguration().getIsChecksumValidationEnabled();
  }

  /**
   * Compute MD5Hash of the given byte array starting from given offset up to given length.
   * @param data byte array from which data is fetched to compute MD5 Hash.
   * @param off offset in the array from where actual data starts.
   * @param len length of the data to be used to compute MD5Hash.
   * @return MD5 Hash of the data as String.
   * @throws AbfsRestOperationException if computation fails.
   */
  @VisibleForTesting
  public String computeMD5Hash(final byte[] data, final int off, final int len)
      throws AbfsRestOperationException {
    try {
      MessageDigest md5Digest = MessageDigest.getInstance(MD5);
      md5Digest.update(data, off, len);
      byte[] md5Bytes = md5Digest.digest();
      return Base64.getEncoder().encodeToString(md5Bytes);
    } catch (NoSuchAlgorithmException ex) {
      throw new AbfsDriverException(ex);
    }
  }

  @VisibleForTesting
  URL getBaseUrl() {
    return baseUrl;
  }

  @VisibleForTesting
  public SASTokenProvider getSasTokenProvider() {
    return this.sasTokenProvider;
  }

  @VisibleForTesting
  void setEncryptionContextProvider(EncryptionContextProvider provider) {
    encryptionContextProvider = provider;
  }

  @VisibleForTesting
  void setIsNamespaceEnabled(final Boolean isNamespaceEnabled) {
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  /**
   * Getter for abfsCounters from AbfsClient.
   * @return AbfsCounters instance.
   */
  public AbfsCounters getAbfsCounters() {
    return abfsCounters;
  }

  public ApiVersion getxMsVersion() {
    return xMsVersion;
  }

  /**
   * Getter for abfsConfiguration from AbfsClient.
   * @return AbfsConfiguration instance
   */
  protected AbfsConfiguration getAbfsConfiguration() {
    return abfsConfiguration;
  }

  public int getNumLeaseThreads() {
    return abfsConfiguration.getNumLeaseThreads();
  }

  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit timeUnit) {
    return executorService.schedule(callable, delay, timeUnit);
  }

  public ListenableFuture<?> submit(Runnable runnable) {
    return executorService.submit(runnable);
  }

  public <V> void addCallback(ListenableFuture<V> future, FutureCallback<V> callback) {
    Futures.addCallback(future, callback, executorService);
  }

  @VisibleForTesting
  protected AccessTokenProvider getTokenProvider() {
    return tokenProvider;
  }

  /**
   * Retrieves a TracingContext object configured for metric tracking.
   * This method creates a TracingContext object with the validated client correlation ID,
   * the host name of the local machine (or "UnknownHost" if unable to determine),
   * the file system operation type set to GET_ATTR, and additional configuration parameters
   * for metric tracking.
   * The TracingContext is intended for use in tracking metrics related to Azure Blob FileSystem (ABFS) operations.
   *
   * @return A TracingContext object configured for metric tracking.
   */
  private TracingContext getMetricTracingContext() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      hostName = "UnknownHost";
    }
    return new TracingContext(TracingContext.validateClientCorrelationID(
        abfsConfiguration.getClientCorrelationId()),
        hostName, FSOperationType.GET_ATTR, true,
        abfsConfiguration.getTracingHeaderFormat(),
        null, abfsCounters.toString());
  }

  /**
   * Synchronized method to suspend or resume timer.
   * @param timerFunctionality resume or suspend.
   * @param timerTask The timertask object.
   * @return true or false.
   */
  boolean timerOrchestrator(TimerFunctionality timerFunctionality, TimerTask timerTask) {
    switch (timerFunctionality) {
      case RESUME:
        if (isMetricCollectionStopped.get()) {
          synchronized (this) {
            if (isMetricCollectionStopped.get()) {
              resumeTimer();
            }
          }
        }
        break;
      case SUSPEND:
        long now = System.currentTimeMillis();
        long lastExecutionTime = abfsCounters.getLastExecutionTime().get();
        if (isMetricCollectionEnabled && (now - lastExecutionTime >= metricAnalysisPeriod)) {
          synchronized (this) {
            if (!isMetricCollectionStopped.get()) {
              timerTask.cancel();
              timer.purge();
              isMetricCollectionStopped.set(true);
              return true;
            }
          }
        }
        break;
      default:
        break;
    }
    return false;
  }

  private void resumeTimer() {
    isMetricCollectionStopped.set(false);
    timer.schedule(new TimerTaskImpl(),
        metricIdlePeriod,
        metricIdlePeriod);
  }

  /**
   * Initiates a metric call to the Azure Blob FileSystem (ABFS) for retrieving file system properties.
   * This method performs a HEAD request to the specified metric URL, using default headers and query parameters.
   *
   * @param tracingContext The tracing context to be used for capturing tracing information.
   * @throws IOException throws IOException.
   */
  public void getMetricCall(TracingContext tracingContext) throws IOException {
    this.isSendMetricCall = true;
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    final URL url = createRequestUrl(new URL(abfsMetricUrl), EMPTY_STRING, abfsUriQueryBuilder.toString());

    final AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.GetFileSystemProperties,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    try {
      op.execute(tracingContext);
    } finally {
      this.isSendMetricCall = false;
    }
  }

  public boolean isSendMetricCall() {
    return isSendMetricCall;
  }

  public boolean isMetricCollectionEnabled() {
    return isMetricCollectionEnabled;
  }

  class TimerTaskImpl extends TimerTask {
    TimerTaskImpl() {
      runningTimerTask = this;
    }
    @Override
    public void run() {
      try {
        if (timerOrchestrator(TimerFunctionality.SUSPEND, this)) {
            try {
              getMetricCall(getMetricTracingContext());
            } finally {
              abfsCounters.initializeMetrics(metricFormat);
            }
        }
      } catch (IOException e) {
      }
    }
  }

  /**
   * Creates an AbfsRestOperation with additional parameters for buffer and SAS token.
   *
   * @param operationType    The type of the operation.
   * @param httpMethod       The HTTP method of the operation.
   * @param url              The URL associated with the operation.
   * @param requestHeaders   The list of HTTP headers for the request.
   * @param buffer           The byte buffer containing data for the operation.
   * @param bufferOffset     The offset within the buffer where the data starts.
   * @param bufferLength     The length of the data within the buffer.
   * @param sasTokenForReuse The SAS token for reusing authentication.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String sasTokenForReuse) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders,
        buffer,
        bufferOffset,
        bufferLength,
        sasTokenForReuse);
  }

  /**
   * Creates an AbfsRestOperation with basic parameters and no buffer or SAS token.
   *
   * @param operationType   The type of the operation.
   * @param httpMethod      The HTTP method of the operation.
   * @param url             The URL associated with the operation.
   * @param requestHeaders  The list of HTTP headers for the request.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders
    );
  }

  /**
   * Creates an AbfsRestOperation with parameters including request headers and SAS token.
   *
   * @param operationType    The type of the operation.
   * @param httpMethod       The HTTP method of the operation.
   * @param url              The URL associated with the operation.
   * @param requestHeaders   The list of HTTP headers for the request.
   * @param sasTokenForReuse The SAS token for reusing authentication.
   * @return An AbfsRestOperation instance.
   */
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasTokenForReuse) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders, sasTokenForReuse);
  }
}
