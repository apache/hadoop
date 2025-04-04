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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.Permissions;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ApiVersion;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidChecksumException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformerInterface;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.extractEtagHeader;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLIENT_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILESYSTEM;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH_ENCODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VENDOR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JAVA_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.MD5;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_ARCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.OS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.PLUS_ENCODE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SEMICOLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.UTF_8;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_IDENTITY_TRANSFORM_CLASS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.ACCEPT_CHARSET;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_MD5;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.USER_AGENT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_KEY_SHA256;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_RESOURCE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;

/**
 * AbfsClient.
 */
public abstract class AbfsClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final String HUNDRED_CONTINUE_USER_AGENT = SINGLE_WHITE_SPACE + HUNDRED_CONTINUE + SEMICOLON;
  public static final String ABFS_CLIENT_TIMER_THREAD_NAME = "abfs-timer-client";

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
  private final IdentityTransformerInterface identityTransformer;

  private final String accountName;
  private final AuthType authType;
  private AccessTokenProvider tokenProvider;
  private SASTokenProvider sasTokenProvider;
  private final AbfsCounters abfsCounters;
  private Timer timer;
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

  private boolean renameResilience;
  private TimerTask runningTimerTask;
  private boolean isSendMetricCall;
  private SharedKeyCredentials metricSharedkeyCredentials = null;

  private KeepAliveCache keepAliveCache;

  private AbfsApacheHttpClient abfsApacheHttpClient;
  private static boolean isNamespaceEnabled = false;

  /**
   * logging the rename failure if metadata is in an incomplete state.
   */
  protected static final LogExactlyOnce ABFS_METADATA_INCOMPLETE_RENAME_FAILURE = new LogExactlyOnce(LOG);

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
      // Version update needed to support x-ms-encryption-context header
      // @link https://learn.microsoft.com/en-us/rest/api/storageservices/put-block?tabs=microsoft-entra-id}
      xMsVersion = ApiVersion.AUG_03_2023; // will be default once server change deployed
      encryptionType = EncryptionType.ENCRYPTION_CONTEXT;
    } else if (abfsConfiguration.getEncodedClientProvidedEncryptionKey() != null) {
      clientProvidedEncryptionKey =
          abfsConfiguration.getEncodedClientProvidedEncryptionKey();
      this.clientProvidedEncryptionKeySHA =
          abfsConfiguration.getEncodedClientProvidedEncryptionKeySHA();
      encryptionType = EncryptionType.GLOBAL_KEY;
    }

    // Version update needed to support x-ms-client-transaction-id header
    if (abfsConfiguration.getIsClientTransactionIdEnabled()) {
      xMsVersion = ApiVersion.NOV_04_2024;
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
    if (abfsConfiguration.getPreferredHttpOperationType()
        == HttpOperationType.APACHE_HTTP_CLIENT) {
      keepAliveCache = new KeepAliveCache(abfsConfiguration);

      abfsApacheHttpClient = new AbfsApacheHttpClient(
          DelegatingSSLSocketFactory.getDefaultFactory(),
          abfsConfiguration.getHttpReadTimeout(),
          keepAliveCache);
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
    if (StringUtils.isNotEmpty(metricFormat.toString())) {
      String metricAccountName = abfsConfiguration.getMetricAccount();
      String metricAccountKey = abfsConfiguration.getMetricAccountKey();
      if (StringUtils.isNotEmpty(metricAccountName) && StringUtils.isNotEmpty(metricAccountKey)) {
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
          throw new IOException("Exception while initializing metric credentials ", e);
        }
      }
    }
    if (isMetricCollectionEnabled) {
      this.timer = new Timer(
              ABFS_CLIENT_TIMER_THREAD_NAME, true);
      timer.schedule(new TimerTaskImpl(),
          metricIdlePeriod,
          metricIdlePeriod);
    }
    this.abfsMetricUrl = abfsConfiguration.getMetricUri();

    final Class<? extends IdentityTransformerInterface> identityTransformerClass =
        abfsConfiguration.getRawConfiguration().getClass(FS_AZURE_IDENTITY_TRANSFORM_CLASS, IdentityTransformer.class,
            IdentityTransformerInterface.class);
    try {
      this.identityTransformer = identityTransformerClass.getConstructor(
          Configuration.class).newInstance(abfsConfiguration.getRawConfiguration());
    } catch (IllegalAccessException | InstantiationException | IllegalArgumentException
             | InvocationTargetException | NoSuchMethodException e) {
      LOG.error("IdentityTransformer Init Falied", e);
      throw new IOException(e);
    }
    LOG.trace("IdentityTransformer init complete");
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
    if (isMetricCollectionEnabled && runningTimerTask != null) {
      runningTimerTask.cancel();
      timer.cancel();
    }
    if (keepAliveCache != null) {
      keepAliveCache.close();
    }
    if (abfsApacheHttpClient != null) {
      abfsApacheHttpClient.close();
    }
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
  protected abstract List<AbfsHttpHeader> createDefaultHeaders();

  /**
   * Create request headers for Rest Operation using the specified API version.
   * @param xMsVersion Azure services API version to be used.
   * @return default request headers
   */
  @VisibleForTesting
  public abstract List<AbfsHttpHeader> createDefaultHeaders(ApiVersion xMsVersion);

  /**
   * Create request headers common to both service endpoints.
   * @param xMsVersion azure services API version to be used.
   * @return common request headers
   */
  protected List<AbfsHttpHeader> createCommonHeaders(ApiVersion xMsVersion) {
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<>();
    requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion.toString()));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT_CHARSET, UTF_8));
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
   * @param path path of the file / directory to be created / overwritten.
   * @param requestHeaders list of headers to be added to the request.
   * @param isCreateFileRequest defines if file or directory has to be created / overwritten.
   * @param contextEncryptionAdapter object that contains the encryptionContext and
   * encryptionKey created from the developer provided implementation of {@link EncryptionContextProvider}
   * @param tracingContext to trace service calls.
   * @throws AzureBlobFileSystemException if namespace is not enabled.
   */
  protected void addEncryptionKeyRequestHeaders(String path,
      List<AbfsHttpHeader> requestHeaders, boolean isCreateFileRequest,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
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

  /**
   * Creates a AbfsUriQueryBuilder with default query parameter timeout.
   * @return default AbfsUriQueryBuilder.
   */
  protected AbfsUriQueryBuilder createDefaultUriQueryBuilder() {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_TIMEOUT, DEFAULT_TIMEOUT);
    return abfsUriQueryBuilder;
  }

  /**
   * Create a new filesystem using Azure REST API Service.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Sets user-defined metadata on filesystem.
   * @param properties list of metadata key-value pairs.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation setFilesystemProperties(Hashtable<String, String> properties,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * List paths and their properties in the current filesystem.
   * @param relativePath to return only blobs within this directory.
   * @param recursive to return all blobs in the path, including those in subdirectories.
   * @param listMaxResults maximum number of blobs to return.
   * @param continuation marker to specify the continuation token.
   * @param tracingContext for tracing the server calls.
   * @param uri to be used for the path conversion.
   * @return {@link ListResponseData}. containing listing response.
   * @throws AzureBlobFileSystemException if rest operation or response parsing fails.
   */
  public abstract ListResponseData listPath(String relativePath, boolean recursive,
      int listMaxResults, String continuation, TracingContext tracingContext, URI uri) throws IOException;

  /**
   * Retrieves user-defined metadata on filesystem.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   * */
  public abstract AbfsRestOperation getFilesystemProperties(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Deletes the filesystem using Azure REST API Service.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException;

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
  public abstract AbfsRestOperation createPath(String path,
      boolean isFile,
      boolean overwrite,
      Permissions permissions,
      boolean isAppendBlob,
      String eTag,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Conditionally creates or overwrites a file at the specified relative path.
   * This method ensures that the file is created or overwritten based on the provided parameters.
   *
   * @param relativePath The relative path of the file to be created or overwritten.
   * @param statistics The file system statistics to be updated.
   * @param permissions The permissions to be set on the file.
   * @param isAppendBlob Specifies if the file is an append blob.
   * @param contextEncryptionAdapter The encryption context adapter for handling encryption.
   * @param tracingContext The tracing context for tracking the operation.
   * @return An AbfsRestOperation object containing the result of the operation.
   * @throws IOException If an I/O error occurs during the operation.
   */
  public abstract AbfsRestOperation conditionalCreateOverwriteFile(String relativePath,
      FileSystem.Statistics statistics,
      Permissions permissions,
      boolean isAppendBlob,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws IOException;

  /**
   * Performs a pre-check for a createNonRecursivePreCheck operation. Checks if parentPath
   * exists or not.
   *
   * @param parentPath parent path of the file to be created.
   * @param tracingContext trace context
   *
   * @throws IOException if parentPath does not exist or server error.
   */
  public abstract void createNonRecursivePreCheck(Path parentPath,
      TracingContext tracingContext)
      throws IOException;

  /**
   * Acquire lease on specified path.
   * @param path on which lease has to be acquired.
   * @param duration for which lease has to be acquired.
   * @param eTag required to acquire lease on the path.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation acquireLease(String path,
      int duration,
      String eTag,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Renew lease on specified path.
   * @param path on which lease has to be renewed.
   * @param leaseId of the lease to be renewed.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation renewLease(String path, String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Release lease on specified path.
   * @param path on which lease has to be released.
   * @param leaseId of the lease to be released.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation releaseLease(String path, String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Break lease on specified path.
   * @param path on which lease has to be broke.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation breakLease(String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

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
   * @return AbfsClientRenameResult result of rename operation indicating the
   * AbfsRest operation, rename recovery and incomplete metadata state failure.
   * @throws AzureBlobFileSystemException failure, excluding any recovery from overload failures.
   */
  public abstract AbfsClientRenameResult renamePath(
      String source,
      String destination,
      String continuation,
      TracingContext tracingContext,
      String sourceEtag,
      boolean isMetadataIncompleteState)
      throws IOException;

  /**
   * Checks if the rest operation results indicate if the path is a directory.
   * @param result executed rest operation containing response from server.
   * @return True if the path is a directory, False otherwise.
   */
  public abstract boolean checkIsDir(AbfsHttpOperation result);

  /**
   * Creates a rest operation for rename.
   * @param url to be used for the operation.
   * @param requestHeaders list of headers to be added to the request.
   * @return un-executed rest operation.
   */
  @VisibleForTesting
  AbfsRestOperation createRenameRestOperation(URL url, List<AbfsHttpHeader> requestHeaders) {
    AbfsRestOperation op = getAbfsRestOperation(
            AbfsRestOperationType.RenamePath,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    return op;
  }

  /**
   * Increments AbfsCounters for rename path attempts by 1.
   * Will be called each time a rename path operation is attempted.
   */
  protected void incrementAbfsRenamePath() {
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

  /**
   * Uploads data to be appended to a file.
   * @param path to which data has to be appended.
   * @param buffer containing data to be appended.
   * @param reqParams containing parameters for append operation like offset, length etc.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation append(String path, byte[] buffer,
      AppendRequestParameters reqParams, String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Returns true if the status code lies in the range of user error.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  public abstract boolean checkUserError(int responseStatusCode);

  /**
   * To check if the failure exception returned by server is due to MD5 Mismatch
   * @param e Exception returned by AbfsRestOperation
   * @return boolean whether exception is due to MD5Mismatch or not
   */
  protected boolean isMd5ChecksumError(final AbfsRestOperationException e) {
    AzureServiceErrorCode storageErrorCode = e.getErrorCode();
    return storageErrorCode == AzureServiceErrorCode.MD5_MISMATCH;
  }

  // For AppendBlob its possible that the append succeeded in the backend but the request failed.
  // However a retry would fail with an InvalidQueryParameterValue
  // (as the current offset would be unacceptable).
  // Hence, we pass/succeed the appendblob append call
  // in case we are doing a retry after checking the length of the file.
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

  /**
   * Flush previously uploaded data to a file.
   * @param path on which data has to be flushed.
   * @param position to which data has to be flushed.
   * @param retainUncommittedData whether to retain uncommitted data after flush.
   * @param isClose specify if this is the last flush to the file.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param leaseId if there is an active lease on the path.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation flush(String path, long position,
      boolean retainUncommittedData, boolean isClose,
      String cachedSasToken, String leaseId,
      ContextEncryptionAdapter contextEncryptionAdapter, TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Flush previously uploaded data to a file.
   * @param buffer containing blockIds to be flushed.
   * @param path on which data has to be flushed.
   * @param isClose specify if this is the last flush to the file.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param leaseId if there is an active lease on the path.
   * @param eTag to specify conditional headers.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation flush(byte[] buffer,
      String path,
      boolean isClose,
      String cachedSasToken,
      String leaseId,
      String eTag,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Set the properties of a file or directory.
   * @param path on which properties have to be set.
   * @param properties list of metadata key-value pairs.
   * @param tracingContext for tracing the server calls.
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation setPathProperties(String path, Hashtable<String, String> properties,
      TracingContext tracingContext, ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException;

  /**
   * Get the properties of a file or directory.
   * @param path of which properties have to be fetched.
   * @param includeProperties to include user defined properties.
   * @param tracingContext for tracing the server calls.
   * @param contextEncryptionAdapter to provide encryption context.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation getPathStatus(String path,
      boolean includeProperties, TracingContext tracingContext,
      ContextEncryptionAdapter contextEncryptionAdapter)
      throws AzureBlobFileSystemException;

  /**
   * Read the contents of the file at specified path.
   * @param path of the file to be read.
   * @param position in the file from where data has to be read.
   * @param buffer to store the data read.
   * @param bufferOffset offset in the buffer to start storing the data.
   * @param bufferLength length of data to be read.
   * @param eTag to specify conditional headers.
   * @param cachedSasToken to be used for the authenticating operation.
   * @param contextEncryptionAdapter to provide encryption context.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation read(String path,
      long position,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      String eTag,
      String cachedSasToken,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

  /**
   * Delete the file or directory at specified path.
   * @param path to be deleted.
   * @param recursive if the path is a directory, delete recursively.
   * @param continuation to specify continuation token.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation deletePath(String path, boolean recursive,
      String continuation,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException;

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
   * @param op Delete request REST operation response with non-null HTTP response.
   * @return REST operation response post idempotency check.
   */
  public AbfsRestOperation deleteIdempotencyCheckOp(final AbfsRestOperation op) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND)
        && DEFAULT_DELETE_CONSIDERED_IDEMPOTENT) {
      // Server has returned HTTP 404, which means path no longer
      // exists. Assuming delete result to be idempotent, return success.
      LOG.debug("Returning success response from delete idempotency logic");
      return getSuccessOp(
          AbfsRestOperationType.DeletePath,
          HTTP_METHOD_DELETE,
          op.getUrl(),
          op.getRequestHeaders());
    }

    return op;
  }

  /**
   * Sets the owner on tha path.
   * @param path on which owner has to be set.
   * @param owner to be set.
   * @param group to be set.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation setOwner(String path, String owner, String group,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Sets the permission on the path.
   * @param path on which permission has to be set.
   * @param permission to be set.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation setPermission(String path, String permission,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Sets the ACL.
   * @param path on which ACL has to be set.
   * @param aclSpecString to be set.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation setAcl(final String path, final String aclSpecString,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    return setAcl(path, aclSpecString, EMPTY_STRING, tracingContext);
  }

  /**
   * Sets the ACL on the path that matches ETag.
   * @param path on which ACL has to be set.
   * @param aclSpecString to be set.
   * @param eTag to specify conditional headers. Set only if etag matches.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation setAcl(String path, String aclSpecString, String eTag,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException;

  /**
   * Retrieves the ACL properties of blob at specified path.
   * @param path of which properties have to be fetched.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public AbfsRestOperation getAclStatus(final String path, TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    return getAclStatus(path, abfsConfiguration.isUpnUsed(), tracingContext);
  }

  /**
   * Retrieves the ACL properties of blob at specified path.
   * @param path of which properties have to be fetched.
   * @param useUPN whether to use UPN with rest operation.
   * @param tracingContext for tracing the server calls.
   * @return executed rest operation containing response from server.
   * @throws AzureBlobFileSystemException if rest operation fails.
   */
  public abstract AbfsRestOperation getAclStatus(String path, boolean useUPN,
      TracingContext tracingContext) throws AzureBlobFileSystemException;

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
  public abstract AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException;

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
      directory = EMPTY_STRING;
    } else if (directory.charAt(0) == '/') {
      directory = directory.substring(1);
    }
    return directory;
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path for which SAS token is required.
   * @param operation for which SAS token is required.
   * @param queryBuilder to which SAS token is appended.
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException if SAS token cannot be acquired.
   */
  protected String appendSASTokenToQuery(String path, String operation, AbfsUriQueryBuilder queryBuilder) throws SASTokenProviderException {
    return appendSASTokenToQuery(path, operation, queryBuilder, null);
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder.
   * @param path for which SAS token is required.
   * @param operation for which SAS token is required.
   * @param queryBuilder to which SAS token is appended.
   * @param cachedSasToken - previously acquired SAS token to be reused.
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException if SAS token cannot be acquired.
   */
  protected String appendSASTokenToQuery(String path,
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
        throw new SASTokenProviderException(String.format(
            "Failed to acquire a SAS token for %s on %s due to %s", operation, path,
            ex.toString()));
      }
    }
    return sasToken;
  }

  /**
   * Creates REST operation URL with empty path for the given query.
   * @param query to be added to the URL.
   * @return URL for the REST operation.
   * @throws AzureBlobFileSystemException if URL creation fails.
   */
  @VisibleForTesting
  protected URL createRequestUrl(final String query) throws AzureBlobFileSystemException {
    return createRequestUrl(EMPTY_STRING, query);
  }

  /**
   * Creates REST operation URL with given path and query.
   * @param path for which URL has to be created.
   * @param query to be added to the URL.
   * @return URL for the REST operation.
   * @throws AzureBlobFileSystemException if URL creation fails.
   */
  @VisibleForTesting
  protected URL createRequestUrl(final String path, final String query)
      throws AzureBlobFileSystemException {
    return createRequestUrl(baseUrl, path, query);
  }

  /**
   * Creates REST operation URL with given baseUrl, path and query.
   * @param baseUrl to be used for the operation.
   * @param path for which URL has to be created.
   * @param query to be added to the URL.
   * @return URL for the REST operation.
   * @throws AzureBlobFileSystemException if URL creation fails.
   */
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

  /**
   * returns the url encoded string for a given value.
   * @param value to be encoded.
   * @return url encoded string.
   * @throws AzureBlobFileSystemException if encoding fails.
   */
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

  protected Boolean getIsPaginatedDeleteEnabled() {
    return abfsConfiguration.isPaginatedDeleteEnabled();
  }

  protected Boolean isPaginatedDelete(boolean isRecursiveDelete, boolean isNamespaceEnabled) {
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
    sb.append(SINGLE_WHITE_SPACE)
        .append(abfsConfiguration.getPreferredHttpOperationType())
        .append(SEMICOLON);

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
  protected void addCheckSumHeaderForWrite(List<AbfsHttpHeader> requestHeaders,
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
  protected void verifyCheckSumForRead(final byte[] buffer,
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
   * Sending MD5 Hash in request headers. For more details refer to
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read">Path - Read Azure Storage Rest API</a>.
   * 1. Range header must be present as one of the request headers.
   * 2. buffer length must be less than or equal to 4 MB.
   * @param requestHeaders to be checked for range header.
   * @param rangeHeader must be present.
   * @param bufferLength must be less than or equal to 4 MB.
   * @return true if all conditions are met.
   */
  protected boolean isChecksumValidationEnabled(List<AbfsHttpHeader> requestHeaders,
      final AbfsHttpHeader rangeHeader, final int bufferLength) {
    return getAbfsConfiguration().getIsChecksumValidationEnabled()
        && requestHeaders.contains(rangeHeader) && bufferLength <= 4 * ONE_MB;
  }

  /**
   * Conditions check for allowing checksum support for write operation.
   * Server will support this if client sends the MD5 Hash as a request header.
   * For azure stoage service documentation and more details refer to
   * <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update">Path - Update Azure Rest API</a>.
   * @return true if checksum validation enabled.
   */
  protected boolean isChecksumValidationEnabled() {
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
        if (isMetricCollectionEnabled && isMetricCollectionStopped.get()) {
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
        sasTokenForReuse,
        abfsConfiguration);
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
  @VisibleForTesting
  public AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders,
        abfsConfiguration
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
        requestHeaders, sasTokenForReuse, abfsConfiguration);
  }

  @VisibleForTesting
  AbfsApacheHttpClient getAbfsApacheHttpClient() {
    return abfsApacheHttpClient;
  }

  @VisibleForTesting
  KeepAliveCache getKeepAliveCache() {
    return keepAliveCache;
  }

  @VisibleForTesting
  protected Timer getTimer() {
    return timer;
  }

  protected String getUserAgent() {
    return userAgent;
  }

  /**
   * Checks if the namespace is enabled.
   *
   * @return True if the namespace is enabled, false otherwise.
   */
  public static boolean getIsNamespaceEnabled() {
    return isNamespaceEnabled;
  }

  /**
   * Sets the namespace enabled status.
   *
   * @param namespaceEnabled True to enable the namespace, false to disable it.
   */
  public static void setIsNamespaceEnabled(final boolean namespaceEnabled) {
    isNamespaceEnabled = namespaceEnabled;
  }

  protected boolean isRenameResilience() {
    return renameResilience;
  }

  /**
   * Parses response of Listing API from server based on Endpoint used.
   * @param result AbfsHttpOperation of list Operation.
   * @param uri to be used for the path conversion.
   * @return {@link ListResponseData} containing the list of entries.
   * @throws IOException if parsing fails
   */
  public abstract ListResponseData parseListPathResults(AbfsHttpOperation result, URI uri) throws IOException;

  /**
   * Parses response of Get Block List from server based on Endpoint used.
   * @param stream InputStream of the response
   * @return List of block IDs
   * @throws IOException if parsing fails
   */
  public abstract List<String> parseBlockListResponse(InputStream stream) throws IOException;

  /**
   * Parses response from ErrorStream returned by server based on Endpoint used.
   * @param stream InputStream of the response
   * @return StorageErrorResponseSchema
   * @throws IOException if parsing fails
   */
  public abstract StorageErrorResponseSchema processStorageErrorResponse(InputStream stream) throws IOException;

  /**
   * Returns user-defined metadata from server response based on Endpoint used.
   * @param result response from server
   * @return user-defined metadata key-value pairs
   * @throws InvalidFileSystemPropertyException if parsing fails
   * @throws InvalidAbfsRestOperationException if parsing fails
   */
  public abstract Hashtable<String, String> getXMSProperties(AbfsHttpOperation result)
      throws InvalidFileSystemPropertyException,
      InvalidAbfsRestOperationException;

  /**
   * Encode attribute with encoding based on Endpoint used.
   * @param value to be encoded
   * @return encoded value
   * @throws UnsupportedEncodingException if encoding fails
   */
  public abstract byte[] encodeAttribute(String value) throws UnsupportedEncodingException;

  /**
   * Decode attribute with decoding based on Endpoint used.
   * @param value to be decoded
   * @return decoded value
   * @throws UnsupportedEncodingException if decoding fails
   */
  public abstract String decodeAttribute(byte[] value) throws UnsupportedEncodingException;

  /**
   * Get the dummy success operation.
   * @param operationType type of the operation
   * @param httpMethod http method
   * @param url url to be used
   * @param requestHeaders list of headers to be sent with the request
   * @return success operation
   */
  protected AbfsRestOperation getSuccessOp(final AbfsRestOperationType operationType,
      final String httpMethod, final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    final AbfsRestOperation successOp = getAbfsRestOperation(
        operationType, httpMethod, url, requestHeaders);
    successOp.hardSetResult(HttpURLConnection.HTTP_OK);
    return successOp;
  }

  /**
   * Get the primary user group name.
   * @return primary user group name
   * @throws AzureBlobFileSystemException if unable to get the primary user group
   */
  private String getPrimaryUserGroup() throws AzureBlobFileSystemException {
    if (!getAbfsConfiguration().getSkipUserGroupMetadataDuringInitialization()) {
      try {
        return UserGroupInformation.getCurrentUser().getPrimaryGroupName();
      } catch (IOException ex) {
        LOG.error("Failed to get primary group for {}, using user name as primary group name",
            getPrimaryUser());
      }
    }
    //Provide a default group name
    return getPrimaryUser();
  }

  /**
   * Get the primary username.
   * @return primary username
   * @throws AzureBlobFileSystemException if unable to get the primary user
   */
  private String getPrimaryUser() throws AzureBlobFileSystemException {
    try {
      return UserGroupInformation.getCurrentUser().getUserName();
    } catch (IOException ex) {
      throw new AbfsDriverException(ex);
    }
  }

  /**
   * Creates a VersionedFileStatus object from the ListResultEntrySchema.
   * @param entry ListResultEntrySchema object.
   * @param uri to be used for the path conversion.
   * @return VersionedFileStatus object.
   * @throws AzureBlobFileSystemException if transformation fails.
   */
  protected VersionedFileStatus getVersionedFileStatusFromEntry(
      ListResultEntrySchema entry, URI uri) throws AzureBlobFileSystemException {
    long blockSize = abfsConfiguration.getAzureBlockSize();
    String owner = null, group = null;
    try{
      if (identityTransformer != null) {
        owner = identityTransformer.transformIdentityForGetRequest(
            entry.owner(), true, getPrimaryUser());
        group = identityTransformer.transformIdentityForGetRequest(
            entry.group(), false, getPrimaryUserGroup());
      }
    } catch (IOException ex) {
      LOG.error("Failed to get owner/group for path {}", entry.name(), ex);
      throw new AbfsDriverException(ex);
    }
    final String encryptionContext = entry.getXMsEncryptionContext();
    final FsPermission fsPermission = entry.permissions() == null
        ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
        : AbfsPermission.valueOf(entry.permissions());
    final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

    long lastModifiedMillis = 0;
    long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
    boolean isDirectory = entry.isDirectory() != null && entry.isDirectory();
    if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
      lastModifiedMillis = DateTimeUtils.parseLastModifiedTime(
          entry.lastModified());
    }

    Path entryPath = new Path(File.separator + entry.name());
    if (uri != null) {
      entryPath = entryPath.makeQualified(uri, entryPath);
    }
    return new VersionedFileStatus(
        owner,
        group,
        fsPermission,
        hasAcl,
        contentLength,
        isDirectory,
        1,
        blockSize,
        lastModifiedMillis,
        entryPath,
        entry.eTag(),
        encryptionContext);
  }
}
