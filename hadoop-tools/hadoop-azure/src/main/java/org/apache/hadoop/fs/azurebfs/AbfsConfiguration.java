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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.Base64StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.BooleanConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerWithOutlierConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.LongConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConfigurationPropertyNotFoundException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.diagnostics.Base64StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.BooleanConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.IntegerConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.LongConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.diagnostics.StringConfigurationBasicValidator;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.FixedSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.KeyProvider;
import org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.INCORRECT_INGRESS_TYPE;

/**
 * Configuration for Azure Blob FileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AbfsConfiguration{

  private final Configuration rawConfig;
  private final String accountName;
  // Service type identified from URL used to initialize FileSystem.
  private final AbfsServiceType fsConfiguredServiceType;
  private final boolean isSecure;
  private static final Logger LOG = LoggerFactory.getLogger(AbfsConfiguration.class);

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
      DefaultValue = DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED)
  private String isNamespaceEnabledAccount;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK,
      DefaultValue = DEFAULT_FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK)
  private boolean isDfsToBlobFallbackEnabled;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_WRITE_MAX_CONCURRENT_REQUESTS,
      DefaultValue = -1)
  private int writeMaxConcurrentRequestCount;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_WRITE_MAX_REQUESTS_TO_QUEUE,
      DefaultValue = -1)
  private int maxWriteRequestsToQueue;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_WRITE_BUFFER_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_WRITE_BUFFER_SIZE)
  private int writeBufferSize;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION,
      DefaultValue = DEFAULT_AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION)
  private boolean enableSmallWriteOptimization;

  @BooleanConfigurationValidatorAnnotation(
      ConfigurationKey = AZURE_READ_SMALL_FILES_COMPLETELY,
      DefaultValue = DEFAULT_READ_SMALL_FILES_COMPLETELY)
  private boolean readSmallFilesCompletely;

  @BooleanConfigurationValidatorAnnotation(
      ConfigurationKey = AZURE_READ_OPTIMIZE_FOOTER_READ,
      DefaultValue = DEFAULT_OPTIMIZE_FOOTER_READ)
  private boolean optimizeFooterRead;

  @IntegerConfigurationValidatorAnnotation(
          ConfigurationKey = AZURE_FOOTER_READ_BUFFER_SIZE,
          DefaultValue = DEFAULT_FOOTER_READ_BUFFER_SIZE)
  private int footerReadBufferSize;

  @BooleanConfigurationValidatorAnnotation(
      ConfigurationKey = FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED,
      DefaultValue = DEFAULT_FS_AZURE_ACCOUNT_IS_EXPECT_HEADER_ENABLED)
  private boolean isExpectHeaderEnabled;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED,
      DefaultValue = DEFAULT_FS_AZURE_ACCOUNT_LEVEL_THROTTLING_ENABLED)
  private boolean accountThrottlingEnabled;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_READ_BUFFER_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_READ_BUFFER_SIZE)
  private int readBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_READ_AHEAD_RANGE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_READ_AHEAD_RANGE)
  private int readAheadRange;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MIN_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MIN_BACKOFF_INTERVAL)
  private int minBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MAX_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MAX_BACKOFF_INTERVAL)
  private int maxBackoffInterval;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED,
      DefaultValue = DEFAULT_STATIC_RETRY_FOR_CONNECTION_TIMEOUT_ENABLED)
  private boolean staticRetryForConnectionTimeoutEnabled;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_STATIC_RETRY_INTERVAL,
      DefaultValue = DEFAULT_STATIC_RETRY_INTERVAL)
  private int staticRetryInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_BACKOFF_INTERVAL)
  private int backoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MAX_IO_RETRIES,
      MinValue = 0,
      DefaultValue = DEFAULT_MAX_RETRY_ATTEMPTS)
  private int maxIoRetries;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT,
      MinValue = 0,
      DefaultValue = DEFAULT_CUSTOM_TOKEN_FETCH_RETRY_COUNT)
  private int customTokenFetchRetryCount;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_HTTP_CONNECTION_TIMEOUT,
          DefaultValue = DEFAULT_HTTP_CONNECTION_TIMEOUT)
  private int httpConnectionTimeout;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_HTTP_READ_TIMEOUT,
          DefaultValue = DEFAULT_HTTP_READ_TIMEOUT)
  private int httpReadTimeout;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_OAUTH_TOKEN_FETCH_RETRY_COUNT,
      MinValue = 0,
      DefaultValue = DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_ATTEMPTS)
  private int oauthTokenFetchRetryCount;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF,
      MinValue = 0,
      DefaultValue = DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MIN_BACKOFF_INTERVAL)
  private int oauthTokenFetchRetryMinBackoff;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF,
      MinValue = 0,
      DefaultValue = DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_MAX_BACKOFF_INTERVAL)
  private int oauthTokenFetchRetryMaxBackoff;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF,
      MinValue = 0,
      DefaultValue = DEFAULT_AZURE_OAUTH_TOKEN_FETCH_RETRY_DELTA_BACKOFF)
  private int oauthTokenFetchRetryDeltaBackoff;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BLOCK_SIZE_PROPERTY_NAME,
      MinValue = 0,
      MaxValue = MAX_AZURE_BLOCK_SIZE,
      DefaultValue = MAX_AZURE_BLOCK_SIZE)
  private long azureBlockSize;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME,
      DefaultValue = AZURE_BLOCK_LOCATION_HOST_DEFAULT)
  private String azureBlockLocationHost;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CONCURRENT_CONNECTION_VALUE_OUT,
      MinValue = 1,
      DefaultValue = MAX_CONCURRENT_WRITE_THREADS)
  private int maxConcurrentWriteThreads;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_LIST_MAX_RESULTS,
      MinValue = 1,
      DefaultValue = DEFAULT_AZURE_LIST_MAX_RESULTS)
  private int listMaxResults;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CONCURRENT_CONNECTION_VALUE_IN,
      MinValue = 1,
      DefaultValue = MAX_CONCURRENT_READ_THREADS)
  private int maxConcurrentReadThreads;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_TOLERATE_CONCURRENT_APPEND,
      DefaultValue = DEFAULT_READ_TOLERATE_CONCURRENT_APPEND)
  private boolean tolerateOobAppends;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ATOMIC_RENAME_KEY,
      DefaultValue = DEFAULT_FS_AZURE_ATOMIC_RENAME_DIRECTORIES)
  private String azureAtomicDirs;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE,
      DefaultValue = DEFAULT_FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE)
  private boolean enableConditionalCreateOverwrite;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ENABLE_MKDIR_OVERWRITE, DefaultValue =
      DEFAULT_FS_AZURE_ENABLE_MKDIR_OVERWRITE)
  private boolean mkdirOverwrite;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_APPEND_BLOB_KEY,
      DefaultValue = DEFAULT_FS_AZURE_APPEND_BLOB_DIRECTORIES)
  private String azureAppendBlobDirs;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_INFINITE_LEASE_KEY,
      DefaultValue = DEFAULT_FS_AZURE_INFINITE_LEASE_DIRECTORIES)
  private String azureInfiniteLeaseDirs;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_LEASE_THREADS,
      MinValue = MIN_LEASE_THREADS,
      DefaultValue = DEFAULT_LEASE_THREADS)
  private int numLeaseThreads;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
      DefaultValue = DEFAULT_AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION)
  private boolean createRemoteFileSystemDuringInitialization;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION,
      DefaultValue = DEFAULT_AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION)
  private boolean skipUserGroupMetadataDuringInitialization;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_READ_AHEAD_QUEUE_DEPTH,
      DefaultValue = DEFAULT_READ_AHEAD_QUEUE_DEPTH)
  private int readAheadQueueDepth;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_READ_AHEAD_BLOCK_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_READ_AHEAD_BLOCK_SIZE)
  private int readAheadBlockSize;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ALWAYS_READ_BUFFER_SIZE,
      DefaultValue = DEFAULT_ALWAYS_READ_BUFFER_SIZE)
  private boolean alwaysReadBufferSize;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_FLUSH,
      DefaultValue = DEFAULT_ENABLE_FLUSH)
  private boolean enableFlush;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_DISABLE_OUTPUTSTREAM_FLUSH,
      DefaultValue = DEFAULT_DISABLE_OUTPUTSTREAM_FLUSH)
  private boolean disableOutputStreamFlush;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_AUTOTHROTTLING,
      DefaultValue = DEFAULT_ENABLE_AUTOTHROTTLING)
  private boolean enableAutoThrottling;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_METRIC_IDLE_TIMEOUT,
      DefaultValue = DEFAULT_METRIC_IDLE_TIMEOUT_MS)
  private int metricIdleTimeout;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_METRIC_ANALYSIS_TIMEOUT,
      DefaultValue = DEFAULT_METRIC_ANALYSIS_TIMEOUT_MS)
  private int metricAnalysisTimeout;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_METRIC_URI,
          DefaultValue = EMPTY_STRING)
  private String metricUri;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_METRIC_ACCOUNT_NAME,
          DefaultValue = EMPTY_STRING)
  private String metricAccount;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_METRIC_ACCOUNT_KEY,
          DefaultValue = EMPTY_STRING)
  private String metricAccountKey;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ACCOUNT_OPERATION_IDLE_TIMEOUT,
      DefaultValue = DEFAULT_ACCOUNT_OPERATION_IDLE_TIMEOUT_MS)
  private int accountOperationIdleTimeout;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ANALYSIS_PERIOD,
          DefaultValue = DEFAULT_ANALYSIS_PERIOD_MS)
  private int analysisPeriod;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ABFS_IO_RATE_LIMIT,
      MinValue = 0,
      DefaultValue = RATE_LIMIT_DEFAULT)
  private int rateLimit;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_USER_AGENT_PREFIX_KEY,
      DefaultValue = DEFAULT_FS_AZURE_USER_AGENT_PREFIX)
  private String userAgentId;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_CLUSTER_NAME,
      DefaultValue = DEFAULT_VALUE_UNKNOWN)
  private String clusterName;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_CLUSTER_TYPE,
      DefaultValue = DEFAULT_VALUE_UNKNOWN)
  private String clusterType;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_CLIENT_CORRELATIONID,
          DefaultValue = EMPTY_STRING)
  private String clientCorrelationId;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_DELEGATION_TOKEN,
      DefaultValue = DEFAULT_ENABLE_DELEGATION_TOKEN)
  private boolean enableDelegationToken;


  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ALWAYS_USE_HTTPS,
          DefaultValue = DEFAULT_ENABLE_HTTPS)
  private boolean alwaysUseHttps;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_USE_UPN,
      DefaultValue = DEFAULT_USE_UPN)
  private boolean useUpn;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ENABLE_CHECK_ACCESS, DefaultValue = DEFAULT_ENABLE_CHECK_ACCESS)
  private boolean isCheckAccessEnabled;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ABFS_LATENCY_TRACK,
          DefaultValue = DEFAULT_ABFS_LATENCY_TRACK)
  private boolean trackLatency;

  @BooleanConfigurationValidatorAnnotation(
      ConfigurationKey = FS_AZURE_ENABLE_READAHEAD,
      DefaultValue = DEFAULT_ENABLE_READAHEAD)
  private boolean enabledReadAhead;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS,
      MinValue = 0,
      DefaultValue = DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS)
  private long sasTokenRenewPeriodForStreamsInSeconds;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ENABLE_ABFS_LIST_ITERATOR, DefaultValue = DEFAULT_ENABLE_ABFS_LIST_ITERATOR)
  private boolean enableAbfsListIterator;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
          FS_AZURE_ABFS_RENAME_RESILIENCE, DefaultValue = DEFAULT_ENABLE_ABFS_RENAME_RESILIENCE)
  private boolean renameResilience;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ABFS_ENABLE_CHECKSUM_VALIDATION, DefaultValue = DEFAULT_ENABLE_ABFS_CHECKSUM_VALIDATION)
  private boolean isChecksumValidationEnabled;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ENABLE_PAGINATED_DELETE, DefaultValue = DEFAULT_ENABLE_PAGINATED_DELETE)
  private boolean isPaginatedDeleteEnabled;

  @LongConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_BLOB_COPY_PROGRESS_WAIT_MILLIS, DefaultValue = DEFAULT_AZURE_BLOB_COPY_PROGRESS_WAIT_MILLIS)
  private long blobCopyProgressPollWaitMillis;

  @LongConfigurationValidatorAnnotation(ConfigurationKey =
          FS_AZURE_BLOB_COPY_MAX_WAIT_MILLIS, DefaultValue = DEFAULT_AZURE_BLOB_COPY_MAX_WAIT_MILLIS)
  private long blobCopyProgressMaxWaitMillis;

  @LongConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_BLOB_ATOMIC_RENAME_LEASE_REFRESH_DURATION, DefaultValue = DEFAULT_AZURE_BLOB_ATOMIC_RENAME_LEASE_REFRESH_DURATION)
  private long blobAtomicRenameLeaseRefreshDuration;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_PRODUCER_QUEUE_MAX_SIZE, DefaultValue = DEFAULT_FS_AZURE_PRODUCER_QUEUE_MAX_SIZE)
  private int producerQueueMaxSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey =
          FS_AZURE_CONSUMER_MAX_LAG, DefaultValue = DEFAULT_FS_AZURE_CONSUMER_MAX_LAG)
  private int listingMaxConsumptionLag;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_BLOB_DIR_RENAME_MAX_THREAD, DefaultValue = DEFAULT_FS_AZURE_BLOB_RENAME_THREAD)
  private int blobRenameDirConsumptionParallelism;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_BLOB_DIR_DELETE_MAX_THREAD, DefaultValue = DEFAULT_FS_AZURE_BLOB_DELETE_THREAD)
  private int blobDeleteDirConsumptionParallelism;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES, DefaultValue = DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES)
  private int maxApacheHttpClientIoExceptionsRetries;

  /**
   * Max idle TTL configuration for connection given in
   * {@value org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys#FS_AZURE_APACHE_HTTP_CLIENT_IDLE_CONNECTION_TTL}
   * with default of
   * {@value org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations#DEFAULT_HTTP_CLIENT_CONN_MAX_IDLE_TIME}
   */
  @LongConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_APACHE_HTTP_CLIENT_IDLE_CONNECTION_TTL,
      DefaultValue = DEFAULT_HTTP_CLIENT_CONN_MAX_IDLE_TIME)
  private long maxApacheHttpClientConnectionIdleTime;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID,
      DefaultValue = DEFAULT_FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID)
  private boolean enableClientTransactionId;

  private String clientProvidedEncryptionKey;
  private String clientProvidedEncryptionKeySHA;

  /**
   * Constructor for AbfsConfiguration for specified service type.
   * @param rawConfig used to initialize the configuration.
   * @param accountName the name of the azure storage account.
   * @param fsConfiguredServiceType service type configured for the file system.
   * @throws IllegalAccessException if the field is not accessible.
   * @throws IOException if an I/O error occurs.
   */
  public AbfsConfiguration(final Configuration rawConfig,
      String accountName,
      AbfsServiceType fsConfiguredServiceType)
      throws IllegalAccessException, IOException {
    this.rawConfig = ProviderUtils.excludeIncompatibleCredentialProviders(
        rawConfig, AzureBlobFileSystem.class);
    this.accountName = accountName;
    this.fsConfiguredServiceType = fsConfiguredServiceType;
    this.isSecure = getBoolean(FS_AZURE_SECURE_MODE, false);

    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(IntegerConfigurationValidatorAnnotation.class)) {
        field.set(this, validateInt(field));
      } else if (field.isAnnotationPresent(IntegerWithOutlierConfigurationValidatorAnnotation.class)) {
        field.set(this, validateIntWithOutlier(field));
      } else if (field.isAnnotationPresent(LongConfigurationValidatorAnnotation.class)) {
        field.set(this, validateLong(field));
      } else if (field.isAnnotationPresent(StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateString(field));
      } else if (field.isAnnotationPresent(Base64StringConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBase64String(field));
      } else if (field.isAnnotationPresent(BooleanConfigurationValidatorAnnotation.class)) {
        field.set(this, validateBoolean(field));
      }
    }
  }

  /**
   * Constructor for AbfsConfiguration for default service type i.e. DFS.
   * @param rawConfig used to initialize the configuration.
   * @param accountName the name of the azure storage account.
   * @throws IllegalAccessException if the field is not accessible.
   * @throws IOException if an I/O error occurs.
   */
  public AbfsConfiguration(final Configuration rawConfig, String accountName)
      throws IllegalAccessException, IOException {
    this(rawConfig, accountName, AbfsServiceType.DFS);
  }

  public Trilean getIsNamespaceEnabledAccount() {
    return Trilean.getTrilean(
        getString(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, isNamespaceEnabledAccount));
  }

  /**
   * Returns the service type to be used based on the filesystem configuration.
   * Precedence is given to service type configured for FNS Accounts using
   * "fs.azure.fns.account.service.type". If not configured, then the service
   * type identified from url used to initialize filesystem will be used.
   * @return the service type.
   */
  public AbfsServiceType getFsConfiguredServiceType() {
    return getCaseInsensitiveEnum(FS_AZURE_FNS_ACCOUNT_SERVICE_TYPE, fsConfiguredServiceType);
  }

  /**
   * Returns the service type configured for FNS Accounts to override the
   * service type identified by URL used to initialize the filesystem.
   * @return the service type.
   */
  public AbfsServiceType getConfiguredServiceTypeForFNSAccounts() {
    return getCaseInsensitiveEnum(FS_AZURE_FNS_ACCOUNT_SERVICE_TYPE, null);
  }

  /**
   * Returns the service type to be used for Ingress Operations irrespective of account type.
   * Default value is the same as the service type configured for the file system.
   * @return the service type.
   */
  public AbfsServiceType getIngressServiceType() {
    return getCaseInsensitiveEnum(FS_AZURE_INGRESS_SERVICE_TYPE, getFsConfiguredServiceType());
  }

  /**
   * Returns whether there is a need to move traffic from DFS to Blob.
   * Needed when the service type is DFS and operations are experiencing compatibility issues.
   * @return true if fallback enabled.
   */
  public boolean isDfsToBlobFallbackEnabled() {
    return isDfsToBlobFallbackEnabled;
  }

  /**
   * Checks if the service type configured is valid for account type used.
   * HNS Enabled accounts cannot have service type as BLOB.
   * @param isHNSEnabled Flag to indicate if HNS is enabled for the account.
   * @throws InvalidConfigurationValueException if the service type is invalid.
   */
  public void validateConfiguredServiceType(boolean isHNSEnabled)
      throws InvalidConfigurationValueException {
    if (isHNSEnabled && getConfiguredServiceTypeForFNSAccounts() == AbfsServiceType.BLOB) {
      throw new InvalidConfigurationValueException(
          FS_AZURE_FNS_ACCOUNT_SERVICE_TYPE, "Service Type Cannot be BLOB for HNS Account");
    } else if (isHNSEnabled && fsConfiguredServiceType == AbfsServiceType.BLOB) {
      throw new InvalidConfigurationValueException(FS_DEFAULT_NAME_KEY,
          "Blob Endpoint Url Cannot be used to initialize filesystem for HNS Account");
    } else if (getFsConfiguredServiceType() == AbfsServiceType.BLOB
        && getIngressServiceType() == AbfsServiceType.DFS) {
      throw new InvalidConfigurationValueException(
          FS_AZURE_INGRESS_SERVICE_TYPE, INCORRECT_INGRESS_TYPE);
    }
  }

  /**
   * Gets the Azure Storage account name corresponding to this instance of configuration.
   * @return the Azure Storage account name
   */
  public String getAccountName() {
    return accountName;
  }

  /**
   * Gets client correlation ID provided in config.
   * @return Client Correlation ID config
   */
  public String getClientCorrelationId() {
    return clientCorrelationId;
  }

  /**
   * Appends an account name to a configuration key yielding the
   * account-specific form.
   * @param key Account-agnostic configuration key
   * @return Account-specific configuration key
   */
  public String accountConf(String key) {
    return key + "." + accountName;
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value if one exists, else null
   */
  public String get(String key) {
    return rawConfig.get(accountConf(key), rawConfig.get(key));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public String getString(String key, String defaultValue) {
    return rawConfig.get(accountConf(key), rawConfig.get(key, defaultValue));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    return rawConfig.getBoolean(accountConf(key), rawConfig.getBoolean(key, defaultValue));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public long getLong(String key, long defaultValue) {
    return rawConfig.getLong(accountConf(key), rawConfig.getLong(key, defaultValue));
  }

  /**
   * Returns the account-specific value if it exists, then looks for an
   * account-agnostic value, and finally tries the default value.
   * @param key Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @return value if one exists, else the default value
   */
  public int getInt(String key, int defaultValue) {
    return rawConfig.getInt(accountConf(key), rawConfig.getInt(key, defaultValue));
  }

  /**
   * Returns the account-specific password in string form if it exists, then
   * looks for an account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value in String form if one exists, else null
   * @throws IOException if parsing fails.
   */
  public String getPasswordString(String key) throws IOException {
    char[] passchars = rawConfig.getPassword(accountConf(key));
    if (passchars == null) {
      passchars = rawConfig.getPassword(key);
    }
    if (passchars != null) {
      return new String(passchars);
    }
    return null;
  }

  /**
   * Returns a value for the key if the value exists and is not null.
   * Otherwise, throws {@link ConfigurationPropertyNotFoundException} with
   * key name.
   *
   * @param key Account-agnostic configuration key
   * @return value if exists
   * @throws IOException if error in fetching password or
   *     ConfigurationPropertyNotFoundException for missing key
   */
  private String getMandatoryPasswordString(String key) throws IOException {
    String value = getPasswordString(key);
    if (value == null) {
      throw new ConfigurationPropertyNotFoundException(key);
    }
    return value;
  }

  /**
   * Returns account-specific token provider class if it exists, else checks if
   * an account-agnostic setting is present for token provider class if AuthType
   * matches with authType passed.
   * @param authType AuthType effective on the account
   * @param name Account-agnostic configuration key
   * @param defaultValue Class returned if none is configured
   * @param xface Interface shared by all possible values
   * @param <U> Interface class type
   * @return Highest-precedence Class object that was found
   */
  public <U> Class<? extends U> getTokenProviderClass(AuthType authType,
      String name,
      Class<? extends U> defaultValue,
      Class<U> xface) {
    Class<?> tokenProviderClass = getAccountSpecificClass(name, defaultValue,
        xface);

    // If there is none set specific for account
    // fall back to generic setting if Auth Type matches
    if ((tokenProviderClass == null)
        && (authType == getAccountAgnosticEnum(
        FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey))) {
      tokenProviderClass = getAccountAgnosticClass(name, defaultValue, xface);
    }

    return (tokenProviderClass == null)
        ? null
        : tokenProviderClass.asSubclass(xface);
  }

  /**
   * Returns the account-specific class if it exists, else returns default value.
   * @param name Account-agnostic configuration key
   * @param defaultValue Class returned if none is configured
   * @param xface Interface shared by all possible values
   * @param <U> Interface class type
   * @return Account specific Class object that was found
   */
  public <U> Class<? extends U> getAccountSpecificClass(String name,
      Class<? extends U> defaultValue,
      Class<U> xface) {
    return rawConfig.getClass(accountConf(name),
        defaultValue,
        xface);
  }

  /**
   * Returns account-agnostic Class if it exists, else returns the default value.
   * @param name Account-agnostic configuration key
   * @param defaultValue Class returned if none is configured
   * @param xface Interface shared by all possible values
   * @param <U> Interface class type
   * @return Account-Agnostic Class object that was found
   */
  public <U> Class<? extends U> getAccountAgnosticClass(String name,
      Class<? extends U> defaultValue,
      Class<U> xface) {
    return rawConfig.getClass(name, defaultValue, xface);
  }

  /**
   * Returns the account-specific enum value if it exists, then
   * looks for an account-agnostic value.
   * @param name Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @param <T> Enum type
   * @return enum value if one exists, else null
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    return rawConfig.getEnum(accountConf(name),
        rawConfig.getEnum(name, defaultValue));
  }

  /**
   * Returns the account-specific enum value if it exists, then
   * looks for an account-agnostic value in case-insensitive manner.
   * @param name Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @param <T> Enum type
   * @return enum value if one exists, else null
   */
  public <T extends Enum<T>> T getCaseInsensitiveEnum(String name, T defaultValue) {
    String configValue = getString(name, null);
    if (configValue != null) {
      for (T enumConstant : defaultValue.getDeclaringClass().getEnumConstants()) { // Step 3: Iterate over enum constants
        if (enumConstant.name().equalsIgnoreCase(configValue)) {
          return enumConstant;
        }
      }
      // No match found
      throw new IllegalArgumentException("No enum constant " + defaultValue.getDeclaringClass().getCanonicalName() + "." + configValue);
    }
    return defaultValue;
  }

  /**
   * Returns the account-agnostic enum value if it exists, else
   * return default.
   * @param name Account-agnostic configuration key
   * @param defaultValue Value returned if none is configured
   * @param <T> Enum type
   * @return enum value if one exists, else null
   */
  public <T extends Enum<T>> T getAccountAgnosticEnum(String name, T defaultValue) {
    return rawConfig.getEnum(name, defaultValue);
  }

  /**
   * Unsets parameter in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   */
  public void unset(String key) {
    rawConfig.unset(key);
  }

  /**
   * Sets String in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   * @param value Configuration value
   */
  public void set(String key, String value) {
    rawConfig.set(key, value);
  }

  /**
   * Sets boolean in the underlying Configuration object.
   * Provided only as a convenience; does not add any account logic.
   * @param key Configuration key
   * @param value Configuration value
   */
  public void setBoolean(String key, boolean value) {
    rawConfig.setBoolean(key, value);
  }

  public boolean isSecureMode() {
    return isSecure;
  }

  public String getStorageAccountKey() throws AzureBlobFileSystemException {
    String key;
    String keyProviderClass = get(AZURE_KEY_ACCOUNT_KEYPROVIDER);
    KeyProvider keyProvider;

    if (keyProviderClass == null) {
      // No key provider was provided so use the provided key as is.
      keyProvider = new SimpleKeyProvider();
    } else {
      // create an instance of the key provider class and verify it
      // implements KeyProvider
      Object keyProviderObject;
      try {
        Class<?> clazz = rawConfig.getClassByName(keyProviderClass);
        keyProviderObject = clazz.newInstance();
      } catch (Exception e) {
        throw new KeyProviderException("Unable to load key provider class.", e);
      }
      if (!(keyProviderObject instanceof KeyProvider)) {
        throw new KeyProviderException(keyProviderClass
                + " specified in config is not a valid KeyProvider class.");
      }
      keyProvider = (KeyProvider) keyProviderObject;
    }
    key = keyProvider.getStorageAccountKey(accountName, rawConfig);

    if (key == null) {
      throw new ConfigurationPropertyNotFoundException(accountName);
    }

    return key;
  }

  public Configuration getRawConfiguration() {
    return this.rawConfig;
  }

  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }

  public boolean isSmallWriteOptimizationEnabled() {
    return this.enableSmallWriteOptimization;
  }

  public boolean readSmallFilesCompletely() {
    return this.readSmallFilesCompletely;
  }

  public boolean optimizeFooterRead() {
    return this.optimizeFooterRead;
  }

  public int getFooterReadBufferSize() {
    return this.footerReadBufferSize;
  }

  public int getReadBufferSize() {
    return this.readBufferSize;
  }

  public int getMinBackoffIntervalMilliseconds() {
    return this.minBackoffInterval;
  }

  public int getMaxBackoffIntervalMilliseconds() {
    return this.maxBackoffInterval;
  }

  public boolean getStaticRetryForConnectionTimeoutEnabled() {
    return staticRetryForConnectionTimeoutEnabled;
  }

  public int getStaticRetryInterval() {
    return staticRetryInterval;
  }

  public int getBackoffIntervalMilliseconds() {
    return this.backoffInterval;
  }

  public int getMaxIoRetries() {
    return this.maxIoRetries;
  }

  public int getCustomTokenFetchRetryCount() {
    return this.customTokenFetchRetryCount;
  }

  public int getHttpConnectionTimeout() {
    return this.httpConnectionTimeout;
  }

  public int getHttpReadTimeout() {
    return this.httpReadTimeout;
  }

  public long getAzureBlockSize() {
    return this.azureBlockSize;
  }

  public boolean isCheckAccessEnabled() {
    return this.isCheckAccessEnabled;
  }

  public long getSasTokenRenewPeriodForStreamsInSeconds() {
    return this.sasTokenRenewPeriodForStreamsInSeconds;
  }

  public String getAzureBlockLocationHost() {
    return this.azureBlockLocationHost;
  }

  public int getMaxConcurrentWriteThreads() {
    return this.maxConcurrentWriteThreads;
  }

  public int getMaxConcurrentReadThreads() {
    return this.maxConcurrentReadThreads;
  }

  public int getListMaxResults() {
    return this.listMaxResults;
  }

  public boolean getTolerateOobAppends() {
    return this.tolerateOobAppends;
  }

  public String getAzureAtomicRenameDirs() {
    return this.azureAtomicDirs;
  }

  public boolean isConditionalCreateOverwriteEnabled() {
    return this.enableConditionalCreateOverwrite;
  }

  public boolean isEnabledMkdirOverwrite() {
    return mkdirOverwrite;
  }

  public String getAppendBlobDirs() {
    return this.azureAppendBlobDirs;
  }

  public boolean isExpectHeaderEnabled() {
    return this.isExpectHeaderEnabled;
  }

  public boolean accountThrottlingEnabled() {
    return accountThrottlingEnabled;
  }

  public String getAzureInfiniteLeaseDirs() {
    return this.azureInfiniteLeaseDirs;
  }

  public int getNumLeaseThreads() {
    return this.numLeaseThreads;
  }

  public boolean getCreateRemoteFileSystemDuringInitialization() {
    // we do not support creating the filesystem when AuthType is SAS
    return this.createRemoteFileSystemDuringInitialization
        && this.getAuthType(this.accountName) != AuthType.SAS;
  }

  public boolean getSkipUserGroupMetadataDuringInitialization() {
    return this.skipUserGroupMetadataDuringInitialization;
  }

  public int getReadAheadQueueDepth() {
    return this.readAheadQueueDepth;
  }

  public int getReadAheadBlockSize() {
    return this.readAheadBlockSize;
  }

  public boolean shouldReadBufferSizeAlways() {
    return this.alwaysReadBufferSize;
  }

  public boolean isFlushEnabled() {
    return this.enableFlush;
  }

  public boolean isOutputStreamFlushDisabled() {
    return this.disableOutputStreamFlush;
  }

  public boolean isAutoThrottlingEnabled() {
    return this.enableAutoThrottling;
  }

  public int getMetricIdleTimeout() {
    return this.metricIdleTimeout;
  }

  public int getMetricAnalysisTimeout() {
    return this.metricAnalysisTimeout;
  }

  public String getMetricUri() {
    return metricUri;
  }

  public String getMetricAccount() {
    return metricAccount;
  }

  public String getMetricAccountKey() {
    return metricAccountKey;
  }

  public int getAccountOperationIdleTimeout() {
    return accountOperationIdleTimeout;
  }

  public int getAnalysisPeriod() {
    return analysisPeriod;
  }

  public int getRateLimit() {
    return rateLimit;
  }

  public String getCustomUserAgentPrefix() {
    return this.userAgentId;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getClusterType() {
    return this.clusterType;
  }

  public DelegatingSSLSocketFactory.SSLChannelMode getPreferredSSLFactoryOption() {
    return getEnum(FS_AZURE_SSL_CHANNEL_MODE_KEY, DEFAULT_FS_AZURE_SSL_CHANNEL_MODE);
  }

  /**
   * @return Config to select netlib for server communication.
   */
  public HttpOperationType getPreferredHttpOperationType() {
    return getEnum(FS_AZURE_NETWORKING_LIBRARY, DEFAULT_NETWORKING_LIBRARY);
  }

  public int getMaxApacheHttpClientIoExceptionsRetries() {
    return maxApacheHttpClientIoExceptionsRetries;
  }

  /**
   * @return {@link #maxApacheHttpClientConnectionIdleTime}.
   */
  public long getMaxApacheHttpClientConnectionIdleTime() {
    return maxApacheHttpClientConnectionIdleTime;
  }

  public boolean getIsClientTransactionIdEnabled() {
    return enableClientTransactionId;
  }

  /**
   * Enum config to allow user to pick format of x-ms-client-request-id header
   * @return tracingContextFormat config if valid, else default ALL_ID_FORMAT
   */
  public TracingHeaderFormat getTracingHeaderFormat() {
    return getEnum(FS_AZURE_TRACINGHEADER_FORMAT, TracingHeaderFormat.ALL_ID_FORMAT);
  }

  public MetricFormat getMetricFormat() {
    return getEnum(FS_AZURE_METRIC_FORMAT, MetricFormat.EMPTY);
  }

  public AuthType getAuthType(String accountName) {
    return getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
  }

  public boolean isDelegationTokenManagerEnabled() {
    return enableDelegationToken;
  }

  public AbfsDelegationTokenManager getDelegationTokenManager() throws IOException {
    return new AbfsDelegationTokenManager(getRawConfiguration());
  }

  public boolean isHttpsAlwaysUsed() {
    return this.alwaysUseHttps;
  }

  public boolean isUpnUsed() {
    return this.useUpn;
  }

  /**
   * Whether {@code AbfsClient} should track and send latency info back to storage servers.
   *
   * @return a boolean indicating whether latency should be tracked.
   */
  public boolean shouldTrackLatency() {
    return this.trackLatency;
  }

  public AccessTokenProvider getTokenProvider() throws TokenAccessProviderException {
    AuthType authType = getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    if (authType == AuthType.OAuth) {
      try {
        Class<? extends AccessTokenProvider> tokenProviderClass =
            getTokenProviderClass(authType,
            FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, null,
            AccessTokenProvider.class);

        AccessTokenProvider tokenProvider;
        if (tokenProviderClass == ClientCredsTokenProvider.class) {
          String authEndpoint =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String clientId =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          String clientSecret =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET);
          tokenProvider = new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
          LOG.trace("ClientCredsTokenProvider initialized");
        } else if (tokenProviderClass == UserPasswordTokenProvider.class) {
          String authEndpoint =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String username =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_NAME);
          String password =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD);
          tokenProvider = new UserPasswordTokenProvider(authEndpoint, username, password);
          LOG.trace("UserPasswordTokenProvider initialized");
        } else if (tokenProviderClass == MsiTokenProvider.class) {
          String authEndpoint = getTrimmedPasswordString(
              FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
          String tenantGuid =
              getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
          String clientId =
              getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          String authority = getTrimmedPasswordString(
              FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY);
          authority = appendSlashIfNeeded(authority);
          tokenProvider = new MsiTokenProvider(authEndpoint, tenantGuid,
              clientId, authority);
          LOG.trace("MsiTokenProvider initialized");
        } else if (tokenProviderClass == RefreshTokenBasedTokenProvider.class) {
          String authEndpoint = getTrimmedPasswordString(
              FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT);
          String refreshToken =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN);
          String clientId =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          tokenProvider = new RefreshTokenBasedTokenProvider(authEndpoint,
              clientId, refreshToken);
          LOG.trace("RefreshTokenBasedTokenProvider initialized");
        } else if (tokenProviderClass == WorkloadIdentityTokenProvider.class) {
          String authority = appendSlashIfNeeded(
              getTrimmedPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY));
          String tenantGuid =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
          String clientId =
              getMandatoryPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          String tokenFile =
              getTrimmedPasswordString(FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE);
          tokenProvider = new WorkloadIdentityTokenProvider(
              authority, tenantGuid, clientId, tokenFile);
          LOG.trace("WorkloadIdentityTokenProvider initialized");
        } else {
          throw new IllegalArgumentException("Failed to initialize " + tokenProviderClass);
        }
        return tokenProvider;
      } catch(IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new TokenAccessProviderException("Unable to load OAuth token provider class.", e);
      }

    } else if (authType == AuthType.Custom) {
      try {
        String configKey = FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;

        Class<? extends CustomTokenProviderAdaptee> customTokenProviderClass
            = getTokenProviderClass(authType, configKey, null,
            CustomTokenProviderAdaptee.class);

        if (customTokenProviderClass == null) {
          throw new IllegalArgumentException(
                  String.format("The configuration value for \"%s\" is invalid.", configKey));
        }
        CustomTokenProviderAdaptee azureTokenProvider = ReflectionUtils
                .newInstance(customTokenProviderClass, rawConfig);
        if (azureTokenProvider == null) {
          throw new IllegalArgumentException("Failed to initialize " + customTokenProviderClass);
        }
        LOG.trace("Initializing {}", customTokenProviderClass.getName());
        azureTokenProvider.initialize(rawConfig, accountName);
        LOG.trace("{} init complete", customTokenProviderClass.getName());
        return new CustomTokenProviderAdapter(azureTokenProvider, getCustomTokenFetchRetryCount());
      } catch(IllegalArgumentException e) {
        throw e;
      } catch (Exception e) {
        throw new TokenAccessProviderException("Unable to load custom token provider class: " + e, e);
      }

    } else {
      throw new TokenAccessProviderException(String.format(
              "Invalid auth type: %s is being used, expecting OAuth", authType));
    }
  }

  /**
   * Returns the SASTokenProvider implementation to be used to generate SAS token.<br>
   * Users can choose between a custom implementation of {@link SASTokenProvider}
   * or an in house implementation {@link FixedSASTokenProvider}.<br>
   * For Custom implementation "fs.azure.sas.token.provider.type" needs to be provided.<br>
   * For Fixed SAS Token use "fs.azure.sas.fixed.token" needs to be provided.<br>
   * In case both are provided, Preference will be given to Custom implementation.<br>
   * Avoid using a custom tokenProvider implementation just to read the configured
   * fixed token, as this could create confusion. Also,implementing the SASTokenProvider
   * requires relying on the raw configurations. It is more stable to depend on
   * the AbfsConfiguration with which a filesystem is initialized, and eliminate
   * chances of dynamic modifications and spurious situations.<br>
   * @return sasTokenProvider object based on configurations provided
   * @throws AzureBlobFileSystemException
   */
  public SASTokenProvider getSASTokenProvider() throws AzureBlobFileSystemException {
    AuthType authType = getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    if (authType != AuthType.SAS) {
      throw new SASTokenProviderException(String.format(
          "Invalid auth type: %s is being used, expecting SAS.", authType));
    }

    try {
      Class<? extends SASTokenProvider> customSasTokenProviderImplementation =
          getTokenProviderClass(authType, FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
              null, SASTokenProvider.class);
      String configuredFixedToken = this.getTrimmedPasswordString(FS_AZURE_SAS_FIXED_TOKEN, EMPTY_STRING);

      if (customSasTokenProviderImplementation == null && configuredFixedToken.isEmpty()) {
        throw new SASTokenProviderException(String.format(
            "At least one of the \"%s\" and \"%s\" must be set.",
            FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, FS_AZURE_SAS_FIXED_TOKEN));
      }

      // Prefer Custom SASTokenProvider Implementation if configured.
      if (customSasTokenProviderImplementation != null) {
        LOG.trace("Using Custom SASTokenProvider implementation because it is given precedence when it is set.");
        SASTokenProvider sasTokenProvider = ReflectionUtils.newInstance(
            customSasTokenProviderImplementation, rawConfig);
        if (sasTokenProvider == null) {
          throw new SASTokenProviderException(String.format(
              "Failed to initialize %s", customSasTokenProviderImplementation));
        }
        LOG.trace("Initializing {}", customSasTokenProviderImplementation.getName());
        sasTokenProvider.initialize(rawConfig, accountName);
        LOG.trace("{} init complete", customSasTokenProviderImplementation.getName());
        return sasTokenProvider;
      } else {
        LOG.trace("Using FixedSASTokenProvider implementation");
        FixedSASTokenProvider fixedSASTokenProvider = new FixedSASTokenProvider(configuredFixedToken);
        return fixedSASTokenProvider;
      }
    } catch (SASTokenProviderException e) {
      throw e;
    } catch (Exception e) {
      throw new SASTokenProviderException(
          "Unable to load SAS token provider class: " + e, e);
    }
  }

  public EncryptionContextProvider createEncryptionContextProvider() {
    try {
      String configKey = FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
      if (get(configKey) == null) {
        return null;
      }
      Class<? extends EncryptionContextProvider> encryptionContextClass =
          getAccountSpecificClass(configKey, null,
              EncryptionContextProvider.class);
      Preconditions.checkArgument(encryptionContextClass != null,
          "The configuration value for %s is invalid, or config key is not account-specific",
          configKey);

      EncryptionContextProvider encryptionContextProvider =
          ReflectionUtils.newInstance(encryptionContextClass, rawConfig);
      Preconditions.checkArgument(encryptionContextProvider != null,
         "Failed to initialize %s", encryptionContextClass);

      LOG.trace("{} init complete", encryptionContextClass.getName());
      return encryptionContextProvider;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to load encryption context provider class: ", e);
    }
  }

  public boolean isReadAheadEnabled() {
    return this.enabledReadAhead;
  }

  @VisibleForTesting
  void setReadAheadEnabled(final boolean enabledReadAhead) {
    this.enabledReadAhead = enabledReadAhead;
  }

  public int getReadAheadRange() {
    return this.readAheadRange;
  }

  int validateInt(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    IntegerConfigurationValidatorAnnotation validator = field.getAnnotation(IntegerConfigurationValidatorAnnotation.class);
    String value = get(validator.ConfigurationKey());

    // validate
    return new IntegerConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  int validateIntWithOutlier(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    IntegerWithOutlierConfigurationValidatorAnnotation validator =
        field.getAnnotation(IntegerWithOutlierConfigurationValidatorAnnotation.class);
    String value = get(validator.ConfigurationKey());

    // validate
    return new IntegerConfigurationBasicValidator(
        validator.OutlierValue(),
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  long validateLong(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    LongConfigurationValidatorAnnotation validator = field.getAnnotation(LongConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new LongConfigurationBasicValidator(
        validator.MinValue(),
        validator.MaxValue(),
        validator.DefaultValue(),
        validator.ConfigurationKey(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateString(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    StringConfigurationValidatorAnnotation validator = field.getAnnotation(StringConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  String validateBase64String(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    Base64StringConfigurationValidatorAnnotation validator = field.getAnnotation((Base64StringConfigurationValidatorAnnotation.class));
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new Base64StringConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  boolean validateBoolean(Field field) throws IllegalAccessException, InvalidConfigurationValueException {
    BooleanConfigurationValidatorAnnotation validator = field.getAnnotation(BooleanConfigurationValidatorAnnotation.class);
    String value = rawConfig.get(validator.ConfigurationKey());

    // validate
    return new BooleanConfigurationBasicValidator(
        validator.ConfigurationKey(),
        validator.DefaultValue(),
        validator.ThrowIfInvalid()).validate(value);
  }

  public ExponentialRetryPolicy getOauthTokenFetchRetryPolicy() {
    return new ExponentialRetryPolicy(oauthTokenFetchRetryCount,
        oauthTokenFetchRetryMinBackoff, oauthTokenFetchRetryMaxBackoff,
        oauthTokenFetchRetryDeltaBackoff);
  }

  public int getWriteMaxConcurrentRequestCount() {
    if (this.writeMaxConcurrentRequestCount < 1) {
      return 4 * Runtime.getRuntime().availableProcessors();
    }
    return this.writeMaxConcurrentRequestCount;
  }

  public int getMaxWriteRequestsToQueue() {
    if (this.maxWriteRequestsToQueue < 1) {
      return 2 * getWriteMaxConcurrentRequestCount();
    }
    return this.maxWriteRequestsToQueue;
  }

  public boolean enableAbfsListIterator() {
    return this.enableAbfsListIterator;
  }

  public String getEncodedClientProvidedEncryptionKey() {
    if (clientProvidedEncryptionKey == null) {
      String accSpecEncKey = accountConf(
          FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY);
      clientProvidedEncryptionKey = rawConfig.get(accSpecEncKey, null);
    }
    return clientProvidedEncryptionKey;
  }

  public String getEncodedClientProvidedEncryptionKeySHA() {
    if (clientProvidedEncryptionKeySHA == null) {
      String accSpecEncKey = accountConf(
          FS_AZURE_ENCRYPTION_ENCODED_CLIENT_PROVIDED_KEY_SHA);
      clientProvidedEncryptionKeySHA = rawConfig.get(accSpecEncKey, null);
    }
    return clientProvidedEncryptionKeySHA;
  }

  @VisibleForTesting
  void setReadBufferSize(int bufferSize) {
    this.readBufferSize = bufferSize;
  }

  @VisibleForTesting
  void setWriteBufferSize(int bufferSize) {
    this.writeBufferSize = bufferSize;
  }

  @VisibleForTesting
  void setEnableFlush(boolean enableFlush) {
    this.enableFlush = enableFlush;
  }

  @VisibleForTesting
  void setDisableOutputStreamFlush(boolean disableOutputStreamFlush) {
    this.disableOutputStreamFlush = disableOutputStreamFlush;
  }

  @VisibleForTesting
  void setListMaxResults(int listMaxResults) {
    this.listMaxResults = listMaxResults;
  }

  @VisibleForTesting
  public void setMaxIoRetries(int maxIoRetries) {
    this.maxIoRetries = maxIoRetries;
  }

  @VisibleForTesting
  void setMaxBackoffIntervalMilliseconds(int maxBackoffInterval) {
    this.maxBackoffInterval = maxBackoffInterval;
  }

  @VisibleForTesting
  void setIsNamespaceEnabledAccount(String isNamespaceEnabledAccount) {
    this.isNamespaceEnabledAccount = isNamespaceEnabledAccount;
  }

  /**
   * Checks if the FixedSASTokenProvider is configured for the current account.
   *
   * @return true if the FixedSASTokenProvider is configured, false otherwise.
   */
  public boolean isFixedSASTokenProviderConfigured() {
    try {
      return getSASTokenProvider() instanceof FixedSASTokenProvider;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Failed to get SAS token provider", e);
      return false;
    }
  }

  private String getTrimmedPasswordString(String key, String defaultValue) throws IOException {
    String value = getPasswordString(key);
    if (StringUtils.isBlank(value)) {
      value = defaultValue;
    }
    return value.trim();
  }

  private String appendSlashIfNeeded(String authority) {
    if (!authority.endsWith(AbfsHttpConstants.FORWARD_SLASH)) {
      authority = authority + AbfsHttpConstants.FORWARD_SLASH;
    }
    return authority;
  }

  @VisibleForTesting
  public void setReadSmallFilesCompletely(boolean readSmallFilesCompletely) {
    this.readSmallFilesCompletely = readSmallFilesCompletely;
  }

  @VisibleForTesting
  public void setOptimizeFooterRead(boolean optimizeFooterRead) {
    this.optimizeFooterRead = optimizeFooterRead;
  }

  @VisibleForTesting
  public void setEnableAbfsListIterator(boolean enableAbfsListIterator) {
    this.enableAbfsListIterator = enableAbfsListIterator;
  }

  public boolean getRenameResilience() {
    return renameResilience;
  }

  public boolean isPaginatedDeleteEnabled() {
    return isPaginatedDeleteEnabled;
  }

  public boolean getIsChecksumValidationEnabled() {
    return isChecksumValidationEnabled;
  }

  @VisibleForTesting
  public void setIsChecksumValidationEnabled(boolean isChecksumValidationEnabled) {
    this.isChecksumValidationEnabled = isChecksumValidationEnabled;
  }

  public long getBlobCopyProgressPollWaitMillis() {
    return blobCopyProgressPollWaitMillis;
  }

  public long getBlobCopyProgressMaxWaitMillis() {
    return blobCopyProgressMaxWaitMillis;
  }

  public long getAtomicRenameLeaseRefreshDuration() {
    return blobAtomicRenameLeaseRefreshDuration;
  }

  public int getProducerQueueMaxSize() {
    return producerQueueMaxSize;
  }

  public int getListingMaxConsumptionLag() {
    return listingMaxConsumptionLag;
  }

  public int getBlobRenameDirConsumptionParallelism() {
    return blobRenameDirConsumptionParallelism;
  }

  public int getBlobDeleteDirConsumptionParallelism() {
    return blobDeleteDirConsumptionParallelism;
  }
}
