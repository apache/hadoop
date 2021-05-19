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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerWithOutlierConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.LongConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.Base64StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.BooleanConfigurationValidatorAnnotation;
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
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.KeyProvider;
import org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;

/**
 * Configuration for Azure Blob FileSystem.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AbfsConfiguration{

  private final Configuration rawConfig;
  private final String accountName;
  private final boolean isSecure;
  private static final Logger LOG = LoggerFactory.getLogger(AbfsConfiguration.class);

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
      DefaultValue = DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED)
  private String isNamespaceEnabledAccount;

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

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_READ_BUFFER_SIZE,
      MinValue = MIN_BUFFER_SIZE,
      MaxValue = MAX_BUFFER_SIZE,
      DefaultValue = DEFAULT_READ_BUFFER_SIZE)
  private int readBufferSize;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MIN_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MIN_BACKOFF_INTERVAL)
  private int minBackoffInterval;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = AZURE_MAX_BACKOFF_INTERVAL,
      DefaultValue = DEFAULT_MAX_BACKOFF_INTERVAL)
  private int maxBackoffInterval;

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

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_USER_AGENT_PREFIX_KEY,
      DefaultValue = DEFAULT_FS_AZURE_USER_AGENT_PREFIX)
  private String userAgentId;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_CLUSTER_NAME,
      DefaultValue = DEFAULT_VALUE_UNKNOWN)
  private String clusterName;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_CLUSTER_TYPE,
      DefaultValue = DEFAULT_VALUE_UNKNOWN)
  private String clusterType;

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

  @LongConfigurationValidatorAnnotation(ConfigurationKey = FS_AZURE_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS,
      MinValue = 0,
      DefaultValue = DEFAULT_SAS_TOKEN_RENEW_PERIOD_FOR_STREAMS_IN_SECONDS)
  private long sasTokenRenewPeriodForStreamsInSeconds;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey =
      FS_AZURE_ENABLE_ABFS_LIST_ITERATOR, DefaultValue = DEFAULT_ENABLE_ABFS_LIST_ITERATOR)
  private boolean enableAbfsListIterator;

  public AbfsConfiguration(final Configuration rawConfig, String accountName)
      throws IllegalAccessException, InvalidConfigurationValueException, IOException {
    this.rawConfig = ProviderUtils.excludeIncompatibleCredentialProviders(
        rawConfig, AzureBlobFileSystem.class);
    this.accountName = accountName;
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

  public Trilean getIsNamespaceEnabledAccount() {
    return Trilean.getTrilean(isNamespaceEnabledAccount);
  }

  /**
   * Gets the Azure Storage account name corresponding to this instance of configuration.
   * @return the Azure Storage account name
   */
  public String getAccountName() {
    return accountName;
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
   * Returns the account-specific password in string form if it exists, then
   * looks for an account-agnostic value.
   * @param key Account-agnostic configuration key
   * @return value in String form if one exists, else null
   * @throws IOException
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

  public int getReadBufferSize() {
    return this.readBufferSize;
  }

  public int getMinBackoffIntervalMilliseconds() {
    return this.minBackoffInterval;
  }

  public int getMaxBackoffIntervalMilliseconds() {
    return this.maxBackoffInterval;
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

        AccessTokenProvider tokenProvider = null;
        if (tokenProviderClass == ClientCredsTokenProvider.class) {
          String authEndpoint = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          String clientSecret = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET);
          tokenProvider = new ClientCredsTokenProvider(authEndpoint, clientId, clientSecret);
          LOG.trace("ClientCredsTokenProvider initialized");
        } else if (tokenProviderClass == UserPasswordTokenProvider.class) {
          String authEndpoint = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
          String username = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_NAME);
          String password = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_USER_PASSWORD);
          tokenProvider = new UserPasswordTokenProvider(authEndpoint, username, password);
          LOG.trace("UserPasswordTokenProvider initialized");
        } else if (tokenProviderClass == MsiTokenProvider.class) {
          String authEndpoint = getTrimmedPasswordString(
              FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT,
              AuthConfigurations.DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
          String tenantGuid = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
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
          String refreshToken = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN);
          String clientId = getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
          tokenProvider = new RefreshTokenBasedTokenProvider(authEndpoint,
              clientId, refreshToken);
          LOG.trace("RefreshTokenBasedTokenProvider initialized");
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

  public SASTokenProvider getSASTokenProvider() throws AzureBlobFileSystemException {
    AuthType authType = getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    if (authType != AuthType.SAS) {
      throw new SASTokenProviderException(String.format(
        "Invalid auth type: %s is being used, expecting SAS", authType));
    }

    try {
      String configKey = FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
      Class<? extends SASTokenProvider> sasTokenProviderClass =
          getTokenProviderClass(authType, configKey, null,
              SASTokenProvider.class);

      Preconditions.checkArgument(sasTokenProviderClass != null,
          String.format("The configuration value for \"%s\" is invalid.", configKey));

      SASTokenProvider sasTokenProvider = ReflectionUtils
          .newInstance(sasTokenProviderClass, rawConfig);
      Preconditions.checkArgument(sasTokenProvider != null,
          String.format("Failed to initialize %s", sasTokenProviderClass));

      LOG.trace("Initializing {}", sasTokenProviderClass.getName());
      sasTokenProvider.initialize(rawConfig, accountName);
      LOG.trace("{} init complete", sasTokenProviderClass.getName());
      return sasTokenProvider;
    } catch (Exception e) {
      throw new TokenAccessProviderException("Unable to load SAS token provider class: " + e, e);
    }
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

  public String getClientProvidedEncryptionKey() {
    String accSpecEncKey = accountConf(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY);
    return rawConfig.get(accSpecEncKey, null);
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

}
