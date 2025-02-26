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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformerInterface;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.security.ContextProviderEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.security.NoContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContextBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientRenameResult;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfInfo;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.services.AbfsPermission;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.ListingSupport;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.azurebfs.services.StaticRetryPolicy;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.CRC64;
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;
import org.apache.hadoop.fs.azurebfs.utils.EncryptionType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.impl.BackReference;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.http.client.utils.URIBuilder;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.METADATA_INCOMPLETE_RENAME_FAILURES;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_HYPHEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_UNDERSCORE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TOKEN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ABFS_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_FOOTER_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BUFFERED_PREAD_DISABLE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_IDENTITY_TRANSFORM_CLASS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.INFINITE_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.isKeyForDirectorySet;

/**
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureBlobFileSystemStore implements Closeable, ListingSupport {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystemStore.class);

  private AbfsClient client;

  /**
   * Variable to hold the client handler which will determine the operative
   * client based on the service type configured.
   * Initialized in the {@link #initializeClient(URI, String, String, boolean)}.
   */
  private AbfsClientHandler clientHandler;
  private URI uri;
  private String userName;
  private String primaryUserGroup;
  private static final String TOKEN_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int GET_SET_AGGREGATE_COUNT = 2;

  private final Map<AbfsLease, Object> leaseRefs;

  private final AbfsConfiguration abfsConfiguration;
  private Set<String> azureInfiniteLeaseDirSet;
  private volatile Trilean isNamespaceEnabled;
  private final AuthType authType;
  private final UserGroupInformation userGroupInformation;
  private final IdentityTransformerInterface identityTransformer;
  private final AbfsPerfTracker abfsPerfTracker;
  private final AbfsCounters abfsCounters;

  /**
   * The set of directories where we should store files as append blobs.
   */
  private Set<String> appendBlobDirSet;

  /** BlockFactory being used by this instance.*/
  private DataBlocks.BlockFactory blockFactory;
  /** Number of active data blocks per AbfsOutputStream */
  private int blockOutputActiveBlocks;
  /** Bounded ThreadPool for this instance. */
  private ExecutorService boundedThreadPool;

  /** ABFS instance reference to be held by the store to avoid GC close. */
  private BackReference fsBackRef;

  /**
   * FileSystem Store for {@link AzureBlobFileSystem} for Abfs operations.
   * Built using the {@link AzureBlobFileSystemStoreBuilder} with parameters
   * required.
   * @param abfsStoreBuilder Builder for AzureBlobFileSystemStore.
   * @throws IOException Throw IOE in case of failure during constructing.
   */
  public AzureBlobFileSystemStore(
      AzureBlobFileSystemStoreBuilder abfsStoreBuilder) throws IOException {
    this.uri = abfsStoreBuilder.uri;
    String[] authorityParts = authorityParts(uri);
    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];
    this.fsBackRef = abfsStoreBuilder.fsBackRef;

    leaseRefs = Collections.synchronizedMap(new WeakHashMap<>());

    try {
      this.abfsConfiguration = new AbfsConfiguration(abfsStoreBuilder.configuration,
          accountName, getAbfsServiceTypeFromUrl());
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }

    LOG.trace("AbfsConfiguration init complete");

    this.isNamespaceEnabled = abfsConfiguration.getIsNamespaceEnabledAccount();

    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.userName = userGroupInformation.getShortUserName();
    LOG.trace("UGI init complete");
    if (!abfsConfiguration.getSkipUserGroupMetadataDuringInitialization()) {
      try {
        this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();
      } catch (IOException ex) {
        LOG.error("Failed to get primary group for {}, using user name as primary group name", userName);
        this.primaryUserGroup = userName;
      }
    } else {
      //Provide a default group name
      this.primaryUserGroup = userName;
    }
    LOG.trace("primaryUserGroup is {}", this.primaryUserGroup);

    updateInfiniteLeaseDirs();
    this.authType = abfsConfiguration.getAuthType(accountName);
    boolean usingOauth = (authType == AuthType.OAuth);
    boolean useHttps = (usingOauth || abfsConfiguration.isHttpsAlwaysUsed()) ? true : abfsStoreBuilder.isSecureScheme;
    this.abfsPerfTracker = new AbfsPerfTracker(fileSystemName, accountName, this.abfsConfiguration);
    this.abfsCounters = abfsStoreBuilder.abfsCounters;
    initializeClient(uri, fileSystemName, accountName, useHttps);
    final Class<? extends IdentityTransformerInterface> identityTransformerClass =
        abfsStoreBuilder.configuration.getClass(FS_AZURE_IDENTITY_TRANSFORM_CLASS, IdentityTransformer.class,
            IdentityTransformerInterface.class);
    try {
      this.identityTransformer =
          identityTransformerClass.getConstructor(Configuration.class).newInstance(abfsStoreBuilder.configuration);
    } catch (IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException(e);
    }
    LOG.trace("IdentityTransformer init complete");

    // Extract the directories that should contain append blobs
    String appendBlobDirs = abfsConfiguration.getAppendBlobDirs();
    if (appendBlobDirs.trim().isEmpty()) {
      this.appendBlobDirSet = new HashSet<String>();
    } else {
      this.appendBlobDirSet = new HashSet<>(Arrays.asList(
          abfsConfiguration.getAppendBlobDirs().split(AbfsHttpConstants.COMMA)));
    }
    this.blockFactory = abfsStoreBuilder.blockFactory;
    this.blockOutputActiveBlocks = abfsStoreBuilder.blockOutputActiveBlocks;
    this.boundedThreadPool = BlockingThreadPoolExecutorService.newInstance(
        abfsConfiguration.getWriteMaxConcurrentRequestCount(),
        abfsConfiguration.getMaxWriteRequestsToQueue(),
        10L, TimeUnit.SECONDS,
        "abfs-bounded");
  }

  /**
   * Updates the client with the namespace information.
   *
   * @param tracingContext the tracing context to be used for the operation
   * @throws AzureBlobFileSystemException if an error occurs while updating the client
   */
  public void updateClientWithNamespaceInfo(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
    AbfsClient.setIsNamespaceEnabled(isNamespaceEnabled);
  }

  /**
   * Checks if the given key in Azure Storage should be stored as a page
   * blob instead of block blob.
   * @param key The key to check.
   * @return True if the key should be stored as a page blob, false otherwise.
   */
  public boolean isAppendBlobKey(String key) {
    return isKeyForDirectorySet(key, appendBlobDirSet);
  }

  /**
   * @return local user name.
   * */
  public String getUser() {
    return this.userName;
  }

  /**
  * @return primary group that user belongs to.
  * */
  public String getPrimaryGroup() {
    return this.primaryUserGroup;
  }

  @Override
  public void close() throws IOException {
    List<ListenableFuture<?>> futures = new ArrayList<>();
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease == null) {
        continue;
      }
      ListenableFuture<?> future = getClient().submit(() -> lease.free());
      futures.add(future);
    }
    try {
      Futures.allAsList(futures).get();
      // shutdown the threadPool and set it to null.
      HadoopExecutors.shutdown(boundedThreadPool, LOG,
          30, TimeUnit.SECONDS);
      boundedThreadPool = null;
    } catch (InterruptedException e) {
      LOG.error("Interrupted freeing leases", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Error freeing leases", e);
    } finally {
      IOUtils.cleanupWithLogger(LOG, getClient());
    }
  }

  byte[] encodeAttribute(String value) throws UnsupportedEncodingException {
    // DFS Client works with ISO_8859_1 encoding, Blob Works with UTF-8.
    return getClient().encodeAttribute(value);
  }

  String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    // DFS Client works with ISO_8859_1 encoding, Blob Works with UTF-8.
    return getClient().decodeAttribute(value);
  }

  private String[] authorityParts(URI uri) throws InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
              .format("'%s' has a malformed authority, expected container name. "
                      + "Authority takes the form "
                      + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
                      uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }

  /**
   * Resolves namespace information of the filesystem from the state of {@link #isNamespaceEnabled}.
   * if the state is UNKNOWN, it will be determined by making a GET_ACL request
   * to the root of the filesystem. GET_ACL call is synchronized to ensure a single
   * call is made to determine the namespace information in case multiple threads are
   * calling this method at the same time. The resolution of namespace information
   * would be stored back as state of {@link #isNamespaceEnabled}.
   *
   * @param tracingContext tracing context
   * @return true if namespace is enabled, false otherwise.
   * @throws AzureBlobFileSystemException server errors.
   */
  public boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try {
      return isNamespaceEnabled();
    } catch (TrileanConversionException e) {
      LOG.debug("isNamespaceEnabled is UNKNOWN; fall back and determine through"
          + " getAcl server call", e);
    }

    return getNamespaceEnabledInformationFromServer(tracingContext);
  }

  private synchronized boolean getNamespaceEnabledInformationFromServer(
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (isNamespaceEnabled != Trilean.UNKNOWN) {
      return isNamespaceEnabled.toBoolean();
    }
    try {
      LOG.debug("Get root ACL status");
      getClient(AbfsServiceType.DFS).getAclStatus(AbfsHttpConstants.ROOT_PATH, tracingContext);
      // If getAcl succeeds, namespace is enabled.
      isNamespaceEnabled = Trilean.getTrilean(true);
    } catch (AbfsRestOperationException ex) {
      // Get ACL status is a HEAD request, its response doesn't contain errorCode
      // So can only rely on its status code to determine account type.
      if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
        // If getAcl fails with anything other than 400, namespace is enabled.
        isNamespaceEnabled = Trilean.getTrilean(true);
        // Continue to throw exception as earlier.
        LOG.debug("Failed to get ACL status with non 400. Inferring namespace enabled", ex);
        throw ex;
      }
      // If getAcl fails with 400, namespace is disabled.
      LOG.debug("Failed to get ACL status with 400. "
          + "Inferring namespace disabled and ignoring error", ex);
      isNamespaceEnabled = Trilean.getTrilean(false);
    } catch (AzureBlobFileSystemException ex) {
      throw ex;
    }
    return isNamespaceEnabled.toBoolean();
  }

  /**
   * @return true if namespace is enabled, false otherwise.
   * @throws TrileanConversionException if namespaceEnabled information is UNKNOWN
   */
  @VisibleForTesting
  boolean isNamespaceEnabled() throws TrileanConversionException {
    return this.isNamespaceEnabled.toBoolean();
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);

    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfiguration.get(AZURE_ABFS_ENDPOINT);
    if (endPoint == null || !endPoint.contains(AbfsHttpConstants.COLON)) {
      uriBuilder.setHost(hostName);
      return uriBuilder;
    }

    // Split ip and port
    String[] data = endPoint.split(AbfsHttpConstants.COLON);
    if (data.length != 2) {
      throw new RuntimeException(String.format("ABFS endpoint is not set correctly : %s, "
              + "Do not specify scheme when using {IP}:{PORT}", endPoint));
    }
    uriBuilder.setHost(data[0].trim());
    uriBuilder.setPort(Integer.parseInt(data[1].trim()));
    uriBuilder.setPath("/" + UriUtils.extractAccountNameFromHostName(hostName));

    return uriBuilder;
  }

  public AbfsConfiguration getAbfsConfiguration() {
    return this.abfsConfiguration;
  }

  public Hashtable<String, String> getFilesystemProperties(
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getFilesystemProperties",
            "getFilesystemProperties")) {
      LOG.debug("getFilesystemProperties for filesystem: {}",
              getClient().getFileSystem());

      final Hashtable<String, String> parsedXmsProperties;

      final AbfsRestOperation op = getClient()
          .getFilesystemProperties(tracingContext);
      perfInfo.registerResult(op.getResult());

      // Handling difference in request headers formats between DFS and Blob Clients.
      parsedXmsProperties = getClient().getXMSProperties(op.getResult());
      perfInfo.registerSuccess(true);

      return parsedXmsProperties;
    }
  }

  public void setFilesystemProperties(
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (properties == null || properties.isEmpty()) {
      LOG.trace("setFilesystemProperties no properties present");
      return;
    }

    LOG.debug("setFilesystemProperties for filesystem: {} with properties: {}",
            getClient().getFileSystem(),
            properties);

    try (AbfsPerfInfo perfInfo = startTracking("setFilesystemProperties",
            "setFilesystemProperties")) {

      final AbfsRestOperation op = getClient()
          .setFilesystemProperties(properties, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public Hashtable<String, String> getPathStatus(final Path path,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("getPathStatus", "getPathStatus")){
      LOG.debug("getPathStatus for filesystem: {} path: {}",
              getClient().getFileSystem(),
              path);

      final Hashtable<String, String> parsedXmsProperties;
      final String relativePath = getRelativePath(path);
      final ContextEncryptionAdapter contextEncryptionAdapter
          = createEncryptionAdapterFromServerStoreContext(relativePath,
          tracingContext);
      final AbfsRestOperation op = getClient()
          .getPathStatus(relativePath, true, tracingContext,
              contextEncryptionAdapter);
      perfInfo.registerResult(op.getResult());
      contextEncryptionAdapter.destroy();

      // Handling difference in request headers formats between DFS and Blob Clients.
      parsedXmsProperties = getClient().getXMSProperties(op.getResult());
      perfInfo.registerSuccess(true);

      return parsedXmsProperties;
    }
  }

  /**
   * Creates an object of {@link ContextEncryptionAdapter}
   * from a file path. It calls {@link  org.apache.hadoop.fs.azurebfs.services.AbfsClient
   * #getPathStatus(String, boolean, TracingContext, EncryptionAdapter)} method to get
   * contextValue (x-ms-encryption-context) from the server. The contextValue is passed
   * to the constructor of EncryptionAdapter to create the required object of
   * EncryptionAdapter.
   * @param path Path of the file for which the object of EncryptionAdapter is required.
   * @return <ul>
   *   <li>
   *     {@link NoContextEncryptionAdapter}: if encryptionType is not of type
   *     {@link org.apache.hadoop.fs.azurebfs.utils.EncryptionType#ENCRYPTION_CONTEXT}.
   *   </li>
   *   <li>
   *     new object of {@link ContextProviderEncryptionAdapter} containing required encryptionKeys for the give file:
   *     if encryptionType is of type {@link org.apache.hadoop.fs.azurebfs.utils.EncryptionType#ENCRYPTION_CONTEXT}.
   *   </li>
   * </ul>
   */
  private ContextEncryptionAdapter createEncryptionAdapterFromServerStoreContext(final String path,
      final TracingContext tracingContext) throws IOException {
    if (getClient().getEncryptionType() != EncryptionType.ENCRYPTION_CONTEXT) {
      return NoContextEncryptionAdapter.getInstance();
    }
    final String responseHeaderEncryptionContext = getClient().getPathStatus(path,
            false, tracingContext, null).getResult()
        .getResponseHeader(X_MS_ENCRYPTION_CONTEXT);
    if (responseHeaderEncryptionContext == null) {
      throw new PathIOException(path,
          "EncryptionContext not present in GetPathStatus response");
    }
    byte[] encryptionContext = responseHeaderEncryptionContext.getBytes(
        StandardCharsets.UTF_8);

    try {
      return new ContextProviderEncryptionAdapter(getClient().getEncryptionContextProvider(),
          new Path(path).toUri().getPath(), encryptionContext);
    } catch (IOException e) {
      LOG.debug("Could not initialize EncryptionAdapter");
      throw e;
    }
  }

  public void setPathProperties(final Path path,
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("setPathProperties", "setPathProperties")){
      LOG.debug("setPathProperties for filesystem: {} path: {} with properties: {}",
              getClient().getFileSystem(),
              path,
              properties);


      final String relativePath = getRelativePath(path);
      final ContextEncryptionAdapter contextEncryptionAdapter
          = createEncryptionAdapterFromServerStoreContext(relativePath,
          tracingContext);
      final AbfsRestOperation op = getClient()
          .setPathProperties(getRelativePath(path), properties,
              tracingContext, contextEncryptionAdapter);
      contextEncryptionAdapter.destroy();
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("createFilesystem", "createFilesystem")){
      LOG.debug("createFilesystem for filesystem: {}",
              getClient().getFileSystem());

      final AbfsRestOperation op = getClient().createFilesystem(tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("deleteFilesystem", "deleteFilesystem")) {
      LOG.debug("deleteFilesystem for filesystem: {}",
              getClient().getFileSystem());

      final AbfsRestOperation op = getClient().deleteFilesystem(tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  /**
   * Checks existence of parent of the given path.
   *
   * @param path Path to check.
   * @param statistics FileSystem statistics.
   * @param overwrite Overwrite flag.
   * @param permission Permission of tha path.
   * @param umask Umask of the path.
   * @param tracingContext tracing context
   *
   * @return OutputStream output stream of the created file.
   * @throws IOException if there is an issue with the operation.
   */
  public OutputStream createNonRecursive(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      final FsPermission permission, final FsPermission umask,
      TracingContext tracingContext)
      throws IOException {
    LOG.debug("CreateNonRecursive for filesystem: {} path: {} overwrite: {} permission: {} umask: {}",
            getClient().getFileSystem(),
            path,
            overwrite,
            permission,
            umask);
    getClient().createNonRecursivePreCheck(path.getParent(),
        tracingContext);
   return createFile(path, statistics, overwrite, permission,
       umask, tracingContext);
  }

  public OutputStream createFile(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      final FsPermission permission, final FsPermission umask,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("createFile", "createPath")) {
      AbfsClient createClient = getClientHandler().getIngressClient();
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("createFile filesystem: {} path: {} overwrite: {} permission: {} umask: {} isNamespaceEnabled: {}",
          createClient.getFileSystem(),
          path,
          overwrite,
          permission,
          umask,
          isNamespaceEnabled);

      String relativePath = getRelativePath(path);
      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      // if "fs.azure.enable.conditional.create.overwrite" is enabled and
      // is a create request with overwrite=true, create will follow different
      // flow.
      boolean triggerConditionalCreateOverwrite = false;
      if (overwrite
          && abfsConfiguration.isConditionalCreateOverwriteEnabled()) {
        triggerConditionalCreateOverwrite = true;
      }

      final ContextEncryptionAdapter contextEncryptionAdapter;
      if (createClient.getEncryptionType() == EncryptionType.ENCRYPTION_CONTEXT) {
        contextEncryptionAdapter = new ContextProviderEncryptionAdapter(
            createClient.getEncryptionContextProvider(), getRelativePath(path));
      } else {
        contextEncryptionAdapter = NoContextEncryptionAdapter.getInstance();
      }
      AbfsRestOperation op;
      if (triggerConditionalCreateOverwrite) {
        op = conditionalCreateOverwriteFile(relativePath,
            statistics,
            new Permissions(isNamespaceEnabled, permission, umask),
            isAppendBlob,
            contextEncryptionAdapter,
            tracingContext
        );

      } else {
        op = createClient.createPath(relativePath, true,
            overwrite,
            new Permissions(isNamespaceEnabled, permission, umask),
            isAppendBlob,
            null,
            contextEncryptionAdapter,
            tracingContext);

      }
      perfInfo.registerResult(op.getResult()).registerSuccess(true);

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);
      String eTag = extractEtagHeader(op.getResult());
      return new AbfsOutputStream(
          populateAbfsOutputStreamContext(
              isAppendBlob,
              lease,
              getClientHandler(),
              statistics,
              relativePath,
              0,
              eTag,
              contextEncryptionAdapter,
              tracingContext));
    }
  }

  /**
   * Conditional create overwrite flow ensures that create overwrites is done
   * only if there is match for eTag of existing file.
   * @param relativePath
   * @param statistics
   * @param permissions contains permission and umask
   * @param isAppendBlob
   * @return
   * @throws AzureBlobFileSystemException
   */
  private AbfsRestOperation conditionalCreateOverwriteFile(final String relativePath,
      final FileSystem.Statistics statistics,
      final Permissions permissions,
      final boolean isAppendBlob,
      final ContextEncryptionAdapter contextEncryptionAdapter,
      final TracingContext tracingContext) throws IOException {
    AbfsRestOperation op;
    AbfsClient createClient = getClientHandler().getIngressClient();
    op = createClient.conditionalCreateOverwriteFile(relativePath, statistics,
        permissions, isAppendBlob, contextEncryptionAdapter, tracingContext);
    return op;
  }

  /**
   * Method to populate AbfsOutputStreamContext with different parameters to
   * be used to construct {@link AbfsOutputStream}.
   *
   * @param isAppendBlob   is Append blob support enabled?
   * @param lease          instance of AbfsLease for this AbfsOutputStream.
   * @param clientHandler  AbfsClientHandler.
   * @param statistics     FileSystem statistics.
   * @param path           Path for AbfsOutputStream.
   * @param position       Position or offset of the file being opened, set to 0
   *                       when creating a new file, but needs to be set for APPEND
   *                       calls on the same file.
   * @param eTag           eTag of the file.
   * @param tracingContext instance of TracingContext for this AbfsOutputStream.
   * @return AbfsOutputStreamContext instance with the desired parameters.
   */
  private AbfsOutputStreamContext populateAbfsOutputStreamContext(
      boolean isAppendBlob,
      AbfsLease lease,
      AbfsClientHandler clientHandler,
      FileSystem.Statistics statistics,
      String path,
      long position,
      String eTag,
      ContextEncryptionAdapter contextEncryptionAdapter,
      TracingContext tracingContext) {
    int bufferSize = abfsConfiguration.getWriteBufferSize();
    if (isAppendBlob && bufferSize > FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE) {
      bufferSize = FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
    }
    return new AbfsOutputStreamContext(abfsConfiguration.getSasTokenRenewPeriodForStreamsInSeconds())
            .withWriteBufferSize(bufferSize)
            .enableExpectHeader(abfsConfiguration.isExpectHeaderEnabled())
            .enableFlush(abfsConfiguration.isFlushEnabled())
            .enableSmallWriteOptimization(abfsConfiguration.isSmallWriteOptimizationEnabled())
            .disableOutputStreamFlush(abfsConfiguration.isOutputStreamFlushDisabled())
            .withStreamStatistics(new AbfsOutputStreamStatisticsImpl())
            .withAppendBlob(isAppendBlob)
            .withWriteMaxConcurrentRequestCount(abfsConfiguration.getWriteMaxConcurrentRequestCount())
            .withMaxWriteRequestsToQueue(abfsConfiguration.getMaxWriteRequestsToQueue())
            .withLease(lease)
            .withEncryptionAdapter(contextEncryptionAdapter)
            .withBlockFactory(getBlockFactory())
            .withBlockOutputActiveBlocks(blockOutputActiveBlocks)
            .withClientHandler(clientHandler)
            .withPosition(position)
            .withFsStatistics(statistics)
            .withPath(path)
            .withExecutorService(new SemaphoredDelegatingExecutor(boundedThreadPool,
                blockOutputActiveBlocks, true))
            .withTracingContext(tracingContext)
            .withAbfsBackRef(fsBackRef)
            .withIngressServiceType(abfsConfiguration.getIngressServiceType())
            .withDFSToBlobFallbackEnabled(abfsConfiguration.isDfsToBlobFallbackEnabled())
            .withETag(eTag)
            .build();
  }

  /**
   * Creates a directory.
   *
   * @param path Path of the directory to create.
   * @param permission Permission of the directory.
   * @param umask Umask of the directory.
   * @param tracingContext tracing context
   *
   * @throws AzureBlobFileSystemException server error.
   */
  public void createDirectory(final Path path, final FsPermission permission,
      final FsPermission umask, TracingContext tracingContext)
      throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("createDirectory", "createPath")) {
      AbfsClient createClient = getClientHandler().getIngressClient();
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("createDirectory filesystem: {} path: {} permission: {} umask: {} isNamespaceEnabled: {}",
              createClient.getFileSystem(),
              path,
              permission,
              umask,
              isNamespaceEnabled);

      boolean overwrite =
          !isNamespaceEnabled || abfsConfiguration.isEnabledMkdirOverwrite();
      Permissions permissions = new Permissions(isNamespaceEnabled,
          permission, umask);
      final AbfsRestOperation op = createClient.createPath(getRelativePath(path),
          false, overwrite, permissions, false, null, null, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public AbfsInputStream openFileForRead(final Path path,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws IOException {
    return openFileForRead(path, Optional.empty(), statistics,
        tracingContext);
  }

  public AbfsInputStream openFileForRead(Path path,
      final Optional<OpenFileParameters> parameters,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForRead",
        "getPathStatus")) {
      LOG.debug("openFileForRead filesystem: {} path: {}",
          getClient().getFileSystem(), path);

      FileStatus fileStatus = parameters.map(OpenFileParameters::getStatus)
          .orElse(null);
      String relativePath = getRelativePath(path);
      String resourceType, eTag;
      long contentLength;
      ContextEncryptionAdapter contextEncryptionAdapter = NoContextEncryptionAdapter.getInstance();
      /*
      * GetPathStatus API has to be called in case of:
      *   1.  fileStatus is null or not an object of VersionedFileStatus: as eTag
      *       would not be there in the fileStatus object.
      *   2.  fileStatus is an object of VersionedFileStatus and the object doesn't
      *       have encryptionContext field when client's encryptionType is
      *       ENCRYPTION_CONTEXT.
      */
      if ((fileStatus instanceof VersionedFileStatus) && (
          getClient().getEncryptionType() != EncryptionType.ENCRYPTION_CONTEXT
              || ((VersionedFileStatus) fileStatus).getEncryptionContext()
              != null)) {
        path = path.makeQualified(this.uri, path);
        Preconditions.checkArgument(fileStatus.getPath().equals(path),
            String.format(
                "Filestatus path [%s] does not match with given path [%s]",
                fileStatus.getPath(), path));
        resourceType = fileStatus.isFile() ? FILE : DIRECTORY;
        contentLength = fileStatus.getLen();
        eTag = ((VersionedFileStatus) fileStatus).getVersion();
        final String encryptionContext
            = ((VersionedFileStatus) fileStatus).getEncryptionContext();
        if (getClient().getEncryptionType() == EncryptionType.ENCRYPTION_CONTEXT) {
          contextEncryptionAdapter = new ContextProviderEncryptionAdapter(
              getClient().getEncryptionContextProvider(), getRelativePath(path),
              encryptionContext.getBytes(StandardCharsets.UTF_8));
        }
      } else {
        AbfsHttpOperation op = getClient().getPathStatus(relativePath, false,
            tracingContext, null).getResult();
        resourceType = getClient().checkIsDir(op) ? DIRECTORY : FILE;
        contentLength = extractContentLength(op);
        eTag = op.getResponseHeader(HttpHeaderConfigurations.ETAG);
        /*
         * For file created with ENCRYPTION_CONTEXT, client shall receive
         * encryptionContext from header field: X_MS_ENCRYPTION_CONTEXT.
         */
        if (getClient().getEncryptionType() == EncryptionType.ENCRYPTION_CONTEXT) {
          final String fileEncryptionContext = op.getResponseHeader(
              HttpHeaderConfigurations.X_MS_ENCRYPTION_CONTEXT);
          if (fileEncryptionContext == null) {
            LOG.debug("EncryptionContext missing in GetPathStatus response");
            throw new PathIOException(path.toString(),
                "EncryptionContext not present in GetPathStatus response headers");
          }
          contextEncryptionAdapter = new ContextProviderEncryptionAdapter(
              getClient().getEncryptionContextProvider(), getRelativePath(path),
              fileEncryptionContext.getBytes(StandardCharsets.UTF_8));
        }
      }

      if (parseIsDirectory(resourceType)) {
        throw new AbfsRestOperationException(
            AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
            AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
            "openFileForRead must be used with files and not directories",
            null);
      }

      perfInfo.registerSuccess(true);

      // Add statistics for InputStream
      return new AbfsInputStream(getClient(), statistics, relativePath,
          contentLength, populateAbfsInputStreamContext(
          parameters.map(OpenFileParameters::getOptions),
          contextEncryptionAdapter),
          eTag, tracingContext);
    }
  }

  private AbfsInputStreamContext populateAbfsInputStreamContext(
      Optional<Configuration> options, ContextEncryptionAdapter contextEncryptionAdapter) {
    boolean bufferedPreadDisabled = options
        .map(c -> c.getBoolean(FS_AZURE_BUFFERED_PREAD_DISABLE, false))
        .orElse(false);
    int footerReadBufferSize = options.map(c -> c.getInt(
        AZURE_FOOTER_READ_BUFFER_SIZE, getAbfsConfiguration().getFooterReadBufferSize()))
        .orElse(getAbfsConfiguration().getFooterReadBufferSize());
    return new AbfsInputStreamContext(getAbfsConfiguration().getSasTokenRenewPeriodForStreamsInSeconds())
            .withReadBufferSize(getAbfsConfiguration().getReadBufferSize())
            .withReadAheadQueueDepth(getAbfsConfiguration().getReadAheadQueueDepth())
            .withTolerateOobAppends(getAbfsConfiguration().getTolerateOobAppends())
            .isReadAheadEnabled(getAbfsConfiguration().isReadAheadEnabled())
            .withReadSmallFilesCompletely(getAbfsConfiguration().readSmallFilesCompletely())
            .withOptimizeFooterRead(getAbfsConfiguration().optimizeFooterRead())
            .withFooterReadBufferSize(footerReadBufferSize)
            .withReadAheadRange(getAbfsConfiguration().getReadAheadRange())
            .withStreamStatistics(new AbfsInputStreamStatisticsImpl())
            .withShouldReadBufferSizeAlways(
                getAbfsConfiguration().shouldReadBufferSizeAlways())
            .withReadAheadBlockSize(getAbfsConfiguration().getReadAheadBlockSize())
            .withBufferedPreadDisabled(bufferedPreadDisabled)
            .withEncryptionAdapter(contextEncryptionAdapter)
            .withAbfsBackRef(fsBackRef)
            .build();
  }

  public OutputStream openFileForWrite(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForWrite", "getPathStatus")) {
      LOG.debug("openFileForWrite filesystem: {} path: {} overwrite: {}",
              getClient().getFileSystem(),
              path,
              overwrite);

      String relativePath = getRelativePath(path);
      AbfsClient writeClient = getClientHandler().getIngressClient();

      final AbfsRestOperation op = getClient()
          .getPathStatus(relativePath, false, tracingContext, null);
      perfInfo.registerResult(op.getResult());

      if (getClient().checkIsDir(op.getResult())) {
        throw new AbfsRestOperationException(
              AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
              AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
              "openFileForWrite must be used with files and not directories",
              null);
      }

      final long contentLength = extractContentLength(op.getResult());
      final long offset = overwrite ? 0 : contentLength;

      perfInfo.registerSuccess(true);

      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);
      final String eTag = extractEtagHeader(op.getResult());
      final ContextEncryptionAdapter contextEncryptionAdapter;
      if (writeClient.getEncryptionType() == EncryptionType.ENCRYPTION_CONTEXT) {
        final String encryptionContext = op.getResult()
            .getResponseHeader(
                HttpHeaderConfigurations.X_MS_ENCRYPTION_CONTEXT);
        if (encryptionContext == null) {
          throw new PathIOException(path.toString(),
              "File doesn't have encryptionContext.");
        }
        contextEncryptionAdapter = new ContextProviderEncryptionAdapter(
            writeClient.getEncryptionContextProvider(), getRelativePath(path),
            encryptionContext.getBytes(StandardCharsets.UTF_8));
      } else {
        contextEncryptionAdapter = NoContextEncryptionAdapter.getInstance();
      }

      return new AbfsOutputStream(
          populateAbfsOutputStreamContext(
              isAppendBlob,
              lease,
              getClientHandler(),
              statistics,
              relativePath,
              offset,
              eTag,
              contextEncryptionAdapter,
              tracingContext));
    }
  }

  /**
   * Break any current lease on an ABFS file.
   *
   * @param path file name
   * @param tracingContext TracingContext instance to track correlation IDs
   * @throws AzureBlobFileSystemException on any exception while breaking the lease
   */
  public void breakLease(final Path path, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("lease path: {}", path);

    getClient().breakLease(getRelativePath(path), tracingContext);
  }

  /**
   * Rename a file or directory.
   * If a source etag is passed in, the operation will attempt to recover
   * from a missing source file by probing the destination for
   * existence and comparing etags.
   * @param source path to source file
   * @param destination destination of rename.
   * @param tracingContext trace context
   * @param sourceEtag etag of source file. may be null or empty
   * @throws AzureBlobFileSystemException failure, excluding any recovery from overload failures.
   * @return true if recovery was needed and succeeded.
   */
  public boolean rename(final Path source,
      final Path destination,
      final TracingContext tracingContext,
      final String sourceEtag) throws IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue;

    LOG.debug("renameAsync filesystem: {} source: {} destination: {}",
        getClient().getFileSystem(),
        source,
        destination);

    String continuation = null;

    String sourceRelativePath = getRelativePath(source);
    String destinationRelativePath = getRelativePath(destination);
    // was any operation recovered from?
    boolean recovered = false;

    do {
      try (AbfsPerfInfo perfInfo = startTracking("rename", "renamePath")) {
        final AbfsClientRenameResult abfsClientRenameResult =
            getClient().renamePath(sourceRelativePath, destinationRelativePath,
                continuation, tracingContext, sourceEtag, false);

        AbfsRestOperation op = abfsClientRenameResult.getOp();
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult()
            .getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();
        // update the recovery flag.
        recovered |= abfsClientRenameResult.isRenameRecovered();
        populateRenameRecoveryStatistics(abfsClientRenameResult);
        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
    return recovered;
  }

  public void delete(final Path path, final boolean recursive,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("delete filesystem: {} path: {} recursive: {}",
        getClient().getFileSystem(),
        path,
        String.valueOf(recursive));

    String continuation = null;

    String relativePath = getRelativePath(path);

    do {
      try (AbfsPerfInfo perfInfo = startTracking("delete", "deletePath")) {
        AbfsRestOperation op = getClient().deletePath(relativePath, recursive,
            continuation, tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult()
            .getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
  }

  public FileStatus getFileStatus(final Path path,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("getFileStatus", "undetermined")) {
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("getFileStatus filesystem: {} path: {} isNamespaceEnabled: {}",
              getClient().getFileSystem(),
              path,
              isNamespaceEnabled);

      final AbfsRestOperation op;
      if (path.isRoot()) {
        if (isNamespaceEnabled) {
          perfInfo.registerCallee("getAclStatus");
          op = getClient().getAclStatus(getRelativePath(path), tracingContext);
        } else {
          perfInfo.registerCallee("getFilesystemProperties");
          op = getClient().getFilesystemProperties(tracingContext);
        }
      } else {
        perfInfo.registerCallee("getPathStatus");
        op = getClient().getPathStatus(getRelativePath(path), false, tracingContext, null);
      }

      perfInfo.registerResult(op.getResult());
      final long blockSize = abfsConfiguration.getAzureBlockSize();
      final AbfsHttpOperation result = op.getResult();

      String eTag = extractEtagHeader(result);
      final String lastModified = result.getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
      final String permissions = result.getResponseHeader((HttpHeaderConfigurations.X_MS_PERMISSIONS));
      final String encryptionContext = op.getResult().getResponseHeader(X_MS_ENCRYPTION_CONTEXT);
      final boolean hasAcl = AbfsPermission.isExtendedAcl(permissions);
      final long contentLength;
      final boolean resourceIsDir;

      if (path.isRoot()) {
        contentLength = 0;
        resourceIsDir = true;
      } else {
        contentLength = extractContentLength(result);
        resourceIsDir = getClient().checkIsDir(result);
      }

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);

      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      perfInfo.registerSuccess(true);

      return new VersionedFileStatus(
              transformedOwner,
              transformedGroup,
              permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                      : AbfsPermission.valueOf(permissions),
              hasAcl,
              contentLength,
              resourceIsDir,
              1,
              blockSize,
              DateTimeUtils.parseLastModifiedTime(lastModified),
              path,
              eTag,
              encryptionContext);
    }
  }

  /**
   * @param path The list path.
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path.
   * */
  @Override
  public FileStatus[] listStatus(final Path path, TracingContext tracingContext) throws IOException {
    return listStatus(path, null, tracingContext);
  }

  /**
   * @param path Path the list path.
   * @param startFrom the entry name that list results should start with.
   *                  For example, if folder "/folder" contains four files: "afile", "bfile", "hfile", "ifile".
   *                  Then listStatus(Path("/folder"), "hfile") will return "/folder/hfile" and "folder/ifile"
   *                  Notice that if startFrom is a non-existent entry name, then the list response contains
   *                  all entries after this non-existent entry in lexical order:
   *                  listStatus(Path("/folder"), "cfile") will return "/folder/hfile" and "/folder/ifile".
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path start from  "startFrom" in lexical order.
   * */
  @InterfaceStability.Unstable
  @Override
  public FileStatus[] listStatus(final Path path, final String startFrom, TracingContext tracingContext) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    listStatus(path, startFrom, fileStatuses, true, null, tracingContext);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  @Override
  public String listStatus(final Path path, final String startFrom,
      List<FileStatus> fileStatuses, final boolean fetchAll,
      String continuation, TracingContext tracingContext) throws IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("listStatus filesystem: {} path: {}, startFrom: {}",
            getClient().getFileSystem(),
            path,
            startFrom);

    final String relativePath = getRelativePath(path);
    AbfsClient listingClient = getClient();

    if (continuation == null || continuation.isEmpty()) {
      // generate continuation token if a valid startFrom is provided.
      if (startFrom != null && !startFrom.isEmpty()) {
        /*
         * Blob Endpoint Does not support startFrom yet. Fallback to DFS Client.
         * startFrom remains null for all HDFS APIs. This is only for internal use.
         */
        listingClient = getClient(AbfsServiceType.DFS);
        continuation = getIsNamespaceEnabled(tracingContext)
            ? generateContinuationTokenForXns(startFrom)
            : generateContinuationTokenForNonXns(relativePath, startFrom);
      }
    }

    do {
      try (AbfsPerfInfo perfInfo = startTracking("listStatus", "listPath")) {
        AbfsRestOperation op = listingClient.listPath(relativePath, false,
            abfsConfiguration.getListMaxResults(), continuation,
            tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = listingClient.getContinuationFromResponse(op.getResult());
        ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
        if (retrievedSchema == null) {
          throw new AbfsRestOperationException(
                  AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                  AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                  "listStatusAsync path not found",
                  null, op.getResult());
        }

        long blockSize = abfsConfiguration.getAzureBlockSize();

        for (ListResultEntrySchema entry : retrievedSchema.paths()) {
          final String owner = identityTransformer.transformIdentityForGetRequest(entry.owner(), true, userName);
          final String group = identityTransformer.transformIdentityForGetRequest(entry.group(), false, primaryUserGroup);
          final String encryptionContext = entry.getXMsEncryptionContext();
          final FsPermission fsPermission = entry.permissions() == null
                  ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                  : AbfsPermission.valueOf(entry.permissions());
          final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

          long lastModifiedMillis = 0;
          long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
          boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
          if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
            lastModifiedMillis = DateTimeUtils.parseLastModifiedTime(
                entry.lastModified());
          }

          Path entryPath = new Path(File.separator + entry.name());
          entryPath = entryPath.makeQualified(this.uri, entryPath);

          fileStatuses.add(
                  new VersionedFileStatus(
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
                          encryptionContext));
        }

        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue =
            fetchAll && continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);

    return continuation;
  }

  // generate continuation token for xns account
  private String generateContinuationTokenForXns(final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    StringBuilder sb = new StringBuilder();
    sb.append(firstEntryName).append("#$").append("0");

    CRC64 crc64 = new CRC64();
    StringBuilder token = new StringBuilder();
    token.append(crc64.compute(sb.toString().getBytes(StandardCharsets.UTF_8)))
            .append(SINGLE_WHITE_SPACE)
            .append("0")
            .append(SINGLE_WHITE_SPACE)
            .append(firstEntryName);

    return Base64.encode(token.toString().getBytes(StandardCharsets.UTF_8));
  }

  // generate continuation token for non-xns account
  private String generateContinuationTokenForNonXns(String path, final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    // Notice: non-xns continuation token requires full path (first "/" is not included) for startFrom
    path = AbfsClient.getDirectoryQueryParameter(path);
    final String startFrom = (path.isEmpty() || path.equals(ROOT_PATH))
            ? firstEntryName
            : path + ROOT_PATH + firstEntryName;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TOKEN_DATE_PATTERN, Locale.US);
    String date = simpleDateFormat.format(new Date());
    String token = String.format("%06d!%s!%06d!%s!%06d!%s!",
          path.length(), path, startFrom.length(), startFrom, date.length(), date);
    String base64EncodedToken = Base64.encode(token.getBytes(StandardCharsets.UTF_8));

    StringBuilder encodedTokenBuilder = new StringBuilder(base64EncodedToken.length() + 5);
    encodedTokenBuilder.append(String.format("%s!%d!", TOKEN_VERSION, base64EncodedToken.length()));

    for (int i = 0; i < base64EncodedToken.length(); i++) {
      char current = base64EncodedToken.charAt(i);
      if (CHAR_FORWARD_SLASH == current) {
        current = CHAR_UNDERSCORE;
      } else if (CHAR_PLUS == current) {
        current = CHAR_STAR;
      } else if (CHAR_EQUALS == current) {
        current = CHAR_HYPHEN;
      }
      encodedTokenBuilder.append(current);
    }

    return encodedTokenBuilder.toString();
  }

  public void setOwner(final Path path, final String owner, final String group,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("setOwner", "setOwner")) {

      LOG.debug(
              "setOwner filesystem: {} path: {} owner: {} group: {}",
              getClient().getFileSystem(),
              path,
              owner,
              group);

      final String transformedOwner = identityTransformer.transformUserOrGroupForSetRequest(owner);
      final String transformedGroup = identityTransformer.transformUserOrGroupForSetRequest(group);

      final AbfsRestOperation op = getClient().setOwner(getRelativePath(path),
              transformedOwner,
              transformedGroup,
              tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void setPermission(final Path path, final FsPermission permission,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("setPermission", "setPermission")) {

      LOG.debug(
              "setPermission filesystem: {} path: {} permission: {}",
              getClient().getFileSystem(),
              path,
              permission);

      final AbfsRestOperation op = getClient().setPermission(getRelativePath(path),
          String.format(AbfsHttpConstants.PERMISSION_FORMAT,
              permission.toOctal()), tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("modifyAclEntries", "getAclStatus")) {

      LOG.debug(
              "modifyAclEntries filesystem: {} path: {} aclSpec: {}",
              getClient().getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> modifyAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean useUpn = AbfsAclHelper.isUpnFormatAclEntries(modifyAclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = getClient()
          .getAclStatus(relativePath, useUpn, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = extractEtagHeader(op.getResult());

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.modifyAclEntriesInternal(aclEntries, modifyAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("modifyAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = getClient()
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAclEntries", "getAclStatus")) {

      LOG.debug(
              "removeAclEntries filesystem: {} path: {} aclSpec: {}",
              getClient().getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> removeAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(removeAclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = getClient()
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = extractEtagHeader(op.getResult());

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.removeAclEntriesInternal(aclEntries, removeAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = getClient()
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeDefaultAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeDefaultAcl", "getAclStatus")) {

      LOG.debug(
              "removeDefaultAcl filesystem: {} path: {}",
              getClient().getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = getClient()
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = extractEtagHeader(op.getResult());
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> defaultAclEntries = new HashMap<>();

      for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
        if (aclEntry.getKey().startsWith("default:")) {
          defaultAclEntries.put(aclEntry.getKey(), aclEntry.getValue());
        }
      }

      aclEntries.keySet().removeAll(defaultAclEntries.keySet());

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeDefaultAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = getClient()
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAcl", "getAclStatus")){

      LOG.debug(
              "removeAcl filesystem: {} path: {}",
              getClient().getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = getClient()
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = extractEtagHeader(op.getResult());

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> newAclEntries = new HashMap<>();

      newAclEntries.put(AbfsHttpConstants.ACCESS_USER, aclEntries.get(AbfsHttpConstants.ACCESS_USER));
      newAclEntries.put(AbfsHttpConstants.ACCESS_GROUP, aclEntries.get(AbfsHttpConstants.ACCESS_GROUP));
      newAclEntries.put(AbfsHttpConstants.ACCESS_OTHER, aclEntries.get(AbfsHttpConstants.ACCESS_OTHER));

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = getClient()
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(newAclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void setAcl(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("setAcl", "getAclStatus")) {

      LOG.debug(
              "setAcl filesystem: {} path: {} aclspec: {}",
              getClient().getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      final boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(aclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = getClient()
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = extractEtagHeader(op.getResult());

      final Map<String, String> getAclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.setAclEntriesInternal(aclEntries, getAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("setAcl", "setAcl")) {
        final AbfsRestOperation setAclOp =
                getClient().setAcl(relativePath,
                AbfsAclHelper.serializeAclSpec(aclEntries), eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public AclStatus getAclStatus(final Path path, TracingContext tracingContext)
      throws IOException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("getAclStatus", "getAclStatus")) {

      LOG.debug(
              "getAclStatus filesystem: {} path: {}",
              getClient().getFileSystem(),
              path);

      AbfsRestOperation op = getClient()
          .getAclStatus(getRelativePath(path), tracingContext);
      AbfsHttpOperation result = op.getResult();
      perfInfo.registerResult(result);

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);
      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      final String permissions = result.getResponseHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS);
      final String aclSpecString = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL);

      final List<AclEntry> aclEntries = AclEntry.parseAclSpec(AbfsAclHelper.processAclString(aclSpecString), true);
      identityTransformer.transformAclEntriesForGetRequest(aclEntries, userName, primaryUserGroup);
      final FsPermission fsPermission = permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
              : AbfsPermission.valueOf(permissions);

      final AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
      aclStatusBuilder.owner(transformedOwner);
      aclStatusBuilder.group(transformedGroup);

      aclStatusBuilder.setPermission(fsPermission);
      aclStatusBuilder.stickyBit(fsPermission.getStickyBit());
      aclStatusBuilder.addEntries(aclEntries);
      perfInfo.registerSuccess(true);
      return aclStatusBuilder.build();
    }
  }

  public void access(final Path path, final FsAction mode,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("access for filesystem: {}, path: {}, mode: {}",
        this.getClient().getFileSystem(), path, mode);
    if (!this.abfsConfiguration.isCheckAccessEnabled()
        || !getIsNamespaceEnabled(tracingContext)) {
      LOG.debug("Returning; either check access is not enabled or the account"
          + " used is not namespace enabled");
      return;
    }
    try (AbfsPerfInfo perfInfo = startTracking("access", "checkAccess")) {
      final AbfsRestOperation op = this.getClient()
          .checkAccess(getRelativePath(path), mode.SYMBOL, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public boolean isInfiniteLeaseKey(String key) {
    if (azureInfiniteLeaseDirSet.isEmpty()) {
      return false;
    }
    return isKeyForDirectorySet(key, azureInfiniteLeaseDirSet);
  }

  /**
   * A on-off operation to initialize AbfsClient for AzureBlobFileSystem
   * Operations.
   *
   * @param uri            Uniform resource identifier for Abfs.
   * @param fileSystemName Name of the fileSystem being used.
   * @param accountName    Name of the account being used to access Azure
   *                       data store.
   * @param isSecure       Tells if https is being used or http.
   * @throws IOException
   */
  private void initializeClient(URI uri, String fileSystemName,
      String accountName, boolean isSecure)
      throws IOException {
    if (this.getClient() != null) {
      return;
    }

    final URIBuilder uriBuilder = getURIBuilder(accountName, isSecure);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + fileSystemName;

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(uri.toString());
    }

    SharedKeyCredentials creds = null;
    AccessTokenProvider tokenProvider = null;
    SASTokenProvider sasTokenProvider = null;

    if (authType == AuthType.OAuth) {
      AzureADAuthenticator.init(abfsConfiguration);
    }

    if (authType == AuthType.SharedKey) {
      LOG.trace("Fetching SharedKey credentials");
      int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
      if (dotIndex <= 0) {
        throw new InvalidUriException(
                uri.toString() + " - account name is not fully qualified.");
      }
      creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
            abfsConfiguration.getStorageAccountKey());
    } else if (authType == AuthType.SAS) {
      LOG.trace("Fetching SAS Token Provider");
      sasTokenProvider = abfsConfiguration.getSASTokenProvider();
    } else {
      LOG.trace("Fetching token provider");
      tokenProvider = abfsConfiguration.getTokenProvider();
      ExtensionHelper.bind(tokenProvider, uri,
            abfsConfiguration.getRawConfiguration());
    }

    // Encryption setup
    EncryptionContextProvider encryptionContextProvider = null;
    if (isSecure) {
      encryptionContextProvider =
          abfsConfiguration.createEncryptionContextProvider();
      if (encryptionContextProvider != null) {
        if (abfsConfiguration.getEncodedClientProvidedEncryptionKey() != null) {
          throw new PathIOException(uri.getPath(),
              "Both global key and encryption context are set, only one allowed");
        }
        encryptionContextProvider.initialize(
            abfsConfiguration.getRawConfiguration(), accountName,
            fileSystemName);
      } else if (abfsConfiguration.getEncodedClientProvidedEncryptionKey() != null) {
        if (abfsConfiguration.getEncodedClientProvidedEncryptionKeySHA() == null) {
          throw new PathIOException(uri.getPath(),
              "Encoded SHA256 hash must be provided for global encryption");
        }
      }
    }

    LOG.trace("Initializing AbfsClient for {}", baseUrl);
    if (tokenProvider != null) {
      this.clientHandler = new AbfsClientHandler(baseUrl, creds, abfsConfiguration,
          tokenProvider, encryptionContextProvider,
          populateAbfsClientContext());
    } else {
      this.clientHandler = new AbfsClientHandler(baseUrl, creds, abfsConfiguration,
          sasTokenProvider, encryptionContextProvider,
          populateAbfsClientContext());
    }

    this.setClient(getClientHandler().getClient());
    LOG.trace("AbfsClient init complete");
  }

  private AbfsServiceType getAbfsServiceTypeFromUrl() {
    if (uri.toString().contains(ABFS_BLOB_DOMAIN_NAME)) {
      return AbfsServiceType.BLOB;
    }
    // In case of DFS Domain name or any other custom endpoint, the service
    // type is to be identified as default DFS.
    LOG.debug("Falling back to default service type DFS");
    return AbfsServiceType.DFS;
  }

  /**
   * Populate a new AbfsClientContext instance with the desired properties.
   *
   * @return an instance of AbfsClientContext.
   */
  private AbfsClientContext populateAbfsClientContext() {
    return new AbfsClientContextBuilder()
        .withExponentialRetryPolicy(
            new ExponentialRetryPolicy(abfsConfiguration))
        .withStaticRetryPolicy(
            new StaticRetryPolicy(abfsConfiguration))
        .withAbfsCounters(abfsCounters)
        .withAbfsPerfTracker(abfsPerfTracker)
        .build();
  }

  public String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    String relPath = path.toUri().getPath();
    if (relPath.isEmpty()) {
      // This means that path passed by user is absolute path of root without "/" at end.
      relPath = ROOT_PATH;
    }
    return relPath;
  }

  /**
   * Extracts the content length from the HTTP operation's response headers.
   *
   * @param op The AbfsHttpOperation instance from which to extract the content length.
   *           This operation contains the HTTP response headers.
   * @return The content length as a long value. If the Content-Length header is
   *         not present or is empty, returns 0.
   */
  private long extractContentLength(AbfsHttpOperation op) {
    long contentLength;
    String contentLengthHeader = op.getResponseHeader(
        HttpHeaderConfigurations.CONTENT_LENGTH);
    if (!StringUtils.isEmpty(contentLengthHeader)) {
      contentLength = Long.parseLong(contentLengthHeader);
    } else {
      contentLength = 0;
    }
    return contentLength;
  }

  private boolean parseIsDirectory(final String resourceType) {
    return resourceType != null
        && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
          InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(AbfsHttpConstants.COMMA);

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split(AbfsHttpConstants.EQUAL, 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = Base64.decode(nameValue[1]);

        final String value;
        try {
          value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        } catch (CharacterCodingException ex) {
          throw new InvalidAbfsRestOperationException(ex);
        }
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  private AbfsPerfInfo startTracking(String callerName, String calleeName) {
    return new AbfsPerfInfo(abfsPerfTracker, callerName, calleeName);
  }

  /**
   * A File status with version info extracted from the etag value returned
   * in a LIST or HEAD request.
   * The etag is included in the java serialization.
   */
  static final class VersionedFileStatus extends FileStatus
      implements EtagSource {

    /**
     * The superclass is declared serializable; this subclass can also
     * be serialized.
     */
    private static final long serialVersionUID = -2009013240419749458L;

    /**
     * The etag of an object.
     * Not-final so that serialization via reflection will preserve the value.
     */
    private String version;

    private String encryptionContext;

    private VersionedFileStatus(
            final String owner, final String group, final FsPermission fsPermission, final boolean hasAcl,
            final long length, final boolean isdir, final int blockReplication,
            final long blocksize, final long modificationTime, final Path path,
            final String version, final String encryptionContext) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
              fsPermission,
              owner,
              group,
              null,
              path,
              hasAcl, false, false);

      this.version = version;
      this.encryptionContext = encryptionContext;
    }

    /** Compare if this object is equal to another object.
     * @param   obj the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof FileStatus)) {
        return false;
      }

      FileStatus other = (FileStatus) obj;

      if (!this.getPath().equals(other.getPath())) {// compare the path
        return false;
      }

      if (other instanceof VersionedFileStatus) {
        return this.version.equals(((VersionedFileStatus) other).version);
      }

      return true;
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return  a hash code value for the path name and version
     */
    @Override
    public int hashCode() {
      int hash = getPath().hashCode();
      hash = 89 * hash + (this.version != null ? this.version.hashCode() : 0);
      return hash;
    }

    /**
     * Returns the version of this FileStatus
     *
     * @return  a string value for the FileStatus version
     */
    public String getVersion() {
      return this.version;
    }

    @Override
    public String getEtag() {
      return getVersion();
    }

    public String getEncryptionContext() {
      return encryptionContext;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "VersionedFileStatus{");
      sb.append(super.toString());
      sb.append("; version='").append(version).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Permissions class contain provided permission and umask in octalNotation.
   * If the object is created for namespace-disabled account, the permission and
   * umask would be null.
   * */
  public static final class Permissions {
    private final String permission;
    private final String umask;

    Permissions(boolean isNamespaceEnabled, FsPermission permission,
        FsPermission umask) {
      if (isNamespaceEnabled) {
        this.permission = getOctalNotation(permission);
        this.umask = getOctalNotation(umask);
      } else {
        this.permission = null;
        this.umask = null;
      }
    }

    private String getOctalNotation(FsPermission fsPermission) {
      Preconditions.checkNotNull(fsPermission, "fsPermission");
      return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
    }

    public Boolean hasPermission() {
      return permission != null && !permission.isEmpty();
    }

    public Boolean hasUmask() {
      return umask != null && !umask.isEmpty();
    }

    public String getPermission() {
      return permission;
    }

    public String getUmask() {
      return umask;
    }

    @Override
    public String toString() {
      return String.format("{\"permission\":%s, \"umask\":%s}", permission,
          umask);
    }
  }

  /**
   * A builder class for AzureBlobFileSystemStore.
   */
  public static final class AzureBlobFileSystemStoreBuilder {

    private URI uri;
    private boolean isSecureScheme;
    private Configuration configuration;
    private AbfsCounters abfsCounters;
    private DataBlocks.BlockFactory blockFactory;
    private int blockOutputActiveBlocks;
    private BackReference fsBackRef;

    public AzureBlobFileSystemStoreBuilder withUri(URI value) {
      this.uri = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withSecureScheme(boolean value) {
      this.isSecureScheme = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withConfiguration(
        Configuration value) {
      this.configuration = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withAbfsCounters(
        AbfsCounters value) {
      this.abfsCounters = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withBlockFactory(
        DataBlocks.BlockFactory value) {
      this.blockFactory = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withBlockOutputActiveBlocks(
        int value) {
      this.blockOutputActiveBlocks = value;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder withBackReference(
        BackReference fsBackRef) {
      this.fsBackRef = fsBackRef;
      return this;
    }

    public AzureBlobFileSystemStoreBuilder build() {
      return this;
    }
  }

  @VisibleForTesting
  public AbfsClient getClient() {
    return this.client;
  }

  @VisibleForTesting
  public AbfsClient getClient(AbfsServiceType serviceType) {
    return getClientHandler().getClient(serviceType);
  }

  @VisibleForTesting
  public AbfsClientHandler getClientHandler() {
    return this.clientHandler;
  }

  @VisibleForTesting
  void setClient(AbfsClient client) {
    this.client = client;
  }

  @VisibleForTesting
  void setClientHandler(AbfsClientHandler clientHandler) {
    this.clientHandler = clientHandler;
  }

  @VisibleForTesting
  DataBlocks.BlockFactory getBlockFactory() {
    return blockFactory;
  }

  @VisibleForTesting
  void setNamespaceEnabled(Trilean isNamespaceEnabled){
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  private void updateInfiniteLeaseDirs() {
    this.azureInfiniteLeaseDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureInfiniteLeaseDirs().split(AbfsHttpConstants.COMMA)));
    // remove the empty string, since isKeyForDirectory returns true for empty strings
    // and we don't want to default to enabling infinite lease dirs
    this.azureInfiniteLeaseDirSet.remove("");
  }

  private AbfsLease maybeCreateLease(String relativePath, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    boolean enableInfiniteLease = isInfiniteLeaseKey(relativePath);
    if (!enableInfiniteLease) {
      return null;
    }
    AbfsLease lease = new AbfsLease(getClient(), relativePath, true,
            INFINITE_LEASE_DURATION, null, tracingContext);
    leaseRefs.put(lease, null);
    return lease;
  }

  @VisibleForTesting
  boolean areLeasesFreed() {
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease != null && !lease.isFreed()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the etag header from a response, stripping any quotations.
   * see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
   * @param result response to process.
   * @return the quote-unwrapped etag.
   */
  public static String extractEtagHeader(AbfsHttpOperation result) {
    String etag = result.getResponseHeader(HttpHeaderConfigurations.ETAG);
    if (etag != null) {
      // strip out any wrapper "" quotes which come back, for consistency with
      // list calls
      if (etag.startsWith("W/\"")) {
        // Weak etag
        etag = etag.substring(3);
      } else if (etag.startsWith("\"")) {
        // strong etag
        etag = etag.substring(1);
      }
      if (etag.endsWith("\"")) {
        // trailing quote
        etag = etag.substring(0, etag.length() - 1);
      }
    }
    return etag;
  }

  /**
   * Increment rename recovery based counters in IOStatistics.
   *
   * @param abfsClientRenameResult Result of an ABFS rename operation.
   */
  private void populateRenameRecoveryStatistics(
      AbfsClientRenameResult abfsClientRenameResult) {
    if (abfsClientRenameResult.isRenameRecovered()) {
      abfsCounters.incrementCounter(RENAME_RECOVERY, 1);
    }
    if (abfsClientRenameResult.isIncompleteMetadataState()) {
      abfsCounters.incrementCounter(METADATA_INCOMPLETE_RENAME_FAILURES, 1);
    }
  }
}
