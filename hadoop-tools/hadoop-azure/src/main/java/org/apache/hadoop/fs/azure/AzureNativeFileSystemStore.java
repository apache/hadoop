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

package org.apache.hadoop.fs.azure;
import static org.apache.hadoop.fs.azure.NativeAzureFileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobContainerWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobDirectoryWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlockBlobWrapper;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.BandwidthGaugeUpdater;
import org.apache.hadoop.fs.azure.metrics.ErrorMetricUpdater;
import org.apache.hadoop.fs.azure.metrics.ResponseReceivedMetricUpdater;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.mortbay.util.ajax.JSON;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.OperationContext;
import com.microsoft.windowsazure.storage.RetryExponentialRetry;
import com.microsoft.windowsazure.storage.RetryNoRetry;
import com.microsoft.windowsazure.storage.StorageCredentials;
import com.microsoft.windowsazure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.windowsazure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.windowsazure.storage.StorageErrorCode;
import com.microsoft.windowsazure.storage.StorageException;
import com.microsoft.windowsazure.storage.blob.BlobListingDetails;
import com.microsoft.windowsazure.storage.blob.BlobProperties;
import com.microsoft.windowsazure.storage.blob.BlobRequestOptions;
import com.microsoft.windowsazure.storage.blob.CloudBlob;
import com.microsoft.windowsazure.storage.blob.CopyStatus;
import com.microsoft.windowsazure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.windowsazure.storage.blob.ListBlobItem;
import com.microsoft.windowsazure.storage.core.Utility;


/**
 * Core implementation of Windows Azure Filesystem for Hadoop.
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage 
 *
 */
@InterfaceAudience.Private
@VisibleForTesting
public class AzureNativeFileSystemStore implements NativeFileSystemStore {
  
  /**
   * Configuration knob on whether we do block-level MD5 validation on
   * upload/download.
   */
  static final String KEY_CHECK_BLOCK_MD5 = "fs.azure.check.block.md5";
  /**
   * Configuration knob on whether we store blob-level MD5 on upload.
   */
  static final String KEY_STORE_BLOB_MD5 = "fs.azure.store.blob.md5";
  static final String DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME = "storageemulator";
  static final String STORAGE_EMULATOR_ACCOUNT_NAME_PROPERTY_NAME = "fs.azure.storage.emulator.account.name";

  public static final Log LOG = LogFactory
      .getLog(AzureNativeFileSystemStore.class);

  private StorageInterface storageInteractionLayer;
  private CloudBlobDirectoryWrapper rootDirectory;
  private CloudBlobContainerWrapper container;

  // Constants local to this class.
  //
  private static final String KEY_ACCOUNT_KEYPROVIDER_PREFIX = "fs.azure.account.keyprovider.";
  private static final String KEY_ACCOUNT_SAS_PREFIX = "fs.azure.sas.";

  // note: this value is not present in core-default.xml as our real default is
  // computed as min(2*cpu,8)
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";

  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.read.request.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.storage.timeout";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.request.size";

  // Property controlling whether to allow reads on blob which are concurrently
  // appended out-of-band.
  private static final String KEY_READ_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";

  // Configurable throttling parameter properties. These properties are located
  // in the core-site.xml configuration file.
  private static final String KEY_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  private static final String KEY_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  private static final String KEY_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  private static final String KEY_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";

  private static final String KEY_SELF_THROTTLE_ENABLE = "fs.azure.selfthrottling.enable";
  private static final String KEY_SELF_THROTTLE_READ_FACTOR = "fs.azure.selfthrottling.read.factor";
  private static final String KEY_SELF_THROTTLE_WRITE_FACTOR = "fs.azure.selfthrottling.write.factor";

  private static final String PERMISSION_METADATA_KEY = "hdi_permission";
  private static final String OLD_PERMISSION_METADATA_KEY = "asv_permission";
  private static final String IS_FOLDER_METADATA_KEY = "hdi_isfolder";
  private static final String OLD_IS_FOLDER_METADATA_KEY = "asv_isfolder";
  static final String VERSION_METADATA_KEY = "hdi_version";
  static final String OLD_VERSION_METADATA_KEY = "asv_version";
  static final String FIRST_WASB_VERSION = "2013-01-01";
  static final String CURRENT_WASB_VERSION = "2013-09-01";
  static final String LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY = "hdi_tmpupload";
  static final String OLD_LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY = "asv_tmpupload";

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String WASB_AUTHORITY_DELIMITER = "@";
  private static final String AZURE_ROOT_CONTAINER = "$root";

  private static final int DEFAULT_CONCURRENT_WRITES = 8;

  // Concurrent reads reads of data written out of band are disable by default.
  private static final boolean DEFAULT_READ_TOLERATE_CONCURRENT_APPEND = false;

  // Default block sizes
  public static final int DEFAULT_DOWNLOAD_BLOCK_SIZE = 4 * 1024 * 1024;
  public static final int DEFAULT_UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024;

  // Retry parameter defaults.
  private static final int DEFAULT_MIN_BACKOFF_INTERVAL = 1 * 1000; // 1s
  private static final int DEFAULT_MAX_BACKOFF_INTERVAL = 30 * 1000; // 30s
  private static final int DEFAULT_BACKOFF_INTERVAL = 1 * 1000; // 1s
  private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 15;

  // Self-throttling defaults. Allowed range = (0,1.0]
  // Value of 1.0 means no self-throttling.
  // Value of x means process data at factor x of unrestricted rate
  private static final boolean DEFAULT_SELF_THROTTLE_ENABLE = true;
  private static final float DEFAULT_SELF_THROTTLE_READ_FACTOR = 1.0f;
  private static final float DEFAULT_SELF_THROTTLE_WRITE_FACTOR = 1.0f;

  private static final int STORAGE_CONNECTION_TIMEOUT_DEFAULT = 90;

  /**
   * MEMBER VARIABLES
   */

  private URI sessionUri;
  private Configuration sessionConfiguration;
  private int concurrentWrites = DEFAULT_CONCURRENT_WRITES;
  private boolean isAnonymousCredentials = false;
  // Set to true if we are connecting using shared access signatures.
  private boolean connectingUsingSAS = false;
  private AzureFileSystemInstrumentation instrumentation;
  private BandwidthGaugeUpdater bandwidthGaugeUpdater;
  private static final JSON PERMISSION_JSON_SERIALIZER = createPermissionJsonSerializer();

  private boolean suppressRetryPolicy = false;
  private boolean canCreateOrModifyContainer = false;
  private ContainerState currentKnownContainerState = ContainerState.Unknown;
  private final Object containerStateLock = new Object();

  private boolean tolerateOobAppends = DEFAULT_READ_TOLERATE_CONCURRENT_APPEND;

  private int downloadBlockSizeBytes = DEFAULT_DOWNLOAD_BLOCK_SIZE;
  private int uploadBlockSizeBytes = DEFAULT_UPLOAD_BLOCK_SIZE;

  // Bandwidth throttling exponential back-off parameters
  //
  private int minBackoff; // the minimum back-off interval (ms) between retries.
  private int maxBackoff; // the maximum back-off interval (ms) between retries.
  private int deltaBackoff; // the back-off interval (ms) between retries.
  private int maxRetries; // the maximum number of retry attempts.

  // Self-throttling parameters
  private boolean selfThrottlingEnabled;
  private float selfThrottlingReadFactor;
  private float selfThrottlingWriteFactor;

  private TestHookOperationContext testHookOperationContext = null;

  // Set if we're running against a storage emulator..
  private boolean isStorageEmulator = false;

  /**
   * A test hook interface that can modify the operation context we use for
   * Azure Storage operations, e.g. to inject errors.
   */
  @VisibleForTesting 
  interface TestHookOperationContext {
    OperationContext modifyOperationContext(OperationContext original);
  }

  /**
   * Suppress the default retry policy for the Storage, useful in unit tests to
   * test negative cases without waiting forever.
   */
  @VisibleForTesting
  void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * Add a test hook to modify the operation context we use for Azure Storage
   * operations.
   * 
   * @param testHook
   *          The test hook, or null to unset previous hooks.
   */
  @VisibleForTesting 
  void addTestHookToOperationContext(TestHookOperationContext testHook) {
    this.testHookOperationContext = testHook;
  }

  /**
   * If we're asked by unit tests to not retry, set the retry policy factory in
   * the client accordingly.
   */
  private void suppressRetryPolicyInClientIfNeeded() {
    if (suppressRetryPolicy) {
      storageInteractionLayer.setRetryPolicyFactory(new RetryNoRetry());
    }
  }

  /**
   * Creates a JSON serializer that can serialize a PermissionStatus object into
   * the JSON string we want in the blob metadata.
   * 
   * @return The JSON serializer.
   */
  private static JSON createPermissionJsonSerializer() {
    JSON serializer = new JSON();
    serializer.addConvertor(PermissionStatus.class,
        new PermissionStatusJsonSerializer());
    return serializer;
  }

  /**
   * A converter for PermissionStatus to/from JSON as we want it in the blob
   * metadata.
   */
  private static class PermissionStatusJsonSerializer implements JSON.Convertor {
    private static final String OWNER_TAG = "owner";
    private static final String GROUP_TAG = "group";
    private static final String PERMISSIONS_TAG = "permissions";

    @Override
    public void toJSON(Object obj, JSON.Output out) {
      PermissionStatus permissionStatus = (PermissionStatus) obj;
      // Don't store group as null, just store it as empty string
      // (which is FileStatus behavior).
      String group = permissionStatus.getGroupName() == null ? ""
          : permissionStatus.getGroupName();
      out.add(OWNER_TAG, permissionStatus.getUserName());
      out.add(GROUP_TAG, group);
      out.add(PERMISSIONS_TAG, permissionStatus.getPermission().toString());
    }

    @Override
    public Object fromJSON(@SuppressWarnings("rawtypes") Map object) {
      return PermissionStatusJsonSerializer.fromJSONMap(object);
    }

    @SuppressWarnings("rawtypes")
    public static PermissionStatus fromJSONString(String jsonString) {
      // The JSON class can only find out about an object's class (and call me)
      // if we store the class name in the JSON string. Since I don't want to
      // do that (it's an implementation detail), I just deserialize as a
      // the default Map (JSON's default behavior) and parse that.
      return fromJSONMap((Map) PERMISSION_JSON_SERIALIZER.fromJSON(jsonString));
    }

    private static PermissionStatus fromJSONMap(
        @SuppressWarnings("rawtypes") Map object) {
      return new PermissionStatus((String) object.get(OWNER_TAG),
          (String) object.get(GROUP_TAG),
          // The initial - below is the Unix file type,
          // which FsPermission needs there but ignores.
          FsPermission.valueOf("-" + (String) object.get(PERMISSIONS_TAG)));
    }
  }

  @VisibleForTesting
  void setAzureStorageInteractionLayer(StorageInterface storageInteractionLayer) {
    this.storageInteractionLayer = storageInteractionLayer;
  }

  @VisibleForTesting
  public BandwidthGaugeUpdater getBandwidthGaugeUpdater() {
    return bandwidthGaugeUpdater;
  }
  
  /**
   * Check if concurrent reads and writes on the same blob are allowed.
   * 
   * @return true if concurrent reads and OOB writes has been configured, false
   *         otherwise.
   */
  private boolean isConcurrentOOBAppendAllowed() {
    return tolerateOobAppends;
  }

  /**
   * Method for the URI and configuration object necessary to create a storage
   * session with an Azure session. It parses the scheme to ensure it matches
   * the storage protocol supported by this file system.
   * 
   * @param uri
   *          - URI for target storage blob.
   * @param conf
   *          - reference to configuration object.
   * 
   * @throws IllegalArgumentException
   *           if URI or job object is null, or invalid scheme.
   */
  @Override
  public void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation) throws AzureException {

    if (null == this.storageInteractionLayer) {
      this.storageInteractionLayer = new StorageInterfaceImpl();
    }

    this.instrumentation = instrumentation;
    this.bandwidthGaugeUpdater = new BandwidthGaugeUpdater(instrumentation);
    if (null == this.storageInteractionLayer) {
      this.storageInteractionLayer = new StorageInterfaceImpl();
    }
    
    // Check that URI exists.
    //
    if (null == uri) {
      throw new IllegalArgumentException(
          "Cannot initialize WASB file system, URI is null");
    }

    // Check that configuration object is non-null.
    //
    if (null == conf) {
      throw new IllegalArgumentException(
          "Cannot initialize WASB file system, URI is null");
    }

    // Incoming parameters validated. Capture the URI and the job configuration
    // object.
    //
    sessionUri = uri;
    sessionConfiguration = conf;

    // Start an Azure storage session.
    //
    createAzureStorageSession();
  }

  /**
   * Method to extract the account name from an Azure URI.
   * 
   * @param uri
   *          -- WASB blob URI
   * @returns accountName -- the account name for the URI.
   * @throws URISyntaxException
   *           if the URI does not have an authority it is badly formed.
   */
  private String getAccountFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority) {
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(),
          "Expected URI with a valid authority");
    }

    // Check if authority container the delimiter separating the account name
    // from the
    // the container.
    //
    if (!authority.contains(WASB_AUTHORITY_DELIMITER)) {
      return authority;
    }

    // Split off the container name and the authority.
    //
    String[] authorityParts = authority.split(WASB_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be
    // specified.
    //
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed WASB authority since there is no container.
      //
      final String errMsg = String
          .format(
              "URI '%s' has a malformed WASB authority, expected container name. "
                  + "Authority takes the form wasb://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Return with the account name. It is possible that this name is NULL.
    //
    return authorityParts[1];
  }

  /**
   * Method to extract the container name from an Azure URI.
   * 
   * @param uri
   *          -- WASB blob URI
   * @returns containerName -- the container name for the URI. May be null.
   * @throws URISyntaxException
   *           if the uri does not have an authority it is badly formed.
   */
  private String getContainerFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority) {
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(),
          "Expected URI with a valid authority");
    }

    // The URI has a valid authority. Extract the container name. It is the
    // second component of the WASB URI authority.
    if (!authority.contains(WASB_AUTHORITY_DELIMITER)) {
      // The authority does not have a container name. Use the default container
      // by
      // setting the container name to the default Azure root container.
      //
      return AZURE_ROOT_CONTAINER;
    }

    // Split off the container name and the authority.
    String[] authorityParts = authority.split(WASB_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be
    // specified.
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed WASB authority since there is no container.
      final String errMsg = String
          .format(
              "URI '%s' has a malformed WASB authority, expected container name."
                  + "Authority takes the form wasb://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Set the container name from the first entry for the split parts of the
    // authority.
    return authorityParts[0];
  }

  /**
   * Get the appropriate return the appropriate scheme for communicating with
   * Azure depending on whether wasb or wasbs is specified in the target URI.
   * 
   * return scheme - HTTPS or HTTP as appropriate.
   */
  private String getHTTPScheme() {
    String sessionScheme = sessionUri.getScheme();
    // Check if we're on a secure URI scheme: wasbs or the legacy asvs scheme.
    if (sessionScheme != null
        && (sessionScheme.equalsIgnoreCase("asvs") || sessionScheme
            .equalsIgnoreCase("wasbs"))) {
      return HTTPS_SCHEME;
    } else {
      // At this point the scheme should be either null or asv or wasb.
      // Intentionally I'm not going to validate it though since I don't feel
      // it's this method's job to ensure a valid URI scheme for this file
      // system.
      return HTTP_SCHEME;
    }
  }

  /**
   * Set the configuration parameters for this client storage session with
   * Azure.
   * 
   * @throws AzureException
   * @throws ConfigurationException
   * 
   */
  private void configureAzureStorageSession() throws AzureException {

    // Assertion: Target session URI already should have been captured.
    if (sessionUri == null) {
      throw new AssertionError(
          "Expected a non-null session URI when configuring storage session");
    }

    // Assertion: A client session already should have been established with
    // Azure.
    if (storageInteractionLayer == null) {
      throw new AssertionError(String.format(
          "Cannot configure storage session for URI '%s' "
              + "if storage session has not been established.",
          sessionUri.toString()));
    }

    // Determine whether or not reads are allowed concurrent with OOB writes.
    tolerateOobAppends = sessionConfiguration.getBoolean(
        KEY_READ_TOLERATE_CONCURRENT_APPEND,
        DEFAULT_READ_TOLERATE_CONCURRENT_APPEND);

    // Retrieve configuration for the minimum stream read and write block size.
    //
    this.downloadBlockSizeBytes = sessionConfiguration.getInt(
        KEY_STREAM_MIN_READ_SIZE, DEFAULT_DOWNLOAD_BLOCK_SIZE);
    this.uploadBlockSizeBytes = sessionConfiguration.getInt(
        KEY_WRITE_BLOCK_SIZE, DEFAULT_UPLOAD_BLOCK_SIZE);

    // The job may want to specify a timeout to use when engaging the
    // storage service. The default is currently 90 seconds. It may
    // be necessary to increase this value for long latencies in larger
    // jobs. If the timeout specified is greater than zero seconds use
    // it, otherwise use the default service client timeout.
    int storageConnectionTimeout = sessionConfiguration.getInt(
        KEY_STORAGE_CONNECTION_TIMEOUT, 0);

    if (0 < storageConnectionTimeout) {
      storageInteractionLayer.setTimeoutInMs(storageConnectionTimeout * 1000);
    }

    // Set the concurrency values equal to the that specified in the
    // configuration file. If it does not exist, set it to the default
    // value calculated as double the number of CPU cores on the client
    // machine. The concurrency value is minimum of double the cores and
    // the read/write property.
    int cpuCores = 2 * Runtime.getRuntime().availableProcessors();

    concurrentWrites = sessionConfiguration.getInt(
        KEY_CONCURRENT_CONNECTION_VALUE_OUT,
        Math.min(cpuCores, DEFAULT_CONCURRENT_WRITES));

    // Set up the exponential retry policy.
    minBackoff = sessionConfiguration.getInt(KEY_MIN_BACKOFF_INTERVAL,
        DEFAULT_MIN_BACKOFF_INTERVAL);

    maxBackoff = sessionConfiguration.getInt(KEY_MAX_BACKOFF_INTERVAL,
        DEFAULT_MAX_BACKOFF_INTERVAL);

    deltaBackoff = sessionConfiguration.getInt(KEY_BACKOFF_INTERVAL,
        DEFAULT_BACKOFF_INTERVAL);

    maxRetries = sessionConfiguration.getInt(KEY_MAX_IO_RETRIES,
        DEFAULT_MAX_RETRY_ATTEMPTS);

    storageInteractionLayer.setRetryPolicyFactory(new RetryExponentialRetry(
        minBackoff, deltaBackoff, maxBackoff, maxRetries));

    // read the self-throttling config.
    selfThrottlingEnabled = sessionConfiguration.getBoolean(
        KEY_SELF_THROTTLE_ENABLE, DEFAULT_SELF_THROTTLE_ENABLE);

    selfThrottlingReadFactor = sessionConfiguration.getFloat(
        KEY_SELF_THROTTLE_READ_FACTOR, DEFAULT_SELF_THROTTLE_READ_FACTOR);

    selfThrottlingWriteFactor = sessionConfiguration.getFloat(
        KEY_SELF_THROTTLE_WRITE_FACTOR, DEFAULT_SELF_THROTTLE_WRITE_FACTOR);

    if (LOG.isDebugEnabled()) {
      LOG.debug(String
          .format(
              "AzureNativeFileSystemStore init. Settings=%d,%b,%d,{%d,%d,%d,%d},{%b,%f,%f}",
              concurrentWrites, tolerateOobAppends,
              ((storageConnectionTimeout > 0) ? storageConnectionTimeout
                  : STORAGE_CONNECTION_TIMEOUT_DEFAULT), minBackoff,
              deltaBackoff, maxBackoff, maxRetries, selfThrottlingEnabled,
              selfThrottlingReadFactor, selfThrottlingWriteFactor));
    }
  }

  /**
   * Connect to Azure storage using anonymous credentials.
   * 
   * @param uri
   *          - URI to target blob (R/O access to public blob)
   * 
   * @throws StorageException
   *           raised on errors communicating with Azure storage.
   * @throws IOException
   *           raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions
   *           raised on creating mal-formed URI's.
   */
  private void connectUsingAnonymousCredentials(final URI uri)
      throws StorageException, IOException, URISyntaxException {
    // Use an HTTP scheme since the URI specifies a publicly accessible
    // container. Explicitly create a storage URI corresponding to the URI
    // parameter for use in creating the service client.
    String accountName = getAccountFromAuthority(uri);
    URI storageUri = new URI(getHTTPScheme() + ":" + PATH_DELIMITER
        + PATH_DELIMITER + accountName);

    // Create the service client with anonymous credentials.
    String containerName = getContainerFromAuthority(uri);
    storageInteractionLayer.createBlobClient(storageUri);
    suppressRetryPolicyInClientIfNeeded();

    // Capture the container reference.
    container = storageInteractionLayer.getContainerReference(containerName);
    rootDirectory = container.getDirectoryReference("");

    // Check for container existence, and our ability to access it.
    try {
      if (!container.exists(getInstrumentedContext())) {
        throw new AzureException("Container " + containerName + " in account "
            + accountName + " not found, and we can't create "
            + " it using anoynomous credentials.");
      }
    } catch (StorageException ex) {
      throw new AzureException("Unable to access container " + containerName
          + " in account " + accountName
          + " using anonymous credentials, and no credentials found for them "
          + " in the configuration.", ex);
    }

    // Accessing the storage server unauthenticated using
    // anonymous credentials.
    isAnonymousCredentials = true;

    // Configure Azure storage session.
    configureAzureStorageSession();
  }

  private void connectUsingCredentials(String accountName,
      StorageCredentials credentials, String containerName)
      throws URISyntaxException, StorageException, AzureException {

    if (isStorageEmulatorAccount(accountName)) {
      isStorageEmulator = true;
      CloudStorageAccount account = CloudStorageAccount
          .getDevelopmentStorageAccount();
      storageInteractionLayer.createBlobClient(account);
    } else {
      URI blobEndPoint = new URI(getHTTPScheme() + "://" + accountName);
      storageInteractionLayer.createBlobClient(blobEndPoint, credentials);
    }
    suppressRetryPolicyInClientIfNeeded();

    // Capture the container reference for debugging purposes.
    container = storageInteractionLayer.getContainerReference(containerName);
    rootDirectory = container.getDirectoryReference("");

    // Can only create container if using account key credentials
    canCreateOrModifyContainer = credentials instanceof StorageCredentialsAccountAndKey;

    // Configure Azure storage session.
    configureAzureStorageSession();
  }

  /**
   * Connect to Azure storage using account key credentials.
   */
  private void connectUsingConnectionStringCredentials(
      final String accountName, final String containerName,
      final String accountKey) throws InvalidKeyException, StorageException,
      IOException, URISyntaxException {
    // If the account name is "acc.blob.core.windows.net", then the
    // rawAccountName is just "acc"
    String rawAccountName = accountName.split("\\.")[0];
    StorageCredentials credentials = new StorageCredentialsAccountAndKey(
        rawAccountName, accountKey);
    connectUsingCredentials(accountName, credentials, containerName);
  }

  /**
   * Connect to Azure storage using shared access signature credentials.
   */
  private void connectUsingSASCredentials(final String accountName,
      final String containerName, final String sas) throws InvalidKeyException,
      StorageException, IOException, URISyntaxException {
    StorageCredentials credentials = new StorageCredentialsSharedAccessSignature(
        sas);
    connectingUsingSAS = true;
    connectUsingCredentials(accountName, credentials, containerName);
  }

  private boolean isStorageEmulatorAccount(final String accountName) {
    return accountName.equalsIgnoreCase(sessionConfiguration.get(
        STORAGE_EMULATOR_ACCOUNT_NAME_PROPERTY_NAME,
        DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME));
  }

  static String getAccountKeyFromConfiguration(String accountName,
      Configuration conf) throws KeyProviderException {
    String key = null;
    String keyProviderClass = conf.get(KEY_ACCOUNT_KEYPROVIDER_PREFIX
        + accountName);
    KeyProvider keyProvider = null;

    if (keyProviderClass == null) {
      // No key provider was provided so use the provided key as is.
      keyProvider = new SimpleKeyProvider();
    } else {
      // create an instance of the key provider class and verify it
      // implements KeyProvider
      Object keyProviderObject = null;
      try {
        Class<?> clazz = conf.getClassByName(keyProviderClass);
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
    key = keyProvider.getStorageAccountKey(accountName, conf);

    return key;
  }

  /**
   * Establish a session with Azure blob storage based on the target URI. The
   * method determines whether or not the URI target contains an explicit
   * account or an implicit default cluster-wide account.
   * 
   * @throws AzureException
   * @throws IOException
   */
  private void createAzureStorageSession() throws AzureException {

    // Make sure this object was properly initialized with references to
    // the sessionUri and sessionConfiguration.
    if (null == sessionUri || null == sessionConfiguration) {
      throw new AzureException("Filesystem object not initialized properly."
          + "Unable to start session with Azure Storage server.");
    }

    // File system object initialized, attempt to establish a session
    // with the Azure storage service for the target URI string.
    try {
      // Inspect the URI authority to determine the account and use the account
      // to start an Azure blob client session using an account key for the
      // the account or anonymously.
      // For all URI's do the following checks in order:
      // 1. Validate that <account> can be used with the current Hadoop
      // cluster by checking it exists in the list of configured accounts
      // for the cluster.
      // 2. Look up the AccountKey in the list of configured accounts for the
      // cluster.
      // 3. If there is no AccountKey, assume anonymous public blob access
      // when accessing the blob.
      //
      // If the URI does not specify a container use the default root container
      // under the account name.

      // Assertion: Container name on the session Uri should be non-null.
      if (getContainerFromAuthority(sessionUri) == null) {
        throw new AssertionError(String.format(
            "Non-null container expected from session URI: %s.",
            sessionUri.toString()));
      }

      // Get the account name.
      String accountName = getAccountFromAuthority(sessionUri);
      if (null == accountName) {
        // Account name is not specified as part of the URI. Throw indicating
        // an invalid account name.
        final String errMsg = String.format(
            "Cannot load WASB file system account name not"
                + " specified in URI: %s.", sessionUri.toString());
        throw new AzureException(errMsg);
      }

      instrumentation.setAccountName(accountName);
      String containerName = getContainerFromAuthority(sessionUri);
      instrumentation.setContainerName(containerName);
      
      // Check whether this is a storage emulator account.
      if (isStorageEmulatorAccount(accountName)) {
        // It is an emulator account, connect to it with no credentials.
        connectUsingCredentials(accountName, null, containerName);
        return;
      }

      // Check whether we have a shared access signature for that container.
      String propertyValue = sessionConfiguration.get(KEY_ACCOUNT_SAS_PREFIX
          + containerName + "." + accountName);
      if (propertyValue != null) {
        // SAS was found. Connect using that.
        connectUsingSASCredentials(accountName, containerName, propertyValue);
        return;
      }

      // Check whether the account is configured with an account key.
      propertyValue = getAccountKeyFromConfiguration(accountName,
          sessionConfiguration);
      if (propertyValue != null) {

        // Account key was found.
        // Create the Azure storage session using the account key and container.
        connectUsingConnectionStringCredentials(
            getAccountFromAuthority(sessionUri),
            getContainerFromAuthority(sessionUri), propertyValue);

        // Return to caller
        return;
      }

      // The account access is not configured for this cluster. Try anonymous
      // access.
      connectUsingAnonymousCredentials(sessionUri);

    } catch (Exception e) {
      // Caught exception while attempting to initialize the Azure File
      // System store, re-throw the exception.
      throw new AzureException(e);
    }
  }

  private enum ContainerState {
    /**
     * We haven't checked the container state yet.
     */
    Unknown,
    /**
     * We checked and the container doesn't exist.
     */
    DoesntExist,
    /**
     * The container exists and doesn't have an WASB version stamp on it.
     */
    ExistsNoVersion,
    /**
     * The container exists and has an unsupported WASB version stamped on it.
     */
    ExistsAtWrongVersion,
    /**
     * The container exists and has the proper WASB version stamped on it.
     */
    ExistsAtRightVersion
  }

  private enum ContainerAccessType {
    /**
     * We're accessing the container for a pure read operation, e.g. read a
     * file.
     */
    PureRead,
    /**
     * We're accessing the container purely to write something, e.g. write a
     * file.
     */
    PureWrite,
    /**
     * We're accessing the container to read something then write, e.g. rename a
     * file.
     */
    ReadThenWrite
  }

  /**
   * This should be called from any method that does any modifications to the
   * underlying container: it makes sure to put the WASB current version in the
   * container's metadata if it's not already there.
   */
  private ContainerState checkContainer(ContainerAccessType accessType)
      throws StorageException, AzureException {
    synchronized (containerStateLock) {
      if (isOkContainerState(accessType)) {
        return currentKnownContainerState;
      }
      if (currentKnownContainerState == ContainerState.ExistsAtWrongVersion) {
        String containerVersion = retrieveVersionAttribute(container);
        throw wrongVersionException(containerVersion);
      }
      // This means I didn't check it before or it didn't exist or
      // we need to stamp the version. Since things may have changed by
      // other machines since then, do the check again and don't depend
      // on past information.

      // Sanity check: we don't expect this at this point.
      if (currentKnownContainerState == ContainerState.ExistsAtRightVersion) {
        throw new AssertionError("Unexpected state: "
            + currentKnownContainerState);
      }

      // Download the attributes - doubles as an existence check with just
      // one service call
      try {
        container.downloadAttributes(getInstrumentedContext());
        currentKnownContainerState = ContainerState.Unknown;
      } catch (StorageException ex) {
        if (ex.getErrorCode().equals(
            StorageErrorCode.RESOURCE_NOT_FOUND.toString())) {
          currentKnownContainerState = ContainerState.DoesntExist;
        } else {
          throw ex;
        }
      }

      if (currentKnownContainerState == ContainerState.DoesntExist) {
        // If the container doesn't exist and we intend to write to it,
        // create it now.
        if (needToCreateContainer(accessType)) {
          storeVersionAttribute(container);
          container.create(getInstrumentedContext());
          currentKnownContainerState = ContainerState.ExistsAtRightVersion;
        }
      } else {
        // The container exists, check the version.
        String containerVersion = retrieveVersionAttribute(container);
        if (containerVersion != null) {
          if (containerVersion.equals(FIRST_WASB_VERSION)) {
            // It's the version from when WASB was called ASV, just
            // fix the version attribute if needed and proceed.
            // We should be good otherwise.
            if (needToStampVersion(accessType)) {
              storeVersionAttribute(container);
              container.uploadMetadata(getInstrumentedContext());
            }
          } else if (!containerVersion.equals(CURRENT_WASB_VERSION)) {
            // Don't know this version - throw.
            currentKnownContainerState = ContainerState.ExistsAtWrongVersion;
            throw wrongVersionException(containerVersion);
          } else {
            // It's our correct version.
            currentKnownContainerState = ContainerState.ExistsAtRightVersion;
          }
        } else {
          // No version info exists.
          currentKnownContainerState = ContainerState.ExistsNoVersion;
          if (needToStampVersion(accessType)) {
            // Need to stamp the version
            storeVersionAttribute(container);
            container.uploadMetadata(getInstrumentedContext());
            currentKnownContainerState = ContainerState.ExistsAtRightVersion;
          }
        }
      }
      return currentKnownContainerState;
    }
  }

  private AzureException wrongVersionException(String containerVersion) {
    return new AzureException("The container " + container.getName()
        + " is at an unsupported version: " + containerVersion
        + ". Current supported version: " + FIRST_WASB_VERSION);
  }

  private boolean needToStampVersion(ContainerAccessType accessType) {
    // We need to stamp the version on the container any time we write to
    // it and we have the correct credentials to be able to write container
    // metadata.
    return accessType != ContainerAccessType.PureRead
        && canCreateOrModifyContainer;
  }

  private static boolean needToCreateContainer(ContainerAccessType accessType) {
    // We need to pro-actively create the container (if it doesn't exist) if
    // we're doing a pure write. No need to create it for pure read or read-
    // then-write access.
    return accessType == ContainerAccessType.PureWrite;
  }

  // Determines whether we have to pull the container information again
  // or we can work based off what we already have.
  private boolean isOkContainerState(ContainerAccessType accessType) {
    switch (currentKnownContainerState) {
    case Unknown:
      // When using SAS, we can't discover container attributes
      // so just live with Unknown state and fail later if it
      // doesn't exist.
      return connectingUsingSAS;
    case DoesntExist:
      return false; // the container could have been created
    case ExistsAtRightVersion:
      return true; // fine to optimize
    case ExistsAtWrongVersion:
      return false;
    case ExistsNoVersion:
      // If there's no version, it's OK if we don't need to stamp the version
      // or we can't anyway even if we wanted to.
      return !needToStampVersion(accessType);
    default:
      throw new AssertionError("Unknown access type: " + accessType);
    }
  }

  private boolean getUseTransactionalContentMD5() {
    return sessionConfiguration.getBoolean(KEY_CHECK_BLOCK_MD5, true);
  }

  private BlobRequestOptions getUploadOptions() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setStoreBlobContentMD5(sessionConfiguration.getBoolean(
        KEY_STORE_BLOB_MD5, false));
    options.setUseTransactionalContentMD5(getUseTransactionalContentMD5());
    options.setConcurrentRequestCount(concurrentWrites);

    options.setRetryPolicyFactory(new RetryExponentialRetry(minBackoff,
        deltaBackoff, maxBackoff, maxRetries));

    return options;
  }

  private BlobRequestOptions getDownloadOptions() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setRetryPolicyFactory(new RetryExponentialRetry(minBackoff,
        deltaBackoff, maxBackoff, maxRetries));
    options.setUseTransactionalContentMD5(getUseTransactionalContentMD5());
    return options;
  }

  @Override
  public DataOutputStream storefile(String key,
      PermissionStatus permissionStatus) throws AzureException {
    try {

      // Check if a session exists, if not create a session with the
      // Azure storage server.
      if (null == storageInteractionLayer) {
        final String errMsg = String.format(
            "Storage session expected for URI '%s' but does not exist.",
            sessionUri);
        throw new AzureException(errMsg);
      }

      // Check if there is an authenticated account associated with the
      // file this instance of the WASB file system. If not the file system
      // has not been authenticated and all access is anonymous.
      if (!isAuthenticatedAccess()) {
        // Preemptively raise an exception indicating no uploads are
        // allowed to anonymous accounts.
        throw new AzureException(new IOException(
            "Uploads to public accounts using anonymous "
                + "access is prohibited."));
      }

      checkContainer(ContainerAccessType.PureWrite);

      /**
       * Note: Windows Azure Blob Storage does not allow the creation of
       * arbitrary directory paths under the default $root directory. This is by
       * design to eliminate ambiguity in specifying a implicit blob address. A
       * blob in the $root container cannot include a / in its name and must be
       * careful not to include a trailing '/' when referencing blobs in the
       * $root container. A '/; in the $root container permits ambiguous blob
       * names as in the following example involving two containers $root and
       * mycontainer: http://myaccount.blob.core.windows.net/$root
       * http://myaccount.blob.core.windows.net/mycontainer If the URL
       * "mycontainer/somefile.txt were allowed in $root then the URL:
       * http://myaccount.blob.core.windows.net/mycontainer/myblob.txt could
       * mean either: (1) container=mycontainer; blob=myblob.txt (2)
       * container=$root; blob=mycontainer/myblob.txt
       * 
       * To avoid this type of ambiguity the Azure blob storage prevents
       * arbitrary path under $root. For a simple and more consistent user
       * experience it was decided to eliminate the opportunity for creating
       * such paths by making the $root container read-only under WASB. 
       */

      // Check that no attempt is made to write to blobs on default
      // $root containers.
      if (AZURE_ROOT_CONTAINER.equals(getContainerFromAuthority(sessionUri))) {
        // Azure containers are restricted to non-root containers.
        final String errMsg = String.format(
            "Writes to '%s' container for URI '%s' are prohibited, "
                + "only updates on non-root containers permitted.",
            AZURE_ROOT_CONTAINER, sessionUri.toString());
        throw new AzureException(errMsg);
      }

      // Get the block blob reference from the store's container and
      // return it.
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);

      // Create the output stream for the Azure blob.
      OutputStream outputStream = blob.openOutputStream(getUploadOptions(),
          getInstrumentedContext());

      // Return to caller with DataOutput stream.
      DataOutputStream dataOutStream = new DataOutputStream(outputStream);
      return dataOutStream;
    } catch (Exception e) {
      // Caught exception while attempting to open the blob output stream.
      // Re-throw as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  /**
   * Default permission to use when no permission metadata is found.
   * 
   * @return The default permission to use.
   */
  private static PermissionStatus defaultPermissionNoBlobMetadata() {
    return new PermissionStatus("", "", FsPermission.getDefault());
  }

  private static void storeMetadataAttribute(CloudBlockBlobWrapper blob,
      String key, String value) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String>();
    }
    metadata.put(key, value);
    blob.setMetadata(metadata);
  }

  private static String getMetadataAttribute(CloudBlockBlobWrapper blob,
      String... keyAlternatives) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      return null;
    }
    for (String key : keyAlternatives) {
      if (metadata.containsKey(key)) {
        return metadata.get(key);
      }
    }
    return null;
  }

  private static void removeMetadataAttribute(CloudBlockBlobWrapper blob,
      String key) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (metadata != null) {
      metadata.remove(key);
      blob.setMetadata(metadata);
    }
  }

  private void storePermissionStatus(CloudBlockBlobWrapper blob,
      PermissionStatus permissionStatus) {
    storeMetadataAttribute(blob, PERMISSION_METADATA_KEY,
        PERMISSION_JSON_SERIALIZER.toJSON(permissionStatus));
    // Remove the old metadata key if present
    removeMetadataAttribute(blob, OLD_PERMISSION_METADATA_KEY);
  }

  private PermissionStatus getPermissionStatus(CloudBlockBlobWrapper blob) {
    String permissionMetadataValue = getMetadataAttribute(blob,
        PERMISSION_METADATA_KEY, OLD_PERMISSION_METADATA_KEY);
    if (permissionMetadataValue != null) {
      return PermissionStatusJsonSerializer
          .fromJSONString(permissionMetadataValue);
    } else {
      return defaultPermissionNoBlobMetadata();
    }
  }

  private static void storeFolderAttribute(CloudBlockBlobWrapper blob) {
    storeMetadataAttribute(blob, IS_FOLDER_METADATA_KEY, "true");
    // Remove the old metadata key if present
    removeMetadataAttribute(blob, OLD_IS_FOLDER_METADATA_KEY);
  }

  private static void storeLinkAttribute(CloudBlockBlobWrapper blob,
      String linkTarget) {
    storeMetadataAttribute(blob, LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY,
        linkTarget);
    // Remove the old metadata key if present
    removeMetadataAttribute(blob,
        OLD_LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
  }

  private static String getLinkAttributeValue(CloudBlockBlobWrapper blob) {
    return getMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY,
        OLD_LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
  }

  private static boolean retrieveFolderAttribute(CloudBlockBlobWrapper blob) {
    HashMap<String, String> metadata = blob.getMetadata();
    return null != metadata
        && (metadata.containsKey(IS_FOLDER_METADATA_KEY) || metadata
            .containsKey(OLD_IS_FOLDER_METADATA_KEY));
  }

  private static void storeVersionAttribute(CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String>();
    }
    metadata.put(VERSION_METADATA_KEY, CURRENT_WASB_VERSION);
    if (metadata.containsKey(OLD_VERSION_METADATA_KEY)) {
      metadata.remove(OLD_VERSION_METADATA_KEY);
    }
    container.setMetadata(metadata);
  }

  private static String retrieveVersionAttribute(
      CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    if (metadata == null) {
      return null;
    } else if (metadata.containsKey(VERSION_METADATA_KEY)) {
      return metadata.get(VERSION_METADATA_KEY);
    } else if (metadata.containsKey(OLD_VERSION_METADATA_KEY)) {
      return metadata.get(OLD_VERSION_METADATA_KEY);
    } else {
      return null;
    }
  }

  @Override
  public void storeEmptyFolder(String key, PermissionStatus permissionStatus)
      throws AzureException {

    if (null == storageInteractionLayer) {
      final String errMsg = String.format(
          "Storage session expected for URI '%s' but does not exist.",
          sessionUri);
      throw new AssertionError(errMsg);
    }

    // Check if there is an authenticated account associated with the file
    // this instance of the WASB file system. If not the file system has not
    // been authenticated and all access is anonymous.
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to anonymous accounts.
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      checkContainer(ContainerAccessType.PureWrite);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeFolderAttribute(blob);
      blob.upload(new ByteArrayInputStream(new byte[0]),
          getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      throw new AzureException(e);
    }
  }

  /**
   * Stores an empty blob that's linking to the temporary file where're we're
   * uploading the initial data.
   */
  @Override
  public void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg = String.format(
          "Storage session expected for URI '%s' but does not exist.",
          sessionUri);
      throw new AssertionError(errMsg);
    }
    // Check if there is an authenticated account associated with the file
    // this instance of the WASB file system. If not the file system has not
    // been authenticated and all access is anonymous.
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to anonymous accounts.
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      checkContainer(ContainerAccessType.PureWrite);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeLinkAttribute(blob, tempBlobKey);
      blob.upload(new ByteArrayInputStream(new byte[0]),
          getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      throw new AzureException(e);
    }
  }

  /**
   * If the blob with the given key exists and has a link in its metadata to a
   * temporary file (see storeEmptyLinkFile), this method returns the key to
   * that temporary file. Otherwise, returns null.
   */
  @Override
  public String getLinkInFileMetadata(String key) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg = String.format(
          "Storage session expected for URI '%s' but does not exist.",
          sessionUri);
      throw new AssertionError(errMsg);
    }

    try {
      checkContainer(ContainerAccessType.PureRead);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.downloadAttributes(getInstrumentedContext());
      return getLinkAttributeValue(blob);
    } catch (Exception e) {
      // Caught exception while attempting download. Re-throw as an Azure
      // storage exception.
      throw new AzureException(e);
    }
  }

  /**
   * Private method to check for authenticated access.
   * 
   * @ returns boolean -- true if access is credentialed and authenticated and
   * false otherwise.
   */
  private boolean isAuthenticatedAccess() throws AzureException {

    if (isAnonymousCredentials) {
      // Access to this storage account is unauthenticated.
      return false;
    }
    // Access is authenticated.
    return true;
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container depending on whether the
   * original file system object was constructed with a short- or long-form URI.
   * If the root directory is non-null the URI in the file constructor was in
   * the long form.
   * 
   * @param includeMetadata
   *          if set, the listed items will have their metadata populated
   *          already.
   * 
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(boolean includeMetadata)
      throws StorageException, URISyntaxException {
    return rootDirectory.listBlobs(
        null,
        false,
        includeMetadata ? EnumSet.of(BlobListingDetails.METADATA) : EnumSet
            .noneOf(BlobListingDetails.class), null, getInstrumentedContext());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI. If the root directory is
   * non-null the URI in the file constructor was in the long form.
   * 
   * @param aPrefix
   *          : string name representing the prefix of containing blobs.
   * @param includeMetadata
   *          if set, the listed items will have their metadata populated
   *          already.
   * 
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix,
      boolean includeMetadata) throws StorageException, URISyntaxException {

    return rootDirectory.listBlobs(
        aPrefix,
        false,
        includeMetadata ? EnumSet.of(BlobListingDetails.METADATA) : EnumSet
            .noneOf(BlobListingDetails.class), null, getInstrumentedContext());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI. It also uses the specified flat
   * or hierarchical option, listing details options, request options, and
   * operation context.
   * 
   * @param aPrefix
   *          string name representing the prefix of containing blobs.
   * @param useFlatBlobListing
   *          - the list is flat if true, or hierarchical otherwise.
   * @param listingDetails
   *          - determine whether snapshots, metadata, committed/uncommitted
   *          data
   * @param options
   *          - object specifying additional options for the request. null =
   *          default options
   * @param opContext
   *          - context of the current operation
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix,
      boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
      BlobRequestOptions options, OperationContext opContext)
      throws StorageException, URISyntaxException {

    CloudBlobDirectoryWrapper directory = this.container
        .getDirectoryReference(aPrefix);
    return directory.listBlobs(null, useFlatBlobListing, listingDetails,
        options, opContext);
  }

  /**
   * This private method uses the root directory or the original container to
   * get the block blob reference depending on whether the original file system
   * object was constructed with a short- or long-form URI. If the root
   * directory is non-null the URI in the file constructor was in the long form.
   * 
   * @param aKey
   *          : a key used to query Azure for the block blob.
   * @returns blob : a reference to the Azure block blob corresponding to the
   *          key.
   * @throws URISyntaxException
   * 
   */
  private CloudBlockBlobWrapper getBlobReference(String aKey)
      throws StorageException, URISyntaxException {

    CloudBlockBlobWrapper blob = this.container.getBlockBlobReference(aKey);

    blob.setStreamMinimumReadSizeInBytes(downloadBlockSizeBytes);
    blob.setWriteBlockSizeInBytes(uploadBlockSizeBytes);

    // Return with block blob.
    return blob;
  }

  /**
   * This private method normalizes the key by stripping the container name from
   * the path and returns a path relative to the root directory of the
   * container.
   * 
   * @param keyUri
   *          - adjust this key to a path relative to the root directory
   * 
   * @returns normKey
   */
  private String normalizeKey(URI keyUri) {
    String normKey;

    // Strip the container name from the path and return the path
    // relative to the root directory of the container.
    int parts = isStorageEmulator ? 4 : 3;
    normKey = keyUri.getPath().split("/", parts)[(parts - 1)];

    // Return the fixed key.
    return normKey;
  }

  /**
   * This private method normalizes the key by stripping the container name from
   * the path and returns a path relative to the root directory of the
   * container.
   * 
   * @param blob
   *          - adjust the key to this blob to a path relative to the root
   *          directory
   * 
   * @returns normKey
   */
  private String normalizeKey(CloudBlockBlobWrapper blob) {
    return normalizeKey(blob.getUri());
  }

  /**
   * This private method normalizes the key by stripping the container name from
   * the path and returns a path relative to the root directory of the
   * container.
   * 
   * @param blob
   *          - adjust the key to this directory to a path relative to the root
   *          directory
   * 
   * @returns normKey
   */
  private String normalizeKey(CloudBlobDirectoryWrapper directory) {
    String dirKey = normalizeKey(directory.getUri());
    // Strip the last delimiter
    if (dirKey.endsWith(PATH_DELIMITER)) {
      dirKey = dirKey.substring(0, dirKey.length() - 1);
    }
    return dirKey;
  }

  /**
   * Default method to creates a new OperationContext for the Azure Storage
   * operation that has listeners hooked to it that will update the metrics for
   * this file system. This method does not bind to receive send request
   * callbacks by default.
   * 
   * @return The OperationContext object to use.
   */
  private OperationContext getInstrumentedContext() {
    // Default is to not bind to receive send callback events.
    return getInstrumentedContext(false);
  }

  /**
   * Creates a new OperationContext for the Azure Storage operation that has
   * listeners hooked to it that will update the metrics for this file system.
   * 
   * @param bindConcurrentOOBIo
   *          - bind to intercept send request call backs to handle OOB I/O.
   * 
   * @return The OperationContext object to use.
   */
  private OperationContext getInstrumentedContext(boolean bindConcurrentOOBIo) {

    OperationContext operationContext = new OperationContext();

    if (selfThrottlingEnabled) {
      SelfThrottlingIntercept.hook(operationContext, selfThrottlingReadFactor,
          selfThrottlingWriteFactor);
    }

    ResponseReceivedMetricUpdater.hook(
        operationContext,
        instrumentation,
        bandwidthGaugeUpdater);
    
    // Bind operation context to receive send request callbacks on this
    // operation.
    // If reads concurrent to OOB writes are allowed, the interception will
    // reset the conditional header on all Azure blob storage read requests.
    if (bindConcurrentOOBIo) {
      SendRequestIntercept.bind(storageInteractionLayer.getCredentials(),
          operationContext, true);
    }

    if (testHookOperationContext != null) {
      operationContext = testHookOperationContext
          .modifyOperationContext(operationContext);
    }
    
    ErrorMetricUpdater.hook(operationContext, instrumentation);

    // Return the operation context.
    return operationContext;
  }

  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {

    // Attempts to check status may occur before opening any streams so first,
    // check if a session exists, if not create a session with the Azure storage
    // server.
    if (null == storageInteractionLayer) {
      final String errMsg = String.format(
          "Storage session expected for URI '%s' but does not exist.",
          sessionUri);
      throw new AssertionError(errMsg);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrieving metadata for " + key);
    }

    try {
      if (checkContainer(ContainerAccessType.PureRead) == ContainerState.DoesntExist) {
        // The container doesn't exist, so spare some service calls and just
        // return null now.
        return null;
      }

      // Handle the degenerate cases where the key does not exist or the
      // key is a container.
      if (key.equals("/")) {
        // The key refers to root directory of container.
        // Set the modification time for root to zero.
        return new FileMetadata(key, 0, defaultPermissionNoBlobMetadata(),
            BlobMaterialization.Implicit);
      }

      CloudBlockBlobWrapper blob = getBlobReference(key);

      // Download attributes and return file metadata only if the blob
      // exists.
      if (null != blob && blob.exists(getInstrumentedContext())) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Found " + key
              + " as an explicit blob. Checking if it's a file or folder.");
        }

        // The blob exists, so capture the metadata from the blob
        // properties.
        blob.downloadAttributes(getInstrumentedContext());
        BlobProperties properties = blob.getProperties();

        if (retrieveFolderAttribute(blob)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a folder blob.");
          }
          return new FileMetadata(key, properties.getLastModified().getTime(),
              getPermissionStatus(blob), BlobMaterialization.Explicit);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a normal blob.");
          }

          return new FileMetadata(
              key, // Always return denormalized key with metadata.
              properties.getLength(), properties.getLastModified().getTime(),
              getPermissionStatus(blob));
        }
      }

      // There is no file with that key name, but maybe it is a folder.
      // Query the underlying folder/container to list the blobs stored
      // there under that key.
      Iterable<ListBlobItem> objects = listRootBlobs(key, true,
          EnumSet.of(BlobListingDetails.METADATA), null,
          getInstrumentedContext());

      // Check if the directory/container has the blob items.
      for (ListBlobItem blobItem : objects) {
        if (blobItem instanceof CloudBlockBlobWrapper) {
          LOG.debug("Found blob as a directory-using this file under it to infer its properties "
              + blobItem.getUri());

          blob = (CloudBlockBlobWrapper) blobItem;
          // The key specifies a directory. Create a FileMetadata object which
          // specifies as such.
          BlobProperties properties = blob.getProperties();

          return new FileMetadata(key, properties.getLastModified().getTime(),
              getPermissionStatus(blob), BlobMaterialization.Implicit);
        }
      }

      // Return to caller with a null metadata object.
      return null;

    } catch (Exception e) {
      // Re-throw the exception as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key) throws AzureException {
    InputStream inStream = null;
    BufferedInputStream inBufStream = null;
    try {
      try {
        // Check if a session exists, if not create a session with the
        // Azure storage server.
        if (null == storageInteractionLayer) {
          final String errMsg = String.format(
              "Storage session expected for URI '%s' but does not exist.",
              sessionUri);
          throw new AssertionError(errMsg);
        }
        checkContainer(ContainerAccessType.PureRead);

        // Get blob reference and open the input buffer stream.
        CloudBlockBlobWrapper blob = getBlobReference(key);
        inStream = blob.openInputStream(getDownloadOptions(),
            getInstrumentedContext(isConcurrentOOBAppendAllowed()));

        inBufStream = new BufferedInputStream(inStream);

        // Return a data input stream.
        DataInputStream inDataStream = new DataInputStream(inBufStream);
        return inDataStream;
      }
      catch (Exception e){
        // close the streams on error.
        // We use nested try-catch as stream.close() can throw IOException.
        if(inBufStream != null){
          inBufStream.close();
        }
        if(inStream != null){
          inStream.close();
        }
        throw e;
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key, long startByteOffset)
      throws AzureException {

    InputStream in = null;
    DataInputStream inDataStream = null;
    try {
      try {
        // Check if a session exists, if not create a session with the
        // Azure storage server.
        if (null == storageInteractionLayer) {
          final String errMsg = String.format(
              "Storage session expected for URI '%s' but does not exist.",
              sessionUri);
          throw new AssertionError(errMsg);
        }
        checkContainer(ContainerAccessType.PureRead);

        // Get blob reference and open the input buffer stream.
        CloudBlockBlobWrapper blob = getBlobReference(key);

        // Open input stream and seek to the start offset.
        in = blob.openInputStream(getDownloadOptions(),
            getInstrumentedContext(isConcurrentOOBAppendAllowed()));

        // Create a data input stream.
        inDataStream = new DataInputStream(in);
        long skippedBytes = inDataStream.skip(startByteOffset);
        if (skippedBytes != startByteOffset) {
          throw new IOException("Couldn't skip the requested number of bytes");
        }
        return inDataStream;
      }
      catch (Exception e){
        // close the streams on error.
        // We use nested try-catch as stream.close() can throw IOException.
        if(inDataStream != null){
          inDataStream.close();
        }
        if(in != null){
          in.close();
        }
        throw e;
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount,
      final int maxListingDepth) throws IOException {
    return list(prefix, maxListingCount, maxListingDepth, null);
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount,
      final int maxListingDepth, String priorLastKey) throws IOException {
    return list(prefix, PATH_DELIMITER, maxListingCount, maxListingDepth,
        priorLastKey);
  }

  @Override
  public PartialListing listAll(String prefix, final int maxListingCount,
      final int maxListingDepth, String priorLastKey) throws IOException {
    return list(prefix, null, maxListingCount, maxListingDepth, priorLastKey);
  }

  /**
   * Searches the given list of {@link FileMetadata} objects for a directory
   * with the given key.
   * 
   * @param list
   *          The list to search.
   * @param key
   *          The key to search for.
   * @return The wanted directory, or null if not found.
   */
  private static FileMetadata getDirectoryInList(
      final Iterable<FileMetadata> list, String key) {
    for (FileMetadata current : list) {
      if (current.isDir() && current.getKey().equals(key)) {
        return current;
      }
    }
    return null;
  }

  private PartialListing list(String prefix, String delimiter,
      final int maxListingCount, final int maxListingDepth, String priorLastKey)
      throws IOException {
    try {
      checkContainer(ContainerAccessType.PureRead);

      if (0 < prefix.length() && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }

      Iterable<ListBlobItem> objects;
      if (prefix.equals("/")) {
        objects = listRootBlobs(true);
      } else {
        objects = listRootBlobs(prefix, true);
      }

      ArrayList<FileMetadata> fileMetadata = new ArrayList<FileMetadata>();
      for (ListBlobItem blobItem : objects) {
        // Check that the maximum listing count is not exhausted.
        //
        if (0 < maxListingCount && fileMetadata.size() >= maxListingCount) {
          break;
        }

        if (blobItem instanceof CloudBlockBlobWrapper) {
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey, properties.getLastModified()
                .getTime(), getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(blobKey, properties.getLength(),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the metadata to the list, but remove any existing duplicate
          // entries first that we may have added by finding nested files.
          FileMetadata existing = getDirectoryInList(fileMetadata, blobKey);
          if (existing != null) {
            fileMetadata.remove(existing);
          }
          fileMetadata.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;
          // Determine format of directory name depending on whether an absolute
          // path is being used or not.
          //
          String dirKey = normalizeKey(directory);
          // Strip the last /
          if (dirKey.endsWith(PATH_DELIMITER)) {
            dirKey = dirKey.substring(0, dirKey.length() - 1);
          }

          // Reached the targeted listing depth. Return metadata for the
          // directory using default permissions.
          //
          // Note: Something smarter should be done about permissions. Maybe
          // inherit the permissions of the first non-directory blob.
          // Also, getting a proper value for last-modified is tricky.
          FileMetadata directoryMetadata = new FileMetadata(dirKey, 0,
              defaultPermissionNoBlobMetadata(), BlobMaterialization.Implicit);

          // Add the directory metadata to the list only if it's not already
          // there.
          if (getDirectoryInList(fileMetadata, dirKey) == null) {
            fileMetadata.add(directoryMetadata);
          }

          // Currently at a depth of one, decrement the listing depth for
          // sub-directories.
          buildUpList(directory, fileMetadata, maxListingCount,
              maxListingDepth - 1);
        }
      }
      // Note: Original code indicated that this may be a hack.
      priorLastKey = null;
      return new PartialListing(priorLastKey,
          fileMetadata.toArray(new FileMetadata[] {}),
          0 == fileMetadata.size() ? new String[] {} : new String[] { prefix });
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Build up a metadata list of blobs in an Azure blob directory. This method
   * uses a in-order first traversal of blob directory structures to maintain
   * the sorted order of the blob names.
   * 
   * @param dir
   *          -- Azure blob directory
   * 
   * @param list
   *          -- a list of file metadata objects for each non-directory blob.
   * 
   * @param maxListingLength
   *          -- maximum length of the built up list.
   */
  private void buildUpList(CloudBlobDirectoryWrapper aCloudBlobDirectory,
      ArrayList<FileMetadata> aFileMetadataList, final int maxListingCount,
      final int maxListingDepth) throws Exception {

    // Push the blob directory onto the stack.
    LinkedList<Iterator<ListBlobItem>> dirIteratorStack = new LinkedList<Iterator<ListBlobItem>>();

    Iterable<ListBlobItem> blobItems = aCloudBlobDirectory.listBlobs(null,
        false, EnumSet.of(BlobListingDetails.METADATA), null,
        getInstrumentedContext());
    Iterator<ListBlobItem> blobItemIterator = blobItems.iterator();

    if (0 == maxListingDepth || 0 == maxListingCount) {
      // Recurrence depth and listing count are already exhausted. Return
      // immediately.
      return;
    }

    // The directory listing depth is unbounded if the maximum listing depth
    // is negative.
    final boolean isUnboundedDepth = (maxListingDepth < 0);

    // Reset the current directory listing depth.
    int listingDepth = 1;

    // Loop until all directories have been traversed in-order. Loop only
    // the following conditions are satisfied:
    // (1) The stack is not empty, and
    // (2) maxListingCount > 0 implies that the number of items in the
    // metadata list is less than the max listing count.
    while (null != blobItemIterator
        && (maxListingCount <= 0 || aFileMetadataList.size() < maxListingCount)) {
      while (blobItemIterator.hasNext()) {
        // Check if the count of items on the list exhausts the maximum
        // listing count.
        //
        if (0 < maxListingCount && aFileMetadataList.size() >= maxListingCount) {
          break;
        }

        ListBlobItem blobItem = blobItemIterator.next();

        // Add the file metadata to the list if this is not a blob
        // directory item.
        if (blobItem instanceof CloudBlockBlobWrapper) {
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey, properties.getLastModified()
                .getTime(), getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(blobKey, properties.getLength(),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the directory metadata to the list only if it's not already
          // there.
          FileMetadata existing = getDirectoryInList(aFileMetadataList, blobKey);
          if (existing != null) {
            aFileMetadataList.remove(existing);
          }
          aFileMetadataList.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;

          // This is a directory blob, push the current iterator onto
          // the stack of iterators and start iterating through the current
          // directory.
          if (isUnboundedDepth || maxListingDepth > listingDepth) {
            // Push the current directory on the stack and increment the listing
            // depth.
            dirIteratorStack.push(blobItemIterator);
            ++listingDepth;

            // The current blob item represents the new directory. Get
            // an iterator for this directory and continue by iterating through
            // this directory.
            blobItems = directory.listBlobs(null, false,
                EnumSet.noneOf(BlobListingDetails.class), null,
                getInstrumentedContext());
            blobItemIterator = blobItems.iterator();
          } else {
            // Determine format of directory name depending on whether an
            // absolute path is being used or not.
            String dirKey = normalizeKey(directory);

            if (getDirectoryInList(aFileMetadataList, dirKey) == null) {
              // Reached the targeted listing depth. Return metadata for the
              // directory using default permissions.
              //
              // Note: Something smarter should be done about permissions. Maybe
              // inherit the permissions of the first non-directory blob.
              // Also, getting a proper value for last-modified is tricky.
              FileMetadata directoryMetadata = new FileMetadata(dirKey, 0,
                  defaultPermissionNoBlobMetadata(),
                  BlobMaterialization.Implicit);

              // Add the directory metadata to the list.
              aFileMetadataList.add(directoryMetadata);
            }
          }
        }
      }

      // Traversal of directory tree

      // Check if the iterator stack is empty. If it is set the next blob
      // iterator to null. This will act as a terminator for the for-loop.
      // Otherwise pop the next iterator from the stack and continue looping.
      //
      if (dirIteratorStack.isEmpty()) {
        blobItemIterator = null;
      } else {
        // Pop the next directory item from the stack and decrement the
        // depth.
        blobItemIterator = dirIteratorStack.pop();
        --listingDepth;

        // Assertion: Listing depth should not be less than zero.
        if (listingDepth < 0) {
          throw new AssertionError("Non-negative listing depth expected");
        }
      }
    }
  }

  /**
   * Deletes the given blob, taking special care that if we get a blob-not-found
   * exception upon retrying the operation, we just swallow the error since what
   * most probably happened is that the first operation succeeded on the server.
   * 
   * @param blob
   *          The blob to delete.
   * @throws StorageException
   */
  private void safeDelete(CloudBlockBlobWrapper blob) throws StorageException {
    OperationContext operationContext = getInstrumentedContext();
    try {
      blob.delete(operationContext);
    } catch (StorageException e) {
      // On exception, check that if:
      // 1. It's a BlobNotFound exception AND
      // 2. It got there after one-or-more retries THEN
      // we swallow the exception.
      if (e.getErrorCode() != null && e.getErrorCode().equals("BlobNotFound")
          && operationContext.getRequestResults().size() > 1
          && operationContext.getRequestResults().get(0).getException() != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Swallowing delete exception on retry: " + e.getMessage());
        }
        return;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      if (checkContainer(ContainerAccessType.ReadThenWrite) == ContainerState.DoesntExist) {
        // Container doesn't exist, no need to do anything
        return;
      }

      // Get the blob reference an delete it.
      CloudBlockBlobWrapper blob = getBlobReference(key);
      if (blob.exists(getInstrumentedContext())) {
        safeDelete(blob);
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public void rename(String srcKey, String dstKey) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + srcKey + " to " + dstKey);
    }

    try {
      // Attempts rename may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure
      // storage server.
      if (null == storageInteractionLayer) {
        final String errMsg = String.format(
            "Storage session expected for URI '%s' but does not exist.",
            sessionUri);
        throw new AssertionError(errMsg);
      }

      checkContainer(ContainerAccessType.ReadThenWrite);
      // Get the source blob and assert its existence. If the source key
      // needs to be normalized then normalize it.
      CloudBlockBlobWrapper srcBlob = getBlobReference(srcKey);

      if (!srcBlob.exists(getInstrumentedContext())) {
        throw new AzureException("Source blob " + srcKey + " does not exist.");
      }

      // Get the destination blob. The destination key always needs to be
      // normalized.
      CloudBlockBlobWrapper dstBlob = getBlobReference(dstKey);

      // Rename the source blob to the destination blob by copying it to
      // the destination blob then deleting it.
      //
      dstBlob.startCopyFromBlob(srcBlob, getInstrumentedContext());
      waitForCopyToComplete(dstBlob, getInstrumentedContext());

      safeDelete(srcBlob);
    } catch (Exception e) {
      // Re-throw exception as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  private void waitForCopyToComplete(CloudBlockBlobWrapper blob,
      OperationContext opContext) throws AzureException {
    boolean copyInProgress = true;
    int exceptionCount = 0;
    while (copyInProgress) {
      try {
        blob.downloadAttributes(opContext);
      } catch (StorageException se) {
        exceptionCount++;
        if(exceptionCount > 10){
          throw new AzureException("Too many storage exceptions during waitForCopyToComplete", se);
        }
      }

      // test for null because mocked filesystem doesn't know about copystates
      // yet.
      copyInProgress = (blob.getCopyState() != null && blob.getCopyState()
          .getStatus() == CopyStatus.PENDING);
      if (copyInProgress) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Changes the permission status on the given key.
   */
  @Override
  public void changePermissionStatus(String key, PermissionStatus newPermission)
      throws AzureException {
    try {
      checkContainer(ContainerAccessType.ReadThenWrite);
      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.downloadAttributes(getInstrumentedContext());
      storePermissionStatus(blob, newPermission);
      blob.uploadMetadata(getInstrumentedContext());
    } catch (Exception e) {
      throw new AzureException(e);
    }
  }

  @Override
  public void purge(String prefix) throws IOException {
    try {

      // Attempts to purge may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure
      // storage server.
      if (null == storageInteractionLayer) {
        final String errMsg = String.format(
            "Storage session expected for URI '%s' but does not exist.",
            sessionUri);
        throw new AssertionError(errMsg);
      }

      if (checkContainer(ContainerAccessType.ReadThenWrite) == ContainerState.DoesntExist) {
        // Container doesn't exist, no need to do anything.
        return;
      }
      // Get all blob items with the given prefix from the container and delete
      // them.
      Iterable<ListBlobItem> objects = listRootBlobs(prefix, false);
      for (ListBlobItem blobItem : objects) {
        ((CloudBlob) blobItem).delete(DeleteSnapshotsOption.NONE, null, null,
            getInstrumentedContext());
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public void updateFolderLastModifiedTime(String key, Date lastModified)
      throws AzureException {
    try {
      checkContainer(ContainerAccessType.ReadThenWrite);
      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.getProperties().setLastModified(lastModified);
      blob.uploadProperties(getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting update the properties. Re-throw as an
      // Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public void updateFolderLastModifiedTime(String key) throws AzureException {
    final Calendar lastModifiedCalendar = Calendar
        .getInstance(Utility.LOCALE_US);
    lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
    Date lastModified = lastModifiedCalendar.getTime();
    updateFolderLastModifiedTime(key, lastModified);
  }

  @Override
  public void dump() throws IOException {
  }

  @Override
  public void close() {
    bandwidthGaugeUpdater.close();
  }
  
  // Finalizer to ensure complete shutdown
  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called");
    close();
    super.finalize();
  }
}
