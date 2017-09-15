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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobContainerWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobDirectoryWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlobWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudBlockBlobWrapper;
import org.apache.hadoop.fs.azure.StorageInterface.CloudPageBlobWrapper;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.BandwidthGaugeUpdater;
import org.apache.hadoop.fs.azure.metrics.ErrorMetricUpdater;
import org.apache.hadoop.fs.azure.metrics.ResponseReceivedMetricUpdater;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.VersionInfo;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageErrorCode;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.core.BaseRequest;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlobType;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.core.Utility;

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

  /**
   * Configuration for User-Agent field.
   */
  static final String USER_AGENT_ID_KEY = "fs.azure.user.agent.prefix";
  static final String USER_AGENT_ID_DEFAULT = "unknown";

  public static final Logger LOG = LoggerFactory.getLogger(AzureNativeFileSystemStore.class);

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
  @VisibleForTesting
  static final String KEY_INPUT_STREAM_VERSION = "fs.azure.input.stream.version.for.internal.use.only";

  // Property controlling whether to allow reads on blob which are concurrently
  // appended out-of-band.
  private static final String KEY_READ_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";

  // Configurable throttling parameter properties. These properties are located
  // in the core-site.xml configuration file.
  private static final String KEY_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  private static final String KEY_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  private static final String KEY_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  private static final String KEY_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";

  private static final String KEY_COPYBLOB_MIN_BACKOFF_INTERVAL =
    "fs.azure.io.copyblob.retry.min.backoff.interval";
  private static final String KEY_COPYBLOB_MAX_BACKOFF_INTERVAL =
    "fs.azure.io.copyblob.retry.max.backoff.interval";
  private static final String KEY_COPYBLOB_BACKOFF_INTERVAL =
    "fs.azure.io.copyblob.retry.backoff.interval";
  private static final String KEY_COPYBLOB_MAX_IO_RETRIES =
    "fs.azure.io.copyblob.retry.max.retries";

  private static final String KEY_SELF_THROTTLE_ENABLE = "fs.azure.selfthrottling.enable";
  private static final String KEY_SELF_THROTTLE_READ_FACTOR = "fs.azure.selfthrottling.read.factor";
  private static final String KEY_SELF_THROTTLE_WRITE_FACTOR = "fs.azure.selfthrottling.write.factor";

  private static final String KEY_AUTO_THROTTLE_ENABLE = "fs.azure.autothrottling.enable";

  private static final String KEY_ENABLE_STORAGE_CLIENT_LOGGING = "fs.azure.storage.client.logging";

  /**
   * Configuration keys to identify if WASB needs to run in Secure mode. In Secure mode
   * all interactions with Azure storage is performed using SAS uris. There are two sub modes
   * within the Secure mode , one is remote SAS key mode where the SAS keys are generated
   * from a remote process and local mode where SAS keys are generated within WASB.
   */
  @VisibleForTesting
  public static final String KEY_USE_SECURE_MODE = "fs.azure.secure.mode";

  /**
   * By default the SAS Key mode is expected to run in Romote key mode. This flags sets it
   * to run on the local mode.
   */
  public static final String KEY_USE_LOCAL_SAS_KEY_MODE = "fs.azure.local.sas.key.mode";

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

  /**
   * Configuration key to indicate the set of directories in WASB where we
   * should store files as page blobs instead of block blobs.
   *
   * Entries should be plain directory names (i.e. not URIs) with no leading or
   * trailing slashes. Delimit the entries with commas.
   */
  public static final String KEY_PAGE_BLOB_DIRECTORIES =
      "fs.azure.page.blob.dir";
  /**
   * The set of directories where we should store files as page blobs.
   */
  private Set<String> pageBlobDirs;

  /**
   * Configuration key to indicate the set of directories in WASB where we
   * should store files as block blobs with block compaction enabled.
   *
   * Entries can be directory paths relative to the container (e.g. "/path") or
   * fully qualified wasb:// URIs (e.g.
   * wasb://container@example.blob.core.windows.net/path)
   */
  public static final String KEY_BLOCK_BLOB_WITH_COMPACTION_DIRECTORIES =
          "fs.azure.block.blob.with.compaction.dir";

  /**
   * The set of directories where we should store files as block blobs with
   * block compaction enabled.
   */
  private Set<String> blockBlobWithCompationDirs;

  /**
   * Configuration key to indicate the set of directories in WASB where
   * we should do atomic folder rename synchronized with createNonRecursive.
   */
  public static final String KEY_ATOMIC_RENAME_DIRECTORIES =
      "fs.azure.atomic.rename.dir";

  /**
   * Configuration key to enable flat listing of blobs. This config is useful
   * only if listing depth is AZURE_UNBOUNDED_DEPTH.
   */
  public static final String KEY_ENABLE_FLAT_LISTING = "fs.azure.flatlist.enable";

  /**
   * The set of directories where we should apply atomic folder rename
   * synchronized with createNonRecursive.
   */
  private Set<String> atomicRenameDirs;

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String WASB_AUTHORITY_DELIMITER = "@";
  private static final String AZURE_ROOT_CONTAINER = "$root";

  private static final int DEFAULT_CONCURRENT_WRITES = 8;

  // Concurrent reads reads of data written out of band are disable by default.
  //
  private static final boolean DEFAULT_READ_TOLERATE_CONCURRENT_APPEND = false;

  // Default block sizes
  public static final int DEFAULT_DOWNLOAD_BLOCK_SIZE = 4 * 1024 * 1024;
  public static final int DEFAULT_UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024;

  private static final int DEFAULT_INPUT_STREAM_VERSION = 2;

  // Retry parameter defaults.
  //

  private static final int DEFAULT_MIN_BACKOFF_INTERVAL = 3 * 1000; // 1s
  private static final int DEFAULT_MAX_BACKOFF_INTERVAL = 30 * 1000; // 30s
  private static final int DEFAULT_BACKOFF_INTERVAL = 3 * 1000; // 1s
  private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 30;

  private static final int DEFAULT_COPYBLOB_MIN_BACKOFF_INTERVAL = 3  * 1000;
  private static final int DEFAULT_COPYBLOB_MAX_BACKOFF_INTERVAL = 90 * 1000;
  private static final int DEFAULT_COPYBLOB_BACKOFF_INTERVAL = 30 * 1000;
  private static final int DEFAULT_COPYBLOB_MAX_RETRY_ATTEMPTS = 15;

  // Self-throttling defaults. Allowed range = (0,1.0]
  // Value of 1.0 means no self-throttling.
  // Value of x means process data at factor x of unrestricted rate
  private static final boolean DEFAULT_SELF_THROTTLE_ENABLE = true;
  private static final float DEFAULT_SELF_THROTTLE_READ_FACTOR = 1.0f;
  private static final float DEFAULT_SELF_THROTTLE_WRITE_FACTOR = 1.0f;

  private static final boolean DEFAULT_AUTO_THROTTLE_ENABLE = false;

  private static final int STORAGE_CONNECTION_TIMEOUT_DEFAULT = 90;

  /**
   * Default values to control SAS Key mode.
   * By default we set the values to false.
   */
  public static final boolean DEFAULT_USE_SECURE_MODE = false;
  private static final boolean DEFAULT_USE_LOCAL_SAS_KEY_MODE = false;

  /**
   * Enable flat listing of blobs as default option. This is useful only if
   * listing depth is AZURE_UNBOUNDED_DEPTH.
   */
  public static final boolean DEFAULT_ENABLE_FLAT_LISTING = false;

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
  private int inputStreamVersion = DEFAULT_INPUT_STREAM_VERSION;

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

  private boolean autoThrottlingEnabled;

  private TestHookOperationContext testHookOperationContext = null;

  // Set if we're running against a storage emulator..
  private boolean isStorageEmulator = false;

  // Configs controlling WASB SAS key mode.
  private boolean useSecureMode = false;
  private boolean useLocalSasKeyMode = false;

  // User-Agent
  private String userAgentId;

  private String delegationToken;

  /** The error message template when container is not accessible. */
  public static final String NO_ACCESS_TO_CONTAINER_MSG = "No credentials found for "
      + "account %s in the configuration, and its container %s is not "
      + "accessible using anonymous credentials. Please check if the container "
      + "exists first. If it is not publicly available, you have to provide "
      + "account credentials.";

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
   * @param uri - URI for target storage blob.
   * @param conf - reference to configuration object.
   * @param instrumentation - the metrics source that will keep track of operations here.
   *
   * @throws IllegalArgumentException if URI or job object is null, or invalid scheme.
   */
  @Override
  public void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation)
      throws IllegalArgumentException, AzureException, IOException  {

    if (null == instrumentation) {
      throw new IllegalArgumentException("Null instrumentation");
    }
    this.instrumentation = instrumentation;

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
          "Cannot initialize WASB file system, conf is null");
    }

    if (!conf.getBoolean(
        NativeAzureFileSystem.SKIP_AZURE_METRICS_PROPERTY_NAME, false)) {
      //If not skip azure metrics, create bandwidthGaugeUpdater
      this.bandwidthGaugeUpdater = new BandwidthGaugeUpdater(instrumentation);
    }

    // Incoming parameters validated. Capture the URI and the job configuration
    // object.
    //
    sessionUri = uri;
    sessionConfiguration = conf;

    useSecureMode = conf.getBoolean(KEY_USE_SECURE_MODE,
        DEFAULT_USE_SECURE_MODE);
    useLocalSasKeyMode = conf.getBoolean(KEY_USE_LOCAL_SAS_KEY_MODE,
        DEFAULT_USE_LOCAL_SAS_KEY_MODE);

    if (null == this.storageInteractionLayer) {
      if (!useSecureMode) {
        this.storageInteractionLayer = new StorageInterfaceImpl();
      } else {
        this.storageInteractionLayer = new SecureStorageInterfaceImpl(
            useLocalSasKeyMode, conf);
      }
    }

    // Configure Azure storage session.
    configureAzureStorageSession();

    // Start an Azure storage session.
    //
    createAzureStorageSession();

    // Extract the directories that should contain page blobs
    pageBlobDirs = getDirectorySet(KEY_PAGE_BLOB_DIRECTORIES);
    LOG.debug("Page blob directories:  {}", setToString(pageBlobDirs));

    // User-agent
    userAgentId = conf.get(USER_AGENT_ID_KEY, USER_AGENT_ID_DEFAULT);

    // Extract the directories that should contain block blobs with compaction
    blockBlobWithCompationDirs = getDirectorySet(
        KEY_BLOCK_BLOB_WITH_COMPACTION_DIRECTORIES);
    LOG.debug("Block blobs with compaction directories:  {}",
        setToString(blockBlobWithCompationDirs));

    // Extract directories that should have atomic rename applied.
    atomicRenameDirs = getDirectorySet(KEY_ATOMIC_RENAME_DIRECTORIES);
    String hbaseRoot;
    try {

      // Add to this the hbase root directory, or /hbase is that is not set.
      hbaseRoot = verifyAndConvertToStandardFormat(
          sessionConfiguration.get("hbase.rootdir", "hbase"));
      if (hbaseRoot != null) {
        atomicRenameDirs.add(hbaseRoot);
      }
    } catch (URISyntaxException e) {
      LOG.warn("Unable to initialize HBase root as an atomic rename directory.");
    }
    LOG.debug("Atomic rename directories: {} ", setToString(atomicRenameDirs));
  }

  /**
   * Helper to format a string for log output from Set<String>
   */
  private String setToString(Set<String> set) {
    StringBuilder sb = new StringBuilder();
    int i = 1;
    for (String s : set) {
      sb.append("/" + s);
      if (i != set.size()) {
        sb.append(", ");
      }
      i++;
    }
    return sb.toString();
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

    // Check if authority container the delimiter separating the account name from the
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
      // The authority does not have a container name. Use the default container by
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
        && (sessionScheme.equalsIgnoreCase("asvs")
         || sessionScheme.equalsIgnoreCase("wasbs"))) {
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

    this.inputStreamVersion = sessionConfiguration.getInt(
        KEY_INPUT_STREAM_VERSION, DEFAULT_INPUT_STREAM_VERSION);

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
    //
    minBackoff = sessionConfiguration.getInt(
        KEY_MIN_BACKOFF_INTERVAL, DEFAULT_MIN_BACKOFF_INTERVAL);

    maxBackoff = sessionConfiguration.getInt(
        KEY_MAX_BACKOFF_INTERVAL, DEFAULT_MAX_BACKOFF_INTERVAL);

    deltaBackoff = sessionConfiguration.getInt(
        KEY_BACKOFF_INTERVAL, DEFAULT_BACKOFF_INTERVAL);

    maxRetries = sessionConfiguration.getInt(
        KEY_MAX_IO_RETRIES, DEFAULT_MAX_RETRY_ATTEMPTS);

    storageInteractionLayer.setRetryPolicyFactory(
          new RetryExponentialRetry(minBackoff, deltaBackoff, maxBackoff, maxRetries));


    // read the self-throttling config.
    selfThrottlingEnabled = sessionConfiguration.getBoolean(
        KEY_SELF_THROTTLE_ENABLE, DEFAULT_SELF_THROTTLE_ENABLE);

    selfThrottlingReadFactor = sessionConfiguration.getFloat(
        KEY_SELF_THROTTLE_READ_FACTOR, DEFAULT_SELF_THROTTLE_READ_FACTOR);

    selfThrottlingWriteFactor = sessionConfiguration.getFloat(
        KEY_SELF_THROTTLE_WRITE_FACTOR, DEFAULT_SELF_THROTTLE_WRITE_FACTOR);

    if (!selfThrottlingEnabled) {
      autoThrottlingEnabled = sessionConfiguration.getBoolean(
          KEY_AUTO_THROTTLE_ENABLE,
          DEFAULT_AUTO_THROTTLE_ENABLE);
      if (autoThrottlingEnabled) {
        ClientThrottlingIntercept.initializeSingleton();
      }
    } else {
      // cannot enable both self-throttling and client-throttling
      autoThrottlingEnabled = false;
    }

    OperationContext.setLoggingEnabledByDefault(sessionConfiguration.
        getBoolean(KEY_ENABLE_STORAGE_CLIENT_LOGGING, false));

    LOG.debug(
        "AzureNativeFileSystemStore init. Settings={},{},{},{{},{},{},{}},{{},{},{}}",
        concurrentWrites, tolerateOobAppends,
        ((storageConnectionTimeout > 0) ? storageConnectionTimeout
          : STORAGE_CONNECTION_TIMEOUT_DEFAULT), minBackoff,
        deltaBackoff, maxBackoff, maxRetries, selfThrottlingEnabled,
        selfThrottlingReadFactor, selfThrottlingWriteFactor);
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
   * @throws URISyntaxException
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
    boolean canAccess;
    try {
      canAccess = container.exists(getInstrumentedContext());
    } catch (StorageException ex) {
      LOG.error("Service returned StorageException when checking existence "
          + "of container {} in account {}", containerName, accountName, ex);
      canAccess = false;
    }
    if (!canAccess) {
      throw new AzureException(String.format(NO_ACCESS_TO_CONTAINER_MSG,
          accountName, containerName));
    }

    // Accessing the storage server unauthenticated using
    // anonymous credentials.
    isAnonymousCredentials = true;
  }

  private void connectUsingCredentials(String accountName,
      StorageCredentials credentials, String containerName)
      throws URISyntaxException, StorageException, AzureException {

    URI blobEndPoint;
    if (isStorageEmulatorAccount(accountName)) {
      isStorageEmulator = true;
      CloudStorageAccount account =
          CloudStorageAccount.getDevelopmentStorageAccount();
      storageInteractionLayer.createBlobClient(account);
    } else {
      blobEndPoint = new URI(getHTTPScheme() + "://" + accountName);
      storageInteractionLayer.createBlobClient(blobEndPoint, credentials);
    }
    suppressRetryPolicyInClientIfNeeded();

    // Capture the container reference for debugging purposes.
    container = storageInteractionLayer.getContainerReference(containerName);
    rootDirectory = container.getDirectoryReference("");

    // Can only create container if using account key credentials
    canCreateOrModifyContainer = credentials instanceof StorageCredentialsAccountAndKey;
  }

  /**
   * Method to set up the Storage Interaction layer in Secure mode.
   * @param accountName - Storage account provided in the initializer
   * @param containerName - Container name provided in the initializer
   * @param sessionUri - URI provided in the initializer
   */
  private void connectToAzureStorageInSecureMode(String accountName,
      String containerName, URI sessionUri)
          throws AzureException, StorageException, URISyntaxException {

    // Assertion: storageInteractionLayer instance has to be a SecureStorageInterfaceImpl
    if (!(this.storageInteractionLayer instanceof SecureStorageInterfaceImpl)) {
      throw new AssertionError("connectToAzureStorageInSASKeyMode() should be called only"
        + " for SecureStorageInterfaceImpl instances");
    }

    ((SecureStorageInterfaceImpl) this.storageInteractionLayer).
      setStorageAccountName(accountName);

    container = storageInteractionLayer.getContainerReference(containerName);
    rootDirectory = container.getDirectoryReference("");

    canCreateOrModifyContainer = true;
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

  @VisibleForTesting
  public static String getAccountKeyFromConfiguration(String accountName,
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
  private void createAzureStorageSession()
      throws AzureException, IOException {

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

      // If the securemode flag is set, WASB uses SecureStorageInterfaceImpl instance
      // to communicate with Azure storage. In SecureStorageInterfaceImpl SAS keys
      // are used to communicate with Azure storage, so connectToAzureStorageInSecureMode
      // instantiates the default container using a SAS Key.
      if (useSecureMode) {
        connectToAzureStorageInSecureMode(accountName, containerName, sessionUri);
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
      if (StringUtils.isNotEmpty(propertyValue)) {
        // Account key was found.
        // Create the Azure storage session using the account key and container.
        connectUsingConnectionStringCredentials(
            getAccountFromAuthority(sessionUri),
            getContainerFromAuthority(sessionUri), propertyValue);
      } else {
        LOG.debug("The account access key is not configured for {}. "
            + "Now try anonymous access.", sessionUri);
        connectUsingAnonymousCredentials(sessionUri);
      }
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
   * Trims a suffix/prefix from the given string. For example if
   * s is given as "/xy" and toTrim is "/", this method returns "xy"
   */
  private static String trim(String s, String toTrim) {
    return StringUtils.removeEnd(StringUtils.removeStart(s, toTrim),
        toTrim);
  }

  /**
   * Checks if the given rawDir belongs to this account/container, and
   * if so returns the canonicalized path for it. Otherwise return null.
   */
  private String verifyAndConvertToStandardFormat(String rawDir) throws URISyntaxException {
    URI asUri = new URI(rawDir);
    if (asUri.getAuthority() == null
        || asUri.getAuthority().toLowerCase(Locale.ENGLISH).equalsIgnoreCase(
      sessionUri.getAuthority().toLowerCase(Locale.ENGLISH))) {
      // Applies to me.
      return trim(asUri.getPath(), "/");
    } else {
      // Doen't apply to me.
      return null;
    }
  }

  /**
   * Take a comma-separated list of directories from a configuration variable
   * and transform it to a set of directories.
   */
  private Set<String> getDirectorySet(final String configVar)
      throws AzureException {
    String[] rawDirs = sessionConfiguration.getStrings(configVar, new String[0]);
    Set<String> directorySet = new HashSet<String>();
    for (String currentDir : rawDirs) {
      String myDir;
      try {
        myDir = verifyAndConvertToStandardFormat(currentDir);
      } catch (URISyntaxException ex) {
        throw new AzureException(String.format(
            "The directory %s specified in the configuration entry %s is not"
            + " a valid URI.",
            currentDir, configVar));
      }
      if (myDir != null) {
        directorySet.add(myDir);
      }
    }
    return directorySet;
  }

  /**
   * Checks if the given key in Azure Storage should be stored as a page
   * blob instead of block blob.
   */
  public boolean isPageBlobKey(String key) {
    return isKeyForDirectorySet(key, pageBlobDirs);
  }

  /**
   * Checks if the given key in Azure Storage should be stored as a block blobs
   * with compaction enabled instead of normal block blob.
   *
   * @param key blob name
   * @return true, if the file is in directory with block compaction enabled.
   */
  public boolean isBlockBlobWithCompactionKey(String key) {
    return isKeyForDirectorySet(key, blockBlobWithCompationDirs);
  }

  /**
   * Checks if the given key in Azure storage should have synchronized
   * atomic folder rename createNonRecursive implemented.
   */
  @Override
  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, atomicRenameDirs);
  }

  public boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    String defaultFS = FileSystem.getDefaultUri(sessionConfiguration).toString();
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + "/")) {
        return true;
      }

      // Allow for blob directories with paths relative to the default file
      // system.
      //
      try {
        URI uriPageBlobDir = new URI(dir);
        if (null == uriPageBlobDir.getAuthority()) {
          // Concatenate the default file system prefix with the relative
          // page blob directory path.
          //
          if (key.startsWith(trim(defaultFS, "/") + "/" + dir + "/")){
            return true;
          }
        }
      } catch (URISyntaxException e) {
        LOG.info("URI syntax error creating URI for {}", dir);
      }
    }
    return false;
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
        if (StorageErrorCode.RESOURCE_NOT_FOUND.toString()
            .equals(ex.getErrorCode())) {
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
    options.setRetryPolicyFactory(
          new RetryExponentialRetry(minBackoff, deltaBackoff, maxBackoff, maxRetries));
    options.setUseTransactionalContentMD5(getUseTransactionalContentMD5());
    return options;
  }

  @Override
  public DataOutputStream storefile(String keyEncoded,
                                    PermissionStatus permissionStatus,
                                    String key)
      throws AzureException {
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
       * Note: Windows Azure Blob Storage does not allow the creation of arbitrary directory
       *      paths under the default $root directory.  This is by design to eliminate
       *      ambiguity in specifying a implicit blob address. A blob in the $root conatiner
       *      cannot include a / in its name and must be careful not to include a trailing
       *      '/' when referencing  blobs in the $root container.
       *      A '/; in the $root container permits ambiguous blob names as in the following
       *      example involving two containers $root and mycontainer:
       *                http://myaccount.blob.core.windows.net/$root
       *                http://myaccount.blob.core.windows.net/mycontainer
       *      If the URL "mycontainer/somefile.txt were allowed in $root then the URL:
       *                http://myaccount.blob.core.windows.net/mycontainer/myblob.txt
       *      could mean either:
       *        (1) container=mycontainer; blob=myblob.txt
       *        (2) container=$root; blob=mycontainer/myblob.txt
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

      // Get the blob reference from the store's container and
      // return it.
      CloudBlobWrapper blob = getBlobReference(keyEncoded);
      storePermissionStatus(blob, permissionStatus);

      // Create the output stream for the Azure blob.
      //
      OutputStream outputStream;

      if (isBlockBlobWithCompactionKey(key)) {
        BlockBlobAppendStream blockBlobOutputStream = new BlockBlobAppendStream(
            (CloudBlockBlobWrapper) blob,
            keyEncoded,
            this.uploadBlockSizeBytes,
            true,
            getInstrumentedContext());

        outputStream = blockBlobOutputStream;
      } else {
        outputStream = openOutputStream(blob);
      }

      DataOutputStream dataOutStream = new SyncableDataOutputStream(outputStream);
      return dataOutStream;
    } catch (Exception e) {
      // Caught exception while attempting to open the blob output stream.
      // Re-throw as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  /**
   * Opens a new output stream to the given blob (page or block blob)
   * to populate it from scratch with data.
   */
  private OutputStream openOutputStream(final CloudBlobWrapper blob)
      throws StorageException {
    if (blob instanceof CloudPageBlobWrapper){
      return new PageBlobOutputStream(
          (CloudPageBlobWrapper) blob, getInstrumentedContext(), sessionConfiguration);
    } else {

      // Handle both ClouldBlockBlobWrapperImpl and (only for the test code path)
      // MockCloudBlockBlobWrapper.
      return ((CloudBlockBlobWrapper) blob).openOutputStream(getUploadOptions(),
                getInstrumentedContext());
    }
  }

  /**
   * Opens a new input stream for the given blob (page or block blob)
   * to read its data.
   */
  private InputStream openInputStream(CloudBlobWrapper blob)
      throws StorageException, IOException {
    if (blob instanceof CloudBlockBlobWrapper) {
      LOG.debug("Using stream seek algorithm {}", inputStreamVersion);
      switch(inputStreamVersion) {
      case 1:
        return blob.openInputStream(getDownloadOptions(),
            getInstrumentedContext(isConcurrentOOBAppendAllowed()));
      case 2:
        return new BlockBlobInputStream((CloudBlockBlobWrapper) blob,
            getDownloadOptions(),
            getInstrumentedContext(isConcurrentOOBAppendAllowed()));
      default:
        throw new IOException("Unknown seek algorithm: " + inputStreamVersion);
      }
    } else {
      return new PageBlobInputStream(
          (CloudPageBlobWrapper) blob, getInstrumentedContext(
              isConcurrentOOBAppendAllowed()));
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

  private static void storeMetadataAttribute(CloudBlobWrapper blob,
      String key, String value) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String>();
    }
    metadata.put(key, value);
    blob.setMetadata(metadata);
  }

  private static String getMetadataAttribute(CloudBlobWrapper blob,
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

  private static void removeMetadataAttribute(CloudBlobWrapper blob,
      String key) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (metadata != null) {
      metadata.remove(key);
      blob.setMetadata(metadata);
    }
  }

  private static void storePermissionStatus(CloudBlobWrapper blob,
      PermissionStatus permissionStatus) {
    storeMetadataAttribute(blob, PERMISSION_METADATA_KEY,
        PERMISSION_JSON_SERIALIZER.toJSON(permissionStatus));
    // Remove the old metadata key if present
    removeMetadataAttribute(blob, OLD_PERMISSION_METADATA_KEY);
  }

  private PermissionStatus getPermissionStatus(CloudBlobWrapper blob) {
    String permissionMetadataValue = getMetadataAttribute(blob,
        PERMISSION_METADATA_KEY, OLD_PERMISSION_METADATA_KEY);
    if (permissionMetadataValue != null) {
      return PermissionStatusJsonSerializer.fromJSONString(
          permissionMetadataValue);
    } else {
      return defaultPermissionNoBlobMetadata();
    }
  }

  private static void storeFolderAttribute(CloudBlobWrapper blob) {
    storeMetadataAttribute(blob, IS_FOLDER_METADATA_KEY, "true");
    // Remove the old metadata key if present
    removeMetadataAttribute(blob, OLD_IS_FOLDER_METADATA_KEY);
  }

  private static void storeLinkAttribute(CloudBlobWrapper blob,
      String linkTarget) throws UnsupportedEncodingException {
    // We have to URL encode the link attribute as the link URI could
    // have URI special characters which unless encoded will result
    // in 403 errors from the server. This is due to metadata properties
    // being sent in the HTTP header of the request which is in turn used
    // on the server side to authorize the request.
    String encodedLinkTarget = null;
    if (linkTarget != null) {
      encodedLinkTarget = URLEncoder.encode(linkTarget, "UTF-8");
    }
    storeMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY,
        encodedLinkTarget);
    // Remove the old metadata key if present
    removeMetadataAttribute(blob,
        OLD_LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
  }

  private static String getLinkAttributeValue(CloudBlobWrapper blob)
      throws UnsupportedEncodingException {
    String encodedLinkTarget = getMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY,
        OLD_LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
    String linkTarget = null;
    if (encodedLinkTarget != null) {
      linkTarget = URLDecoder.decode(encodedLinkTarget, "UTF-8");
    }
    return linkTarget;
  }

  private static boolean retrieveFolderAttribute(CloudBlobWrapper blob) {
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

      CloudBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeFolderAttribute(blob);
      openOutputStream(blob).close();
    } catch (StorageException e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      throw new AzureException(e);
    } catch (URISyntaxException e) {
      throw new AzureException(e);
    } catch (IOException e) {
      Throwable t = e.getCause();
      if (t != null && t instanceof StorageException) {
        StorageException se = (StorageException) t;
        // If we got this exception, the blob should have already been created
        if (!"LeaseIdMissing".equals(se.getErrorCode())) {
          throw new AzureException(e);
        }
      } else {
        throw new AzureException(e);
      }
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

      CloudBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeLinkAttribute(blob, tempBlobKey);
      openOutputStream(blob).close();
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

      CloudBlobWrapper blob = getBlobReference(key);
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
   * @param useFlatBlobListing
   *          if set the list is flat, otherwise it is hierarchical.
   *
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   *
   */
  private Iterable<ListBlobItem> listRootBlobs(boolean includeMetadata,
      boolean useFlatBlobListing) throws StorageException, URISyntaxException {
    return rootDirectory.listBlobs(
        null,
        useFlatBlobListing,
        includeMetadata
            ? EnumSet.of(BlobListingDetails.METADATA)
            : EnumSet.noneOf(BlobListingDetails.class),
        null,
        getInstrumentedContext());
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
   * @param useFlatBlobListing
   *          if set the list is flat, otherwise it is hierarchical.
   *
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   *
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix, boolean includeMetadata,
      boolean useFlatBlobListing) throws StorageException, URISyntaxException {

    Iterable<ListBlobItem> list = rootDirectory.listBlobs(aPrefix,
        useFlatBlobListing,
        includeMetadata
            ? EnumSet.of(BlobListingDetails.METADATA)
            : EnumSet.noneOf(BlobListingDetails.class),
        null,
        getInstrumentedContext());
    return list;
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
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix, boolean useFlatBlobListing,
      EnumSet<BlobListingDetails> listingDetails, BlobRequestOptions options,
      OperationContext opContext) throws StorageException, URISyntaxException {

    CloudBlobDirectoryWrapper directory =  this.container.getDirectoryReference(aPrefix);
    return directory.listBlobs(
        null,
        useFlatBlobListing,
        listingDetails,
        options,
        opContext);
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
  private CloudBlobWrapper getBlobReference(String aKey)
      throws StorageException, URISyntaxException {

    CloudBlobWrapper blob = null;
    if (isPageBlobKey(aKey)) {
      blob = this.container.getPageBlobReference(aKey);
    } else {
      blob = this.container.getBlockBlobReference(aKey);
    blob.setStreamMinimumReadSizeInBytes(downloadBlockSizeBytes);
    blob.setWriteBlockSizeInBytes(uploadBlockSizeBytes);
    }

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
  private String normalizeKey(CloudBlobWrapper blob) {
    return normalizeKey(blob.getUri());
  }

  /**
   * This private method normalizes the key by stripping the container name from
   * the path and returns a path relative to the root directory of the
   * container.
   *
   * @param directory
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

    // Set User-Agent
    operationContext.getSendingRequestEventHandler().addListener(new StorageEvent<SendingRequestEvent>() {
      @Override
      public void eventOccurred(SendingRequestEvent eventArg) {
        HttpURLConnection connection = (HttpURLConnection) eventArg.getConnectionObject();
        String userAgentInfo = String.format(Utility.LOCALE_US, "WASB/%s (%s) %s",
                VersionInfo.getVersion(), userAgentId, BaseRequest.getUserAgent());
        connection.setRequestProperty(Constants.HeaderConstants.USER_AGENT, userAgentInfo);
      }
    });

    if (selfThrottlingEnabled) {
      SelfThrottlingIntercept.hook(operationContext, selfThrottlingReadFactor,
          selfThrottlingWriteFactor);
    } else if (autoThrottlingEnabled) {
      ClientThrottlingIntercept.hook(operationContext);
    }

    if (bandwidthGaugeUpdater != null) {
      //bandwidthGaugeUpdater is null when we config to skip azure metrics
      ResponseReceivedMetricUpdater.hook(
         operationContext,
         instrumentation,
         bandwidthGaugeUpdater);
    }

    // Bind operation context to receive send request callbacks on this operation.
    // If reads concurrent to OOB writes are allowed, the interception will reset
    // the conditional header on all Azure blob storage read requests.
    if (bindConcurrentOOBIo) {
      SendRequestIntercept.bind(operationContext);
    }

    if (testHookOperationContext != null) {
      operationContext =
          testHookOperationContext.modifyOperationContext(operationContext);
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

    LOG.debug("Retrieving metadata for {}", key);

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

      CloudBlobWrapper blob = getBlobReference(key);

      // Download attributes and return file metadata only if the blob
      // exists.
      if (null != blob && blob.exists(getInstrumentedContext())) {

        LOG.debug("Found {} as an explicit blob. Checking if it's a file or folder.", key);

        try {
          // The blob exists, so capture the metadata from the blob
          // properties.
          blob.downloadAttributes(getInstrumentedContext());
          BlobProperties properties = blob.getProperties();

          if (retrieveFolderAttribute(blob)) {
            LOG.debug("{} is a folder blob.", key);
            return new FileMetadata(key, properties.getLastModified().getTime(),
                getPermissionStatus(blob), BlobMaterialization.Explicit);
          } else {

            LOG.debug("{} is a normal blob.", key);

            return new FileMetadata(
                key, // Always return denormalized key with metadata.
                getDataLength(blob, properties),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }
        } catch(StorageException e){
          if (!NativeAzureFileSystemHelper.isFileNotFoundException(e)) {
            throw e;
          }
        }
      }

      // There is no file with that key name, but maybe it is a folder.
      // Query the underlying folder/container to list the blobs stored
      // there under that key.
      //
      Iterable<ListBlobItem> objects =
          listRootBlobs(
              key,
              true,
              EnumSet.of(BlobListingDetails.METADATA),
              null,
          getInstrumentedContext());

      // Check if the directory/container has the blob items.
      for (ListBlobItem blobItem : objects) {
        if (blobItem instanceof CloudBlockBlobWrapper
            || blobItem instanceof CloudPageBlobWrapper) {
          LOG.debug("Found blob as a directory-using this file under it to infer its properties {}",
              blobItem.getUri());

          blob = (CloudBlobWrapper) blobItem;
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
  public InputStream retrieve(String key) throws AzureException, IOException {
    return retrieve(key, 0);
  }

  @Override
  public InputStream retrieve(String key, long startByteOffset)
      throws AzureException, IOException {
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

        InputStream inputStream = openInputStream(getBlobReference(key));
        if (startByteOffset > 0) {
          // Skip bytes and ignore return value. This is okay
          // because if you try to skip too far you will be positioned
          // at the end and reads will not return data.
          inputStream.skip(startByteOffset);
        }
        return inputStream;
    } catch (IOException e) {
        throw e;
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
  private static FileMetadata getFileMetadataInList(
      final Iterable<FileMetadata> list, String key) {
    for (FileMetadata current : list) {
      if (current.getKey().equals(key)) {
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

      // Enable flat listing option only if depth is unbounded and config
      // KEY_ENABLE_FLAT_LISTING is enabled.
      boolean enableFlatListing = false;
      if (maxListingDepth < 0 && sessionConfiguration.getBoolean(
        KEY_ENABLE_FLAT_LISTING, DEFAULT_ENABLE_FLAT_LISTING)) {
        enableFlatListing = true;
      }

      Iterable<ListBlobItem> objects;
      if (prefix.equals("/")) {
        objects = listRootBlobs(true, enableFlatListing);
      } else {
        objects = listRootBlobs(prefix, true, enableFlatListing);
      }

      ArrayList<FileMetadata> fileMetadata = new ArrayList<FileMetadata>();
      for (ListBlobItem blobItem : objects) {
        // Check that the maximum listing count is not exhausted.
        //
        if (0 < maxListingCount
            && fileMetadata.size() >= maxListingCount) {
          break;
        }

        if (blobItem instanceof CloudBlockBlobWrapper || blobItem instanceof CloudPageBlobWrapper) {
          String blobKey = null;
          CloudBlobWrapper blob = (CloudBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey,
                properties.getLastModified().getTime(),
                getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                getDataLength(blob, properties),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the metadata to the list, but remove any existing duplicate
          // entries first that we may have added by finding nested files.
          FileMetadata existing = getFileMetadataInList(fileMetadata, blobKey);
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
          if (getFileMetadataInList(fileMetadata, dirKey) == null) {
            fileMetadata.add(directoryMetadata);
          }

          if (!enableFlatListing) {
            // Currently at a depth of one, decrement the listing depth for
            // sub-directories.
            buildUpList(directory, fileMetadata, maxListingCount,
                maxListingDepth - 1);
          }
        }
      }
      // Note: Original code indicated that this may be a hack.
      priorLastKey = null;
      PartialListing listing = new PartialListing(priorLastKey,
          fileMetadata.toArray(new FileMetadata[] {}),
          0 == fileMetadata.size() ? new String[] {}
      : new String[] { prefix });
      return listing;
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
   * @param aCloudBlobDirectory Azure blob directory
   * @param aFileMetadataList a list of file metadata objects for each
   *                          non-directory blob.
   * @param maxListingCount maximum length of the built up list.
   */
  private void buildUpList(CloudBlobDirectoryWrapper aCloudBlobDirectory,
      ArrayList<FileMetadata> aFileMetadataList, final int maxListingCount,
      final int maxListingDepth) throws Exception {

    // Push the blob directory onto the stack.
    //
    AzureLinkedStack<Iterator<ListBlobItem>> dirIteratorStack =
        new AzureLinkedStack<Iterator<ListBlobItem>>();

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
        //
        if (blobItem instanceof CloudBlockBlobWrapper || blobItem instanceof CloudPageBlobWrapper) {
          String blobKey = null;
          CloudBlobWrapper blob = (CloudBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey,
                properties.getLastModified().getTime(),
                getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                getDataLength(blob, properties),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the directory metadata to the list only if it's not already
          // there.
          FileMetadata existing = getFileMetadataInList(aFileMetadataList, blobKey);
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

            if (getFileMetadataInList(aFileMetadataList, dirKey) == null) {
              // Reached the targeted listing depth. Return metadata for the
              // directory using default permissions.
              //
              // Note: Something smarter should be done about permissions. Maybe
              // inherit the permissions of the first non-directory blob.
              // Also, getting a proper value for last-modified is tricky.
              //
              FileMetadata directoryMetadata = new FileMetadata(dirKey,
                  0,
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
   * Return the actual data length of the blob with the specified properties.
   * If it is a page blob, you can't rely on the length from the properties
   * argument and you must get it from the file. Otherwise, you can.
   */
  private long getDataLength(CloudBlobWrapper blob, BlobProperties properties)
    throws AzureException {
    if (blob instanceof CloudPageBlobWrapper) {
      try {
        return PageBlobInputStream.getPageBlobDataSize((CloudPageBlobWrapper) blob,
            getInstrumentedContext(
                isConcurrentOOBAppendAllowed()));
      } catch (Exception e) {
        throw new AzureException(
            "Unexpected exception getting page blob actual data size.", e);
      }
    }
    return properties.getLength();
  }

  /**
   * Deletes the given blob, taking special care that if we get a
   * blob-not-found exception upon retrying the operation, we just
   * swallow the error since what most probably happened is that
   * the first operation succeeded on the server.
   * @param blob The blob to delete.
   * @param lease Azure blob lease, or null if no lease is to be used.
   * @throws StorageException
   */
  private void safeDelete(CloudBlobWrapper blob, SelfRenewingLease lease) throws StorageException {
    OperationContext operationContext = getInstrumentedContext();
    try {
      blob.delete(operationContext, lease);
    } catch (StorageException e) {
      if (!NativeAzureFileSystemHelper.isFileNotFoundException(e)) {
        LOG.error("Encountered Storage Exception for delete on Blob: {}"
            + ", Exception Details: {} Error Code: {}",
            blob.getUri(), e.getMessage(), e.getErrorCode());
      }
      // On exception, check that if:
      // 1. It's a BlobNotFound exception AND
      // 2. It got there after one-or-more retries THEN
      // we swallow the exception.
      if (e.getErrorCode() != null
          && "BlobNotFound".equals(e.getErrorCode())
          && operationContext.getRequestResults().size() > 1
          && operationContext.getRequestResults().get(0).getException() != null) {
        LOG.debug("Swallowing delete exception on retry: {}", e.getMessage());
        return;
      } else {
        throw e;
      }
    } finally {
      if (lease != null) {
        lease.free();
      }
    }
  }

  /**
   * API implementation to delete a blob in the back end azure storage.
   */
  @Override
  public boolean delete(String key, SelfRenewingLease lease) throws IOException {
    try {
      if (checkContainer(ContainerAccessType.ReadThenWrite) == ContainerState.DoesntExist) {
        // Container doesn't exist, no need to do anything
        return true;
      }
      // Get the blob reference and delete it.
      CloudBlobWrapper blob = getBlobReference(key);
      safeDelete(blob, lease);
      return true;
    } catch (Exception e) {
      if (e instanceof StorageException
          && NativeAzureFileSystemHelper.isFileNotFoundException(
              (StorageException) e)) {
        // the file or directory does not exist
        return false;
      }
      throw new AzureException(e);
    }
  }

  /**
   * API implementation to delete a blob in the back end azure storage.
   */
  @Override
  public boolean delete(String key) throws IOException {
    try {
      return delete(key, null);
    } catch (IOException e) {
      Throwable t = e.getCause();
      if (t != null && t instanceof StorageException) {
        StorageException se = (StorageException) t;
        if ("LeaseIdMissing".equals(se.getErrorCode())){
          SelfRenewingLease lease = null;
          try {
            lease = acquireLease(key);
            return delete(key, lease);
          } catch (AzureException e3) {
            LOG.warn("Got unexpected exception trying to acquire lease on "
                + key + "." + e3.getMessage());
            throw e3;
          } finally {
            try {
              if (lease != null){
                lease.free();
              }
            } catch (Exception e4){
              LOG.error("Unable to free lease on " + key, e4);
            }
          }
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @Override
  public void rename(String srcKey, String dstKey) throws IOException {
    rename(srcKey, dstKey, false, null);
  }

  @Override
  public void rename(String srcKey, String dstKey, boolean acquireLease,
      SelfRenewingLease existingLease) throws IOException {

    LOG.debug("Moving {} to {}", srcKey, dstKey);

    if (acquireLease && existingLease != null) {
      throw new IOException("Cannot acquire new lease if one already exists.");
    }

    CloudBlobWrapper srcBlob = null;
    CloudBlobWrapper dstBlob = null;
    SelfRenewingLease lease = null;
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
      //

      srcBlob = getBlobReference(srcKey);
      if (!srcBlob.exists(getInstrumentedContext())) {
        throw new AzureException("Source blob " + srcKey + " does not exist.");
      }

      /**
       * Conditionally get a lease on the source blob to prevent other writers
       * from changing it. This is used for correctness in HBase when log files
       * are renamed. It generally should do no harm other than take a little
       * more time for other rename scenarios. When the HBase master renames a
       * log file folder, the lease locks out other writers.  This
       * prevents a region server that the master thinks is dead, but is still
       * alive, from committing additional updates.  This is different than
       * when HBase runs on HDFS, where the region server recovers the lease
       * on a log file, to gain exclusive access to it, before it splits it.
       */
      if (acquireLease) {
        lease = srcBlob.acquireLease();
      } else if (existingLease != null) {
        lease = existingLease;
      }

      // Get the destination blob. The destination key always needs to be
      // normalized.
      //
      dstBlob = getBlobReference(dstKey);

      // Rename the source blob to the destination blob by copying it to
      // the destination blob then deleting it.
      //
      // Copy blob operation in Azure storage is very costly. It will be highly
      // likely throttled during Azure storage gc. Short term fix will be using
      // a more intensive exponential retry policy when the cluster is getting
      // throttled.
      try {
        dstBlob.startCopyFromBlob(srcBlob, null, getInstrumentedContext());
      } catch (StorageException se) {
        if (se.getHttpStatusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
          int copyBlobMinBackoff = sessionConfiguration.getInt(
            KEY_COPYBLOB_MIN_BACKOFF_INTERVAL,
            DEFAULT_COPYBLOB_MIN_BACKOFF_INTERVAL);

          int copyBlobMaxBackoff = sessionConfiguration.getInt(
            KEY_COPYBLOB_MAX_BACKOFF_INTERVAL,
            DEFAULT_COPYBLOB_MAX_BACKOFF_INTERVAL);

          int copyBlobDeltaBackoff = sessionConfiguration.getInt(
            KEY_COPYBLOB_BACKOFF_INTERVAL,
            DEFAULT_COPYBLOB_BACKOFF_INTERVAL);

          int copyBlobMaxRetries = sessionConfiguration.getInt(
            KEY_COPYBLOB_MAX_IO_RETRIES,
            DEFAULT_COPYBLOB_MAX_RETRY_ATTEMPTS);

          BlobRequestOptions options = new BlobRequestOptions();
          options.setRetryPolicyFactory(new RetryExponentialRetry(
            copyBlobMinBackoff, copyBlobDeltaBackoff, copyBlobMaxBackoff,
            copyBlobMaxRetries));
          dstBlob.startCopyFromBlob(srcBlob, options, getInstrumentedContext());
        } else {
          throw se;
        }
      }
      waitForCopyToComplete(dstBlob, getInstrumentedContext());
      safeDelete(srcBlob, lease);
    } catch (StorageException e) {
      if (e.getHttpStatusCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
        LOG.warn("Rename: CopyBlob: StorageException: ServerBusy: Retry complete, will attempt client side copy for page blob");
        InputStream ipStream = null;
        OutputStream opStream = null;
        try {
          if (srcBlob.getProperties().getBlobType() == BlobType.PAGE_BLOB){
            ipStream = openInputStream(srcBlob);
            opStream = openOutputStream(dstBlob);
            byte[] buffer = new byte[PageBlobFormatHelpers.PAGE_SIZE];
            int len;
            while ((len = ipStream.read(buffer)) != -1) {
              opStream.write(buffer, 0, len);
            }
            opStream.flush();
            opStream.close();
            ipStream.close();
          } else {
            throw new AzureException(e);
          }
          safeDelete(srcBlob, lease);
        } catch(StorageException se) {
          LOG.warn("Rename: CopyBlob: StorageException: Failed");
          throw new AzureException(se);
        } finally {
          IOUtils.closeStream(ipStream);
          IOUtils.closeStream(opStream);
        }
      } else {
        throw new AzureException(e);
      }
    } catch (URISyntaxException e) {
      // Re-throw exception as an Azure storage exception.
      throw new AzureException(e);
    }
  }

  private void waitForCopyToComplete(CloudBlobWrapper blob, OperationContext opContext){
    boolean copyInProgress = true;
    while (copyInProgress) {
      try {
        blob.downloadAttributes(opContext);
        }
      catch (StorageException se){
      }

      // test for null because mocked filesystem doesn't know about copystates yet.
      copyInProgress = (blob.getCopyState() != null && blob.getCopyState().getStatus() == CopyStatus.PENDING);
      if (copyInProgress) {
        try {
          Thread.sleep(1000);
          }
          catch (InterruptedException ie){
            //ignore
        }
      }
    }
  }

  /**
   * Checks whether an explicit file/folder exists.
   * This is used by redo of atomic rename.
   * There was a bug(apache jira HADOOP-12780) during atomic rename if
   * process crashes after an inner directory has been renamed but still
   * there are file under that directory to be renamed then after the
   * process comes again it tries to redo the renames. It checks whether
   * the directory exists or not by calling filesystem.exist.
   * But filesystem.Exists will treat that directory as implicit directory
   * and return true as file exists under that directory. So It will try
   * try to rename that directory and will fail as the corresponding blob
   * does not exist. So this method explicitly checks for the blob.
   */
  @Override
  public boolean explicitFileExists(String key) throws AzureException {
    CloudBlobWrapper blob;
    try {
      blob = getBlobReference(key);
      if (null != blob && blob.exists(getInstrumentedContext())) {
        return true;
      }

      return false;
    } catch (StorageException e) {
      throw new AzureException(e);
    } catch (URISyntaxException e) {
      throw new AzureException(e);
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
      CloudBlobWrapper blob = getBlobReference(key);
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
      Iterable<ListBlobItem> objects = listRootBlobs(prefix, false, false);
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

  /**
   * Get a lease on the blob identified by key. This lease will be renewed
   * indefinitely by a background thread.
   */
  @Override
  public SelfRenewingLease acquireLease(String key) throws AzureException {
    LOG.debug("acquiring lease on {}", key);
    try {
      checkContainer(ContainerAccessType.ReadThenWrite);
      CloudBlobWrapper blob = getBlobReference(key);
      return blob.acquireLease();
    }
    catch (Exception e) {

      // Caught exception while attempting to get lease. Re-throw as an
      // Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public void updateFolderLastModifiedTime(String key, Date lastModified,
      SelfRenewingLease folderLease)
      throws AzureException {
    try {
      checkContainer(ContainerAccessType.ReadThenWrite);
      CloudBlobWrapper blob = getBlobReference(key);
      //setLastModified function is not available in 2.0.0 version. blob.uploadProperties automatically updates last modified
      //timestamp to current time
      blob.uploadProperties(getInstrumentedContext(), folderLease);
    } catch (Exception e) {

      // Caught exception while attempting to update the properties. Re-throw as an
      // Azure storage exception.
      throw new AzureException(e);
    }
  }

  @Override
  public void updateFolderLastModifiedTime(String key,
      SelfRenewingLease folderLease) throws AzureException {
    final Calendar lastModifiedCalendar = Calendar
        .getInstance(Utility.LOCALE_US);
    lastModifiedCalendar.setTimeZone(Utility.UTC_ZONE);
    Date lastModified = lastModifiedCalendar.getTime();
    updateFolderLastModifiedTime(key, lastModified, folderLease);
  }

  @Override
  public void dump() throws IOException {
  }

  @Override
  public void close() {
    if (bandwidthGaugeUpdater != null) {
      bandwidthGaugeUpdater.close();
      bandwidthGaugeUpdater = null;
    }
  }

  // Finalizer to ensure complete shutdown
  @Override
  protected void finalize() throws Throwable {
    LOG.debug("finalize() called");
    close();
    super.finalize();
  }

  @Override
  public DataOutputStream retrieveAppendStream(String key, int bufferSize) throws IOException {

    try {

      if (isPageBlobKey(key)) {
        throw new UnsupportedOperationException("Append not supported for Page Blobs");
      }

      CloudBlobWrapper blob =  this.container.getBlockBlobReference(key);

      OutputStream outputStream;

      BlockBlobAppendStream blockBlobOutputStream = new BlockBlobAppendStream(
          (CloudBlockBlobWrapper) blob,
          key,
          bufferSize,
          isBlockBlobWithCompactionKey(key),
          getInstrumentedContext());

      outputStream = blockBlobOutputStream;

      DataOutputStream dataOutStream = new SyncableDataOutputStream(
          outputStream);

      return dataOutStream;
    } catch(Exception ex) {
      throw new AzureException(ex);
    }
  }
}
