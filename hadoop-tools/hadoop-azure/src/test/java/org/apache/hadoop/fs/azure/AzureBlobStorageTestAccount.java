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
 **/

package org.apache.hadoop.fs.azure;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemMetricsSystem;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.microsoft.windowsazure.storage.AccessCondition;
import com.microsoft.windowsazure.storage.CloudStorageAccount;
import com.microsoft.windowsazure.storage.StorageCredentials;
import com.microsoft.windowsazure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.windowsazure.storage.StorageCredentialsAnonymous;
import com.microsoft.windowsazure.storage.blob.BlobContainerPermissions;
import com.microsoft.windowsazure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.windowsazure.storage.blob.BlobOutputStream;
import com.microsoft.windowsazure.storage.blob.CloudBlobClient;
import com.microsoft.windowsazure.storage.blob.CloudBlobContainer;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;
import com.microsoft.windowsazure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.windowsazure.storage.blob.SharedAccessBlobPolicy;
import com.microsoft.windowsazure.storage.core.Base64;

/**
 * Helper class to create WASB file systems backed by either a mock in-memory
 * implementation or a real Azure Storage account. See RunningLiveWasbTests.txt
 * for instructions on how to connect to a real Azure Storage account.
 */
public final class AzureBlobStorageTestAccount {

  private static final String ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
  private static final String SAS_PROPERTY_NAME = "fs.azure.sas.";
  private static final String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
  private static final String TEST_ACCOUNT_NAME_PROPERTY_NAME = "fs.azure.test.account.name";
  public static final String MOCK_ACCOUNT_NAME = "mockAccount.blob.core.windows.net";
  public static final String MOCK_CONTAINER_NAME = "mockContainer";
  public static final String WASB_AUTHORITY_DELIMITER = "@";
  public static final String WASB_SCHEME = "wasb";
  public static final String PATH_DELIMITER = "/";
  public static final String AZURE_ROOT_CONTAINER = "$root";
  public static final String MOCK_WASB_URI = "wasb://" + MOCK_CONTAINER_NAME
      + WASB_AUTHORITY_DELIMITER + MOCK_ACCOUNT_NAME + "/";
  private static final String USE_EMULATOR_PROPERTY_NAME = "fs.azure.test.emulator";

  private static final String KEY_DISABLE_THROTTLING = "fs.azure.disable.bandwidth.throttling";
  private static final String KEY_READ_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
  public static final String DEFAULT_PAGE_BLOB_DIRECTORY = "pageBlobs";
  public static final String DEFAULT_ATOMIC_RENAME_DIRECTORIES = "/atomicRenameDir1,/atomicRenameDir2";

  private CloudStorageAccount account;
  private CloudBlobContainer container;
  private CloudBlockBlob blob;
  private NativeAzureFileSystem fs;
  private AzureNativeFileSystemStore storage;
  private MockStorageInterface mockStorage;
  private String pageBlobDirectory;
  private static final ConcurrentLinkedQueue<MetricsRecord> allMetrics =
      new ConcurrentLinkedQueue<MetricsRecord>();
  private static boolean metricsConfigSaved = false;
  
  private AzureBlobStorageTestAccount(NativeAzureFileSystem fs,
      CloudStorageAccount account,
      CloudBlobContainer container) {
    this.account = account;
    this.container = container;
    this.fs = fs;
  }

  /**
   * Create a test account with an initialized storage reference.
   * 
   * @param storage
   *          -- store to be accessed by the account
   * @param account
   *          -- Windows Azure account object
   * @param container
   *          -- Windows Azure container object
   */
  private AzureBlobStorageTestAccount(AzureNativeFileSystemStore storage,
      CloudStorageAccount account, CloudBlobContainer container) {
    this.account = account;
    this.container = container;
    this.storage = storage;
  }

  /**
   * Create a test account sessions with the default root container.
   * 
   * @param fs
   *          - file system, namely WASB file system
   * @param account
   *          - Windows Azure account object
   * @param blob
   *          - block blob reference
   */
  private AzureBlobStorageTestAccount(NativeAzureFileSystem fs,
      CloudStorageAccount account, CloudBlockBlob blob) {

    this.account = account;
    this.blob = blob;
    this.fs = fs;
  }

  private AzureBlobStorageTestAccount(NativeAzureFileSystem fs,
      MockStorageInterface mockStorage) {
    this.fs = fs;
    this.mockStorage = mockStorage;
  }
  
  private static void addRecord(MetricsRecord record) {
    allMetrics.add(record);
  }

  public static String getMockContainerUri() {
    return String.format("http://%s/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME);
  }

  public static String toMockUri(String path) {
    return String.format("http://%s/%s/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME, path);
  }

  public static String toMockUri(Path path) {
    // Remove the first SEPARATOR
    return toMockUri(path.toUri().getRawPath().substring(1)); 
  }
  
  public static Path pageBlobPath() {
    return new Path("/" + DEFAULT_PAGE_BLOB_DIRECTORY);
  }

  public static Path pageBlobPath(String fileName) {
    return new Path(pageBlobPath(), fileName);
  }

  public Number getLatestMetricValue(String metricName, Number defaultValue)
      throws IndexOutOfBoundsException{
    boolean found = false;
    Number ret = null;
    for (MetricsRecord currentRecord : allMetrics) {
      // First check if this record is coming for my file system.
      if (wasGeneratedByMe(currentRecord)) {
        for (AbstractMetric currentMetric : currentRecord.metrics()) {
          if (currentMetric.name().equalsIgnoreCase(metricName)) {
            found = true;
            ret = currentMetric.value();
            break;
          }
        }
      }
    }
    if (!found) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw new IndexOutOfBoundsException(metricName);
    }
    return ret;
  }

  /**
   * Checks if the given record was generated by my WASB file system instance.
   * @param currentRecord The metrics record to check.
   * @return
   */
  private boolean wasGeneratedByMe(MetricsRecord currentRecord) {
    String myFsId = fs.getInstrumentation().getFileSystemInstanceId().toString();
    for (MetricsTag currentTag : currentRecord.tags()) {
      if (currentTag.name().equalsIgnoreCase("wasbFileSystemId")) {
        return currentTag.value().equals(myFsId);
      }
    }
    return false;
  }


  /**
   * Gets the blob reference to the given blob key.
   * 
   * @param blobKey
   *          The blob key (no initial slash).
   * @return The blob reference.
   */
  public CloudBlockBlob getBlobReference(String blobKey)
      throws Exception {
    return container.getBlockBlobReference(
        String.format(blobKey));
  }

  /**
   * Acquires a short lease on the given blob in this test account.
   * 
   * @param blobKey
   *          The key to the blob (no initial slash).
   * @return The lease ID.
   */
  public String acquireShortLease(String blobKey) throws Exception {
    return getBlobReference(blobKey).acquireLease(60, null);
  }

  /**
   * Releases the lease on the container.
   * 
   * @param leaseID
   *          The lease ID.
   */
  public void releaseLease(String leaseID, String blobKey) throws Exception {
    AccessCondition accessCondition = new AccessCondition();
    accessCondition.setLeaseID(leaseID);
    getBlobReference(blobKey).releaseLease(accessCondition);
  }

  private static void saveMetricsConfigFile() {
    if (!metricsConfigSaved) {
      new org.apache.hadoop.metrics2.impl.ConfigBuilder()
      .add("azure-file-system.sink.azuretestcollector.class",
          StandardCollector.class.getName())
      .save("hadoop-metrics2-azure-file-system.properties");
      metricsConfigSaved = true;
    }
  }

  public static AzureBlobStorageTestAccount createMock() throws Exception {
    return createMock(new Configuration());
  }

  public static AzureBlobStorageTestAccount createMock(Configuration conf) throws Exception {
    saveMetricsConfigFile();
    configurePageBlobDir(conf);
    configureAtomicRenameDir(conf);
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    NativeAzureFileSystem fs = new NativeAzureFileSystem(store);
    setMockAccountKey(conf);
    // register the fs provider.

    fs.initialize(new URI(MOCK_WASB_URI), conf);
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, mockStorage);
    return testAcct;
  }

  /**
   * Set the page blob directories configuration to the default if it is not
   * already set. Some tests may set it differently (e.g. the page blob
   * tests in TestNativeAzureFSPageBlobLive).
   * @param conf The configuration to conditionally update.
   */
  private static void configurePageBlobDir(Configuration conf) {
    if (conf.get(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES) == null) {
      conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES,
          "/" + DEFAULT_PAGE_BLOB_DIRECTORY);
    }
  }

  /** Do the same for the atomic rename directories configuration */
  private static void configureAtomicRenameDir(Configuration conf) {
    if (conf.get(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES) == null) {
      conf.set(AzureNativeFileSystemStore.KEY_ATOMIC_RENAME_DIRECTORIES,
          DEFAULT_ATOMIC_RENAME_DIRECTORIES);
    }
  }

  /**
   * Creates a test account that goes against the storage emulator.
   * 
   * @return The test account, or null if the emulator isn't setup.
   */
  public static AzureBlobStorageTestAccount createForEmulator()
      throws Exception {
    saveMetricsConfigFile();
    NativeAzureFileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = createTestConfiguration();
    if (!conf.getBoolean(USE_EMULATOR_PROPERTY_NAME, false)) {
      // Not configured to test against the storage emulator.
      System.out
        .println("Skipping emulator Azure test because configuration " +
            "doesn't indicate that it's running." +
            " Please see RunningLiveWasbTests.txt for guidance.");
      return null;
    }
    CloudStorageAccount account =
        CloudStorageAccount.getDevelopmentStorageAccount();
    fs = new NativeAzureFileSystem();
    String containerName = String.format("wasbtests-%s-%tQ",
        System.getProperty("user.name"), new Date());
    container = account.createCloudBlobClient().getContainerReference(
        containerName);
    container.create();

    // Set account URI and initialize Azure file system.
    URI accountUri = createAccountUri(DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME,
        containerName);
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    //
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, account, container);

    return testAcct;
  }

  public static AzureBlobStorageTestAccount createOutOfBandStore(
      int uploadBlockSize, int downloadBlockSize) throws Exception {

    saveMetricsConfigFile();

    CloudBlobContainer container = null;
    Configuration conf = createTestConfiguration();
    CloudStorageAccount account = createTestAccount(conf);
    if (null == account) {
      return null;
    }

    String containerName = String.format("wasbtests-%s-%tQ",
        System.getProperty("user.name"), new Date());

    // Create the container.
    container = account.createCloudBlobClient().getContainerReference(
        containerName);
    container.create();

    String accountName = conf.get(TEST_ACCOUNT_NAME_PROPERTY_NAME);

    // Ensure that custom throttling is disabled and tolerate concurrent
    // out-of-band appends.
    conf.setBoolean(KEY_DISABLE_THROTTLING, true);
    conf.setBoolean(KEY_READ_TOLERATE_CONCURRENT_APPEND, true);

    // Set account URI and initialize Azure file system.
    URI accountUri = createAccountUri(accountName, containerName);

    // Set up instrumentation.
    //
    AzureFileSystemMetricsSystem.fileSystemStarted();
    String sourceName = NativeAzureFileSystem.newMetricsSourceName();
    String sourceDesc = "Azure Storage Volume File System metrics";

    AzureFileSystemInstrumentation instrumentation = new AzureFileSystemInstrumentation(conf);

    AzureFileSystemMetricsSystem.registerSource(
        sourceName, sourceDesc, instrumentation);
    
    
    // Create a new AzureNativeFileSystemStore object.
    AzureNativeFileSystemStore testStorage = new AzureNativeFileSystemStore();

    // Initialize the store with the throttling feedback interfaces.
    testStorage.initialize(accountUri, conf, instrumentation);

    // Create test account initializing the appropriate member variables.
    //
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(testStorage, account, container);

    return testAcct;
  }

  /**
   * Sets the mock account key in the given configuration.
   * 
   * @param conf
   *          The configuration.
   */
  public static void setMockAccountKey(Configuration conf) {
    setMockAccountKey(conf, MOCK_ACCOUNT_NAME);
  }

  /**
   * Sets the mock account key in the given configuration.
   * 
   * @param conf
   *          The configuration.
   */
  public static void setMockAccountKey(Configuration conf, String accountName) {
    conf.set(ACCOUNT_KEY_PROPERTY_NAME + accountName,
        Base64.encode(new byte[] { 1, 2, 3 }));  
  }

  private static URI createAccountUri(String accountName)
      throws URISyntaxException {
    return new URI(WASB_SCHEME + ":" + PATH_DELIMITER + PATH_DELIMITER
        + accountName);
  }

  private static URI createAccountUri(String accountName, String containerName)
      throws URISyntaxException {
    return new URI(WASB_SCHEME + ":" + PATH_DELIMITER + PATH_DELIMITER
        + containerName + WASB_AUTHORITY_DELIMITER + accountName);
  }

  public static AzureBlobStorageTestAccount create() throws Exception {
    return create("");
  }

  public static AzureBlobStorageTestAccount create(String containerNameSuffix)
      throws Exception {
    return create(containerNameSuffix,
        EnumSet.of(CreateOptions.CreateContainer));
  }

  // Create a test account which uses throttling.
  public static AzureBlobStorageTestAccount createThrottled() throws Exception {
    return create("",
        EnumSet.of(CreateOptions.useThrottling, CreateOptions.CreateContainer));
  }

  public static AzureBlobStorageTestAccount create(Configuration conf)
      throws Exception {
    return create("", EnumSet.of(CreateOptions.CreateContainer), conf);
  }

  static CloudStorageAccount createStorageAccount(String accountName,
      Configuration conf, boolean allowAnonymous) throws URISyntaxException,
      KeyProviderException {
    String accountKey = AzureNativeFileSystemStore
        .getAccountKeyFromConfiguration(accountName, conf);
    StorageCredentials credentials;
    if (accountKey == null && allowAnonymous) {
      credentials = StorageCredentialsAnonymous.ANONYMOUS;
    } else {
      credentials = new StorageCredentialsAccountAndKey(
          accountName.split("\\.")[0], accountKey);
    }
    if (credentials == null) {
      return null;
    } else {
      return new CloudStorageAccount(credentials);
    }
  }

  public static Configuration createTestConfiguration() {
    return createTestConfiguration(null);
  }

  private static Configuration createTestConfiguration(Configuration conf) {
    if (conf == null) {
      conf = new Configuration();
    }

    conf.addResource(TEST_CONFIGURATION_FILE_NAME);
    return conf;
  }

  static CloudStorageAccount createTestAccount()
      throws URISyntaxException, KeyProviderException
  {
    return createTestAccount(createTestConfiguration());
  }

  static CloudStorageAccount createTestAccount(Configuration conf)
      throws URISyntaxException, KeyProviderException {
    String testAccountName = conf.get(TEST_ACCOUNT_NAME_PROPERTY_NAME);
    if (testAccountName == null) {
      System.out
        .println("Skipping live Azure test because of missing test account." +
                 " Please see RunningLiveWasbTests.txt for guidance.");
      return null;
    }
    return createStorageAccount(testAccountName, conf, false);
  }

  public static enum CreateOptions {
    UseSas, Readonly, CreateContainer, useThrottling
  }

  public static AzureBlobStorageTestAccount create(String containerNameSuffix,
      EnumSet<CreateOptions> createOptions) throws Exception {
    return create(containerNameSuffix, createOptions, null);
  }

  public static AzureBlobStorageTestAccount create(String containerNameSuffix,
      EnumSet<CreateOptions> createOptions, Configuration initialConfiguration)
      throws Exception {
    saveMetricsConfigFile();
    NativeAzureFileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = createTestConfiguration(initialConfiguration);
    configurePageBlobDir(conf);
    configureAtomicRenameDir(conf);
    CloudStorageAccount account = createTestAccount(conf);
    if (account == null) {
      return null;
    }
    fs = new NativeAzureFileSystem();
    String containerName = String.format("wasbtests-%s-%tQ%s",
        System.getProperty("user.name"), new Date(), containerNameSuffix);
    container = account.createCloudBlobClient().getContainerReference(
        containerName);
    if (createOptions.contains(CreateOptions.CreateContainer)) {
      container.create();
    }
    String accountName = conf.get(TEST_ACCOUNT_NAME_PROPERTY_NAME);
    if (createOptions.contains(CreateOptions.UseSas)) {
      String sas = generateSAS(container,
          createOptions.contains(CreateOptions.Readonly));
      if (!createOptions.contains(CreateOptions.CreateContainer)) {
        // The caller doesn't want the container to be pre-created,
        // so delete it now that we have generated the SAS.
        container.delete();
      }
      // Remove the account key from the configuration to make sure we don't
      // cheat and use that.
      conf.set(ACCOUNT_KEY_PROPERTY_NAME + accountName, "");
      // Set the SAS key.
      conf.set(SAS_PROPERTY_NAME + containerName + "." + accountName, sas);
    }

    // Check if throttling is turned on and set throttling parameters
    // appropriately.
    if (createOptions.contains(CreateOptions.useThrottling)) {
      conf.setBoolean(KEY_DISABLE_THROTTLING, false);
    } else {
      conf.setBoolean(KEY_DISABLE_THROTTLING, true);
    }

    // Set account URI and initialize Azure file system.
    URI accountUri = createAccountUri(accountName, containerName);
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    //
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, account, container);

    return testAcct;
  }

  private static String generateContainerName() throws Exception {
    String containerName =
        String.format ("wasbtests-%s-%tQ",
            System.getProperty("user.name"),
            new Date());
    return containerName;
  }

  private static String generateSAS(CloudBlobContainer container,
      boolean readonly) throws Exception {

    // Create a container if it does not exist.
    container.createIfNotExists();

    // Create a new shared access policy.
    SharedAccessBlobPolicy sasPolicy = new SharedAccessBlobPolicy();

    // Create a UTC Gregorian calendar value.
    GregorianCalendar calendar = new GregorianCalendar(
        TimeZone.getTimeZone("UTC"));

    // Specify the current time as the start time for the shared access
    // signature.
    //
    calendar.setTime(new Date());
    sasPolicy.setSharedAccessStartTime(calendar.getTime());

    // Use the start time delta one hour as the end time for the shared
    // access signature.
    calendar.add(Calendar.HOUR, 10);
    sasPolicy.setSharedAccessExpiryTime(calendar.getTime());

    if (readonly) {
      // Set READ permissions
      sasPolicy.setPermissions(EnumSet.of(
          SharedAccessBlobPermissions.READ,
          SharedAccessBlobPermissions.LIST));
    } else {
      // Set READ and WRITE permissions.
      //
      sasPolicy.setPermissions(EnumSet.of(
          SharedAccessBlobPermissions.READ,
          SharedAccessBlobPermissions.WRITE,
          SharedAccessBlobPermissions.LIST));
    }

    // Create the container permissions.
    BlobContainerPermissions containerPermissions = new BlobContainerPermissions();

    // Turn public access to the container off.
    containerPermissions.setPublicAccess(BlobContainerPublicAccessType.OFF);

    container.uploadPermissions(containerPermissions);

    // Create a shared access signature for the container.
    String sas = container.generateSharedAccessSignature(sasPolicy, null);
    // HACK: when the just generated SAS is used straight away, we get an
    // authorization error intermittently. Sleeping for 1.5 seconds fixes that
    // on my box.
    Thread.sleep(1500);

    // Return to caller with the shared access signature.
    return sas;
  }

  public static void primePublicContainer(CloudBlobClient blobClient,
      String accountName, String containerName, String blobName, int fileSize)
      throws Exception {

    // Create a container if it does not exist. The container name
    // must be lower case.
    CloudBlobContainer container = blobClient
        .getContainerReference(containerName);

    container.createIfNotExists();

    // Create a new shared access policy.
    SharedAccessBlobPolicy sasPolicy = new SharedAccessBlobPolicy();

    // Set READ and WRITE permissions.
    //
    sasPolicy.setPermissions(EnumSet.of(
        SharedAccessBlobPermissions.READ,
        SharedAccessBlobPermissions.WRITE,
        SharedAccessBlobPermissions.LIST,
        SharedAccessBlobPermissions.DELETE));

    // Create the container permissions.
    BlobContainerPermissions containerPermissions = new BlobContainerPermissions();

    // Turn public access to the container off.
    containerPermissions
        .setPublicAccess(BlobContainerPublicAccessType.CONTAINER);

    // Set the policy using the values set above.
    containerPermissions.getSharedAccessPolicies().put("testwasbpolicy",
        sasPolicy);
    container.uploadPermissions(containerPermissions);

    // Create a blob output stream.
    CloudBlockBlob blob = container.getBlockBlobReference(blobName);
    BlobOutputStream outputStream = blob.openOutputStream();

    outputStream.write(new byte[fileSize]);
    outputStream.close();
  }

  public static AzureBlobStorageTestAccount createAnonymous(
      final String blobName, final int fileSize) throws Exception {

    NativeAzureFileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = createTestConfiguration(), noTestAccountConf = new Configuration();

    // Set up a session with the cloud blob client to generate SAS and check the
    // existence of a container and capture the container object.
    CloudStorageAccount account = createTestAccount(conf);
    if (account == null) {
      return null;
    }
    CloudBlobClient blobClient = account.createCloudBlobClient();

    // Capture the account URL and the account name.
    String accountName = conf.get(TEST_ACCOUNT_NAME_PROPERTY_NAME);

    // Generate a container name and create a shared access signature string for
    // it.
    //
    String containerName = generateContainerName();

    // Set up public container with the specified blob name.
    primePublicContainer(blobClient, accountName, containerName, blobName,
        fileSize);

    // Capture the blob container object. It should exist after generating the
    // shared access signature.
    container = blobClient.getContainerReference(containerName);
    if (null == container || !container.exists()) {
      final String errMsg = String
          .format("Container '%s' expected but not found while creating SAS account.");
      throw new Exception(errMsg);
    }

    // Set the account URI.
    URI accountUri = createAccountUri(accountName, containerName);

    // Initialize the Native Azure file system with anonymous credentials.
    fs = new NativeAzureFileSystem();
    fs.initialize(accountUri, noTestAccountConf);

    // Create test account initializing the appropriate member variables.
    AzureBlobStorageTestAccount testAcct = new AzureBlobStorageTestAccount(fs,
        account, container);

    // Return to caller with test account.
    return testAcct;
  }

  private static CloudBlockBlob primeRootContainer(CloudBlobClient blobClient,
      String accountName, String blobName, int fileSize) throws Exception {

    // Create a container if it does not exist. The container name
    // must be lower case.
    CloudBlobContainer container = blobClient.getContainerReference("https://"
        + accountName + "/" + "$root");
    container.createIfNotExists();

    // Create a blob output stream.
    CloudBlockBlob blob = container.getBlockBlobReference(blobName);
    BlobOutputStream outputStream = blob.openOutputStream();

    outputStream.write(new byte[fileSize]);
    outputStream.close();

    // Return a reference to the block blob object.
    return blob;
  }

  public static AzureBlobStorageTestAccount createRoot(final String blobName,
      final int fileSize) throws Exception {

    NativeAzureFileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = createTestConfiguration();

    // Set up a session with the cloud blob client to generate SAS and check the
    // existence of a container and capture the container object.
    CloudStorageAccount account = createTestAccount(conf);
    if (account == null) {
      return null;
    }
    CloudBlobClient blobClient = account.createCloudBlobClient();

    // Capture the account URL and the account name.
    String accountName = conf.get(TEST_ACCOUNT_NAME_PROPERTY_NAME);

    // Set up public container with the specified blob name.
    CloudBlockBlob blobRoot = primeRootContainer(blobClient, accountName,
        blobName, fileSize);

    // Capture the blob container object. It should exist after generating the
    // shared access signature.
    container = blobClient.getContainerReference(AZURE_ROOT_CONTAINER);
    if (null == container || !container.exists()) {
      final String errMsg = String
          .format("Container '%s' expected but not found while creating SAS account.");
      throw new Exception(errMsg);
    }

    // Set the account URI without a container name.
    URI accountUri = createAccountUri(accountName);

    // Initialize the Native Azure file system with anonymous credentials.
    fs = new NativeAzureFileSystem();
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    // Set the container value to null for the default root container.
    //
    AzureBlobStorageTestAccount testAcct = new AzureBlobStorageTestAccount(
        fs, account, blobRoot);

    // Return to caller with test account.
    return testAcct;
  }

  public void closeFileSystem() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

  public void cleanup() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (container != null) {
      container.deleteIfExists();
      container = null;
    }
    if (blob != null) {
      // The blob member variable is set for blobs under root containers.
      // Delete blob objects created for root container tests when cleaning
      // up the test account.
      blob.delete();
      blob = null;
    }
  }

  public NativeAzureFileSystem getFileSystem() {
    return fs;
  }

  public AzureNativeFileSystemStore getStore() {
    return this.storage;
  }

  /**
   * Gets the real blob container backing this account if it's not a mock.
   * 
   * @return A container, or null if it's a mock.
   */
  public CloudBlobContainer getRealContainer() {
    return container;
  }

  /**
   * Gets the real blob account backing this account if it's not a mock.
   * 
   * @return An account, or null if it's a mock.
   */
  public CloudStorageAccount getRealAccount() {
    return account;
  }

  /**
   * Gets the mock storage interface if this account is backed by a mock.
   * 
   * @return The mock storage, or null if it's backed by a real account.
   */
  public MockStorageInterface getMockStorage() {
    return mockStorage;
  }
  
  public static class StandardCollector implements MetricsSink {
    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      addRecord(record);
    }

    @Override
    public void flush() {
    }
  }

  public void setPageBlobDirectory(String directory) {
    this.pageBlobDirectory = directory;
  }

  public String getPageBlobDirectory() {
    return pageBlobDirectory;
  }
}
