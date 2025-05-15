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
import java.net.URI;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.AzcopyToolHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.WASB_ACCOUNT_NAME_DOMAIN_SUFFIX;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

/**
 * Base for AzureBlobFileSystem Integration tests.
 *
 * <I>Important: This is for integration tests only.</I>
 */
public abstract class AbstractAbfsIntegrationTest extends
        AbstractAbfsTestWithTimeout {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractAbfsIntegrationTest.class);

  private boolean isIPAddress;
  private NativeAzureFileSystem wasb;
  private AzureBlobFileSystem abfs;
  private String abfsScheme;

  private Configuration rawConfig;
  private AbfsConfiguration abfsConfig;
  private String fileSystemName;
  private String accountName;
  private String testUrl;
  private AuthType authType;
  private boolean useConfiguredFileSystem = false;
  private boolean usingFilesystemForSASTests = false;
  public static final int SHORTENED_GUID_LEN = 12;

  protected AbstractAbfsIntegrationTest() throws Exception {
    fileSystemName = TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
    rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);

    this.accountName = rawConfig.get(FS_AZURE_ACCOUNT_NAME);
    if (accountName == null) {
      // check if accountName is set using different config key
      accountName = rawConfig.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    }
    assumeTrue("Not set: " + FS_AZURE_ABFS_ACCOUNT_NAME,
            accountName != null && !accountName.isEmpty());

    final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    URI defaultUri = null;

    abfsConfig = new AbfsConfiguration(rawConfig, accountName, identifyAbfsServiceTypeFromUrl(abfsUrl));

    authType = abfsConfig.getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    assumeValidAuthConfigsPresent();

    abfsScheme = authType == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
            : FileSystemUriSchemes.ABFS_SECURE_SCHEME;

    try {
      defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }

    this.testUrl = defaultUri.toString();
    abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    if (isAppendBlobEnabled()) {
      String appendblobDirs = this.testUrl + "," + abfsConfig.get(FS_AZURE_CONTRACT_TEST_URI);
      rawConfig.set(FS_AZURE_APPEND_BLOB_KEY, appendblobDirs);
    }
    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfig.get(AZURE_ABFS_ENDPOINT);
    if (endPoint != null && endPoint.contains(":") && endPoint.split(":").length == 2) {
      this.isIPAddress = true;
    } else {
      this.isIPAddress = false;
    }

    // For tests, we want to enforce checksum validation so that any regressions can be caught.
    abfsConfig.setIsChecksumValidationEnabled(true);
  }

  protected boolean getIsNamespaceEnabled(AzureBlobFileSystem fs)
      throws IOException {
    return fs.getIsNamespaceEnabled(getTestTracingContext(fs, false));
  }

  public static TracingContext getSampleTracingContext(AzureBlobFileSystem fs,
      boolean needsPrimaryReqId) {
    String correlationId, fsId;
    TracingHeaderFormat format;
    correlationId = "test-corr-id";
    fsId = "test-filesystem-id";
    format = TracingHeaderFormat.ALL_ID_FORMAT;
    return new TracingContext(correlationId, fsId,
        FSOperationType.TEST_OP, needsPrimaryReqId, format, null);
  }

  public TracingContext getTestTracingContext(AzureBlobFileSystem fs,
      boolean needsPrimaryReqId) {
    String correlationId, fsId;
    TracingHeaderFormat format;
    if (fs == null) {
      correlationId = "test-corr-id";
      fsId = "test-filesystem-id";
      format = TracingHeaderFormat.ALL_ID_FORMAT;
    } else {
      AbfsConfiguration abfsConf = fs.getAbfsStore().getAbfsConfiguration();
      correlationId = abfsConf.getClientCorrelationId();
      fsId = fs.getFileSystemId();
      format = abfsConf.getTracingHeaderFormat();
    }
    return new TracingContext(correlationId, fsId,
        FSOperationType.TEST_OP, needsPrimaryReqId, format, null);
  }

  @Before
  public void setup() throws Exception {
    //Create filesystem first to make sure getWasbFileSystem() can return an existing filesystem.
    createFileSystem();

    // Only live account without namespace support can run ABFS&WASB
    // compatibility tests
    if (!isIPAddress && (abfsConfig.getAuthType(accountName) != AuthType.SAS)
        && !abfs.getIsNamespaceEnabled(getTestTracingContext(
            getFileSystem(), false))) {
      final URI wasbUri = new URI(
          abfsUrlToWasbUrl(getTestUrl(), abfsConfig.isHttpsAlwaysUsed()));
      final AzureNativeFileSystemStore azureNativeFileSystemStore =
          new AzureNativeFileSystemStore();

      // update configuration with wasb credentials
      String accountNameWithoutDomain = accountName.split("\\.")[0];
      String wasbAccountName = accountNameWithoutDomain + WASB_ACCOUNT_NAME_DOMAIN_SUFFIX;
      String keyProperty = FS_AZURE_ACCOUNT_KEY + "." + wasbAccountName;
      if (rawConfig.get(keyProperty) == null) {
        rawConfig.set(keyProperty, getAccountKey());
      }

      azureNativeFileSystemStore.initialize(
          wasbUri,
          rawConfig,
          new AzureFileSystemInstrumentation(rawConfig));

      wasb = new NativeAzureFileSystem(azureNativeFileSystemStore);
      wasb.initialize(wasbUri, rawConfig);
    }
  }

  @After
  public void teardown() throws Exception {
    try {
      IOUtils.closeStream(wasb);
      wasb = null;

      if (abfs == null) {
        return;
      }
      TracingContext tracingContext = getTestTracingContext(getFileSystem(), false);

      if (usingFilesystemForSASTests) {
        abfsConfig.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey.name());
        AzureBlobFileSystem tempFs = (AzureBlobFileSystem) FileSystem.newInstance(rawConfig);
        tempFs.getAbfsStore().deleteFilesystem(tracingContext);
      }
      else if (!useConfiguredFileSystem) {
        // Delete all uniquely created filesystem from the account
        final AzureBlobFileSystemStore abfsStore = abfs.getAbfsStore();
        abfsStore.deleteFilesystem(tracingContext);

        AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
            new Callable<Hashtable<String, String>>() {
              @Override
              public Hashtable<String, String> call() throws Exception {
                return abfsStore.getFilesystemProperties(tracingContext);
              }
            });
        if (FILE_SYSTEM_NOT_FOUND.getStatusCode() != ex.getStatusCode()) {
          LOG.warn("Deleted test filesystem may still exist: {}", abfs, ex);
        }
      }
    } catch (Exception e) {
      LOG.warn("During cleanup: {}", e, e);
    } finally {
      IOUtils.closeStream(abfs);
      abfs = null;
    }
  }

  public AccessTokenProvider getAccessTokenProvider(final AzureBlobFileSystem fs) {
    return ITestAbfsClient.getAccessTokenProvider(fs.getAbfsStore().getClient());
  }

  public void loadConfiguredFileSystem() throws Exception {
    // disable auto-creation of filesystem
    abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
          false);

    // AbstractAbfsIntegrationTest always uses a new instance of FileSystem,
    // need to disable that and force filesystem provided in test configs.
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_CONTRACT_TEST_URI);

    String[] authorityParts =
        (new URI(rawConfig.get(FS_AZURE_CONTRACT_TEST_URI))).getRawAuthority().split(
      AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);
    this.fileSystemName = authorityParts[0];

    // Reset URL with configured filesystem
    final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    URI defaultUri = null;

    defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);

    this.testUrl = defaultUri.toString();
    abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        defaultUri.toString());

    useConfiguredFileSystem = true;
  }

  /**
   * Create a filesystem for SAS tests using the SharedKey authentication.
   * We do not allow filesystem creation with SAS because certain type of SAS do not have
   * required permissions, and it is not known what type of SAS is configured by user.
   * @throws Exception
   */
  protected void createFilesystemForSASTests() throws Exception {
    createFilesystemWithTestFileForSASTests(null);
  }

  /**
   * Create a filesystem for SAS tests along with a test file using SharedKey authentication.
   * We do not allow filesystem creation with SAS because certain type of SAS do not have
   * required permissions, and it is not known what type of SAS is configured by user.
   * @param testPath path of the test file.
   * @throws Exception
   */
  protected void createFilesystemWithTestFileForSASTests(Path testPath) throws Exception {
    try (AzureBlobFileSystem tempFs = (AzureBlobFileSystem) FileSystem.newInstance(rawConfig)){
      ContractTestUtils.assertPathExists(tempFs, "This path should exist",
          new Path("/"));
      if (testPath != null) {
        tempFs.create(testPath).close();
      }
      abfsConfig.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SAS.name());
      usingFilesystemForSASTests = true;
    }
  }

  public AzureBlobFileSystem getFileSystem() throws IOException {
    return abfs;
  }

  public AzureBlobFileSystem getFileSystem(Configuration configuration) throws Exception{
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(configuration);
    return fs;
  }

  public AzureBlobFileSystem getFileSystem(String abfsUri) throws Exception {
    abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, abfsUri);
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(rawConfig);
    return fs;
  }

  /**
   * Creates the filesystem; updates the {@link #abfs} field.
   * @return the created filesystem.
   * @throws IOException failure during create/init.
   */
  public AzureBlobFileSystem createFileSystem() throws IOException {
    if (abfs == null) {
      abfs = (AzureBlobFileSystem) FileSystem.newInstance(rawConfig);
    }
    return abfs;
  }


  protected NativeAzureFileSystem getWasbFileSystem() {
    return wasb;
  }

  protected String getHostName() {
    // READ FROM ENDPOINT, THIS IS CALLED ONLY WHEN TESTING AGAINST DEV-FABRIC
    String endPoint = abfsConfig.get(AZURE_ABFS_ENDPOINT);
    return endPoint.split(":")[0];
  }

  protected void setTestUrl(String testUrl) {
    this.testUrl = testUrl;
  }

  protected String getTestUrl() {
    return testUrl;
  }

  protected void setFileSystemName(String fileSystemName) {
    this.fileSystemName = fileSystemName;
  }

  protected String getMethodName() {
    return methodName.getMethodName();
  }

  protected String getFileSystemName() {
    return fileSystemName;
  }

  protected String getAccountName() {
    return this.accountName;
  }

  protected String getAccountKey() {
    return abfsConfig.get(FS_AZURE_ACCOUNT_KEY);
  }

  public AbfsConfiguration getConfiguration() {
    return abfsConfig;
  }

  public AbfsConfiguration getConfiguration(AzureBlobFileSystem fs) {
    return fs.getAbfsStore().getAbfsConfiguration();
  }

  public Map<String, Long> getInstrumentationMap(AzureBlobFileSystem fs) {
    return fs.getInstrumentationMap();
  }

  public Configuration getRawConfiguration() {
    return abfsConfig.getRawConfiguration();
  }

  public AuthType getAuthType() {
    return this.authType;
  }

  public String getAbfsScheme() {
    return this.abfsScheme;
  }

  protected boolean isIPAddress() {
    return isIPAddress;
  }

  /**
   * Write a buffer to a file.
   * @param path path
   * @param buffer buffer
   * @throws IOException failure
   */
  protected void write(Path path, byte[] buffer) throws IOException {
    ContractTestUtils.writeDataset(getFileSystem(), path, buffer, buffer.length,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT, false);
  }

  /**
   * Touch a file in the test store. Will overwrite any existing file.
   * @param path path
   * @throws IOException failure.
   */
  protected void touch(Path path) throws IOException {
    ContractTestUtils.touch(getFileSystem(), path);
  }

  protected static String wasbUrlToAbfsUrl(final String wasbUrl) {
    return convertTestUrls(
        wasbUrl, FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX,
        FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX, false);
  }

  protected static String abfsUrlToWasbUrl(final String abfsUrl, final boolean isAlwaysHttpsUsed) {
    return convertTestUrls(
        abfsUrl, FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX,
        FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX, isAlwaysHttpsUsed);
  }

  private AbfsServiceType identifyAbfsServiceTypeFromUrl(String defaultUri) {
    if (defaultUri.contains(ABFS_BLOB_DOMAIN_NAME)) {
      return AbfsServiceType.BLOB;
    }
    return AbfsServiceType.DFS;
  }

  private static String convertTestUrls(
      final String url,
      final String fromNonSecureScheme,
      final String fromSecureScheme,
      final String fromDnsPrefix,
      final String toNonSecureScheme,
      final String toSecureScheme,
      final String toDnsPrefix,
      final boolean isAlwaysHttpsUsed) {
    String data = null;
    if (url.startsWith(fromNonSecureScheme + "://") && isAlwaysHttpsUsed) {
      data = url.replace(fromNonSecureScheme + "://", toSecureScheme + "://");
    } else if (url.startsWith(fromNonSecureScheme + "://")) {
      data = url.replace(fromNonSecureScheme + "://", toNonSecureScheme + "://");
    } else if (url.startsWith(fromSecureScheme + "://")) {
      data = url.replace(fromSecureScheme + "://", toSecureScheme + "://");
    }

    if (data != null) {
      data = data.replace("." + fromDnsPrefix + ".",
          "." + toDnsPrefix + ".");
    }
    return data;
  }

  public Path getTestPath() {
    Path path = new Path(UriUtils.generateUniqueTestPath());
    return path;
  }

  public AzureBlobFileSystemStore getAbfsStore(final AzureBlobFileSystem fs) {
    return fs.getAbfsStore();
  }

  public AbfsClient getAbfsClient(final AzureBlobFileSystemStore abfsStore) {
    return abfsStore.getClient();
  }

  public void setAbfsClient(AzureBlobFileSystemStore abfsStore,
      AbfsClient client) {
    abfsStore.setClient(client);
  }

  public Path makeQualified(Path path) throws java.io.IOException {
    return getFileSystem().makeQualified(path);
  }

  /**
   * Create a path under the test path provided by
   * {@link #getTestPath()}.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return getFileSystem().makeQualified(
        new Path(getTestPath(), getUniquePath(filepath)));
  }

  /**
   * Generate a unique path using the given filepath.
   * @param filepath path string
   * @return unique path created from filepath and a GUID
   */
  protected Path getUniquePath(String filepath) {
    if (filepath.equals("/")) {
      return new Path(filepath);
    }
    return new Path(filepath + StringUtils
        .right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN));
  }

  /**
   * Get any Delegation Token manager created by the filesystem.
   * @return the DT manager or null.
   * @throws IOException failure
   */
  protected AbfsDelegationTokenManager getDelegationTokenManager()
      throws IOException {
    return getFileSystem().getDelegationTokenManager();
  }

  /**
   * Generic create File and enabling AbfsOutputStream Flush.
   *
   * @param fs   AzureBlobFileSystem that is initialised in the test.
   * @param path Path of the file to be created.
   * @return AbfsOutputStream for writing.
   * @throws AzureBlobFileSystemException
   */
  protected AbfsOutputStream createAbfsOutputStreamWithFlushEnabled(
      AzureBlobFileSystem fs,
      Path path) throws IOException {
    AzureBlobFileSystemStore abfss = fs.getAbfsStore();
    abfss.getAbfsConfiguration().setDisableOutputStreamFlush(false);

    return (AbfsOutputStream) abfss.createFile(path, fs.getFsStatistics(),
        true, FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()),
        getTestTracingContext(fs, false));
  }

  /**
   * Custom assertion for AbfsStatistics which have statistics, expected
   * value and map of statistics and value as its parameters.
   * @param statistic the AbfsStatistics which needs to be asserted.
   * @param expectedValue the expected value of the statistics.
   * @param metricMap map of (String, Long) with statistics name as key and
   *                  statistics value as map value.
   */
  protected long assertAbfsStatistics(AbfsStatistic statistic,
      long expectedValue, Map<String, Long> metricMap) {
    assertEquals("Mismatch in " + statistic.getStatName(), expectedValue,
        (long) metricMap.get(statistic.getStatName()));
    return expectedValue;
  }

  protected void assumeValidTestConfigPresent(final Configuration conf, final String key) {
    String configuredValue = conf.get(accountProperty(key, accountName),
        conf.get(key, ""));
    Assume.assumeTrue(String.format("Missing Required Test Config: %s.", key),
        !configuredValue.isEmpty());
  }

  protected void assumeValidAuthConfigsPresent() {
    final AuthType currentAuthType = getAuthType();
    Assume.assumeFalse(
        "SAS Based Authentication Not Allowed For Integration Tests",
        currentAuthType == AuthType.SAS);
    if (currentAuthType == AuthType.SharedKey) {
      assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_ACCOUNT_KEY);
    } else {
      assumeValidTestConfigPresent(getRawConfiguration(),
          FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME);
    }
  }

  protected boolean isAppendBlobEnabled() {
    return getRawConfiguration().getBoolean(FS_AZURE_TEST_APPENDBLOB_ENABLED, false);
  }

  protected AbfsServiceType getAbfsServiceType() {
    return abfsConfig.getFsConfiguredServiceType();
  }

  /**
   * Returns the service type to be used for Ingress Operations irrespective of account type.
   * Default value is the same as the service type configured for the file system.
   * @return the service type.
   */
  public AbfsServiceType getIngressServiceType() {
    return abfsConfig.getIngressServiceType();
  }

  /**
   * Create directory with implicit parent directory.
   * @param path path to create. Can be relative or absolute.
   */
  protected void createAzCopyFolder(Path path) throws Exception {
    Assume.assumeTrue(getAbfsServiceType() == AbfsServiceType.BLOB);
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_FIXED_SAS_TOKEN);
    String sasToken = getRawConfiguration().get(FS_AZURE_TEST_FIXED_SAS_TOKEN);
    AzcopyToolHelper azcopyHelper = AzcopyToolHelper.getInstance(sasToken);
    azcopyHelper.createFolderUsingAzcopy(getAzcopyAbsolutePath(path));
  }

  /**
   * Create file with implicit parent directory.
   * @param path path to create. Can be relative or absolute.
   */
  protected void createAzCopyFile(Path path) throws Exception {
    Assume.assumeTrue(getAbfsServiceType() == AbfsServiceType.BLOB);
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_FIXED_SAS_TOKEN);
    String sasToken = getRawConfiguration().get(FS_AZURE_TEST_FIXED_SAS_TOKEN);
    AzcopyToolHelper azcopyHelper = AzcopyToolHelper.getInstance(sasToken);
    azcopyHelper.createFileUsingAzcopy(getAzcopyAbsolutePath(path));
  }

  private String getAzcopyAbsolutePath(Path path) throws IOException {
    String pathFromContainerRoot = getFileSystem().makeQualified(path).toUri().getPath();
    return HTTPS_SCHEME + COLON + FORWARD_SLASH + FORWARD_SLASH
        + accountName + FORWARD_SLASH + fileSystemName + pathFromContainerRoot;
  }

  /**
   * Utility method to assume that the test is running against a Blob service.
   * Otherwise, the test will be skipped.
   */
  protected void assumeBlobServiceType() {
    Assume.assumeTrue("Blob service type is required for this test",
        getAbfsServiceType() == AbfsServiceType.BLOB);
  }

  /**
   * Utility method to assume that the test is running against a DFS service.
   * Otherwise, the test will be skipped.
   */
  protected void assumeDfsServiceType() {
    Assume.assumeTrue("DFS service type is required for this test",
        getAbfsServiceType() == AbfsServiceType.DFS);
  }

  /**
   * Utility method to assume that the test is running against a HNS Enabled account.
   * Otherwise, the test will be skipped.
   * @throws IOException if an error occurs while checking the account type.
   */
  protected void assumeHnsEnabled() throws IOException {
    assumeHnsEnabled("HNS-Enabled account must be used for this test");
  }

  /**
   * Utility method to assume that the test is running against a HNS Enabled account.
   * @param errorMessage error message to be displayed if the test is skipped.
   * @throws IOException if an error occurs while checking the account type.
   */
  protected void assumeHnsEnabled(String errorMessage) throws IOException {
    Assume.assumeTrue(errorMessage, getIsNamespaceEnabled(getFileSystem()));
  }

  /**
   * Utility method to assume that the test is running against a HNS Disabled account.
   * Otherwise, the test will be skipped.
   * @throws IOException if an error occurs while checking the account type.
   */
  protected void assumeHnsDisabled() throws IOException {
    assumeHnsDisabled("HNS-Enabled account must not be used for this test");
  }

  /**
   * Utility method to assume that the test is running against a HNS Disabled account.
   * @param message error message to be displayed if the test is skipped.
   * @throws IOException if an error occurs while checking the account type.
   */
  protected void assumeHnsDisabled(String message) throws IOException {
    Assume.assumeFalse(message, getIsNamespaceEnabled(getFileSystem()));
  }

  /**
   * Assert that the path contains the expected DNS suffix.
   * If service type is blob, then path should have blob domain name.
   * @param path to be asserted.
   */
  protected void assertPathDns(Path path) {
    String expectedDns = getAbfsServiceType() == AbfsServiceType.BLOB
        ? ABFS_BLOB_DOMAIN_NAME : ABFS_DFS_DOMAIN_NAME;
    Assertions.assertThat(path.toString())
        .describedAs("Path does not contain expected DNS")
        .contains(expectedDns);
  }

  /**
   * Checks a list of futures for exceptions.
   *
   * This method iterates over a list of futures, waits for each task to complete,
   * and handles any exceptions thrown by the lambda expressions. If a
   * RuntimeException is caught, it increments the exceptionCaught counter.
   * If an unexpected exception is caught, it prints the exception to the standard error.
   * Finally, it asserts that no RuntimeExceptions were caught.
   *
   * @param futures The list of futures to check for exceptions.
   */
  protected void checkFuturesForExceptions(List<Future<?>> futures, int exceptionVal) {
    int exceptionCaught = 0;
    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }
    assertEquals(exceptionCaught, exceptionVal);
  }

  /**
   * Assumes that recovery through client transaction ID is enabled.
   * Namespace is enabled for the given AzureBlobFileSystem.
   * Service type is DFS.
   * Assumes that the client transaction ID is enabled in the configuration.
   *
   * @throws IOException in case of an error
   */
  protected void assumeRecoveryThroughClientTransactionID(boolean isCreate)
      throws IOException {
    // Assumes that recovery through client transaction ID is enabled.
    Assume.assumeTrue("Recovery through client transaction ID is not enabled",
        getConfiguration().getIsClientTransactionIdEnabled());
    // Assumes that service type is DFS.
    assumeDfsServiceType();
    // Assumes that namespace is enabled for the given AzureBlobFileSystem.
    assumeHnsEnabled();
    if (isCreate) {
      // Assume that create client is DFS client.
      Assume.assumeTrue("Ingress service type is not DFS",
          AbfsServiceType.DFS.equals(getIngressServiceType()));
      // Assume that append blob is not enabled in DFS client.
      Assume.assumeFalse("Append blob is enabled in DFS client",
          isAppendBlobEnabled());
    }
  }
}
