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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.WASB_ACCOUNT_NAME_DOMAIN_SUFFIX;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
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
  private static final int SHORTENED_GUID_LEN = 12;

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

    abfsConfig = new AbfsConfiguration(rawConfig, accountName);

    authType = abfsConfig.getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    abfsScheme = authType == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
            : FileSystemUriSchemes.ABFS_SECURE_SCHEME;

    if (authType == AuthType.SharedKey) {
      assumeTrue("Not set: " + FS_AZURE_ACCOUNT_KEY,
          abfsConfig.get(FS_AZURE_ACCOUNT_KEY) != null);
      // Update credentials
    } else {
      assumeTrue("Not set: " + FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME,
          abfsConfig.get(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME) != null);
    }

    final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    URI defaultUri = null;

    try {
      defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }

    this.testUrl = defaultUri.toString();
    abfsConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    if (abfsConfig.get(FS_AZURE_TEST_APPENDBLOB_ENABLED) == "true") {
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

  protected void createFilesystemForSASTests() throws Exception {
    // The SAS tests do not have permission to create a filesystem
    // so first create temporary instance of the filesystem using SharedKey
    // then re-use the filesystem it creates with SAS auth instead of SharedKey.
    try (AzureBlobFileSystem tempFs = (AzureBlobFileSystem) FileSystem.newInstance(rawConfig)){
      ContractTestUtils.assertPathExists(tempFs, "This path should exist",
          new Path("/"));
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
}
