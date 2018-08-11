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
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AbstractWasbTestWithTimeout;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azure.metrics.AzureFileSystemInstrumentation;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.FILE_SYSTEM_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeTrue;
import static org.hamcrest.CoreMatchers.*;

/**
 * Base for AzureBlobFileSystem Integration tests.
 *
 * <I>Important: This is for integration tests only.</I>
 */
public abstract class AbstractAbfsIntegrationTest extends
    AbstractWasbTestWithTimeout {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractAbfsIntegrationTest.class);

  private final boolean isEmulator;
  private NativeAzureFileSystem wasb;
  private AzureBlobFileSystem abfs;
  private String abfsScheme;

  private Configuration configuration;
  private String fileSystemName;
  private String accountName;
  private String testUrl;

  protected AbstractAbfsIntegrationTest(final boolean secure) {
    this(secure ? FileSystemUriSchemes.ABFS_SECURE_SCHEME : FileSystemUriSchemes.ABFS_SCHEME);
  }

  protected AbstractAbfsIntegrationTest() {
    this(FileSystemUriSchemes.ABFS_SCHEME);
  }

  private AbstractAbfsIntegrationTest(final String scheme) {
    abfsScheme = scheme;
    fileSystemName = ABFS_TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
    configuration = new Configuration();
    configuration.addResource(ABFS_TEST_RESOURCE_XML);

    String accountName = configuration.get(FS_AZURE_TEST_ACCOUNT_NAME, "");
    assumeTrue("Not set: " + FS_AZURE_TEST_ACCOUNT_NAME,
        !accountName.isEmpty());
    assertThat("The key in " + FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
            + " is not bound to an ABFS account",
        accountName, containsString("dfs.core.windows.net"));
    String fullKey = FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
        + accountName;
    assumeTrue("Not set: " + fullKey,
        configuration.get(fullKey) != null);

    final String abfsUrl = this.getFileSystemName() + "@" + this.getAccountName();
    URI defaultUri = null;

    try {
      defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }

    this.testUrl = defaultUri.toString();
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    configuration.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    this.isEmulator = this.configuration.getBoolean(FS_AZURE_EMULATOR_ENABLED, false);
    this.accountName = this.configuration.get(FS_AZURE_TEST_ACCOUNT_NAME);
  }


  @Before
  public void setup() throws Exception {
    //Create filesystem first to make sure getWasbFileSystem() can return an existing filesystem.
    createFileSystem();

    if (!isEmulator) {
      final URI wasbUri = new URI(abfsUrlToWasbUrl(getTestUrl()));
      final AzureNativeFileSystemStore azureNativeFileSystemStore =
          new AzureNativeFileSystemStore();
      azureNativeFileSystemStore.initialize(
          wasbUri,
          getConfiguration(),
          new AzureFileSystemInstrumentation(getConfiguration()));

      wasb = new NativeAzureFileSystem(azureNativeFileSystemStore);
      wasb.initialize(wasbUri, configuration);
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

      final AzureBlobFileSystemStore abfsStore = abfs.getAbfsStore();
      abfsStore.deleteFilesystem();

      AbfsRestOperationException ex = intercept(
              AbfsRestOperationException.class,
              new Callable<Hashtable<String, String>>() {
                @Override
                public Hashtable<String, String> call() throws Exception {
                  return abfsStore.getFilesystemProperties();
                }
              });
      if (FILE_SYSTEM_NOT_FOUND.getStatusCode() != ex.getStatusCode()) {
        LOG.warn("Deleted test filesystem may still exist: {}", abfs, ex);
      }
    } catch (Exception e) {
      LOG.warn("During cleanup: {}", e, e);
    } finally {
      IOUtils.closeStream(abfs);
      abfs = null;
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
    configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, abfsUri);
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(configuration);
    return fs;
  }

  /**
   * Creates the filesystem; updates the {@link #abfs} field.
   * @return the created filesystem.
   * @throws IOException failure during create/init.
   */
  public AzureBlobFileSystem createFileSystem() throws IOException {
    Preconditions.checkState(abfs == null,
        "existing ABFS instance exists: %s", abfs);
    abfs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    return abfs;
  }


  protected NativeAzureFileSystem getWasbFileSystem() {
    return wasb;
  }

  protected String getHostName() {
    return configuration.get(FS_AZURE_TEST_HOST_NAME);
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
  protected String getFileSystemName() {
    return fileSystemName;
  }

  protected String getAccountName() {
    return configuration.get(FS_AZURE_TEST_ACCOUNT_NAME);
  }

  protected String getAccountKey() {
    return configuration.get(
        FS_AZURE_TEST_ACCOUNT_KEY_PREFIX
            + getAccountName());
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  protected boolean isEmulator() {
    return isEmulator;
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
        FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX);
  }

  protected static String abfsUrlToWasbUrl(final String abfsUrl) {
    return convertTestUrls(
        abfsUrl, FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME, FileSystemUriSchemes.ABFS_DNS_PREFIX,
        FileSystemUriSchemes.WASB_SCHEME, FileSystemUriSchemes.WASB_SECURE_SCHEME, FileSystemUriSchemes.WASB_DNS_PREFIX);
  }

  private static String convertTestUrls(
      final String url,
      final String fromNonSecureScheme,
      final String fromSecureScheme,
      final String fromDnsPrefix,
      final String toNonSecureScheme,
      final String toSecureScheme,
      final String toDnsPrefix) {
    String data = null;
    if (url.startsWith(fromNonSecureScheme + "://")) {
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

  /**
   * Create a path under the test path provided by
   * {@link #getTestPath()}.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return getFileSystem().makeQualified(
        new Path(getTestPath(), filepath));
  }

}
