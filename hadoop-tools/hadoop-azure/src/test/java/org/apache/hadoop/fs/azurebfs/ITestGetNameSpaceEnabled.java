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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.accountProperty;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test getIsNamespaceEnabled call.
 */
public class ITestGetNameSpaceEnabled extends AbstractAbfsIntegrationTest {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";
  private static final String FILESYSTEM_NOT_FOUND_ERROR = "The specified filesystem does not exist.";
  private static final String CONTAINER_NOT_FOUND_ERROR = "The specified container does not exist.";

  private boolean isUsingXNSAccount;
  public ITestGetNameSpaceEnabled() throws Exception {
    isUsingXNSAccount = getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
  }

  @Test
  public void testXNSAccount() throws IOException {
    assumeThat(isUsingXNSAccount)
        .as("Skip this test because the account being used for test is a non XNS account")
        .isTrue();
    assertTrue(
       getIsNamespaceEnabled(getFileSystem()), "Expecting getIsNamespaceEnabled() return true");
  }

  @Test
  public void testNonXNSAccount() throws IOException {
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    assumeThat(isUsingXNSAccount)
        .as("Skip this test because the account being used for test is a XNS account")
        .isFalse();
    assertFalse(
       getIsNamespaceEnabled(getFileSystem()), "Expecting getIsNamespaceEnabled() return false");
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsTrue() throws Exception {
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    assumeThat(getAbfsServiceType())
        .as("Blob Endpoint Does not Allow FS init on HNS Account")
        .isEqualTo(AbfsServiceType.DFS);
    AzureBlobFileSystem fs = getNewFSWithHnsConf(TRUE_STR);
    Assertions.assertThat(getIsNamespaceEnabled(fs)).describedAs(
        "getIsNamespaceEnabled should return true when the "
            + "config is set as true").isTrue();
    fs.getAbfsStore().deleteFilesystem(getTestTracingContext(fs, false));
    unsetAndAssert();
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsFalse() throws Exception {
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    AzureBlobFileSystem fs = getNewFSWithHnsConf(FALSE_STR);
    Assertions.assertThat(getIsNamespaceEnabled(fs)).describedAs(
        "getIsNamespaceEnabled should return false when the "
            + "config is set as false").isFalse();
    fs.getAbfsStore().deleteFilesystem(getTestTracingContext(fs, false));
    unsetAndAssert();
  }

  private void unsetAndAssert() throws Exception {
    AzureBlobFileSystem fs = getNewFSWithHnsConf(
        DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    boolean expectedValue = this.getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assertions.assertThat(getIsNamespaceEnabled(fs)).describedAs(
        "getIsNamespaceEnabled should return the value "
            + "configured for fs.azure.test.namespace.enabled")
        .isEqualTo(expectedValue);
    fs.getAbfsStore().deleteFilesystem(getTestTracingContext(fs, false));
  }

  private AzureBlobFileSystem getNewFSWithHnsConf(
      String isNamespaceEnabledAccount) throws Exception {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);
    rawConfig.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, isNamespaceEnabledAccount);
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
        this.getAccountName()), isNamespaceEnabledAccount);
    rawConfig
        .setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    rawConfig.set(FS_DEFAULT_NAME_KEY,
        getNonExistingUrl());
    return (AzureBlobFileSystem) FileSystem.get(rawConfig);
  }

  private String getNonExistingUrl() {
    String testUri = this.getTestUrl();
    return getAbfsScheme() + "://" + UUID.randomUUID() + testUri
        .substring(testUri.indexOf("@"));
  }

  @Test
  public void testFailedRequestWhenFSNotExist() throws Exception {
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    AbfsConfiguration config = this.getConfiguration();
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    String testUri = this.getTestUrl();
    String nonExistingFsUrl = getAbfsScheme() + "://" + UUID.randomUUID()
            + testUri.substring(testUri.indexOf("@"));
    config.setBoolean(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, isUsingXNSAccount);
    AzureBlobFileSystem fs = this.getFileSystem(nonExistingFsUrl);
    fs.getAbfsStore().getAbfsConfiguration()
        .setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);

    FileNotFoundException ex = intercept(FileNotFoundException.class, ()-> {
      fs.getFileStatus(new Path("/")); // Run a dummy FS call
    });

    String expectedExceptionMessage = getAbfsServiceType() == AbfsServiceType.DFS
        ? FILESYSTEM_NOT_FOUND_ERROR
        : CONTAINER_NOT_FOUND_ERROR;

    Assertions.assertThat(ex.getMessage()).describedAs(
            "Expecting FileNotFoundException with message: " + expectedExceptionMessage)
        .contains(expectedExceptionMessage);
  }

  @Test
  public void testEnsureGetAclCallIsMadeOnceWhenConfigIsInvalid()
      throws Exception {
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsMadeOnceForInvalidConf(" ");
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsMadeOnceForInvalidConf("Invalid conf");
    unsetConfAndEnsureGetAclCallIsMadeOnce();
  }

  @Test
  public void testEnsureGetAclCallIsNeverMadeWhenConfigIsValid()
      throws Exception {
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsNeverMadeForValidConf(FALSE_STR.toLowerCase());
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsNeverMadeForValidConf(FALSE_STR.toUpperCase());
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsNeverMadeForValidConf(TRUE_STR.toLowerCase());
    unsetConfAndEnsureGetAclCallIsMadeOnce();
    ensureGetAclCallIsNeverMadeForValidConf(TRUE_STR.toUpperCase());
    unsetConfAndEnsureGetAclCallIsMadeOnce();
  }

  @Test
  public void testEnsureGetAclCallIsMadeOnceWhenConfigIsNotPresent()
      throws IOException {
    unsetConfAndEnsureGetAclCallIsMadeOnce();
  }

  private void ensureGetAclCallIsMadeOnceForInvalidConf(String invalidConf)
      throws Exception {
    this.getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .setIsNamespaceEnabledAccountForTesting(Trilean.getTrilean(invalidConf));
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  private void ensureGetAclCallIsNeverMadeForValidConf(String validConf)
      throws Exception {
    this.getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .setIsNamespaceEnabledAccountForTesting(Trilean.getTrilean(validConf));
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, never())
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  private void unsetConfAndEnsureGetAclCallIsMadeOnce() throws IOException {
    this.getFileSystem().getAbfsStore()
        .getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  private AbfsClient callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient()
      throws IOException {
    final AzureBlobFileSystem abfs = Mockito.spy(this.getFileSystem());
    final AzureBlobFileSystemStore abfsStore = Mockito.spy(abfs.getAbfsStore());
    final AbfsDfsClient mockClient = mock(AbfsDfsClient.class);
    doReturn(abfsStore).when(abfs).getAbfsStore();
    doReturn(mockClient).when(abfsStore).getClient();
    doReturn(mockClient).when(abfsStore).getClient(any());
    doReturn(mock(AbfsRestOperation.class)).when(mockClient)
        .getAclStatus(anyString(), any(TracingContext.class));
    getIsNamespaceEnabled(abfs);
    return mockClient;
  }

  @Test
  public void ensureGetAclDetermineHnsStatusAccurately() throws Exception {
    ensureGetAclDetermineHnsStatusAccuratelyInternal(HTTP_BAD_REQUEST,
        false, false);
    ensureGetAclDetermineHnsStatusAccuratelyInternal(HTTP_NOT_FOUND,
        true, true);
    ensureGetAclDetermineHnsStatusAccuratelyInternal(HTTP_INTERNAL_ERROR,
        true, true);
    ensureGetAclDetermineHnsStatusAccuratelyInternal(HTTP_UNAVAILABLE,
        true, true);
  }

  private void ensureGetAclDetermineHnsStatusAccuratelyInternal(int statusCode,
      boolean expectedValue, boolean isExceptionExpected) throws Exception {
    AzureBlobFileSystemStore store = Mockito.spy(getFileSystem().getAbfsStore());
    AbfsClient mockClient = mock(AbfsClient.class);
    store.getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);
    doReturn(mockClient).when(store).getClient(AbfsServiceType.DFS);
    AbfsRestOperationException ex = new AbfsRestOperationException(
        statusCode, null, Integer.toString(statusCode), null);
    doThrow(ex).when(mockClient).getAclStatus(anyString(), any(TracingContext.class));

    if (isExceptionExpected) {
      try {
        store.getIsNamespaceEnabled(getTestTracingContext(getFileSystem(), false));
        Assertions.fail(
            "Exception Should have been thrown with status code: " + statusCode);
      } catch (AbfsRestOperationException caughtEx) {
        Assertions.assertThat(caughtEx.getStatusCode()).isEqualTo(statusCode);
        Assertions.assertThat(caughtEx.getErrorMessage()).isEqualTo(ex.getErrorMessage());
      }
    }
    // This should not trigger extra getAcl() call in case of exceptions.
    boolean isHnsEnabled = store.getIsNamespaceEnabled(
        getTestTracingContext(getFileSystem(), false));
    Assertions.assertThat(isHnsEnabled).isEqualTo(expectedValue);
    Assertions.assertThat(store.getClient().getIsNamespaceEnabled())
        .describedAs("ABFS Client should return same isNameSpace value as store")
        .isEqualTo(expectedValue);

    // GetAcl() should be called only once to determine the HNS status.
    Mockito.verify(mockClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  @Test
  public void testAccountSpecificConfig() throws Exception {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
        this.getAccountName()));
    String testAccountName = "testAccount.dfs.core.windows.net";
    String otherAccountName = "otherAccount.dfs.core.windows.net";
    String dummyAcountKey = "dummyKey";
    String defaultUri = this.getTestUrl().replace(this.getAccountName(), testAccountName);
    String otherUri = this.getTestUrl().replace(this.getAccountName(), otherAccountName);

    // Set both account specific and account agnostic config for test account
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName), FALSE_STR);
    rawConfig.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, TRUE_STR);
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_KEY, testAccountName), dummyAcountKey);
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_KEY, otherAccountName), dummyAcountKey);
    // Assert that account specific config takes precedence
    rawConfig.set(FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);
    // Assert that other account still uses account agnostic config
    rawConfig.set(FS_DEFAULT_NAME_KEY, otherUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, true);

    // Set only the account specific config for test account
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName), FALSE_STR);
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    // Assert that only account specific config is enough for test account
    rawConfig.set(FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);

    // Set only account agnostic config
    rawConfig.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, FALSE_STR);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName));
    rawConfig.set(FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);

    // Unset both account specific and account agnostic config
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName));
    rawConfig.set(FS_DEFAULT_NAME_KEY, defaultUri);
    rawConfig.set(AZURE_MAX_IO_RETRIES, "0");
    // Assert that file system init fails with UnknownHost exception as getAcl() is needed.
    try {
      assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);
    } catch (Exception e) {
      Assertions.assertThat(e.getCause().getMessage())
          .describedAs("getAcl() to determine HNS Nature of account should"
              + "fail with Unknown Host Exception").contains("UnknownHostException");
    }
  }

  /**
   * Tests the behavior of AbfsConfiguration when the namespace-enabled
   * configuration set based on config provided.
   *
   * Expects the namespace value based on config provided.
   *
   * @throws Exception if any error occurs during configuration setup or evaluation
   */
  @Test
  public void testNameSpaceConfig() throws Exception {
    Configuration configuration = getConfigurationWithoutHnsConfig();
    AzureBlobFileSystem abfs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration, "bogusAccountName");

    // Test that the namespace value when config is not set
    Assertions.assertThat(abfsConfig.getIsNamespaceEnabledAccount())
        .describedAs("Namespace enabled should be unknown in case config is not set")
        .isEqualTo(Trilean.UNKNOWN);

    // In case no namespace config is present, file system init calls getAcl() to determine account type.
    Assertions.assertThat(abfs.getIsNamespaceEnabled(getTestTracingContext(abfs, false)))
        .describedAs("getIsNamespaceEnabled should return account type based on getAcl() call")
        .isEqualTo(abfs.getAbfsClient().getIsNamespaceEnabled());

    // In case no namespace config is present, file system init calls getAcl() to determine account type.
    Assertions.assertThat(abfs.getAbfsStore().getAbfsConfiguration().getIsNamespaceEnabledAccount())
        .describedAs("getIsNamespaceEnabled() should return updated account type based on getAcl() call")
        .isNotEqualTo(Trilean.UNKNOWN);

    configuration.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, TRUE_STR);
    abfs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    abfsConfig = new AbfsConfiguration(configuration, "bogusAccountName");

    // Test that the namespace enabled config is set correctly
    Assertions.assertThat(abfsConfig.getIsNamespaceEnabledAccount())
        .describedAs("Namespace enabled should be true in case config is set to true")
        .isEqualTo(Trilean.TRUE);

    // In case namespace config is present, same value will be return.
    Assertions.assertThat(abfs.getIsNamespaceEnabled(getTestTracingContext(abfs, false)))
        .describedAs("getIsNamespaceEnabled() should return true when config is set to true")
        .isEqualTo(true);

    // In case namespace config is present, same value will be return.
    Assertions.assertThat(abfs.getAbfsClient().getIsNamespaceEnabled())
        .describedAs("Client's getIsNamespaceEnabled() should return true when config is set to true")
        .isEqualTo(true);

    configuration.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, FALSE_STR);
    abfs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    abfsConfig = new AbfsConfiguration(configuration, "bogusAccountName");

    // Test that the namespace enabled config is set correctly
    Assertions.assertThat(abfsConfig.getIsNamespaceEnabledAccount())
        .describedAs("Namespace enabled should be false in case config is set to false")
        .isEqualTo(Trilean.FALSE);

    // In case namespace config is present, same value will be return.
    Assertions.assertThat(abfs.getIsNamespaceEnabled(getTestTracingContext(abfs, false)))
        .describedAs("getIsNamespaceEnabled() should return false when config is set to false")
        .isEqualTo(false);

    // In case namespace config is present, same value will be return.
    Assertions.assertThat(abfs.getAbfsClient().getIsNamespaceEnabled())
        .describedAs("Client's getIsNamespaceEnabled() should return false when config is set to false")
        .isEqualTo(false);
  }

  /**
   * Tests to check that the namespace configuration is set correctly
   * during the initialization of the AzureBlobFileSystem.
   *
   *
   * @throws Exception if any error occurs during configuration setup or evaluation
   */
  @Test
  public void testFsInitShouldSetNamespaceConfig() throws Exception {
    // Mock the AzureBlobFileSystem and its dependencies
    AzureBlobFileSystem mockFileSystem = Mockito.spy((AzureBlobFileSystem)
        FileSystem.newInstance(getConfigurationWithoutHnsConfig()));
    AzureBlobFileSystemStore mockStore = Mockito.spy(mockFileSystem.getAbfsStore());
    AbfsClient abfsClient = Mockito.spy(mockStore.getClient());
    Mockito.doReturn(abfsClient).when(mockStore).getClient();
    Mockito.doReturn(abfsClient).when(mockStore).getClient(any());
    abfsClient.getIsNamespaceEnabled();
    // Verify that getAclStatus is called once during initialization
    Mockito.verify(abfsClient, times(0))
        .getAclStatus(anyString(), any(TracingContext.class));

    mockStore.getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);
    // In case someone wrongly configured the namespace in between the processing,
    // abfsclient.getIsNamespaceEnabled() should throw an exception.
    String errorMessage = intercept(InvalidConfigurationValueException.class, () -> {
      abfsClient.getIsNamespaceEnabled();
    }).getMessage();
    Assertions.assertThat(errorMessage)
        .describedAs("Client should throw exception when namespace is unknown")
        .contains("Failed to determine account type");

    // In case of unknown namespace, store's getIsNamespaceEnabled should call getAclStatus
    // to determine the namespace status.
    mockStore.getIsNamespaceEnabled(getTestTracingContext(mockFileSystem, false));
    Mockito.verify(abfsClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));

    abfsClient.getIsNamespaceEnabled();
    // Verify that client's getNamespaceEnabled will not call getAclStatus again
    Mockito.verify(abfsClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  /**
   * Returns the configuration without the HNS config set.
   *
   * @return Configuration without HNS config
   */
  private Configuration getConfigurationWithoutHnsConfig() {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
        this.getAccountName()));
    String defaultUri = getRawConfiguration().get(FS_DEFAULT_NAME_KEY).
        replace(ABFS_BLOB_DOMAIN_NAME, ABFS_DFS_DOMAIN_NAME);
    // Assert that account specific config takes precedence
    rawConfig.set(FS_DEFAULT_NAME_KEY, defaultUri);
    return rawConfig;
  }

  private void assertFileSystemInitWithExpectedHNSSettings(
      Configuration configuration, boolean expectedIsHnsEnabledValue) throws IOException {
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration)) {
      Assertions.assertThat(getIsNamespaceEnabled(fs)).describedAs(
          "getIsNamespaceEnabled should return true when the "
              + "account specific config is not set").isEqualTo(expectedIsHnsEnabledValue);
    } catch (Exception e) {
      throw e;
    }
  }
}
