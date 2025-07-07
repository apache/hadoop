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

import org.junit.Assume;
import org.junit.Test;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.accountProperty;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
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

/**
 * Test getIsNamespaceEnabled call.
 */
public class ITestGetNameSpaceEnabled extends AbstractAbfsIntegrationTest {

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  private boolean isUsingXNSAccount;
  public ITestGetNameSpaceEnabled() throws Exception {
    isUsingXNSAccount = getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
  }

  @Test
  public void testXNSAccount() throws IOException {
    Assume.assumeTrue("Skip this test because the account being used for test is a non XNS account",
            isUsingXNSAccount);
    assertTrue("Expecting getIsNamespaceEnabled() return true",
        getIsNamespaceEnabled(getFileSystem()));
  }

  @Test
  public void testNonXNSAccount() throws IOException {
    Assume.assumeFalse("Skip this test because the account being used for test is a XNS account",
            isUsingXNSAccount);
    assertFalse("Expecting getIsNamespaceEnabled() return false",
        getIsNamespaceEnabled(getFileSystem()));
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsTrue() throws Exception {
    AzureBlobFileSystem fs = getNewFSWithHnsConf(TRUE_STR);
    Assertions.assertThat(getIsNamespaceEnabled(fs)).describedAs(
        "getIsNamespaceEnabled should return true when the "
            + "config is set as true").isTrue();
    fs.getAbfsStore().deleteFilesystem(getTestTracingContext(fs, false));
    unsetAndAssert();
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsFalse() throws Exception {
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
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
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
    AbfsConfiguration config = this.getConfiguration();
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    String testUri = this.getTestUrl();
    String nonExistingFsUrl = getAbfsScheme() + "://" + UUID.randomUUID()
            + testUri.substring(testUri.indexOf("@"));
    AzureBlobFileSystem fs = this.getFileSystem(nonExistingFsUrl);

    intercept(FileNotFoundException.class,
            "\"The specified filesystem does not exist.\", 404",
            ()-> {
              fs.getFileStatus(new Path("/")); // Run a dummy FS call
            });
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


  /**
   * Tests the behavior of AbfsConfiguration when the namespace-enabled
   * configuration is set based on account specific config.
   *
   * Expects the namespace value based on account specific config provided.
   *
   * @throws Exception if any error occurs during configuration setup or evaluation
   */
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
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);
    // Assert that other account still uses account agnostic config
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, otherUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, true);

    // Set only the account specific config for test account
    rawConfig.set(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName), FALSE_STR);
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    // Assert that only account specific config is enough for test account
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);

    // Set only account agnostic config
    rawConfig.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, FALSE_STR);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName));
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri);
    assertFileSystemInitWithExpectedHNSSettings(rawConfig, false);

    // Unset both account specific and account agnostic config
    rawConfig.unset(FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    rawConfig.unset(accountProperty(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, testAccountName));
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri);
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
    String testAccountName = "testAccount.dfs.core.windows.net";
    String defaultUri = this.getTestUrl().replace(this.getAccountName(), testAccountName);
    // Assert that account specific config takes precedence
    rawConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri);
    return rawConfig;
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
        .getAbfsConfiguration()
        .setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, times(1))
        .getAclStatus(anyString(), any(TracingContext.class));
  }

  private AbfsClient callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient()
      throws IOException {
    final AzureBlobFileSystem abfs = this.getFileSystem();
    final AzureBlobFileSystemStore abfsStore = abfs.getAbfsStore();
    final AbfsClient mockClient = mock(AbfsClient.class);
    doReturn(mock(AbfsRestOperation.class)).when(mockClient)
        .getAclStatus(anyString(), any(TracingContext.class));
    abfsStore.setClient(mockClient);
    getIsNamespaceEnabled(abfs);
    return mockClient;
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
