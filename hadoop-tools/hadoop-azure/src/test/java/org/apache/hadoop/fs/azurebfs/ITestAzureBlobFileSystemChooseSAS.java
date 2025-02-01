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
import java.nio.file.AccessDeniedException;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.FixedSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.AccountSASGenerator;
import org.apache.hadoop.fs.azurebfs.utils.Base64;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.accountProperty;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APP_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests to validate the choice between using a custom SASTokenProvider
 * implementation and FixedSASTokenProvider.
 */
public class ITestAzureBlobFileSystemChooseSAS extends AbstractAbfsIntegrationTest{

  private String accountSAS = null;
  private static final String TEST_PATH = "testPath";

  /**
   * To differentiate which SASTokenProvider was used we will use different type of SAS Tokens.
   * FixedSASTokenProvider will return an Account SAS with only read permissions.
   * SASTokenProvider will return a User Delegation SAS Token with both read and write permissions.
=   */
  public ITestAzureBlobFileSystemChooseSAS() throws Exception {
    // SAS Token configured might not have permissions for creating file system.
    // Shared Key must be configured to create one. Once created, a new instance
    // of same file system will be used with SAS Authentication.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    createFilesystemWithTestFileForSASTests(new Path(TEST_PATH));
    generateAccountSAS();
  }

  /**
   * Generates an Account SAS Token using the Account Shared Key to be used as a fixed SAS Token.
   * Account SAS used here will have only read permissions to resources.
   * This will be used by individual tests to set in the configurations.
   * @throws AzureBlobFileSystemException
   */
  private void generateAccountSAS() throws AzureBlobFileSystemException {
    final String accountKey = getConfiguration().getStorageAccountKey();
    AccountSASGenerator configAccountSASGenerator = new AccountSASGenerator(Base64.decode(accountKey));
    // Setting only read permissions.
    configAccountSASGenerator.setPermissions("r");
    accountSAS = configAccountSASGenerator.getAccountSAS(getAccountName());
  }

  /**
   * Tests the scenario where both the custom SASTokenProvider and a fixed SAS token are configured.
   * Custom implementation of SASTokenProvider class should be chosen and User Delegation SAS should be used.
   * @throws Exception
   */
  @Test
  public void testBothProviderFixedTokenConfigured() throws Exception {
    assumeDfsServiceType();
    assumeHnsEnabled();
    AbfsConfiguration testAbfsConfig = new AbfsConfiguration(
        getRawConfiguration(), this.getAccountName());
    removeAnyPresetConfiguration(testAbfsConfig);

    // Configuring a SASTokenProvider class which provides a user delegation SAS.
    testAbfsConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        MockDelegationSASTokenProvider.class.getName());
    // Make sure test configs required by MockDelegationSASTokenProvider are set.
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_TEST_APP_ID);
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_TEST_APP_SECRET);
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID);
    assumeValidTestConfigPresent(this.getRawConfiguration(), FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID);

    // configuring the Fixed SAS token which is an Account SAS.
    testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);

    // Creating a new file system with updated configs.
    try (AzureBlobFileSystem newTestFs = (AzureBlobFileSystem)
        FileSystem.newInstance(testAbfsConfig.getRawConfiguration())) {

      // Asserting that MockDelegationSASTokenProvider is used.
      Assertions.assertThat(testAbfsConfig.getSASTokenProvider())
          .describedAs("Custom SASTokenProvider Class must be used")
          .isInstanceOf(MockDelegationSASTokenProvider.class);

      // Assert that User Delegation SAS is used and both read and write operations are permitted.
      Path testPath = path(getMethodName());
      newTestFs.create(testPath).close();
      newTestFs.open(testPath).close();
    }
  }

  /**
   * Tests the scenario where only the fixed token is configured, and no token provider class is set.
   * Account SAS Token configured as fixed SAS should be used.
   * Also verifies that Account Specific as well as Account Agnostic Fixed SAS Token Works.
   * @throws IOException
   */
  @Test
  public void testOnlyFixedTokenConfigured() throws Exception {
    AbfsConfiguration testAbfsConfig = new AbfsConfiguration(
        getRawConfiguration(), this.getAccountName());

    // setting an Account Specific Fixed SAS token.
    removeAnyPresetConfiguration(testAbfsConfig);
    testAbfsConfig.set(accountProperty(FS_AZURE_SAS_FIXED_TOKEN, this.getAccountName()), accountSAS);
    testOnlyFixedTokenConfiguredInternal(testAbfsConfig);

    // setting an Account Agnostic Fixed SAS token.
    removeAnyPresetConfiguration(testAbfsConfig);
    testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);
    testOnlyFixedTokenConfiguredInternal(testAbfsConfig);
  }

  private void testOnlyFixedTokenConfiguredInternal(AbfsConfiguration testAbfsConfig) throws Exception {
    // Creating a new filesystem with updated configs.
    try (AzureBlobFileSystem newTestFs = (AzureBlobFileSystem)
        FileSystem.newInstance(testAbfsConfig.getRawConfiguration())) {

      // Asserting that FixedSASTokenProvider is used.
      Assertions.assertThat(testAbfsConfig.getSASTokenProvider())
          .describedAs("FixedSASTokenProvider Class must be used")
          .isInstanceOf(FixedSASTokenProvider.class);

      // Assert that Account SAS is used and only read operations are permitted.
      Path testPath = path(getMethodName());
      intercept(AccessDeniedException.class, () -> {
        newTestFs.create(testPath);
      });
      // Read Operation is permitted
      newTestFs.getFileStatus(new Path(TEST_PATH));
    }
  }

  /**
   * Tests the scenario where both the token provider class and the fixed token are not configured.
   * The code errors out at the initialization stage itself.
   * @throws IOException
   */
  @Test
  public void testBothProviderFixedTokenUnset() throws Exception {
    AbfsConfiguration testAbfsConfig = new AbfsConfiguration(
        getRawConfiguration(), this.getAccountName());
    removeAnyPresetConfiguration(testAbfsConfig);

    intercept(SASTokenProviderException.class, () -> {
      FileSystem.newInstance(testAbfsConfig.getRawConfiguration());
    });
  }

  private void removeAnyPresetConfiguration(AbfsConfiguration testAbfsConfig) {
    testAbfsConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    testAbfsConfig.unset(FS_AZURE_SAS_FIXED_TOKEN);
    testAbfsConfig.unset(accountProperty(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, this.getAccountName()));
    testAbfsConfig.unset(accountProperty(FS_AZURE_SAS_FIXED_TOKEN, this.getAccountName()));
  }
}
