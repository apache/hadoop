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

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.AccountSASGenerator;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_FIXED_TOKEN;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests to validate the choice between using a SASTokenProvider to generate
 * a SAS or using a Fixed SAS Token configured by user.
 */
public class ITestAzureBlobFileSystemChooseSAS extends AbstractAbfsIntegrationTest{

  private String accountSAS = null;


  /**
   * To differentiate which config was used we will use different type of SAS Tokens.
   * For Fixed SAS Token we will use an Account SAS with permissions to do File system level operations.
   * For SASTokenProvider we will use a User Delegation SAS Token Provider
   * such that File System level operations are not permitted.
   */
  public ITestAzureBlobFileSystemChooseSAS() throws Exception {
    // SAS Token configured might not have permissions for creating file system.
    // Shared Key must be configured to create one. Once created, a new instance
    // of same file system will be used with SAS Authentication.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    createFilesystemForSASTests();
    super.setup();
    generateAccountSAS();  }

  /**
   * Generates a Account SAS Token using the Account Shared Key to be used as a fixed SAS Token.
   * This will be used by individual tests to set in the configurations.
   * @throws AzureBlobFileSystemException
   */
  private void generateAccountSAS() throws AzureBlobFileSystemException {
    final String accountKey = getConfiguration().getStorageAccountKey();
    AccountSASGenerator configAccountSASGenerator = new AccountSASGenerator(Base64.decode(accountKey));
    accountSAS = configAccountSASGenerator.getAccountSAS(getAccountName());
  }

  /**
   * Tests the scenario where both the SASTokenProvider and a fixed SAS token are configured.
   * SASTokenProvider class should be chosen and User Delegation SAS should be used.
   * @throws Exception
   */
  @Test
  public void testBothProviderFixedTokenConfigured() throws Exception {
    AbfsConfiguration testAbfsConfig = new AbfsConfiguration(
        getRawConfiguration(), this.getAccountName());
    removeAnyPresetConfiguration(testAbfsConfig);

    // Configuring a SASTokenProvider class which provides a user delegation SAS.
    testAbfsConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        MockDelegationSASTokenProvider.class.getName());

    // configuring the Fixed SAS token which is an Account SAS.
    testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);

    // Creating a new file system with updated configs.
    try (AzureBlobFileSystem newTestFs = (AzureBlobFileSystem)
        FileSystem.newInstance(testAbfsConfig.getRawConfiguration())) {
      TracingContext tracingContext = getTestTracingContext(newTestFs, true);

      // Asserting that filesystem level operations fails with User Delegation SAS.
      intercept(SASTokenProviderException.class, () -> {
            newTestFs.getAbfsStore().getFilesystemProperties(tracingContext);
          });

      // Asserting that User delegation SAS token is otherwise valid and blob level operations succeed.
      Path testPath = new Path("/testCorrectSASToken");
      newTestFs.create(testPath).close();
    }
  }

  /**
   * Tests the scenario where only the fixed token is configured, and no token provider class is set.
   * Account SAS Token configured as fixed SAS should be used.
   * @throws IOException
   */
  @Test
  public void testOnlyFixedTokenConfigured() throws Exception {
    AbfsConfiguration testAbfsConfig = new AbfsConfiguration(
        getRawConfiguration(), this.getAccountName());
    removeAnyPresetConfiguration(testAbfsConfig);

    // setting an account SAS token in the fixed token field.
    testAbfsConfig.set(FS_AZURE_SAS_FIXED_TOKEN, accountSAS);

    // Creating a new filesystem with updated configs.
    try (AzureBlobFileSystem newTestFs = (AzureBlobFileSystem)
        FileSystem.newInstance(testAbfsConfig.getRawConfiguration())) {

      // Asserting that account SAS is used as both filesystem and blob level operations succeed.
      newTestFs.getFileStatus(new Path("/"));
      Path testPath = new Path("/testCorrectSASToken");
      newTestFs.create(testPath).close();
      newTestFs.delete(new Path("/"), true);
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

    intercept(TokenAccessProviderException.class, () -> {
      AzureBlobFileSystem newTestFs = (AzureBlobFileSystem) FileSystem.newInstance(
          testAbfsConfig.getRawConfiguration());
    });
  }

  private void removeAnyPresetConfiguration(AbfsConfiguration testAbfsConfig) {
    testAbfsConfig.unset(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    testAbfsConfig.unset(FS_AZURE_SAS_FIXED_TOKEN);  }
}
