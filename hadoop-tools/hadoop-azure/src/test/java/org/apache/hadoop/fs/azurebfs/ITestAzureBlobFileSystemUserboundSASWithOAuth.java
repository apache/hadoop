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
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.extensions.MockUserBoundSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.AUTHORIZATION_PERMISSION_MISS_MATCH;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_FILE_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;

/**
 * Test Perform Authorization Check operation for UserboundSASWithOAuth auth type
 */
public class ITestAzureBlobFileSystemUserboundSASWithOAuth extends AbstractAbfsIntegrationTest {
  private static final String TEST_GROUP = UUID.randomUUID().toString();

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAzureBlobFileSystemUserboundSASWithOAuth.class);

  private boolean isHNSEnabled;

  public ITestAzureBlobFileSystemUserboundSASWithOAuth() throws Exception {
    // These tests rely on specific settings in azure-auth-keys.xml:
    String sasProvider = getRawConfiguration().get(
        FS_AZURE_SAS_TOKEN_PROVIDER_TYPE);
    assumeThat(
        MockUserBoundSASTokenProvider.class.getCanonicalName()).isEqualTo(
        sasProvider);
    assumeThat(getRawConfiguration().get(
        TestConfigurationKeys.FS_AZURE_TEST_APP_ID)).isNotNull();
    assumeThat(getRawConfiguration().get(
        TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET)).isNotNull();
    assumeThat(getRawConfiguration().get(
        TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID)).isNotNull();
    assumeThat(getRawConfiguration().get(
        TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID)).isNotNull();
    //todo: check if this would still be relevant for user bound SAS testing
//     The test uses shared key to create a random filesystem and then creates another
//     instance of this filesystem using SAS+OAuth authorization.
    //assumeThat(this.getAuthType()).isEqualTo(AuthType.SharedKey);
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    if (!isHNSEnabled) {
      assumeBlobServiceType();
    }
    createFilesystemForSASTests();
    super.setup();
  }

  @Test
  // FileSystemProperties are not supported by delegation SAS (hence user-bound SAS too) and should throw exception
  public void testSetFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String>
        properties = new Hashtable<>();
    properties.put("FileSystemProperties", "true");
    TracingContext tracingContext = getTestTracingContext(fs, true);
    assertThrows(IOException.class, () -> fs.getAbfsStore()
        .setFilesystemProperties(properties, tracingContext));
    assertThrows(IOException.class,
        () -> fs.getAbfsStore().getFilesystemProperties(tracingContext));
  }


  @Test
  public void testSignatureMaskOnExceptionMessage() throws Exception {
    intercept(IOException.class, "sig=XXXX",
        () -> getFileSystem().getAbfsClient()
            .renamePath("testABC/test.xt", "testABC/abc.txt",
                null, getTestTracingContext(getFileSystem(), false),
                null, false));
  }


  @Test
  public void testSASQuesMarkPrefix() throws Exception {
    AbfsConfiguration testConfig = this.getConfiguration();
    // the SAS Token Provider is changed
    testConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, "org.apache.hadoop.fs.azurebfs.extensions.MockWithPrefixSASTokenProvider");

    AzureBlobFileSystem testFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    Path testFile = new Path("/testSASPrefixQuesMark");

    // the creation of this filesystem should work correctly even when a SAS Token is generated with a ? prefix
    testFs.create(testFile).close();
  }

  @Test
  // Verify OAuth token provider and user-bound SAS provider are both configured and usable
  //CURRENTLY ONLY WORKING WITH THE REMOVED (BUT OPTIONAL) UDK PARAM
  public void testOAuthTokenProviderAndSASTokenFlow() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    // Verify AbfsConfiguration has an OAuth token provider configured
    AbfsConfiguration config = fs.getAbfsStore().getAbfsConfiguration();
    config.set("fs.azure.account.auth.type", "UserboundSASWithOAuth");

    AccessTokenProvider tokenProvider = config.getTokenProvider();
    assertNotNull(tokenProvider, "AccessTokenProvider must be configured for UserboundSASWithOAuth");

    // Acquire an OAuth token and assert it is non-empty
    AzureADToken token = tokenProvider.getToken();
    assertNotNull(token, "OAuth token must not be null");
    assertNotNull(token.getAccessToken(), "OAuth access token must not be null");
    assertFalse(token.getAccessToken().isEmpty(), "OAuth access token must not be empty");

    // Verify SASTokenProvider for user-bound SAS is present and usable
    SASTokenProvider sasProvider = config.getSASTokenProviderForUserBoundSAS();
    assertNotNull(sasProvider, "SASTokenProvider for user-bound SAS must be configured");
    assertTrue(sasProvider instanceof MockUserBoundSASTokenProvider,
        "Expected MockUserBoundSASTokenProvider to be used for tests");

    // Request a SAS token and assert we get a non-empty result
    String sasToken = sasProvider.getSASToken(getAccountName(), getFileSystemName(), "/", SASTokenProvider.GET_PROPERTIES_OPERATION);
    assertNotNull(sasToken, "SAS token must not be null");
    assertFalse(sasToken.isEmpty(), "SAS token must not be empty");
  }
}
