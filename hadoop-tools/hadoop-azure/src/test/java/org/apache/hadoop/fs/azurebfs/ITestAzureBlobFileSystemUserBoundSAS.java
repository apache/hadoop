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
import java.lang.reflect.Field;
import java.nio.file.AccessDeniedException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.extensions.MockInvalidSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockUserBoundSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.permission.AclEntry;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_END_USER_OBJECT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_END_USER_TENANT_ID;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for AzureBlobFileSystem using User-Bound SAS and OAuth.
 * Covers scenarios for token provider configuration, SAS token validity, and basic file operations.
 */
public class ITestAzureBlobFileSystemUserBoundSAS
    extends AbstractAbfsIntegrationTest {

  private static Path testPath = new Path("/test.txt");

  private static final String TEST_OBJECT_ID = "123456789";

  private static final String INVALID_OAUTH_TOKEN_VALUE = "InvalidOAuthTokenValue";

  /**
   * Constructor. Ensures tests run with SharedKey authentication.
   * @throws Exception if auth type is not SharedKey
   */
  protected ITestAzureBlobFileSystemUserBoundSAS() throws Exception {
    assumeThat(this.getAuthType()).isEqualTo(AuthType.SharedKey);
    assumeThat(this.getConfiguration().getIsNamespaceEnabledAccount().toBoolean()).
        isEqualTo(true);
  }

  /**
   * Sets up the test environment and configures the AbfsConfiguration for user-bound SAS tests.
   * @throws Exception if setup fails
   */
  @BeforeEach
  @Override
  public void setup() throws Exception {
    AbfsConfiguration abfsConfig = this.getConfiguration();
    String accountName = getAccountName();

    createFilesystemForUserBoundSASTests();
    super.setup();

    // Set all required configs on the raw configuration
    abfsConfig.set(
        FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID + "." + accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));
    abfsConfig.set(FS_AZURE_BLOB_FS_CLIENT_SERVICE_PRINCIPAL_OBJECT_ID,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID + "." + accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET + "." + accountName,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));
    abfsConfig.set(FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));
    abfsConfig.set(FS_AZURE_TEST_END_USER_TENANT_ID,
        abfsConfig.get(FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID));
    abfsConfig.set(FS_AZURE_TEST_END_USER_OBJECT_ID,
        abfsConfig.get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID));
    abfsConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        MockUserBoundSASTokenProvider.class.getName());
  }


  /**
   * Injects a mock AccessTokenProvider into the AbfsClient of the given filesystem.
   * @param fs AzureBlobFileSystem instance
   * @param mockProvider AccessTokenProvider to inject
   * @throws Exception if reflection fails
   */
  private void injectMockTokenProvider(AzureBlobFileSystem fs,
      AccessTokenProvider mockProvider) throws Exception {
    Field abfsStoreField = AzureBlobFileSystem.class.getDeclaredField(
        "abfsStore");
    abfsStoreField.setAccessible(true);
    AzureBlobFileSystemStore store
        = (AzureBlobFileSystemStore) abfsStoreField.get(fs);

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField(
        "client");
    abfsClientField.setAccessible(true);
    AbfsClient client = (AbfsClient) abfsClientField.get(store);

    Field tokenProviderField = AbfsClient.class.getDeclaredField(
        "tokenProvider");
    tokenProviderField.setAccessible(true);
    tokenProviderField.set(client, mockProvider);
  }

  /**
   * Helper to create a new AzureBlobFileSystem instance for tests.
   * @return AzureBlobFileSystem instance
   * @throws RuntimeException if creation fails
   */
  private AzureBlobFileSystem createTestFileSystem() throws RuntimeException {
    try {
      return (AzureBlobFileSystem) FileSystem.newInstance(
          getRawConfiguration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test that file creation fails when the end user object ID does not match the service principal object ID.
   * @throws Exception if test fails
   */
  @Test
  public void testShouldFailWhenSduoidMismatchesServicePrincipalId()
      throws Exception {
    this.getConfiguration()
        .set(FS_AZURE_TEST_END_USER_OBJECT_ID, TEST_OBJECT_ID);
    AzureBlobFileSystem testFs = createTestFileSystem();
    intercept(AccessDeniedException.class,
        () -> {
          testFs.create(testPath);
        });
  }

  /**
   * Verifies that both OAuth token provider and user-bound SAS token provider are configured and usable.
   * @throws Exception if test fails
   */
  @Test
  public void testOAuthTokenProviderAndSASTokenFlow() throws Exception {
    AzureBlobFileSystem testFs = createTestFileSystem();

    AbfsConfiguration abfsConfiguration = testFs.getAbfsStore()
        .getAbfsConfiguration();

    // Verify AbfsConfiguration has an OAuth token provider configured
    AccessTokenProvider tokenProvider = abfsConfiguration.getTokenProvider();
    assertNotNull(tokenProvider,
        "AccessTokenProvider must be configured for UserboundSASWithOAuth");

    // Acquire an OAuth token and assert it is non-empty
    AzureADToken token = tokenProvider.getToken();
    assertNotNull(token, "OAuth token must not be null");
    assertNotNull(token.getAccessToken(),
        "OAuth access token must not be null");
    assertFalse(token.getAccessToken().isEmpty(),
        "OAuth access token must not be empty");

    // Verify AbfsConfiguration has an SASTokenProvider configured
    SASTokenProvider sasProvider
        = abfsConfiguration.getUserBoundSASTokenProvider(
        AuthType.UserboundSASWithOAuth);
    assertNotNull(sasProvider,
        "SASTokenProvider for user-bound SAS must be configured");
    assertInstanceOf(MockUserBoundSASTokenProvider.class, sasProvider,
        "Expected MockUserBoundSASTokenProvider to be used for tests");

    // Request a SAS token and assert we get a non-empty result
    String sasToken = sasProvider.getSASToken(
        "abfsdrivercanaryhns.dfs.core.windows.net", "userbound", "/",
        SASTokenProvider.GET_PROPERTIES_OPERATION);
    assertNotNull(sasToken, "SAS token must not be null");
    assertFalse(sasToken.isEmpty(), "SAS token must not be empty");
  }

/**
 * Performs and validates basic file and directory operations, including rename.
 * Operations tested: create, open, write, read, list, mkdir, existence check, ACL (if HNS), rename, and delete.
 * @throws Exception if any operation fails
 */
  @Test
  public void testBasicOperations() throws Exception {
    AzureBlobFileSystem testFs = createTestFileSystem();

    // 1. Create file
    testFs.create(testPath).close();

    // 2. Open file
    testFs.open(testPath).close();

    // 3. Get file status
    testFs.getFileStatus(testPath);

    // 4. Write to file (overwrite)
    try (FSDataOutputStream out = testFs.create(testPath, true)) {
      out.writeUTF("hello");
    }

    // 5. Read from file
    try (FSDataInputStream in = testFs.open(testPath)) {
      String content = in.readUTF();
      assertEquals("hello", content);
    }

    // 6. List parent directory
    FileStatus[] files = testFs.listStatus(testPath.getParent());
    assertTrue(files.length > 0);

    // 7. Check file existence
    assertTrue(testFs.exists(testPath));

    // 8. Create directory and a file under it
    Path dirPath = new Path("/testDirAcl");
    Path filePath = new Path(dirPath, "fileInDir.txt");

    assertTrue(testFs.mkdirs(dirPath));

    // 9. ACL operations (only for HNS accounts)
    if (getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false)) {
      // Set ACL
      List<AclEntry> aclSpec = Arrays.asList(
          AclEntry.parseAclEntry("user::rwx", true),
          AclEntry.parseAclEntry("group::r-x", true),
          AclEntry.parseAclEntry("other::---", true)
      );
      testFs.setAcl(dirPath, aclSpec);

      // Get ACL
      List<AclEntry> returnedAcl = testFs.getAclStatus(dirPath).getEntries();
      assertNotNull(returnedAcl);
    }

    // 10. Rename file
    Path renamedPath = new Path("/testRenamed.txt");
    assertTrue(testFs.rename(testPath, renamedPath));
    assertFalse(testFs.exists(testPath));
    assertTrue(testFs.exists(renamedPath));

    // 11. Delete file (non-recursive)
    assertTrue(testFs.delete(renamedPath, false));
    assertFalse(testFs.exists(renamedPath));

    // 12. Delete directory (recursive)
    assertTrue(testFs.delete(dirPath, true));
    assertFalse(testFs.exists(dirPath));
    assertFalse(testFs.exists(filePath));
  }

  /**
   * Test that file creation fails when an invalid OAuth token is used.
   * @throws Exception if test fails
   */
  @Test
  public void testCreateFailsWithInvalidOAuthToken() throws Exception {
    AzureBlobFileSystem testFs = createTestFileSystem();

    // Create mock token provider with invalid token
    AccessTokenProvider mockProvider = Mockito.mock(AccessTokenProvider.class);
    AzureADToken mockToken = Mockito.mock(AzureADToken.class);
    Mockito.when(mockToken.getAccessToken()).thenReturn(
        INVALID_OAUTH_TOKEN_VALUE);
    Mockito.when(mockProvider.getToken()).thenReturn(mockToken);

    // Inject mock provider into AbfsClient
    injectMockTokenProvider(testFs, mockProvider);

    intercept(AccessDeniedException.class, () -> {
      testFs.create(testPath);
    });
  }

  /**
   * Test that file creation fails when an invalid SAS token is used.
   * @throws Exception if test fails
   */
  @Test
  public void testGPSFailsWithInvalidSASToken() throws Exception {
    AbfsConfiguration abfsConfig = this.getConfiguration();
    abfsConfig.set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        MockInvalidSASTokenProvider.class.getName());
    AzureBlobFileSystem invalidSASTokenFs = createTestFileSystem();
    intercept(AccessDeniedException.class,
        () -> {
          invalidSASTokenFs.create(testPath);
        }
    );
  }


  /**
   * Test file operations with a valid and then expired SAS token.
   * Verifies that operations succeed with a valid token and fail with an expired token.
   * @throws Exception if test fails
   */
  @Test
  public void testOperationWithValidAndExpiredSASToken() throws Exception {
    AzureBlobFileSystem testFs = createTestFileSystem();

    // Get a real SAS token from the configured provider
    AbfsConfiguration abfsConfig = testFs.getAbfsStore().getAbfsConfiguration();
    SASTokenProvider realSasProvider
        = abfsConfig.getUserBoundSASTokenProvider(
        AuthType.UserboundSASWithOAuth);
    assertNotNull(realSasProvider,
        "SASTokenProvider for user-bound SAS must be configured");
    String validSasToken = realSasProvider.getSASToken(
        getAccountName(),
        testFs.toString(),
        String.valueOf(testPath),
        SASTokenProvider.CREATE_FILE_OPERATION);
    assertNotNull(validSasToken, "SAS token must not be null");
    assertFalse(validSasToken.isEmpty(), "SAS token must not be empty");

    // Operation should work with valid SAS token
    testFs.create(testPath); // Should succeed

    // Modify the ske/se fields to be expired and inject a mock provider
    String expiredDate = OffsetDateTime.now(ZoneOffset.UTC)
        .minusDays(1)
        .format(DateTimeFormatter.ISO_DATE_TIME);
    String expiredSasToken = Arrays.stream(validSasToken.split("&"))
        .map(kv -> {
          String[] pair = kv.split("=", 2);
          if (pair[0].equals("ske") || pair[0].equals("se")) {
            return pair[0] + "=" + expiredDate;
          } else {
            return kv;
          }
        })
        .collect(Collectors.joining("&"));

    // Create a mock SASTokenProvider that returns the expired SAS token
    SASTokenProvider mockSasProvider = Mockito.mock(
        SASTokenProvider.class);
    Mockito.when(
            mockSasProvider.getSASToken(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString()))
        .thenReturn(expiredSasToken);

    // Inject the mock provider into the AbfsClient
    injectMockSASTokenProvider(testFs, mockSasProvider);

    // Try a file operation and expect failure due to expired SAS token
    intercept(AccessDeniedException.class,
        () -> {
          testFs.getFileStatus(testPath);
        }
    );
  }

  // Helper method to inject a mock SASTokenProvider into the AbfsClient
  private void injectMockSASTokenProvider(AzureBlobFileSystem fs,
      SASTokenProvider provider) throws Exception {
    Field abfsStoreField = AzureBlobFileSystem.class.getDeclaredField(
        "abfsStore");
    abfsStoreField.setAccessible(true);
    AzureBlobFileSystemStore store
        = (AzureBlobFileSystemStore) abfsStoreField.get(fs);

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField(
        "client");
    abfsClientField.setAccessible(true);
    AbfsClient client = (AbfsClient) abfsClientField.get(store);

    Field sasProviderField = AbfsClient.class.getDeclaredField(
        "sasTokenProvider");
    sasProviderField.setAccessible(true);
    sasProviderField.set(client, provider);
  }
}
