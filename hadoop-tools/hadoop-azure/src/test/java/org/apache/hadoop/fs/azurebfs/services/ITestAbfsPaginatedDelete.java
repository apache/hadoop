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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.test.ReflectionUtils;
import org.apache.hadoop.util.Lists;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SECURE_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_PAGINATED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClientUtils.getHeaderValue;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests to verify server side pagination feature is supported from driver.
 */
public class ITestAbfsPaginatedDelete extends AbstractAbfsIntegrationTest {

  /**
   * File system using super-user OAuth, used to create the directory.
   */
  private AzureBlobFileSystem superUserFs;

  /**
   * File system using NoRBAC user OAuth, used to delete the directory.
   * This user will have default ACL permissions set on  root path including delete.
   * Since this is not a super-user, azure servers will trigger recursive ACL
   * checks on root path when delete is called using this user OAuth token.
   */
  private AzureBlobFileSystem testUserFs;

  /**
   * Service supports Pagination only for HNS Accounts.
   */
  private boolean isHnsEnabled;

  public ITestAbfsPaginatedDelete() throws Exception {
  }

  /**
   * Create file system instances for both super-user and test user.
   * @throws Exception
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    this.superUserFs = getFileSystem();

    assumeValidTestConfigPresent(this.getRawConfiguration(),
        FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    isHnsEnabled = this.getConfiguration().getBoolean(
        FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);

    assumeTestUserCredentialsConfigured();
    this.testUserFs = isHnsEnabled ? createTestUserFs() : null;
  }

  private AzureBlobFileSystem createTestUserFs() throws IOException {
    // Test User Credentials.
    String firstTestUserGuid = getConfiguration().get(
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);
    String clientId = getConfiguration().getString(
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID, "");
    String clientSecret = getConfiguration().getString(
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET, "");

    Configuration testUserConf = new Configuration(getRawConfiguration());
    setTestUserConf(testUserConf, FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth.name());
    setTestUserConf(testUserConf, FS_AZURE_BLOB_FS_CLIENT_ID, clientId);
    setTestUserConf(testUserConf, FS_AZURE_BLOB_FS_CLIENT_SECRET, clientSecret);
    setTestUserConf(testUserConf, FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME,
        ClientCredsTokenProvider.class.getName());

    testUserConf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    testUserConf.setBoolean(String.format("fs.%s.impl.disable.cache", ABFS_SECURE_SCHEME), true);

    setDefaultAclOnRoot(firstTestUserGuid);
    return (AzureBlobFileSystem) FileSystem.newInstance(testUserConf);
  }

  private void setTestUserConf(Configuration conf, String key, String value) {
    conf.set(key, value);
    conf.set(key + "." + getAccountName(), value);
  }

  /**
   * Test to check that recursive deletePath works with paginated enabled and
   * disabled for both empty and non-empty directory.
   * When enabled appropriate xMsVersion should be used.
   * @throws Exception
   */
  @Test
  public void testRecursiveDeleteWithPagination() throws Exception {
    Assume.assumeTrue(
        getFileSystem().getAbfsStore().getClient() instanceof AbfsDfsClient);
    testRecursiveDeleteWithPaginationInternal(false, true,
        AbfsHttpConstants.ApiVersion.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(false, true,
        AbfsHttpConstants.ApiVersion.AUG_03_2023);
    testRecursiveDeleteWithPaginationInternal(false, false,
        AbfsHttpConstants.ApiVersion.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(false, false,
        AbfsHttpConstants.ApiVersion.AUG_03_2023);
    testRecursiveDeleteWithPaginationInternal(true, true,
        AbfsHttpConstants.ApiVersion.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(true, false,
        AbfsHttpConstants.ApiVersion.AUG_03_2023);
  }

  /**
   * Test to check that non-recursive delete works with both paginated enabled
   * and disabled only for empty directories.
   * Pagination should not be set when recursive is false.
   * @throws Exception
   */
  @Test
  public void testNonRecursiveDeleteWithPagination() throws Exception {
    assumeDfsServiceType();
    testNonRecursiveDeleteWithPaginationInternal(true);
    testNonRecursiveDeleteWithPaginationInternal(false);
  }

  /**
   * Test to check that with pagination enabled, invalid CT will fail
   * @throws Exception
   */
  @Test
  public void testRecursiveDeleteWithInvalidCT() throws Exception {
    Assume.assumeTrue(
        getFileSystem().getAbfsStore().getClient() instanceof AbfsDfsClient);
    testRecursiveDeleteWithInvalidCTInternal(true);
    testRecursiveDeleteWithInvalidCTInternal(false);
  }

  private void testRecursiveDeleteWithPaginationInternal(boolean isEmptyDir,
      boolean isPaginatedDeleteEnabled, AbfsHttpConstants.ApiVersion xMsVersion)
      throws Exception {
    final AzureBlobFileSystem fs = getUserFileSystem();
    TracingContext testTC = getTestTracingContext(fs, true);

    Path testPath;
    if (isEmptyDir) {
      testPath = new Path("/emptyPath" + StringUtils.right(
          UUID.randomUUID().toString(), 10));
      superUserFs.mkdirs(testPath);
    } else {
      testPath = createSmallDir();
    }

    // Set the paginated enabled value and xMsVersion at spiedClient level.
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    ReflectionUtils.setFinalField(AbfsClient.class, spiedClient, "xMsVersion", xMsVersion);
    Mockito.doReturn(isPaginatedDeleteEnabled).when(spiedClient).getIsPaginatedDeleteEnabled();

    AbfsRestOperation op = spiedClient.deletePath(
        testPath.toString(), true, null, testTC);

    // Getting the xMsVersion that was used to make the request
    String xMsVersionUsed = getHeaderValue(op.getRequestHeaders(), X_MS_VERSION);
    String urlUsed = op.getUrl().toString();

    // Assert that appropriate xMsVersion and query param was used to make request
    if (isPaginatedDeleteEnabled && isHnsEnabled) {
      Assertions.assertThat(urlUsed)
          .describedAs("Url must have paginated = true as query param")
          .contains(QUERY_PARAM_PAGINATED);
      if (xMsVersion.compareTo(AbfsHttpConstants.ApiVersion.AUG_03_2023) < 0) {
        Assertions.assertThat(xMsVersionUsed)
            .describedAs("Request was made with wrong x-ms-version")
            .isEqualTo(AbfsHttpConstants.ApiVersion.AUG_03_2023.toString());
      } else if (xMsVersion.compareTo(AbfsHttpConstants.ApiVersion.AUG_03_2023) >= 0) {
        Assertions.assertThat(xMsVersionUsed)
            .describedAs("Request was made with wrong x-ms-version")
            .isEqualTo(xMsVersion.toString());
      }
    } else {
      Assertions.assertThat(urlUsed)
          .describedAs("Url must not have paginated = true as query param")
          .doesNotContain(QUERY_PARAM_PAGINATED);
      Assertions.assertThat(xMsVersionUsed)
          .describedAs("Request was made with wrong x-ms-version")
          .isEqualTo(xMsVersion.toString());
    }

    // Assert that deletion was successful in every scenario.
    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
        spiedClient.getPathStatus(testPath.toString(), false, testTC, null));
    assertStatusCode(e, HTTP_NOT_FOUND);
  }

  private void testNonRecursiveDeleteWithPaginationInternal(boolean isPaginatedDeleteEnabled) throws Exception{
    final AzureBlobFileSystem fs = getUserFileSystem();
    TracingContext testTC = getTestTracingContext(fs, true);

    Path testPath = new Path("/emptyPath");
    superUserFs.mkdirs(testPath);

    // Set the paginated enabled value at spiedClient level.
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    Mockito.doReturn(isPaginatedDeleteEnabled).when(spiedClient).getIsPaginatedDeleteEnabled();

    AbfsRestOperation op = spiedClient.deletePath(
        testPath.toString(), false, null, testTC);

    // Getting the url that was used to make the request
    String urlUsed = op.getUrl().toString();

    // Assert that paginated query param was not set to make request
    Assertions.assertThat(urlUsed)
          .describedAs("Url must not have paginated as query param")
          .doesNotContain(QUERY_PARAM_PAGINATED);

    // Assert that deletion was successful in every scenario.
    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
        spiedClient.getPathStatus(testPath.toString(), false, testTC, null));
    assertStatusCode(e, HTTP_NOT_FOUND);
  }

  private void testRecursiveDeleteWithInvalidCTInternal(boolean isPaginatedEnabled) throws Exception {
    final AzureBlobFileSystem fs = getUserFileSystem();

    Path testPath = createSmallDir();
    String randomCT = "randomContinuationToken1234";
    TracingContext testTC = getTestTracingContext(this.testUserFs, true);

    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    Mockito.doReturn(isPaginatedEnabled).when(spiedClient).getIsPaginatedDeleteEnabled();

    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
        spiedClient.deletePath(testPath.toString(), true, randomCT, testTC));
    assertStatusCode(e, HTTP_BAD_REQUEST);
  }

  /**
   * Provide test user with default ACL permissions on root.
   * @param uid
   * @throws IOException
   */
  private void setDefaultAclOnRoot(String uid)
      throws IOException {
    List<AclEntry> aclSpec = Lists.newArrayList(AclTestHelpers.aclEntry(
        AclEntryScope.ACCESS, AclEntryType.USER, uid, FsAction.ALL),
        AclTestHelpers.aclEntry(AclEntryScope.DEFAULT, AclEntryType.USER, uid, FsAction.ALL));
    // Use SuperUser Privilege to set ACL on root for test user.
    this.superUserFs.modifyAclEntries(new Path("/"), aclSpec);
  }

  private Path createSmallDir() throws IOException {
    String rootPath = "/smallDir" + StringUtils.right(
        UUID.randomUUID().toString(), 10);
    String firstFilePath = rootPath + "/placeholderFile";
    this.superUserFs.create(new Path(firstFilePath));

    for (int i = 1; i <= 2; i++) {
      String dirPath = "/dirLevel1-" + i + "/dirLevel2-" + i;
      String filePath = rootPath + dirPath + "/file-" + i;
      this.superUserFs.create(new Path(filePath));
    }
    return new Path(rootPath);
  }

  /**
   * Select the filesystem to be used for delete API.
   * For HNS Disabled accounts, test User FS won't have permissions as ACL is not supported
   * @return
   */
  private AzureBlobFileSystem getUserFileSystem() {
    return this.isHnsEnabled ? this.testUserFs : this.superUserFs;
  }

  private void assertStatusCode(final AbfsRestOperationException e, final int statusCode) {
    Assertions.assertThat(e.getStatusCode())
        .describedAs("Request Should fail with Bad Request instead of %s",
            e.toString())
        .isEqualTo(statusCode);
  }

  private void assumeTestUserCredentialsConfigured() {
    assumeValidTestConfigPresent(getRawConfiguration(),
        FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT);
    assumeValidTestConfigPresent(getRawConfiguration(),
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);
    assumeValidTestConfigPresent(getRawConfiguration(),
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID);
    assumeValidTestConfigPresent(getRawConfiguration(),
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET);
  }
}
