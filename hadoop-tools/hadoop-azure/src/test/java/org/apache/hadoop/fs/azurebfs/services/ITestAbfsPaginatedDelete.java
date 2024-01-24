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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
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
import org.apache.hadoop.util.Lists;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
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

public class ITestAbfsPaginatedDelete extends AbstractAbfsIntegrationTest {

  private AzureBlobFileSystem superUserFs;
  private AzureBlobFileSystem firstTestUserFs;
  private String firstTestUserGuid;

  private boolean isHnsEnabled;
  public ITestAbfsPaginatedDelete() throws Exception {
  }

  @Override
  public void setup() throws Exception {
    isHnsEnabled = this.getConfiguration().getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    loadConfiguredFileSystem();
    super.setup();
    this.superUserFs = getFileSystem();
    this.firstTestUserGuid = getConfiguration()
        .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);

    if(isHnsEnabled) {
      // setting up ACL permissions for test user
      setFirstTestUserFsAuth();
      setDefaultAclOnRoot(this.firstTestUserGuid);
    }
  }

  /**
   * Test to check that recursive deletePath works with paginated enabled and
   * disabled for both empty and non-empty directory.
   * When enabled appropriate xMsVersion should be used.
   * @throws Exception
   */
  @Test
  public void testRecursiveDeleteWithPagination() throws Exception {
    testRecursiveDeleteWithPaginationInternal(false, true,
        AbfsHttpConstants.API_VERSION.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(false, true,
        AbfsHttpConstants.API_VERSION.AUG_03_2023);
    testRecursiveDeleteWithPaginationInternal(false, false,
        AbfsHttpConstants.API_VERSION.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(false, false,
        AbfsHttpConstants.API_VERSION.AUG_03_2023);
    testRecursiveDeleteWithPaginationInternal(true, true,
        AbfsHttpConstants.API_VERSION.DEC_12_2019);
    testRecursiveDeleteWithPaginationInternal(true, false,
        AbfsHttpConstants.API_VERSION.AUG_03_2023);
  }

  /**
   * Test to check that non-recursive delete works with both paginated enabled
   * and disabled only for empty directories.
   * Pagination should not be set when recursive is false.
   * @throws Exception
   */
  @Test
  public void testNonRecursiveDeleteWithPagination() throws Exception {
    testNonRecursiveDeleteWithPaginationInternal(true);
    testNonRecursiveDeleteWithPaginationInternal(false);
  }

  /**
   * Test to check that with pagination enabled, invalid CT will fail
   * @throws Exception
   */
  @Test
  public void testRecursiveDeleteWithInvalidCT() throws Exception {
    testRecursiveDeleteWithInvalidCTInternal(true);
    testRecursiveDeleteWithInvalidCTInternal(false);
  }

  private void testRecursiveDeleteWithPaginationInternal(boolean isEmptyDir,
      boolean isPaginatedDeleteEnabled,
      AbfsHttpConstants.API_VERSION xMsVersion) throws Exception {
    final AzureBlobFileSystem fs = getUserFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, true);
    Path testPath;
    if (isEmptyDir) {
      testPath = new Path("/emptyPath" + StringUtils.right(
          UUID.randomUUID().toString(), 10));
      fs.mkdirs(testPath);
    } else {
      testPath = createSmallDir();
    }

    // Set the paginated enabled value and xMsVersion at spiedClient level.
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    ITestAbfsClient.setAbfsClientField(spiedClient, "xMsVersion", xMsVersion);
    spiedClient.getAbfsConfiguration().setIsPaginatedDeleteEnabled(isPaginatedDeleteEnabled);

    AbfsRestOperation op = spiedClient.deletePath(
        testPath.toString(), true, null, testTracingContext);

    // Getting the xMsVersion that was used to make the request
    String xMsVersionUsed = getHeaderValue(op.getRequestHeaders(), X_MS_VERSION);
    String urlUsed = op.getUrl().toString();

    // Assert that appropriate xMsVersion and query param was used to make request
    if (isPaginatedDeleteEnabled) {
      Assertions.assertThat(urlUsed)
          .describedAs("Url must have paginated = true as query param")
          .contains(QUERY_PARAM_PAGINATED);
      if (xMsVersion.compareTo(AbfsHttpConstants.API_VERSION.AUG_03_2023) < 0) {
        Assertions.assertThat(xMsVersionUsed)
            .describedAs("Request was made with wrong x-ms-version")
            .isEqualTo(AbfsHttpConstants.API_VERSION.AUG_03_2023.toString());
      } else if (xMsVersion.compareTo(AbfsHttpConstants.API_VERSION.AUG_03_2023) >= 0) {
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
        spiedClient.getPathStatus(testPath.toString(), false, testTracingContext, null));
    Assertions.assertThat(e.getStatusCode())
        .describedAs("Path should have been deleted").isEqualTo(HTTP_NOT_FOUND);
  }

  private void testNonRecursiveDeleteWithPaginationInternal(boolean isPaginatedDeleteEnabled) throws Exception{
    final AzureBlobFileSystem fs = getUserFileSystem();
    TracingContext testTracingContext = getTestTracingContext(fs, true);
    Path testPath = new Path("/emptyPath");
    fs.mkdirs(testPath);

    // Set the paginated enabled value and xMsVersion at spiedClient level.
    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    spiedClient.getAbfsConfiguration().setIsPaginatedDeleteEnabled(isPaginatedDeleteEnabled);

    AbfsRestOperation op = spiedClient.deletePath(
        testPath.toString(), false, null, testTracingContext);

    // Getting the url that was used to make the request
    String urlUsed = op.getUrl().toString();

    // Assert that paginated query param was not set to make request
    Assertions.assertThat(urlUsed)
          .describedAs("Url must not have paginated as query param")
          .doesNotContain(QUERY_PARAM_PAGINATED);

    // Assert that deletion was successful in every scenario.
    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
        spiedClient.getPathStatus(testPath.toString(), false, testTracingContext, null));
    Assertions.assertThat(e.getStatusCode())
        .describedAs("Path should have been deleted").isEqualTo(HTTP_NOT_FOUND);
  }

  private void testRecursiveDeleteWithInvalidCTInternal(boolean isPaginatedEnabled) throws Exception {
    final AzureBlobFileSystem fs = getUserFileSystem();
    Path smallDirPath = createSmallDir();
    String randomCT = "randomContinuationToken1234";
    TracingContext testTracingContext = getTestTracingContext(this.firstTestUserFs, true);

    AbfsClient spiedClient = Mockito.spy(fs.getAbfsStore().getClient());
    spiedClient.getAbfsConfiguration().setIsPaginatedDeleteEnabled(isPaginatedEnabled);

    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () ->
        spiedClient.deletePath(smallDirPath.toString(), true, randomCT, testTracingContext));
    Assertions.assertThat(e.getStatusCode())
        .describedAs("Request Should fail with Bad Request").isEqualTo(HTTP_BAD_REQUEST);
  }

  private AzureBlobFileSystem getUserFileSystem() {
    // For HNS account only Server will trigger Pagination for ACL checks
    // And for ACL Checks file system user should not be superUser.
    return this.isHnsEnabled ? this.firstTestUserFs : this.superUserFs;
  }

  private void setFirstTestUserFsAuth() throws IOException {
    if (this.firstTestUserFs != null) {
      return;
    }
    checkIfConfigIsSet(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT
        + "." + getAccountName());
    Configuration conf = getRawConfiguration();
    setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_ID, FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID);
    setTestFsConf(FS_AZURE_BLOB_FS_CLIENT_SECRET,
        FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET);
    conf.set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth.name());
    conf.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME + "."
        + getAccountName(), ClientCredsTokenProvider.class.getName());
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
        false);
    this.firstTestUserFs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
  }

  private void setTestFsConf(final String fsConfKey,
      final String testFsConfKey) {
    final String confKeyWithAccountName = fsConfKey + "." + getAccountName();
    final String confValue = getConfiguration()
        .getString(testFsConfKey, "");
    getRawConfiguration().set(confKeyWithAccountName, confValue);
  }

  private void setDefaultAclOnRoot(String uid)
      throws IOException {
    List<AclEntry> aclSpec =  Lists.newArrayList(AclTestHelpers
            .aclEntry(AclEntryScope.ACCESS, AclEntryType.USER, uid, FsAction.ALL),
        AclTestHelpers.aclEntry(AclEntryScope.DEFAULT, AclEntryType.USER, uid, FsAction.ALL));
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

  private void checkIfConfigIsSet(String configKey){
    AbfsConfiguration conf = getConfiguration();
    String value = conf.get(configKey);
    Assume.assumeTrue(configKey + " config is mandatory for the test to run",
        value != null && value.trim().length() > 1);
  }
}
