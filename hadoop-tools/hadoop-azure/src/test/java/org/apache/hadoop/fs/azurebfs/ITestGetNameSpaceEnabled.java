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

import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.junit.Assume;
import org.junit.Test;
import org.assertj.core.api.Assertions;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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
            getFileSystem().getIsNamespaceEnabled());
  }

  @Test
  public void testNonXNSAccount() throws IOException {
    Assume.assumeFalse("Skip this test because the account being used for test is a XNS account",
            isUsingXNSAccount);
    assertFalse("Expecting getIsNamespaceEnabled() return false",
        getFileSystem().getIsNamespaceEnabled());
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsTrue() throws Exception {
    AzureBlobFileSystem fs = getNewFSWithHnsConf(TRUE_STR);
    Assertions.assertThat(fs.getIsNamespaceEnabled()).describedAs(
        "getIsNamespaceEnabled should return true when the "
            + "config is set as true").isTrue();
    fs.getAbfsStore().deleteFilesystem();
    unsetAndAssert();
  }

  @Test
  public void testGetIsNamespaceEnabledWhenConfigIsFalse() throws Exception {
    AzureBlobFileSystem fs = getNewFSWithHnsConf(FALSE_STR);
    Assertions.assertThat(fs.getIsNamespaceEnabled()).describedAs(
        "getIsNamespaceEnabled should return false when the "
            + "config is set as false").isFalse();
    fs.getAbfsStore().deleteFilesystem();
    unsetAndAssert();
  }

  private void unsetAndAssert() throws Exception {
    AzureBlobFileSystem fs = getNewFSWithHnsConf(
        DEFAULT_FS_AZURE_ACCOUNT_IS_HNS_ENABLED);
    boolean expectedValue = this.getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assertions.assertThat(fs.getIsNamespaceEnabled()).describedAs(
        "getIsNamespaceEnabled should return the value "
            + "configured for fs.azure.test.namespace.enabled")
        .isEqualTo(expectedValue);
    fs.getAbfsStore().deleteFilesystem();
  }

  private AzureBlobFileSystem getNewFSWithHnsConf(
      String isNamespaceEnabledAccount) throws Exception {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);
    rawConfig.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, isNamespaceEnabledAccount);
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
        .setNamespaceEnabled(Trilean.getTrilean(invalidConf));
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, times(1)).getAclStatus(anyString());
  }

  private void ensureGetAclCallIsNeverMadeForValidConf(String validConf)
      throws Exception {
    this.getFileSystem().getAbfsStore()
        .setNamespaceEnabled(Trilean.getTrilean(validConf));
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, never()).getAclStatus(anyString());
  }

  private void unsetConfAndEnsureGetAclCallIsMadeOnce() throws IOException {
    this.getFileSystem().getAbfsStore().setNamespaceEnabled(Trilean.UNKNOWN);
    AbfsClient mockClient =
        callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient();
    verify(mockClient, times(1)).getAclStatus(anyString());
  }

  private AbfsClient callAbfsGetIsNamespaceEnabledAndReturnMockAbfsClient()
      throws IOException {
    final AzureBlobFileSystem abfs = this.getFileSystem();
    final AzureBlobFileSystemStore abfsStore = abfs.getAbfsStore();
    final AbfsClient mockClient = mock(AbfsClient.class);
    doReturn(mock(AbfsRestOperation.class)).when(mockClient)
        .getAclStatus(anyString());
    abfsStore.setClient(mockClient);
    abfs.getIsNamespaceEnabled();
    return mockClient;
  }

}
