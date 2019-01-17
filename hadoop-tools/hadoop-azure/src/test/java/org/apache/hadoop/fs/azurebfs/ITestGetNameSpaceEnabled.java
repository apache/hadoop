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
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test getIsNamespaceEnabled call.
 */
public class ITestGetNameSpaceEnabled extends AbstractAbfsIntegrationTest {

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
  public void testFailedRequestWhenFSNotExist() throws Exception {
    AbfsConfiguration config = this.getConfiguration();
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    String testUri = this.getTestUrl();
    String nonExistingFsUrl = getAbfsScheme() + "://" + UUID.randomUUID()
            + testUri.substring(testUri.indexOf("@"));
    AzureBlobFileSystem fs = this.getFileSystem(nonExistingFsUrl);

    intercept(AbfsRestOperationException.class,
            "\"The specified filesystem does not exist.\", 404",
            ()-> {
              fs.getIsNamespaceEnabled();
            });
  }

  @Test
  public void testFailedRequestWhenCredentialsNotCorrect() throws Exception {
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    Configuration config = this.getRawConfiguration();
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    String accountName = this.getAccountName();
    String configkKey = FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME + "." + accountName;
    // Provide a wrong sharedKey
    String secret = config.get(configkKey);
    secret = (char) (secret.charAt(0) + 1) + secret.substring(1);
    config.set(configkKey, secret);

    AzureBlobFileSystem fs = this.getFileSystem(config);
    intercept(AbfsRestOperationException.class,
            "\"Server failed to authenticate the request. Make sure the value of Authorization header is formed correctly including the signature.\", 403",
            ()-> {
              fs.getIsNamespaceEnabled();
            });
  }
}