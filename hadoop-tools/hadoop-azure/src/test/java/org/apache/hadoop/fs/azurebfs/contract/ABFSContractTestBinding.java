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

package org.apache.hadoop.fs.azurebfs.contract;

import java.net.URI;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.junit.Assume;

/**
 * Bind ABFS contract tests to the Azure test setup/teardown.
 */
public class ABFSContractTestBinding extends AbstractAbfsIntegrationTest {
  private final URI testUri;

  public ABFSContractTestBinding() throws Exception {
    this(true);
  }

  public ABFSContractTestBinding(
      final boolean useExistingFileSystem) throws Exception{
    if (useExistingFileSystem) {
      AbfsConfiguration configuration = getConfiguration();
      String testUrl = configuration.get(TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI);
      Assume.assumeTrue("Contract tests are skipped because of missing config property :"
      + TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI, testUrl != null);

      if (getAuthType() != AuthType.SharedKey) {
        testUrl = testUrl.replaceFirst(FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME);
      }
      setTestUrl(testUrl);

      this.testUri = new URI(testUrl);
      //Get container for contract tests
      configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, this.testUri.toString());
      String[] splitAuthority = this.testUri.getAuthority().split("\\@");
      setFileSystemName(splitAuthority[0]);
    } else {
      this.testUri = new URI(super.getTestUrl());
    }
  }

  public boolean isSecureMode() {
    return this.getAuthType() == AuthType.SharedKey ? false : true;
  }
}
