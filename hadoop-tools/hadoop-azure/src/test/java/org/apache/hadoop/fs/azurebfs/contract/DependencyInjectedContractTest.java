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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.azurebfs.DependencyInjectedTest;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;

/**
 * Dependency inject for ABFS contract tests.
 */
public class DependencyInjectedContractTest extends DependencyInjectedTest {
  private final URI testUri;

  public DependencyInjectedContractTest(final boolean secure) throws Exception {
    this(secure, true);
  }

  public DependencyInjectedContractTest(final boolean secure, final boolean useExistedFileSystem) throws Exception{
    super(secure);
    if (useExistedFileSystem) {
      Configuration configuration = getConfiguration();
      String testUrl = configuration.get(TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI);

      if (secure) {
        testUrl = testUrl.replaceFirst(FileSystemUriSchemes.ABFS_SCHEME, FileSystemUriSchemes.ABFS_SECURE_SCHEME);
      }
      updateTestUrl(testUrl);

      this.testUri = new URI(testUrl);
      //Get container for contract tests
      configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, this.testUri.toString());
      String[] splitAuthority = this.testUri.getAuthority().split("\\@");
      updateFileSystemName(splitAuthority[0]);
    } else {
      this.testUri = new URI(super.getTestUrl());
    }
  }

  public Configuration getConfiguration() {
    return super.getConfiguration();
  }
}