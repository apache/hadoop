/*
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

import org.junit.Test;

import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class ITestABFSJceksFiltering extends AbstractAbfsIntegrationTest {

  public ITestABFSJceksFiltering() throws Exception {
  }

  @Test
  public void testIncompatibleCredentialProviderIsExcluded() throws Exception {
    Configuration rawConfig = getRawConfiguration();
    rawConfig.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        "jceks://abfs@a@b.c.d/tmp/a.jceks,jceks://file/tmp/secret.jceks");
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(rawConfig)) {
      assertNotNull("filesystem", fs);
      String providers = fs.getConf().get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
      assertEquals("jceks://file/tmp/secret.jceks", providers);
    }
  }
}
