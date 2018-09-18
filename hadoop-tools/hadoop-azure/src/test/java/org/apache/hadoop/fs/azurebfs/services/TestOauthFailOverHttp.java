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

import java.net.URI;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Oauth fail fast when uri scheme is incorrect.
 */
public class TestOauthFailOverHttp {

  @Test
  public void testOauthFailWithSchemeAbfs() throws Exception {
    Configuration conf = new Configuration();
    final String account = "fakeaccount.dfs.core.windows.net";
    conf.set(FS_AZURE_ABFS_ACCOUNT_NAME, account);
    conf.setEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.OAuth);
    URI defaultUri = new URI(FileSystemUriSchemes.ABFS_SCHEME,
            "fakecontainer@" + account,
            null,
            null,
            null);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    // IllegalArgumentException is expected
    // when authenticating using Oauth and scheme is not abfss
    intercept(IllegalArgumentException.class, "Incorrect URI",
        () -> FileSystem.get(conf));
  }
}
