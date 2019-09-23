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

import java.lang.reflect.Field;
import java.net.URL;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * Test Oauth fail fast when uri scheme is incorrect.
 */
public class ITestOauthOverAbfsScheme extends AbstractAbfsIntegrationTest {

  public ITestOauthOverAbfsScheme() throws Exception {
    Assume.assumeTrue("ITestOauthOverAbfsScheme is skipped because auth type is not OAuth",
            getAuthType() == AuthType.OAuth);
  }

  @Test
  public void testOauthOverSchemeAbfs() throws Exception {
    String[] urlWithoutScheme = this.getTestUrl().split(":");
    String fsUrl;
    // update filesystem scheme to use abfs://
    fsUrl = FileSystemUriSchemes.ABFS_SCHEME + ":" + urlWithoutScheme[1];

    Configuration config = getRawConfiguration();
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUrl.toString());

    AbfsClient client = this.getFileSystem(config).getAbfsClient();

    Field baseUrlField = AbfsClient.class.
            getDeclaredField("baseUrl");
    baseUrlField.setAccessible(true);
    String url = ((URL) baseUrlField.get(client)).toString();

    Assume.assumeTrue("OAuth authentication over scheme abfs must use HTTPS",
            url.startsWith(FileSystemUriSchemes.HTTPS_SCHEME));

  }
}
