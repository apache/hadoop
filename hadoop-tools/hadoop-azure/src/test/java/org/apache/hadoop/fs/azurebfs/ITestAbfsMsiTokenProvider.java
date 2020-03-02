/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assume.assumeThat;

/**
 * Test MsiTokenProvider
 */
public final class ITestAbfsMsiTokenProvider
    extends AbstractAbfsIntegrationTest {

  public ITestAbfsMsiTokenProvider() throws Exception {
    super();
  }

  @Test
  public void test() throws IOException {
    AbfsConfiguration conf = getConfiguration();
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT),
        not(isEmptyString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT),
        not(isEmptyString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
        not(isEmptyString()));
    assumeThat(conf.get(FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY),
        not(isEmptyString()));

    String authEndpoint = conf.getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT);
    String tenantGuid = conf.getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT);
    String clientId = conf.getPasswordString(FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID);
    String authority = conf.getPasswordString(FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY);
    AccessTokenProvider tokenProvider = new MsiTokenProvider(authEndpoint,
        tenantGuid, clientId, authority);

    AzureADToken token = null;
    token = tokenProvider.getToken();
    assertThat(token.getAccessToken(), not(isEmptyString()));
    assertThat(token.getExpiry().after(new Date()), is(true));
  }

}
