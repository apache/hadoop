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
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ALWAYS_USE_HTTPS;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Parameterized test of ABFS CLIENT URL scheme verification.
 */
@ParameterizedClass
@MethodSource("params")
public class ITestClientUrlScheme extends AbstractAbfsIntegrationTest{

  public boolean useSecureScheme;

  public boolean alwaysUseHttps;

  public static Iterable<Object[]> params() {
    return Arrays.asList(
            new Object[][]{
                    {false, false},
                    {false, true},
                    {true, true},
                    {true, false}
            });
  }

  public ITestClientUrlScheme(boolean pUseSecureScheme,
      boolean pAlwaysUseHttps) throws Exception {
    super();
    this.useSecureScheme = pUseSecureScheme;
    this.alwaysUseHttps = pAlwaysUseHttps;
    // authentication like OAUTH must use HTTPS
    assumeThat(getAuthType())
        .as("ITestClientUrlScheme is skipped because auth type is not SharedKey")
        .isEqualTo(AuthType.SharedKey);
  }

  @Test
  public void testClientUrlScheme() throws Exception {
    String[] urlWithoutScheme = this.getTestUrl().split(":");
    String fsUrl;
    // update filesystem scheme
    if (useSecureScheme) {
      fsUrl = FileSystemUriSchemes.ABFS_SECURE_SCHEME + ":" + urlWithoutScheme[1];
    } else {
      fsUrl = FileSystemUriSchemes.ABFS_SCHEME + ":" + urlWithoutScheme[1];
    }

    Configuration config = getRawConfiguration();
    config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUrl.toString());
    config.setBoolean(FS_AZURE_ALWAYS_USE_HTTPS, alwaysUseHttps);
    // HTTP is enabled only when "abfs://XXX" is used and FS_AZURE_ALWAYS_USE_HTTPS
    // is set as false, otherwise HTTPS should be used.
    boolean expectHttpConnection = !useSecureScheme && !alwaysUseHttps;

    AbfsClient client = null;
    try {
      client = this.getFileSystem(config).getAbfsClient();
    } catch (AbfsRestOperationException e) {
      if (AzureServiceErrorCode.ACCOUNT_REQUIRES_HTTPS.equals(e.getErrorCode())
          && expectHttpConnection) {
        // if we get here, the error message was the account supports HTTPS only
        // and this parameterized test is trying to create an HTTP one.
        // we can implicitly infer that the scheme setup went through,
        // otherwise it would not have been rejected at the far end
        return;
      } else {
        throw e;
      }
    }

    Field baseUrlField = AbfsClient.class.
            getDeclaredField("baseUrl");
    baseUrlField.setAccessible(true);

    String url = ((URL) baseUrlField.get(client)).toString();

    if (expectHttpConnection) {
      Assertions.assertTrue(url.startsWith(FileSystemUriSchemes.HTTP_SCHEME));
    } else {
      Assertions.assertTrue(url.startsWith(FileSystemUriSchemes.HTTPS_SCHEME));
    }
  }
}
