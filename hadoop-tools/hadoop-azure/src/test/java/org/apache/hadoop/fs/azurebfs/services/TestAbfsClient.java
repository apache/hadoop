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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CLIENT_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SEMICOLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;

/**
 * Test useragent of abfs client.
 *
 */
@RunWith(JUnit4.class)
public final class TestAbfsClient {

  private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
  private static final String FS_AZURE_USER_AGENT_PREFIX = "Partner Service";

  private final Pattern userAgentStringPattern;

  public TestAbfsClient(){
    StringBuilder regEx = new StringBuilder();
    regEx.append("^");
    regEx.append(APN_VERSION);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(CLIENT_VERSION);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append("\\(");
    regEx.append("JavaJRE");
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append("\\d+(\\.\\d+_?\\d*)*");  //  Regex for java version
    regEx.append(SEMICOLON);
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append("[a-zA-Z].*");            //   Regex for OS name and version
    regEx.append(FORWARD_SLASH);
    regEx.append(".*");                    //  Regex for OS arch
    regEx.append("(;[a-zA-Z].*)?");         // Regex for sslProviderName
    regEx.append("(;[a-zA-Z].*)?");         // Regex for tokenProvider
    regEx.append(SINGLE_WHITE_SPACE);
    regEx.append(".+");                      //cluster name
    regEx.append(FORWARD_SLASH);
    regEx.append(".+");            // cluster type
    regEx.append("\\)");
    regEx.append("( .*)?");                      //  Regex for user agent prefix
    this.userAgentStringPattern = Pattern.compile(regEx.toString());
  }

  private String getUserAgentString(AbfsConfiguration config,
                                 boolean includeSSLProvider)
      throws MalformedURLException {
    AbfsClient client = new AbfsClient(new URL("http://azure.com"), null,
        config, null, (AccessTokenProvider) null, null);
    String sslProviderName = null;
    if (includeSSLProvider) {
      sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory().getProviderName();
    }
    return client.initializeUserAgent(config, sslProviderName);
  }

  @Test
  public void verifyUnknownUserAgent()
      throws IOException, IllegalAccessException {
    final Configuration configuration = new Configuration();
    configuration.unset(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(
        abfsConfiguration, false);
    Assertions.assertThat(userAgentStr).describedAs("User-Agent string ["+userAgentStr
        + "] should be of the pattern: "+this.userAgentStringPattern.pattern()).matches(this.userAgentStringPattern);
  }

  @Test
  public void verifyUserAgent() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, FS_AZURE_USER_AGENT_PREFIX);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, false);
    Assertions.assertThat(userAgentStr).describedAs("User-Agent string ["+userAgentStr
        + "] should be of the pattern: " + this.userAgentStringPattern
        .pattern()).matches(this.userAgentStringPattern).describedAs(
        "User-Agent string should contain " + FS_AZURE_USER_AGENT_PREFIX)
        .contains(FS_AZURE_USER_AGENT_PREFIX);
  }

  @Test
  public void verifyUserAgentWithSSLProvider() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, FS_AZURE_USER_AGENT_PREFIX);
    configuration.set(ConfigurationKeys.FS_AZURE_SSL_CHANNEL_MODE_KEY,
        DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE.name());
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration,
        ACCOUNT_NAME);
    String userAgentStr = getUserAgentString(abfsConfiguration, true);
    Assertions.assertThat(userAgentStr).describedAs("User-Agent string ["+userAgentStr
        + "] should be of the pattern: " + this.userAgentStringPattern
        .pattern()).matches(this.userAgentStringPattern).describedAs(
        "User-Agent string should contain " + FS_AZURE_USER_AGENT_PREFIX)
        .contains(FS_AZURE_USER_AGENT_PREFIX)
        .describedAs("User-Agent string " + "should contain sslProvider")
        .contains(
            DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE.name());
  }
}
