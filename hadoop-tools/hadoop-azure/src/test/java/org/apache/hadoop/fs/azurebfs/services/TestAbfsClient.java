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

import java.net.URL;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

/**
 * Test useragent of abfs client.
 *
 */
public final class TestAbfsClient {

  @Test
  public void verifyUnknownUserAgent() throws Exception {
    String expectedUserAgentPattern = "Azure Blob FS\\/1.0 \\(JavaJRE ([^\\)]+)\\)";
    final Configuration configuration = new Configuration();
    configuration.unset(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY);
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration);
    AbfsClient abfsClient = new AbfsClient(new URL("http://azure.com"), null, abfsConfiguration, null);
    String userAgent = abfsClient.initializeUserAgent(abfsConfiguration);
    Pattern pattern = Pattern.compile(expectedUserAgentPattern);
    Assert.assertTrue(pattern.matcher(userAgent).matches());
  }

  @Test
  public void verifyUserAgent() throws Exception {
    String expectedUserAgentPattern = "Azure Blob FS\\/1.0 \\(JavaJRE ([^\\)]+)\\) Partner Service";
    final Configuration configuration = new Configuration();
    configuration.set(ConfigurationKeys.FS_AZURE_USER_AGENT_PREFIX_KEY, "Partner Service");
    AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration);
    AbfsClient abfsClient = new AbfsClient(new URL("http://azure.com"), null, abfsConfiguration, null);
    String userAgent = abfsClient.initializeUserAgent(abfsConfiguration);
    Pattern pattern = Pattern.compile(expectedUserAgentPattern);
    Assert.assertTrue(pattern.matcher(userAgent).matches());
  }
}