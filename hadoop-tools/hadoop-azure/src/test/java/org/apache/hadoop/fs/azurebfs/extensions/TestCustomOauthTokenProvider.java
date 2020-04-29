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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.net.URI;
import java.util.Date;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter;

import static org.apache.hadoop.fs.azurebfs.extensions.WrappingTokenProvider.*;

/**
 * Test custom OAuth token providers.
 * This is a unit test not an E2E integration test because that would
 * require OAuth auth setup, always.
 * Instead this just checks that the creation works and that everything
 * is propagated.
 */
@SuppressWarnings("UseOfObsoleteDateTimeApi")
public class TestCustomOauthTokenProvider extends AbstractAbfsTestWithTimeout {

  public TestCustomOauthTokenProvider() throws Exception {
  }

  /**
   * If you switch to a custom provider, it is loaded and initialized.
   */
  @Test
  public void testCustomProviderBinding() throws Throwable {
    Configuration conf = new Configuration();
    WrappingTokenProvider.enable(conf);
    AbfsConfiguration abfs = new AbfsConfiguration(conf,
        "not-a-real-account");
    CustomTokenProviderAdapter provider =
        (CustomTokenProviderAdapter) abfs.getTokenProvider();
    assertEquals("User agent", INITED, provider.getUserAgentSuffix());

    // now mimic the bind call
    ExtensionHelper.bind(provider,
        new URI("abfs://store@user.dfs.core.windows.net"),
        conf);
    assertEquals("User agent", BOUND,
        ExtensionHelper.getUserAgentSuffix(provider, ""));
    AzureADToken token = provider.getToken();
    assertEquals("Access token propagation",
        ACCESS_TOKEN, token.getAccessToken());
    Date expiry = token.getExpiry();
    long time = expiry.getTime();
    assertTrue("date wrong: " + expiry,
        time <= System.currentTimeMillis());
    // once closed, the UA state changes.
    provider.close();
    assertEquals("User agent", CLOSED, provider.getUserAgentSuffix());
  }
}
