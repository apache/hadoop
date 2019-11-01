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
 *
 */

package org.apache.hadoop.fs.adl.live;

import com.microsoft.azure.datalake.store.SSLSocketFactoryEx.SSLChannelMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_HTTP_TIMEOUT;
import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_SSL_CHANNEL_MODE;

/**
 * Tests interactions with SDK and ensures configuration is having the desired
 * effect.
 */
public class TestAdlSdkConfiguration {
  @Test
  public void testDefaultTimeout() throws IOException {
    AdlFileSystem fs = null;
    Configuration conf = null;
    int effectiveTimeout;

    conf = AdlStorageConfiguration.getConfiguration();
    conf.setInt(ADL_HTTP_TIMEOUT, -1);
    try {
      fs = (AdlFileSystem)
          (AdlStorageConfiguration.createStorageConnector(conf));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Can not initialize ADL FileSystem. "
          + "Please check test.fs.adl.name property.", e);
    }

    // Skip this test if we can't get a real FS
    Assume.assumeNotNull(fs);
    effectiveTimeout = fs.getAdlClient().getDefaultTimeout();
    Assert.assertFalse("A negative timeout is not supposed to take effect",
        effectiveTimeout < 0);

    conf = AdlStorageConfiguration.getConfiguration();
    conf.setInt(ADL_HTTP_TIMEOUT, 17);
    try {
      fs = (AdlFileSystem)
          (AdlStorageConfiguration.createStorageConnector(conf));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Can not initialize ADL FileSystem. "
          + "Please check test.fs.adl.name property.", e);
    }

    effectiveTimeout = fs.getAdlClient().getDefaultTimeout();
    Assert.assertEquals("Timeout is getting set",
        effectiveTimeout, 17);

    // The default value may vary by SDK, so that value is not tested here.
  }

  @Test
  public void testSSLChannelModeConfig()
      throws IOException, URISyntaxException {
    testSSLChannelMode(SSLChannelMode.OpenSSL, "OpenSSL");
    testSSLChannelMode(SSLChannelMode.Default_JSE, "Default_JSE");
    testSSLChannelMode(SSLChannelMode.Default, "Default");
    // If config set is invalid, SSL channel mode will be Default.
    testSSLChannelMode(SSLChannelMode.Default, "Invalid");
    // Config value is case insensitive.
    testSSLChannelMode(SSLChannelMode.OpenSSL, "openssl");
  }

  public void testSSLChannelMode(SSLChannelMode expectedMode,
      String sslChannelModeConfigValue) throws IOException, URISyntaxException {

    AdlFileSystem fs = null;
    Configuration conf = null;

    conf = AdlStorageConfiguration.getConfiguration();
    conf.set(ADL_SSL_CHANNEL_MODE, sslChannelModeConfigValue);
    fs = (AdlFileSystem) (AdlStorageConfiguration.createStorageConnector(conf));
    Assume.assumeNotNull(fs);

    SSLChannelMode sslChannelMode = fs.getAdlClient().getSSLChannelMode();
    Assert.assertEquals(
        "Unexpected SSL Channel Mode for adl.ssl.channel.mode config value : "
            + sslChannelModeConfigValue, expectedMode, sslChannelMode);
  }
}
