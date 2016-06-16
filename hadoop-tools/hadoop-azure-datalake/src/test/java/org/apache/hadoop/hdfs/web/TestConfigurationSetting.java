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

package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.TestableAdlFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.ConfCredentialBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class is responsible for testing adl file system required configuration
 * and feature set keys.
 */
public class TestConfigurationSetting {

  @Test
  public void testAllConfiguration() throws URISyntaxException, IOException {
    TestableAdlFileSystem fs = new TestableAdlFileSystem();
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY,
        "http://localhost:1111/refresh");
    conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
        "credential");
    conf.set(HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY, "MY_CLIENTID");
    conf.set(HdfsClientConfigKeys.ACCESS_TOKEN_PROVIDER_KEY,
        ConfCredentialBasedAccessTokenProvider.class.getName());
    conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, "true");

    URI uri = new URI("adl://localhost:1234");
    fs.initialize(uri, conf);

    // Default setting check
    Assert.assertEquals(true, fs.isFeatureRedirectOff());
    Assert.assertEquals(true, fs.isFeatureGetBlockLocationLocallyBundled());
    Assert.assertEquals(true, fs.isFeatureConcurrentReadWithReadAhead());
    Assert.assertEquals(false, fs.isOverrideOwnerFeatureOn());
    Assert.assertEquals(8 * 1024 * 1024, fs.getMaxBufferSize());
    Assert.assertEquals(2, fs.getMaxConcurrentConnection());

    fs.close();

    // Configuration toggle check
    conf.set("adl.feature.override.redirection.off", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureRedirectOff());
    fs.close();
    conf.set("adl.feature.override.redirection.off", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureRedirectOff());
    fs.close();

    conf.set("adl.feature.override.getblocklocation.locally.bundled", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureGetBlockLocationLocallyBundled());
    fs.close();
    conf.set("adl.feature.override.getblocklocation.locally.bundled", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureGetBlockLocationLocallyBundled());
    fs.close();

    conf.set("adl.feature.override.readahead", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isFeatureConcurrentReadWithReadAhead());
    fs.close();
    conf.set("adl.feature.override.readahead", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isFeatureConcurrentReadWithReadAhead());
    fs.close();

    conf.set("adl.feature.override.readahead.max.buffersize", "101");
    fs.initialize(uri, conf);
    Assert.assertEquals(101, fs.getMaxBufferSize());
    fs.close();
    conf.set("adl.feature.override.readahead.max.buffersize", "12134565");
    fs.initialize(uri, conf);
    Assert.assertEquals(12134565, fs.getMaxBufferSize());
    fs.close();

    conf.set("adl.debug.override.localuserasfileowner", "true");
    fs.initialize(uri, conf);
    Assert.assertEquals(true, fs.isOverrideOwnerFeatureOn());
    fs.close();
    conf.set("adl.debug.override.localuserasfileowner", "false");
    fs.initialize(uri, conf);
    Assert.assertEquals(false, fs.isOverrideOwnerFeatureOn());
    fs.close();
  }
}
