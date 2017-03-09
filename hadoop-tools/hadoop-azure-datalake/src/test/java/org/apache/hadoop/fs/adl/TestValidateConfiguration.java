/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_BLOCK_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_EXPERIMENT_POSITIONAL_READ_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_REPLICATION_FACTOR;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_CLIENT_ID_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_CLIENT_SECRET_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_REFRESH_TOKEN_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.AZURE_AD_REFRESH_URL_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_CLASS_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_TYPE_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .DEFAULT_READ_AHEAD_BUFFER_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .DEFAULT_WRITE_AHEAD_BUFFER_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys.LATENCY_TRACKER_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys.LATENCY_TRACKER_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys.READ_AHEAD_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .TOKEN_PROVIDER_TYPE_CLIENT_CRED;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .TOKEN_PROVIDER_TYPE_REFRESH_TOKEN;
import static org.apache.hadoop.fs.adl.AdlConfKeys.WRITE_BUFFER_SIZE_KEY;

/**
 * Validate configuration keys defined for adl storage file system instance.
 */
public class TestValidateConfiguration {

  @Test
  public void validateConfigurationKeys() {
    Assert
        .assertEquals("dfs.adls.oauth2.refresh.url", AZURE_AD_REFRESH_URL_KEY);
    Assert.assertEquals("dfs.adls.oauth2.access.token.provider",
        AZURE_AD_TOKEN_PROVIDER_CLASS_KEY);
    Assert.assertEquals("dfs.adls.oauth2.client.id", AZURE_AD_CLIENT_ID_KEY);
    Assert.assertEquals("dfs.adls.oauth2.refresh.token",
        AZURE_AD_REFRESH_TOKEN_KEY);
    Assert
        .assertEquals("dfs.adls.oauth2.credential", AZURE_AD_CLIENT_SECRET_KEY);
    Assert.assertEquals("adl.debug.override.localuserasfileowner",
        ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER);

    Assert.assertEquals("dfs.adls.oauth2.access.token.provider.type",
        AZURE_AD_TOKEN_PROVIDER_TYPE_KEY);

    Assert.assertEquals("adl.feature.client.cache.readahead",
        READ_AHEAD_BUFFER_SIZE_KEY);

    Assert.assertEquals("adl.feature.client.cache.drop.behind.writes",
        WRITE_BUFFER_SIZE_KEY);

    Assert.assertEquals("RefreshToken", TOKEN_PROVIDER_TYPE_REFRESH_TOKEN);

    Assert.assertEquals("ClientCredential", TOKEN_PROVIDER_TYPE_CLIENT_CRED);

    Assert.assertEquals("adl.dfs.enable.client.latency.tracker",
        LATENCY_TRACKER_KEY);

    Assert.assertEquals(true, LATENCY_TRACKER_DEFAULT);

    Assert.assertEquals(true, ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT);

    Assert.assertEquals("adl.feature.experiment.positional.read.enable",
        ADL_EXPERIMENT_POSITIONAL_READ_KEY);

    Assert.assertEquals(1, ADL_REPLICATION_FACTOR);
    Assert.assertEquals(256 * 1024 * 1024, ADL_BLOCK_SIZE);
    Assert.assertEquals(false, ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT);
    Assert.assertEquals(4 * 1024 * 1024, DEFAULT_READ_AHEAD_BUFFER_SIZE);
    Assert.assertEquals(4 * 1024 * 1024, DEFAULT_WRITE_AHEAD_BUFFER_SIZE);
  }
}
