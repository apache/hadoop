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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_BLOCK_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_ENABLEUPN_FOR_OWNERGROUP_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_ENABLEUPN_FOR_OWNERGROUP_KEY;
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Validate configuration keys defined for adl storage file system instance.
 */
public class TestValidateConfiguration {

  @Test
  public void validateConfigurationKeys() {
    assertEquals("fs.adl.oauth2.refresh.url", AZURE_AD_REFRESH_URL_KEY);
    assertEquals("fs.adl.oauth2.access.token.provider",
        AZURE_AD_TOKEN_PROVIDER_CLASS_KEY);
    assertEquals("fs.adl.oauth2.client.id", AZURE_AD_CLIENT_ID_KEY);
    assertEquals("fs.adl.oauth2.refresh.token",
        AZURE_AD_REFRESH_TOKEN_KEY);
    assertEquals("fs.adl.oauth2.credential", AZURE_AD_CLIENT_SECRET_KEY);
    assertEquals("adl.debug.override.localuserasfileowner",
        ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER);

    assertEquals("fs.adl.oauth2.access.token.provider.type",
        AZURE_AD_TOKEN_PROVIDER_TYPE_KEY);

    assertEquals("adl.feature.client.cache.readahead",
        READ_AHEAD_BUFFER_SIZE_KEY);

    assertEquals("adl.feature.client.cache.drop.behind.writes",
        WRITE_BUFFER_SIZE_KEY);

    assertEquals("RefreshToken", TOKEN_PROVIDER_TYPE_REFRESH_TOKEN);

    assertEquals("ClientCredential", TOKEN_PROVIDER_TYPE_CLIENT_CRED);

    assertEquals("adl.enable.client.latency.tracker",
        LATENCY_TRACKER_KEY);

    assertEquals(true, LATENCY_TRACKER_DEFAULT);

    assertEquals(true, ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT);

    assertEquals("adl.feature.experiment.positional.read.enable",
        ADL_EXPERIMENT_POSITIONAL_READ_KEY);

    assertEquals(1, ADL_REPLICATION_FACTOR);
    assertEquals(256 * 1024 * 1024, ADL_BLOCK_SIZE);
    assertEquals(false, ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT);
    assertEquals(4 * 1024 * 1024, DEFAULT_READ_AHEAD_BUFFER_SIZE);
    assertEquals(4 * 1024 * 1024, DEFAULT_WRITE_AHEAD_BUFFER_SIZE);

    assertEquals("adl.feature.ownerandgroup.enableupn",
        ADL_ENABLEUPN_FOR_OWNERGROUP_KEY);
    assertEquals(false,
        ADL_ENABLEUPN_FOR_OWNERGROUP_DEFAULT);
  }

  @Test
  public void testSetDeprecatedKeys() throws ClassNotFoundException {
    Configuration conf = new Configuration(true);
    setDeprecatedKeys(conf);

    // Force AdlFileSystem static initialization to register deprecated keys.
    Class.forName(AdlFileSystem.class.getName());

    assertDeprecatedKeys(conf);
  }

  @Test
  public void testLoadDeprecatedKeys()
      throws IOException, ClassNotFoundException {
    Configuration saveConf = new Configuration(false);
    setDeprecatedKeys(saveConf);

    final File testRootDir = GenericTestUtils.getTestDir();
    File confXml = new File(testRootDir, "testLoadDeprecatedKeys.xml");
    OutputStream out = new FileOutputStream(confXml);
    saveConf.writeXml(out);
    out.close();

    Configuration conf = new Configuration(true);
    conf.addResource(confXml.toURI().toURL());

    // Trigger loading the configuration resources by getting any key.
    conf.get("dummy.key");

    // Force AdlFileSystem static initialization to register deprecated keys.
    Class.forName(AdlFileSystem.class.getName());

    assertDeprecatedKeys(conf);
  }

  @Test
  public void testGetAccountNameFromFQDN() {
    assertEquals("dummy", AdlFileSystem.
        getAccountNameFromFQDN("dummy.azuredatalakestore.net"));
    assertEquals("localhost", AdlFileSystem.
        getAccountNameFromFQDN("localhost"));
  }

  @Test
  public void testPropagateAccountOptionsDefault() {
    Configuration conf = new Configuration(false);
    conf.set("fs.adl.oauth2.client.id", "defaultClientId");
    conf.set("fs.adl.oauth2.credential", "defaultCredential");
    conf.set("some.other.config", "someValue");
    Configuration propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "dummy");
    assertEquals("defaultClientId",
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("defaultCredential",
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));
  }

  @Test
  public void testPropagateAccountOptionsSpecified() {
    Configuration conf = new Configuration(false);
    conf.set("fs.adl.account.dummy.oauth2.client.id", "dummyClientId");
    conf.set("fs.adl.account.dummy.oauth2.credential", "dummyCredential");
    conf.set("some.other.config", "someValue");

    Configuration propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "dummy");
    assertEquals("dummyClientId",
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("dummyCredential",
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));

    propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "anotherDummy");
    assertEquals(null,
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals(null,
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));
  }

  @Test
  public void testPropagateAccountOptionsAll() {
    Configuration conf = new Configuration(false);
    conf.set("fs.adl.oauth2.client.id", "defaultClientId");
    conf.set("fs.adl.oauth2.credential", "defaultCredential");
    conf.set("some.other.config", "someValue");
    conf.set("fs.adl.account.dummy1.oauth2.client.id", "dummyClientId1");
    conf.set("fs.adl.account.dummy1.oauth2.credential", "dummyCredential1");
    conf.set("fs.adl.account.dummy2.oauth2.client.id", "dummyClientId2");
    conf.set("fs.adl.account.dummy2.oauth2.credential", "dummyCredential2");

    Configuration propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "dummy1");
    assertEquals("dummyClientId1",
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("dummyCredential1",
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));

    propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "dummy2");
    assertEquals("dummyClientId2",
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("dummyCredential2",
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));

    propagatedConf =
        AdlFileSystem.propagateAccountOptions(conf, "anotherDummy");
    assertEquals("defaultClientId",
        propagatedConf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("defaultCredential",
        propagatedConf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("someValue",
        propagatedConf.get("some.other.config"));
  }

  private void setDeprecatedKeys(Configuration conf) {
    conf.set("dfs.adls.oauth2.access.token.provider.type", "dummyType");
    conf.set("dfs.adls.oauth2.client.id", "dummyClientId");
    conf.set("dfs.adls.oauth2.refresh.token", "dummyRefreshToken");
    conf.set("dfs.adls.oauth2.refresh.url", "dummyRefreshUrl");
    conf.set("dfs.adls.oauth2.credential", "dummyCredential");
    conf.set("dfs.adls.oauth2.access.token.provider", "dummyClass");
    conf.set("adl.dfs.enable.client.latency.tracker", "dummyTracker");
  }

  private void assertDeprecatedKeys(Configuration conf) {
    assertEquals("dummyType",
        conf.get(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY));
    assertEquals("dummyClientId",
        conf.get(AZURE_AD_CLIENT_ID_KEY));
    assertEquals("dummyRefreshToken",
        conf.get(AZURE_AD_REFRESH_TOKEN_KEY));
    assertEquals("dummyRefreshUrl",
        conf.get(AZURE_AD_REFRESH_URL_KEY));
    assertEquals("dummyCredential",
        conf.get(AZURE_AD_CLIENT_SECRET_KEY));
    assertEquals("dummyClass",
        conf.get(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY));
    assertEquals("dummyTracker",
        conf.get(LATENCY_TRACKER_KEY));
  }
}
