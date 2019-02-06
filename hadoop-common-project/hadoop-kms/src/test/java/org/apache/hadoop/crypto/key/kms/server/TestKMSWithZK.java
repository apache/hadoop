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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

public class TestKMSWithZK {

  protected Configuration createBaseKMSConf(File keyStoreDir) throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(KMSConfiguration.KEY_PROVIDER_URI,
        "jceks://file@" + new Path(keyStoreDir.getAbsolutePath(),
            "kms.keystore").toUri());
    conf.set("hadoop.kms.authentication.type", "simple");
    conf.setBoolean(KMSConfiguration.KEY_AUTHORIZATION_ENABLE, false);

    conf.set(KMSACLs.Type.GET_KEYS.getAclConfigKey(), "foo");
    return conf;
  }

  @Test
  public void testMultipleKMSInstancesWithZKSigner() throws Exception {
    final File testDir = TestKMS.getTestDir();
    Configuration conf = createBaseKMSConf(testDir);

    TestingServer zkServer = new TestingServer();
    zkServer.start();

    MiniKMS kms1 = null;
    MiniKMS kms2 = null;

    conf.set(KMSAuthenticationFilter.CONFIG_PREFIX +
        AuthenticationFilter.SIGNER_SECRET_PROVIDER, "zookeeper");
    conf.set(KMSAuthenticationFilter.CONFIG_PREFIX +
            ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    conf.set(KMSAuthenticationFilter.CONFIG_PREFIX +
            ZKSignerSecretProvider.ZOOKEEPER_PATH, "/secret");
    TestKMS.writeConf(testDir, conf);

    try {
      kms1 = new MiniKMS.Builder()
          .setKmsConfDir(testDir).setLog4jConfFile("log4j.properties").build();
      kms1.start();

      kms2 = new MiniKMS.Builder()
          .setKmsConfDir(testDir).setLog4jConfFile("log4j.properties").build();
      kms2.start();

      final URL url1 = new URL(kms1.getKMSUrl().toExternalForm() +
          KMSRESTConstants.SERVICE_VERSION +  "/" +
          KMSRESTConstants.KEYS_NAMES_RESOURCE);
      final URL url2 = new URL(kms2.getKMSUrl().toExternalForm() +
          KMSRESTConstants.SERVICE_VERSION + "/" +
          KMSRESTConstants.KEYS_NAMES_RESOURCE);

      final DelegationTokenAuthenticatedURL.Token token =
          new DelegationTokenAuthenticatedURL.Token();
      final DelegationTokenAuthenticatedURL aUrl =
          new DelegationTokenAuthenticatedURL();

      UserGroupInformation ugiFoo = UserGroupInformation.createUserForTesting(
          "foo", new String[]{"gfoo"});
      UserGroupInformation ugiBar = UserGroupInformation.createUserForTesting(
          "bar", new String[]{"gBar"});

      ugiFoo.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          HttpURLConnection conn = aUrl.openConnection(url1, token);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          return null;
        }
      });

      ugiBar.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          HttpURLConnection conn = aUrl.openConnection(url2, token);
          Assert.assertEquals(HttpURLConnection.HTTP_OK,
              conn.getResponseCode());
          return null;
        }
      });

      ugiBar.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final DelegationTokenAuthenticatedURL.Token emptyToken =
              new DelegationTokenAuthenticatedURL.Token();
          HttpURLConnection conn = aUrl.openConnection(url2, emptyToken);
          Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
              conn.getResponseCode());
          return null;
        }
      });

    } finally {
      if (kms2 != null) {
        kms2.stop();
      }
      if (kms1 != null) {
        kms1.stop();
      }
      zkServer.stop();
    }

  }

}
