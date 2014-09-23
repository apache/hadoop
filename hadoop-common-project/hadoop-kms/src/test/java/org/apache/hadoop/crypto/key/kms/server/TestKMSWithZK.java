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
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

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
