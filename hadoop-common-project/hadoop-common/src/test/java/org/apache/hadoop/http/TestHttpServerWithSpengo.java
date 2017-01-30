/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import static org.junit.Assert.assertTrue;

/**
 * This class is tested for http server with SPENGO authentication.
 */
public class TestHttpServerWithSpengo {

  static final Log LOG = LogFactory.getLog(TestHttpServerWithSpengo.class);

  private static final String SECRET_STR = "secret";
  private static final String HTTP_USER = "HTTP";
  private static final String PREFIX = "hadoop.http.authentication.";
  private static final long TIMEOUT = 20000;

  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal =
      KerberosTestUtils.getServerPrincipal();
  private static String realm = KerberosTestUtils.getRealm();

  private static File testRootDir = new File("target",
      TestHttpServerWithSpengo.class.getName() + "-root");
  private static MiniKdc testMiniKDC;
  private static File secretFile = new File(testRootDir, SECRET_STR);

  @BeforeClass
  public static void setUp() throws Exception {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(
          httpSpnegoKeytabFile, HTTP_USER + "/localhost");
    } catch (Exception e) {
      assertTrue("Couldn't setup MiniKDC", false);
    }
    Writer w = new FileWriter(secretFile);
    w.write("secret");
    w.close();
  }

  @AfterClass
  public static void tearDown() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }
  }

  /**
   * groupA
   *  - userA
   * groupB
   *  - userA, userB
   * groupC
   *  - userC
   * SPNEGO filter has been enabled.
   * userA has the privilege to impersonate users in groupB.
   * userA has admin access to all default servlets, but userB
   * and userC don't have. So "/logs" can only be accessed by userA.
   * @throws Exception
   */
  @Test
  public void testAuthenticationWithProxyUser() throws Exception {

    Configuration spengoConf = getSpengoConf(new Configuration());

    //setup logs dir
    System.setProperty("hadoop.log.dir", testRootDir.getAbsolutePath());

    // Setup user group
    UserGroupInformation.createUserForTesting("userA",
        new String[]{"groupA", "groupB"});
    UserGroupInformation.createUserForTesting("userB",
        new String[]{"groupB"});
    UserGroupInformation.createUserForTesting("userC",
        new String[]{"groupC"});

    // Make userA impersonate users in groupB
    spengoConf.set("hadoop.proxyuser.userA.hosts", "*");
    spengoConf.set("hadoop.proxyuser.userA.groups", "groupB");
    ProxyUsers.refreshSuperUserGroupsConfiguration(spengoConf);

    HttpServer2 httpServer = null;
    try {
      // Create http server to test.
      httpServer = getCommonBuilder()
          .setConf(spengoConf)
          .setACL(new AccessControlList("userA groupA"))
          .build();
      httpServer.start();

      // Get signer to encrypt token
      Signer signer = getSignerToEncrypt();

      // setup auth token for userA
      AuthenticatedURL.Token token = getEncryptedAuthToken(signer, "userA");

      String serverURL = "http://" +
          NetUtils.getHostPortString(httpServer.getConnectorAddress(0)) + "/";

      // The default authenticator is kerberos.
      AuthenticatedURL authUrl = new AuthenticatedURL();

      // userA impersonates userB, it's allowed.
      for (String servlet :
          new String[]{"stacks", "jmx", "conf"}) {
        HttpURLConnection conn = authUrl
            .openConnection(new URL(serverURL + servlet + "?doAs=userB"),
                token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      }

      // userA cannot impersonate userC, it fails.
      for (String servlet :
          new String[]{"stacks", "jmx", "conf"}){
        HttpURLConnection conn = authUrl
            .openConnection(new URL(serverURL + servlet + "?doAs=userC"),
                token);
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            conn.getResponseCode());
      }

      // "/logs" and "/logLevel" require admin authorization,
      // only userA has the access.
      for (String servlet :
          new String[]{"logLevel", "logs"}) {
        HttpURLConnection conn = authUrl
            .openConnection(new URL(serverURL + servlet), token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      }

      // Setup token for userB
      token = getEncryptedAuthToken(signer, "userB");

      // userB cannot access these servlets.
      for (String servlet :
          new String[]{"logLevel", "logs"}) {
        HttpURLConnection conn = authUrl
            .openConnection(new URL(serverURL + servlet), token);
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
            conn.getResponseCode());
      }

    } finally {
      if (httpServer != null) {
        httpServer.stop();
      }
    }
  }


  private AuthenticatedURL.Token getEncryptedAuthToken(Signer signer,
      String user) throws Exception {
    AuthenticationToken token =
        new AuthenticationToken(user, user, "kerberos");
    token.setExpires(System.currentTimeMillis() + TIMEOUT);
    return new AuthenticatedURL.Token(signer.sign(token.toString()));
  }

  private Signer getSignerToEncrypt() throws Exception {
    SignerSecretProvider secretProvider =
        StringSignerSecretProviderCreator.newStringSignerSecretProvider();
    Properties secretProviderProps = new Properties();
    secretProviderProps.setProperty(
        AuthenticationFilter.SIGNATURE_SECRET, SECRET_STR);
    secretProvider.init(secretProviderProps, null, TIMEOUT);
    return new Signer(secretProvider);
  }

  private Configuration getSpengoConf(Configuration conf) {
    conf = new Configuration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    conf.set(PREFIX + "type", "kerberos");
    conf.setBoolean(PREFIX + "simple.anonymous.allowed", false);
    conf.set(PREFIX + "signature.secret.file",
        secretFile.getAbsolutePath());
    conf.set(PREFIX + "kerberos.keytab",
        httpSpnegoKeytabFile.getAbsolutePath());
    conf.set(PREFIX + "kerberos.principal", httpSpnegoPrincipal);
    conf.set(PREFIX + "cookie.domain", realm);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);
    return conf;
  }

  private HttpServer2.Builder getCommonBuilder() throws Exception {
    return new HttpServer2.Builder().setName("test")
        .addEndpoint(new URI("http://localhost:0"))
        .setFindPort(true);
  }
}
