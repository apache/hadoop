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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authentication.util.StringSignerSecretProviderCreator;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.ietf.jgss.GSSException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;

import static org.junit.Assert.assertTrue;

/**
 * This class is tested for http server with SPENGO authentication.
 */
public class TestHttpServerWithSpengo {

  static final Logger LOG =
      LoggerFactory.getLogger(TestHttpServerWithSpengo.class);

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

  private static UserGroupInformation authUgi;

  @BeforeClass
  public static void setUp() throws Exception {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(
          httpSpnegoKeytabFile, HTTP_USER + "/localhost", "keytab-user");
    } catch (Exception e) {
      assertTrue("Couldn't setup MiniKDC", false);
    }

    System.setProperty("sun.security.krb5.debug", "true");
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    authUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        "keytab-user", httpSpnegoKeytabFile.toString());
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

      // userA cannot impersonate userC, but for /stacks, /jmx and /conf,
      // they doesn't require users to authorize by default, so they
      // can be accessed.
      for (String servlet :
          new String[]{"stacks", "jmx", "conf"}){
        HttpURLConnection conn = authUrl
            .openConnection(new URL(serverURL + servlet + "?doAs=userC"),
                token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK,
            conn.getResponseCode());
      }

      // "/logs" and "/logLevel" require admin authorization,
      // only userA has the access.
      for (String servlet :
          new String[]{"logLevel", "logs"}) {
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

  @Test
  public void testSessionCookie() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    conf.set(PREFIX + "type", "kerberos");
    conf.setBoolean(PREFIX + "simple.anonymous.allowed", false);
    conf.set(PREFIX + "signer.secret.provider",
        TestSignerSecretProvider.class.getName());

    conf.set(PREFIX + "kerberos.keytab",
        httpSpnegoKeytabFile.getAbsolutePath());
    conf.set(PREFIX + "kerberos.principal", httpSpnegoPrincipal);
    conf.set(PREFIX + "cookie.domain", realm);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        true);

    //setup logs dir
    System.setProperty("hadoop.log.dir", testRootDir.getAbsolutePath());

    HttpServer2 httpServer = null;
    // Create http server to test.
    httpServer = getCommonBuilder()
        .setConf(conf)
        .build();
    httpServer.start();

    // Get signer to encrypt token
    final Signer signer = new Signer(new TestSignerSecretProvider());
    final AuthenticatedURL authUrl = new AuthenticatedURL();

    final URL url = new URL("http://" + NetUtils.getHostPortString(
        httpServer.getConnectorAddress(0)) + "/conf");

    // this illustrates an inconsistency with AuthenticatedURL.  the
    // authenticator is only called when the token is not set.  if the
    // authenticator fails then it must throw an AuthenticationException to
    // the caller, yet the caller may see 401 for subsequent requests
    // that require re-authentication like token expiration.
    final UserGroupInformation simpleUgi =
        UserGroupInformation.createRemoteUser("simple-user");

    authUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        TestSignerSecretProvider.rollSecret();
        HttpURLConnection conn = null;
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();

        // initial request should trigger authentication and set the token.
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        String cookie = token.toString();

        // token should not change.
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        Assert.assertEquals(cookie, token.toString());

        // roll secret to invalidate token.
        TestSignerSecretProvider.rollSecret();
        conn = authUrl.openConnection(url, token);
        // this may or may not happen.  under normal circumstances the
        // jdk will silently renegotiate and the client never sees a 401.
        // however in some cases the jdk will give up doing spnego.  since
        // the token is already set, the authenticator isn't invoked (which
        // would do the spnego if the jdk doesn't), which causes the client
        // to see a 401.
        if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
          // if this happens, the token should be cleared which means the
          // next request should succeed and receive a new token.
          Assert.assertFalse(token.isSet());
          conn = authUrl.openConnection(url, token);
        }

        // token should change.
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        Assert.assertNotEquals(cookie, token.toString());
        cookie = token.toString();

        // token should not change.
        for (int i=0; i < 3; i++) {
          conn = authUrl.openConnection(url, token);
          Assert.assertEquals("attempt"+i,
              HttpURLConnection.HTTP_OK, conn.getResponseCode());
          Assert.assertTrue(token.isSet());
          Assert.assertEquals(cookie, token.toString());
        }

        // blow out the kerberos creds test only auth token is used.
        Subject s = Subject.getSubject(AccessController.getContext());
        Set<Object> oldCreds = new HashSet<>(s.getPrivateCredentials());
        s.getPrivateCredentials().clear();

        // token should not change.
        for (int i=0; i < 3; i++) {
          try {
            conn = authUrl.openConnection(url, token);
            Assert.assertEquals("attempt"+i,
                HttpURLConnection.HTTP_OK, conn.getResponseCode());
          } catch (AuthenticationException ae) {
            Assert.fail("attempt"+i+" "+ae);
          }
          Assert.assertTrue(token.isSet());
          Assert.assertEquals(cookie, token.toString());
        }

        // invalidate token.  connections should fail now and token should be
        // unset.
        TestSignerSecretProvider.rollSecret();
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(
            HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        Assert.assertFalse(token.isSet());
        Assert.assertEquals("", token.toString());

        // restore the kerberos creds, should work again.
        s.getPrivateCredentials().addAll(oldCreds);
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(
            HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        cookie = token.toString();

        // token should not change.
        for (int i=0; i < 3; i++) {
          conn = authUrl.openConnection(url, token);
          Assert.assertEquals("attempt"+i,
              HttpURLConnection.HTTP_OK, conn.getResponseCode());
          Assert.assertTrue(token.isSet());
          Assert.assertEquals(cookie, token.toString());
        }
        return null;
      }
    });

    simpleUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        TestSignerSecretProvider.rollSecret();
        AuthenticatedURL authUrl = new AuthenticatedURL();
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        HttpURLConnection conn = null;

        // initial connect with unset token will trigger authenticator which
        // should fail since we have no creds and leave token unset.
        try {
          authUrl.openConnection(url, token);
          Assert.fail("should fail with no credentials");
        } catch (AuthenticationException ae) {
          Assert.assertNotNull(ae.getCause());
          Assert.assertEquals(GSSException.class, ae.getCause().getClass());
          GSSException gsse = (GSSException)ae.getCause();
          Assert.assertEquals(GSSException.NO_CRED, gsse.getMajor());
        } catch (Throwable t) {
          Assert.fail("Unexpected exception" + t);
        }
        Assert.assertFalse(token.isSet());

        // create a valid token and save its value.
        token = getEncryptedAuthToken(signer, "valid");
        String cookie = token.toString();

        // server should accept token.  after the request the token should
        // be set to the same value (ie. server didn't reissue cookie)
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        Assert.assertEquals(cookie, token.toString());

        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        Assert.assertTrue(token.isSet());
        Assert.assertEquals(cookie, token.toString());

        // change the secret to effectively invalidate the cookie.  see above
        // regarding inconsistency.  the authenticator has no way to know the
        // token is bad, so the client will encounter a 401 instead of
        // AuthenticationException.
        TestSignerSecretProvider.rollSecret();
        conn = authUrl.openConnection(url, token);
        Assert.assertEquals(
            HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
        Assert.assertFalse(token.isSet());
        Assert.assertEquals("", token.toString());
        return null;
      }
    });
  }

  public static class TestSignerSecretProvider extends SignerSecretProvider {
    static int n = 0;
    static byte[] secret;

    static void rollSecret() {
      secret = ("secret[" + (n++) + "]").getBytes();
    }

    public TestSignerSecretProvider() {
    }

    @Override
    public void init(Properties config, ServletContext servletContext,
            long tokenValidity) throws Exception {
      rollSecret();
    }

    @Override
    public byte[] getCurrentSecret() {
      return secret;
    }

    @Override
    public byte[][] getAllSecrets() {
      return new byte[][]{secret};
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
