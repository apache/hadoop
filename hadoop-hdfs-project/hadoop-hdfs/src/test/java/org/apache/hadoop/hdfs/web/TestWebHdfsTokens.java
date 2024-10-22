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

package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.SIMPLE;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWebHdfsTokens {
  private static final String PREFIX = "hadoop.http.authentication.";
  private static Configuration conf;
  URI uri = null;

  //secure cluster
  private static MiniKdc kdc = null;
  private static File baseDir;
  private static File keytabFile;
  private static String username = "webhdfs-tokens-test";
  private static String principal;
  private static String keystoresDir;
  private static String sslConfDir;

  @BeforeClass
  public static void setUp() {
    conf = new Configuration();
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
      FileUtil.fullyDelete(baseDir);
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    }
  }

  private static void initEnv(){
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createUserForTesting(
            "LoginUser", new String[]{"supergroup"}));
  }

  private static void initSecureConf(Configuration secureConf)
      throws Exception {

    baseDir = GenericTestUtils.getTestDir(
        TestWebHdfsTokens.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, secureConf);
    UserGroupInformation.setConfiguration(secureConf);
    KerberosName.resetDefaultRealm();
    assertTrue("Expected secureConfiguration to enable security",
        UserGroupInformation.isSecurityEnabled());

    keytabFile = new File(baseDir, username + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    principal = username + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();
    kdc.createPrincipal(keytabFile, username, username + "/" + krbInstance,
        "HTTP/" + krbInstance);

    secureConf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    secureConf.set(PREFIX + "type", "kerberos");
    secureConf.set(PREFIX + "kerberos.keytab", keytab);
    secureConf.set(PREFIX + "kerberos.principal", spnegoPrincipal);
    secureConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, principal);
    secureConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    secureConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, principal);
    secureConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    secureConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        spnegoPrincipal);
    secureConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    secureConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    secureConf.set(DFS_HTTP_POLICY_KEY,
        HttpConfig.Policy.HTTP_AND_HTTPS.name());
    secureConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    secureConf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
    secureConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    secureConf.set(DFS_DATANODE_HTTP_ADDRESS_KEY, "localhost:0");
    secureConf.setBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    secureConf.setBoolean(IGNORE_SECURE_PORTS_FOR_TESTING_KEY, true);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestWebHdfsTokens.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir,
        secureConf, false);

    secureConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    secureConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }



  private WebHdfsFileSystem spyWebhdfsInSecureSetup() throws IOException {
    WebHdfsFileSystem fsOrig = new WebHdfsFileSystem();
    fsOrig.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);
    WebHdfsFileSystem fs = spy(fsOrig);
    return fs;
  }

  @Test(timeout = 5000)
  public void testTokenForNonTokenOp() throws IOException {
    initEnv();
    WebHdfsFileSystem fs = spyWebhdfsInSecureSetup();
    Token<?> token = mock(Token.class);
    doReturn(token).when(fs).getDelegationToken(null);

    // should get/set/renew token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs).getDelegationToken(null);
    verify(fs).setDelegationToken(token);
    reset(fs);

    // should return prior token
    fs.toUrl(GetOpParam.Op.OPEN, null);
    verify(fs).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(token);
  }

  @Test(timeout = 5000)
  public void testNoTokenForGetToken() throws IOException {
    initEnv();
    checkNoTokenForOperation(GetOpParam.Op.GETDELEGATIONTOKEN);
  }

  @Test(timeout = 5000)
  public void testNoTokenForRenewToken() throws IOException {
    initEnv();
    checkNoTokenForOperation(PutOpParam.Op.RENEWDELEGATIONTOKEN);
  }

  @Test(timeout = 5000)
  public void testNoTokenForCancelToken() throws IOException {
    initEnv();
    checkNoTokenForOperation(PutOpParam.Op.CANCELDELEGATIONTOKEN);
  }

  private void checkNoTokenForOperation(HttpOpParam.Op op) throws IOException {
    WebHdfsFileSystem fs = spyWebhdfsInSecureSetup();
    doReturn(null).when(fs).getDelegationToken(null);
    fs.initialize(URI.create("webhdfs://127.0.0.1:0"), conf);

    // do not get a token!
    fs.toUrl(op, null);
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).getDelegationToken(null);
    verify(fs, never()).setDelegationToken(any());
  }

  @Test(timeout = 10000)
  public void testGetOpRequireAuth() {
    for (HttpOpParam.Op op : GetOpParam.Op.values()) {
      boolean expect = (op == GetOpParam.Op.GETDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth());
    }
  }

  @Test(timeout = 10000)
  public void testPutOpRequireAuth() {
    for (HttpOpParam.Op op : PutOpParam.Op.values()) {
      boolean expect = (op == PutOpParam.Op.RENEWDELEGATIONTOKEN || op == PutOpParam.Op.CANCELDELEGATIONTOKEN);
      assertEquals(expect, op.getRequireAuth());
    }
  }

  @Test(timeout = 10000)
  public void testPostOpRequireAuth() {
    for (HttpOpParam.Op op : PostOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }

  @Test(timeout = 10000)
  public void testDeleteOpRequireAuth() {
    for (HttpOpParam.Op op : DeleteOpParam.Op.values()) {
      assertFalse(op.getRequireAuth());
    }
  }
  
  @Test
  public void testLazyTokenFetchForWebhdfs() throws Exception {
    MiniDFSCluster cluster = null;
    UserGroupInformation ugi = null;
    try {
      final Configuration clusterConf = new HdfsConfiguration(conf);
      initSecureConf(clusterConf);

      cluster = new MiniDFSCluster.Builder(clusterConf).numDataNodes(1).build();
      cluster.waitActive();

      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principal, keytabFile.getAbsolutePath());

      //test with swebhdfs
      uri = DFSUtil.createUri(
          "swebhdfs", cluster.getNameNode().getHttpsAddress());
      validateLazyTokenFetch(ugi, clusterConf);

      //test with webhdfs
      uri = DFSUtil.createUri(
          "webhdfs", cluster.getNameNode().getHttpAddress());
      validateLazyTokenFetch(ugi, clusterConf);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }

      // Reset UGI so that other tests are not affected.
      UserGroupInformation.reset();
      UserGroupInformation.setConfiguration(new Configuration());
    }
  }


  @Test
  public void testSetTokenServiceAndKind() throws Exception {
    initEnv();
    MiniDFSCluster cluster = null;

    try {
      final Configuration clusterConf = new HdfsConfiguration(conf);
      SecurityUtil.setAuthenticationMethod(SIMPLE, clusterConf);
      clusterConf.setBoolean(DFSConfigKeys
              .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

      // trick the NN into thinking s[ecurity is enabled w/o it trying
      // to login from a keytab
      UserGroupInformation.setConfiguration(clusterConf);
      cluster = new MiniDFSCluster.Builder(clusterConf).numDataNodes(0).build();
      cluster.waitActive();
      SecurityUtil.setAuthenticationMethod(KERBEROS, clusterConf);
      final WebHdfsFileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem
              (clusterConf, "webhdfs");
      Whitebox.setInternalState(fs, "canRefreshDelegationToken", true);

      URLConnectionFactory factory = new URLConnectionFactory(new ConnectionConfigurator() {
        @Override
        public HttpURLConnection configure(HttpURLConnection conn)
                throws IOException {
          return conn;
        }
      }) {
        @Override
        public URLConnection openConnection(URL url) throws IOException {
          return super.openConnection(new URL(url + "&service=foo&kind=bar"));
        }
      };
      Whitebox.setInternalState(fs, "connectionFactory", factory);
      Token<?> token1 = fs.getDelegationToken();
      Assert.assertEquals(new Text("bar"), token1.getKind());

      final HttpOpParam.Op op = GetOpParam.Op.GETDELEGATIONTOKEN;
      Token<DelegationTokenIdentifier> token2 =
          fs.new FsPathResponseRunner<Token<DelegationTokenIdentifier>>(
              op, null, new RenewerParam(null)) {
            @Override
            Token<DelegationTokenIdentifier> decodeResponse(Map<?, ?> json)
                throws IOException {
              return WebHdfsTestUtil.convertJsonToDelegationToken(json);
            }
          }.run();

      Assert.assertEquals(new Text("bar"), token2.getKind());
      Assert.assertEquals(new Text("foo"), token2.getService());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void validateLazyTokenFetch(UserGroupInformation ugi,
      final Configuration clusterConf) throws Exception {

    String testUser = ugi.getUserName();

    WebHdfsFileSystem fs = ugi.doAs(
        new PrivilegedExceptionAction<WebHdfsFileSystem>() {
        @Override
        public WebHdfsFileSystem run() throws IOException {
          return spy((WebHdfsFileSystem) FileSystem.newInstance(uri,
              clusterConf));
        }
      });

    // verify token ops don't get a token
    Assert.assertNull(fs.getRenewToken());
    Token<?> token = fs.getDelegationToken(null);
    fs.renewDelegationToken(token);
    fs.cancelDelegationToken(token);
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).setDelegationToken(any());
    Assert.assertNull(fs.getRenewToken());
    reset(fs);

    // verify first non-token op gets a token
    final Path p = new Path("/f");
    fs.create(p, (short)1).close();
    verify(fs, times(1)).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, times(1)).getDelegationToken(any());
    verify(fs, times(1)).setDelegationToken(any());
    token = fs.getRenewToken();
    Assert.assertNotNull(token);      
    Assert.assertEquals(testUser, getTokenOwner(token));
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    reset(fs);

    // verify prior token is reused
    fs.getFileStatus(p);
    verify(fs, times(1)).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    Token<?> token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify renew of expired token fails w/o getting a new token
    token = fs.getRenewToken();
    fs.cancelDelegationToken(token);
    try {
      fs.renewDelegationToken(token);
      Assert.fail("should have failed");
    } catch (InvalidToken it) {
    } catch (Exception ex) {
      Assert.fail("wrong exception:"+ex);
    }
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify cancel of expired token fails w/o getting a new token
    try {
      fs.cancelDelegationToken(token);
      Assert.fail("should have failed");
    } catch (InvalidToken it) {
    } catch (Exception ex) {
      Assert.fail("wrong exception:"+ex);
    }
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify an expired token is replaced with a new token
    InputStream is = fs.open(p);
    is.read();
    is.close();
    verify(fs, times(3)).getDelegationToken(); // first bad, then good
    verify(fs, times(1)).replaceExpiredDelegationToken();
    verify(fs, times(1)).getDelegationToken(null);
    verify(fs, times(1)).setDelegationToken(any());
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertNotSame(token, token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertEquals(testUser, getTokenOwner(token2));
    reset(fs);

    // verify with open because it's a little different in how it
    // opens connections
    fs.cancelDelegationToken(fs.getRenewToken());
    is = fs.open(p);
    is.read();
    is.close();
    verify(fs, times(3)).getDelegationToken(); // first bad, then good
    verify(fs, times(1)).replaceExpiredDelegationToken();
    verify(fs, times(1)).getDelegationToken(null);
    verify(fs, times(1)).setDelegationToken(any());
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertNotSame(token, token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertEquals(testUser, getTokenOwner(token2));
    reset(fs);

    // verify fs close cancels the token
    fs.close();
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    verify(fs, times(1)).cancelDelegationToken(eq(token2));

    // add a token to ugi for a new fs, verify it uses that token
    token = fs.getDelegationToken(null);
    ugi.addToken(token);
    fs = ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws IOException {
        return spy((WebHdfsFileSystem) FileSystem.newInstance(uri, clusterConf));
      }
    });
    Assert.assertNull(fs.getRenewToken());
    fs.getFileStatus(new Path("/"));
    verify(fs, times(1)).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, times(1)).setDelegationToken(eq(token));
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify it reuses the prior ugi token
    fs.getFileStatus(new Path("/"));
    verify(fs, times(1)).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify an expired ugi token is not replaced with a new token if the
    // ugi does not contain a new token
    fs.cancelDelegationToken(token);
    for (int i=0; i<2; i++) {
      try {
        fs.getFileStatus(new Path("/"));
        Assert.fail("didn't fail");
      } catch (InvalidToken it) {
      } catch (Exception ex) {
        Assert.fail("wrong exception:"+ex);
      }
      verify(fs, times(1)).getDelegationToken();
      verify(fs, times(1)).replaceExpiredDelegationToken();
      verify(fs, never()).getDelegationToken(anyString());
      verify(fs, never()).setDelegationToken(any());
      token2 = fs.getRenewToken();
      Assert.assertNotNull(token2);
      Assert.assertEquals(fs.getTokenKind(), token.getKind());
      Assert.assertSame(token, token2);
      reset(fs);
    }
    
    // verify an expired ugi token is replaced with a new token
    // if one is available in UGI
    token = fs.getDelegationToken(null);
    ugi.addToken(token);
    reset(fs);
    fs.getFileStatus(new Path("/"));
    verify(fs, times(2)).getDelegationToken(); // first bad, then good
    verify(fs, times(1)).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, times(1)).setDelegationToken(eq(token));
    token2 = fs.getRenewToken();
    Assert.assertNotNull(token2);
    Assert.assertEquals(fs.getTokenKind(), token.getKind());
    Assert.assertSame(token, token2);
    reset(fs);

    // verify fs close does NOT cancel the ugi token
    fs.close();
    verify(fs, never()).getDelegationToken();
    verify(fs, never()).replaceExpiredDelegationToken();
    verify(fs, never()).getDelegationToken(anyString());
    verify(fs, never()).setDelegationToken(any());
    verify(fs, never()).cancelDelegationToken(any(Token.class));
  } 
  
  private String getTokenOwner(Token<?> token) throws IOException {
    // webhdfs doesn't register properly with the class loader
    @SuppressWarnings({ "rawtypes", "unchecked" })
    Token<?> clone = new Token(token);
    clone.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    return clone.decodeIdentifier().getUser().getUserName();
  }
}
