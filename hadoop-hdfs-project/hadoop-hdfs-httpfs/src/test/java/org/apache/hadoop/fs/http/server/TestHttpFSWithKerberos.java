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
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.DelegationTokenRenewer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.KerberosTestUtils;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public class TestHttpFSWithKerberos extends HFSTestCase {

  @After
  public void resetUGI() {
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
  }

  private void createHttpFSServer() throws Exception {
    File homeDir = TestDirHelper.getTestDir();
    Assert.assertTrue(new File(homeDir, "conf").mkdir());
    Assert.assertTrue(new File(homeDir, "log").mkdir());
    Assert.assertTrue(new File(homeDir, "temp").mkdir());
    HttpFSServerWebApp.setHomeDirForCurrentThread(homeDir.getAbsolutePath());

    File secretFile = new File(new File(homeDir, "conf"), "secret");
    Writer w = new FileWriter(secretFile);
    w.write("secret");
    w.close();

    //HDFS configuration
    File hadoopConfDir = new File(new File(homeDir, "conf"), "hadoop-conf");
    hadoopConfDir.mkdirs();
    String fsDefaultName = TestHdfsHelper.getHdfsConf()
      .get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    conf.writeXml(os);
    os.close();

    conf = new Configuration(false);
    conf.set("httpfs.proxyuser.client.hosts", "*");
    conf.set("httpfs.proxyuser.client.groups", "*");

    conf.set("httpfs.authentication.type", "kerberos");

    conf.set("httpfs.authentication.signature.secret.file",
             secretFile.getAbsolutePath());
    File httpfsSite = new File(new File(homeDir, "conf"), "httpfs-site.xml");
    os = new FileOutputStream(httpfsSite);
    conf.writeXml(os);
    os.close();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("webapp");
    WebAppContext context = new WebAppContext(url.getPath(), "/webhdfs");
    Server server = TestJettyHelper.getJettyServer();
    server.setHandler(context);
    server.start();
    HttpFSServerWebApp.get().setAuthority(TestJettyHelper.getAuthority());
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testValidHttpFSAccess() throws Exception {
    createHttpFSServer();

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        URL url = new URL(TestJettyHelper.getJettyURL(),
                          "/webhdfs/v1/?op=GETHOMEDIRECTORY");
        AuthenticatedURL aUrl = new AuthenticatedURL();
        AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
        HttpURLConnection conn = aUrl.openConnection(url, aToken);
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        return null;
      }
    });
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testInvalidadHttpFSAccess() throws Exception {
    createHttpFSServer();

    URL url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(),
                        HttpURLConnection.HTTP_UNAUTHORIZED);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDelegationTokenHttpFSAccess() throws Exception {
    createHttpFSServer();

    KerberosTestUtils.doAsClient(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        //get delegation token doing SPNEGO authentication
        URL url = new URL(TestJettyHelper.getJettyURL(),
                          "/webhdfs/v1/?op=GETDELEGATIONTOKEN");
        AuthenticatedURL aUrl = new AuthenticatedURL();
        AuthenticatedURL.Token aToken = new AuthenticatedURL.Token();
        HttpURLConnection conn = aUrl.openConnection(url, aToken);
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
        JSONObject json = (JSONObject) new JSONParser()
          .parse(new InputStreamReader(conn.getInputStream()));
        json =
          (JSONObject) json
            .get(DelegationTokenAuthenticator.DELEGATION_TOKEN_JSON);
        String tokenStr = (String) json
          .get(DelegationTokenAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON);

        //access httpfs using the delegation token
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //try to renew the delegation token without SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        Assert.assertEquals(conn.getResponseCode(),
                            HttpURLConnection.HTTP_UNAUTHORIZED);

        //renew the delegation token with SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
        conn = aUrl.openConnection(url, aToken);
        conn.setRequestMethod("PUT");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //cancel delegation token, no need for SPNEGO credentials
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);

        //try to access httpfs with the canceled delegation token
        url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" +
                      tokenStr);
        conn = (HttpURLConnection) url.openConnection();
        Assert.assertEquals(conn.getResponseCode(),
                            HttpURLConnection.HTTP_UNAUTHORIZED);
        return null;
      }
    });
  }

  @SuppressWarnings("deprecation")
  private void testDelegationTokenWithFS(Class fileSystemClass)
    throws Exception {
    createHttpFSServer();
    Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", fileSystemClass.getName());
    conf.set("fs.hdfs.impl.disable.cache", "true");
    URI uri = new URI( "webhdfs://" +
                       TestJettyHelper.getJettyURL().toURI().getAuthority());
    FileSystem fs = FileSystem.get(uri, conf);
    Token<?> tokens[] = fs.addDelegationTokens("foo", null);
    fs.close();
    Assert.assertEquals(1, tokens.length);
    fs = FileSystem.get(uri, conf);
    ((DelegationTokenRenewer.Renewable) fs).setDelegationToken(tokens[0]);
    fs.listStatus(new Path("/"));
    fs.close();
  }

  private void testDelegationTokenWithinDoAs(
    final Class fileSystemClass, boolean proxyUser) throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab("client",
                                             "/Users/tucu/tucu.keytab");
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    if (proxyUser) {
      ugi = UserGroupInformation.createProxyUser("foo", ugi);
    }
    conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    ugi.doAs(
      new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          testDelegationTokenWithFS(fileSystemClass);
          return null;
        }
      });
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDelegationTokenWithHttpFSFileSystem() throws Exception {
    testDelegationTokenWithinDoAs(HttpFSFileSystem.class, false);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDelegationTokenWithWebhdfsFileSystem() throws Exception {
    testDelegationTokenWithinDoAs(WebHdfsFileSystem.class, false);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDelegationTokenWithHttpFSFileSystemProxyUser()
    throws Exception {
    testDelegationTokenWithinDoAs(HttpFSFileSystem.class, true);
  }

  // TODO: WebHdfsFilesystem does work with ProxyUser HDFS-3509
  //    @Test
  //    @TestDir
  //    @TestJetty
  //    @TestHdfs
  //    public void testDelegationTokenWithWebhdfsFileSystemProxyUser()
  //      throws Exception {
  //      testDelegationTokenWithinDoAs(WebHdfsFileSystem.class, true);
  //    }

}
