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

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.client.HttpFSKerberosAuthenticator;
import org.apache.hadoop.lib.server.Service;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

public class TestHttpFSServer extends HFSTestCase {

  @Test
  @TestDir
  @TestJetty
  public void server() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();

    Configuration httpfsConf = new Configuration(false);
    HttpFSServerWebApp server = new HttpFSServerWebApp(dir, dir, dir, dir, httpfsConf);
    server.init();
    server.destroy();
  }

  public static class MockGroups implements Service,Groups {

    @Override
    public void init(org.apache.hadoop.lib.server.Server server) throws ServiceException {
    }

    @Override
    public void postInit() throws ServiceException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class[] getServiceDependencies() {
      return new Class[0];
    }

    @Override
    public Class getInterface() {
      return Groups.class;
    }

    @Override
    public void serverStatusChange(org.apache.hadoop.lib.server.Server.Status oldStatus,
                                   org.apache.hadoop.lib.server.Server.Status newStatus) throws ServiceException {
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      return Arrays.asList(HadoopUsersConfTestHelper.getHadoopUserGroups(user));
    }

  }

  private void createHttpFSServer(boolean addDelegationTokenAuthHandler)
    throws Exception {
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
    String fsDefaultName = TestHdfsHelper.getHdfsConf().get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    conf.writeXml(os);
    os.close();

    //HTTPFS configuration
    conf = new Configuration(false);
    if (addDelegationTokenAuthHandler) {
     conf.set("httpfs.authentication.type",
              HttpFSKerberosAuthenticationHandlerForTesting.class.getName());
    }
    conf.set("httpfs.services.ext", MockGroups.class.getName());
    conf.set("httpfs.admin.group", HadoopUsersConfTestHelper.
      getHadoopUserGroups(HadoopUsersConfTestHelper.getHadoopUsers()[0])[0]);
    conf.set("httpfs.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".groups",
             HadoopUsersConfTestHelper.getHadoopProxyUserGroups());
    conf.set("httpfs.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".hosts",
             HadoopUsersConfTestHelper.getHadoopProxyUserHosts());
    conf.set("httpfs.authentication.signature.secret.file", secretFile.getAbsolutePath());
    conf.set("httpfs.hadoop.config.dir", hadoopConfDir.toString());
    File httpfsSite = new File(new File(homeDir, "conf"), "httpfs-site.xml");
    os = new FileOutputStream(httpfsSite);
    conf.writeXml(os);
    os.close();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("webapp");
    WebAppContext context = new WebAppContext(url.getPath(), "/webhdfs");
    Server server = TestJettyHelper.getJettyServer();
    server.addHandler(context);
    server.start();
    if (addDelegationTokenAuthHandler) {
      HttpFSServerWebApp.get().setAuthority(TestJettyHelper.getAuthority());
    }
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void instrumentation() throws Exception {
    createHttpFSServer(false);

    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1?user.name={0}&op=instrumentation", "nobody"));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_UNAUTHORIZED);

    url = new URL(TestJettyHelper.getJettyURL(),
                  MessageFormat.format("/webhdfs/v1?user.name={0}&op=instrumentation",
                                       HadoopUsersConfTestHelper.getHadoopUsers()[0]));
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line = reader.readLine();
    reader.close();
    Assert.assertTrue(line.contains("\"counters\":{"));

    url = new URL(TestJettyHelper.getJettyURL(),
                  MessageFormat.format("/webhdfs/v1/foo?user.name={0}&op=instrumentation",
                                       HadoopUsersConfTestHelper.getHadoopUsers()[0]));
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_BAD_REQUEST);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testHdfsAccess() throws Exception {
    createHttpFSServer(false);

    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1/?user.name={0}&op=liststatus", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    reader.readLine();
    reader.close();
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testGlobFilter() throws Exception {
    createHttpFSServer(false);

    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path("/tmp"));
    fs.create(new Path("/tmp/foo.txt")).close();

    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1/tmp?user.name={0}&op=liststatus&filter=f*", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    reader.readLine();
    reader.close();
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testOpenOffsetLength() throws Exception {
    createHttpFSServer(false);

    byte[] array = new byte[]{0, 1, 2, 3};
    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path("/tmp"));
    OutputStream os = fs.create(new Path("/tmp/foo"));
    os.write(array);
    os.close();

    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1/tmp/foo?user.name={0}&op=open&offset=1&length=2", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    InputStream is = conn.getInputStream();
    Assert.assertEquals(1, is.read());
    Assert.assertEquals(2, is.read());
    Assert.assertEquals(-1, is.read());
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testPutNoOperation() throws Exception {
    createHttpFSServer(false);

    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1/foo?user.name={0}", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoInput(true);
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_BAD_REQUEST);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDelegationTokenOperations() throws Exception {
    createHttpFSServer(true);

    URL url = new URL(TestJettyHelper.getJettyURL(),
                      "/webhdfs/v1/?op=GETHOMEDIRECTORY");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
                        conn.getResponseCode());


    AuthenticationToken token =
      new AuthenticationToken("u", "p",
        HttpFSKerberosAuthenticationHandlerForTesting.TYPE);
    token.setExpires(System.currentTimeMillis() + 100000000);
    Signer signer = new Signer("secret".getBytes());
    String tokenSigned = signer.sign(token.toString());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=GETHOMEDIRECTORY");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("Cookie",
                            AuthenticatedURL.AUTH_COOKIE  + "=" + tokenSigned);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
                        conn.getResponseCode());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=GETDELEGATIONTOKEN");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("Cookie",
                            AuthenticatedURL.AUTH_COOKIE  + "=" + tokenSigned);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
                        conn.getResponseCode());

    JSONObject json = (JSONObject)
      new JSONParser().parse(new InputStreamReader(conn.getInputStream()));
    json = (JSONObject)
      json.get(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_JSON);
    String tokenStr = (String)
        json.get(HttpFSKerberosAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON);

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" + tokenStr);
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
                        conn.getResponseCode());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED,
                        conn.getResponseCode());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token=" + tokenStr);
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Cookie",
                            AuthenticatedURL.AUTH_COOKIE  + "=" + tokenSigned);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
                        conn.getResponseCode());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token=" + tokenStr);
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
                        conn.getResponseCode());

    url = new URL(TestJettyHelper.getJettyURL(),
                  "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" + tokenStr);
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN,
                        conn.getResponseCode());
  }

}
