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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.lib.service.security.DummyGroupMapping;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;

public class TestHttpFSServer extends HFSTestCase {

  @Test
  @TestDir
  @TestJetty
  public void server() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration hoopConf = new Configuration(false);
    HttpFSServerWebApp server = new HttpFSServerWebApp(dir, dir, dir, dir, hoopConf);
    server.init();
    server.destroy();
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

    String fsDefaultName = TestHdfsHelper.getHdfsConf().get("fs.default.name");
    Configuration conf = new Configuration(false);
    conf.set("httpfs.hadoop.conf:fs.default.name", fsDefaultName);
    conf.set("httpfs.groups." + CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, DummyGroupMapping.class.getName());
    conf.set("httpfs.authentication.signature.secret.file", secretFile.getAbsolutePath());
    File hoopSite = new File(new File(homeDir, "conf"), "httpfs-site.xml");
    OutputStream os = new FileOutputStream(hoopSite);
    conf.writeXml(os);
    os.close();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("webapp");
    WebAppContext context = new WebAppContext(url.getPath(), "/webhdfs");
    Server server = TestJettyHelper.getJettyServer();
    server.addHandler(context);
    server.start();
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void instrumentation() throws Exception {
    createHttpFSServer();

    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1?user.name={0}&op=instrumentation", "nobody"));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_UNAUTHORIZED);

    url = new URL(TestJettyHelper.getJettyURL(),
                  MessageFormat.format("/webhdfs/v1?user.name={0}&op=instrumentation", "root"));
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line = reader.readLine();
    reader.close();
    Assert.assertTrue(line.contains("\"counters\":{"));

    url = new URL(TestJettyHelper.getJettyURL(),
                  MessageFormat.format("/webhdfs/v1/foo?user.name={0}&op=instrumentation", "root"));
    conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_BAD_REQUEST);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testHdfsAccess() throws Exception {
    createHttpFSServer();

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
    createHttpFSServer();

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
  public void testPutNoOperation() throws Exception {
    createHttpFSServer();

    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    URL url = new URL(TestJettyHelper.getJettyURL(),
                      MessageFormat.format("/webhdfs/v1/foo?user.name={0}", user));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoInput(true);
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    Assert.assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_BAD_REQUEST);
  }

}
