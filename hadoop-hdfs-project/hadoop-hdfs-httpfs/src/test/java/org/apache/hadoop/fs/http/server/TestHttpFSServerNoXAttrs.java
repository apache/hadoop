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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;

/**
 * This test class ensures that everything works as expected when XAttr
 * support is turned off HDFS.  This is the default configuration.  The other
 * tests operate with XAttr support turned on.
 */
public class TestHttpFSServerNoXAttrs extends HTestCase {

  private MiniDFSCluster miniDfs;
  private Configuration nnConf;

  /**
   * Fire up our own hand-rolled MiniDFSCluster.  We do this here instead
   * of relying on TestHdfsHelper because we don't want to turn on XAttr
   * support.
   *
   * @throws Exception
   */
  private void startMiniDFS() throws Exception {

    File testDirRoot = TestDirHelper.getTestDir();

    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir",
              new File(testDirRoot, "hadoop-log").getAbsolutePath());
    }
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data",
              new File(testDirRoot, "hadoop-data").getAbsolutePath());
    }

    Configuration conf = HadoopUsersConfTestHelper.getBaseConf();
    HadoopUsersConfTestHelper.addUserConf(conf);
    conf.set("fs.hdfs.impl.disable.cache", "true");
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");

    // Explicitly turn off XAttr support
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, false);

    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    builder.numDataNodes(2);
    miniDfs = builder.build();
    nnConf = miniDfs.getConfiguration(0);
  }

  /**
   * Create an HttpFS Server to talk to the MiniDFSCluster we created.
   * @throws Exception
   */
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

    // HDFS configuration
    File hadoopConfDir = new File(new File(homeDir, "conf"), "hadoop-conf");
    if ( !hadoopConfDir.mkdirs() ) {
      throw new IOException();
    }

    String fsDefaultName =
            nnConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);

    // Explicitly turn off XAttr support
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, false);

    File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    conf.writeXml(os);
    os.close();

    // HTTPFS configuration
    conf = new Configuration(false);
    conf.set("httpfs.hadoop.config.dir", hadoopConfDir.toString());
    conf.set("httpfs.proxyuser." +
                    HadoopUsersConfTestHelper.getHadoopProxyUser() + ".groups",
            HadoopUsersConfTestHelper.getHadoopProxyUserGroups());
    conf.set("httpfs.proxyuser." +
                    HadoopUsersConfTestHelper.getHadoopProxyUser() + ".hosts",
            HadoopUsersConfTestHelper.getHadoopProxyUserHosts());
    conf.set("httpfs.authentication.signature.secret.file",
            secretFile.getAbsolutePath());

    File httpfsSite = new File(new File(homeDir, "conf"), "httpfs-site.xml");
    os = new FileOutputStream(httpfsSite);
    conf.writeXml(os);
    os.close();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("webapp");
    if ( url == null ) {
      throw new IOException();
    }
    WebAppContext context = new WebAppContext(url.getPath(), "/webhdfs");
    Server server = TestJettyHelper.getJettyServer();
    server.addHandler(context);
    server.start();
  }

  /**
   * Talks to the http interface to get the json output of a *STATUS command
   * on the given file.
   *
   * @param filename The file to query.
   * @param command Either GETXATTRS, SETXATTR, or REMOVEXATTR
   * @throws Exception
   */
  private void getStatus(String filename, String command)
          throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    // Remove leading / from filename
    if ( filename.charAt(0) == '/' ) {
      filename = filename.substring(1);
    }
    String pathOps = MessageFormat.format(
            "/webhdfs/v1/{0}?user.name={1}&op={2}",
            filename, user, command);
    URL url = new URL(TestJettyHelper.getJettyURL(), pathOps);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();
    int resp = conn.getResponseCode();
    BufferedReader reader;
    Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, resp);
    reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
    String res = reader.readLine();
    Assert.assertTrue(res.contains("RemoteException"));
    Assert.assertTrue(res.contains("XAttr"));
    Assert.assertTrue(res.contains("rejected"));
  }

  /**
   * General-purpose http PUT command to the httpfs server.
   * @param filename The file to operate upon
   * @param command The command to perform (SETXATTR, etc)
   * @param params Parameters
   */
  private void putCmd(String filename, String command,
                      String params) throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    // Remove leading / from filename
    if ( filename.charAt(0) == '/' ) {
      filename = filename.substring(1);
    }
    String pathOps = MessageFormat.format(
            "/webhdfs/v1/{0}?user.name={1}{2}{3}&op={4}",
            filename, user, (params == null) ? "" : "&",
            (params == null) ? "" : params, command);
    URL url = new URL(TestJettyHelper.getJettyURL(), pathOps);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.connect();
    int resp = conn.getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, resp);
    BufferedReader reader;
    reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
    String err = reader.readLine();
    Assert.assertTrue(err.contains("RemoteException"));
    Assert.assertTrue(err.contains("XAttr"));
    Assert.assertTrue(err.contains("rejected"));
  }
  
  /**
   * Ensure that GETXATTRS, SETXATTR, REMOVEXATTR fail.
   */
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testWithXAttrs() throws Exception {
    final String name1 = "user.a1";
    final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
    final String dir = "/noXAttr";
    final String path = dir + "/file";
    
    startMiniDFS();
    createHttpFSServer();
    
    FileSystem fs = FileSystem.get(nnConf);
    fs.mkdirs(new Path(dir));
    OutputStream os = fs.create(new Path(path));
    os.write(1);
    os.close();
    
    /* GETXATTRS, SETXATTR, REMOVEXATTR fail */
    getStatus(path, "GETXATTRS");
    putCmd(path, "SETXATTR", TestHttpFSServer.setXAttrParam(name1, value1));
    putCmd(path, "REMOVEXATTR", "xattr.name=" + name1);
  }
}