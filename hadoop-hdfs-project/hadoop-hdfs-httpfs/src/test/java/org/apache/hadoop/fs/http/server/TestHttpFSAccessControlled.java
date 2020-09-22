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
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.Test;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;

/**
 * This test class ensures that everything works as expected when
 * support with the access controlled HTTPFS file system.
 */
public class TestHttpFSAccessControlled extends HTestCase {

  private MiniDFSCluster miniDfs;
  private Configuration nnConf;

  /**
   * Fire up our own hand-rolled MiniDFSCluster.  We do this here instead
   * of relying on TestHdfsHelper because we don't want to turn on ACL
   * support.
   *
   * @throws Exception
   */
  private void startMiniDFS() throws Exception {

    File testDirRoot = TestDirHelper.getTestDir();

    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop." +
                      "log.dir",
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
    server.setHandler(context);
    server.start();
  }

  /**
   * Talks to the http interface to get the json output of a *STATUS command
   * on the given file.
   *
   * @param filename The file to query.
   * @param message Failure message
   * @param command Command to test
   * @param expectOK Is this operation expected to succeed?
   * @throws Exception
   */
  private void getCmd(String filename, String message, String command, boolean expectOK)
          throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    String outMsg = message + " (" + command + ")";
    // Remove leading / from filename
    if ( filename.charAt(0) == '/' ) {
      filename = filename.substring(1);
    }
    String pathOps = MessageFormat.format(
            "/webhdfs/v1/{0}?user.name={1}&op={2}",
            filename, user, command);
    URL url = new URL(TestJettyHelper.getJettyURL(), pathOps);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.connect();
    int resp = conn.getResponseCode();
    if ( expectOK ) {
      Assert.assertEquals( outMsg, HttpURLConnection.HTTP_OK, resp);
    } else {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_FORBIDDEN, resp);
    }
  }

  /**
   * General-purpose http PUT command to the httpfs server.
   * @param filename The file to operate upon
   * @param message Failure message
   * @param command The command to perform (SETPERMISSION, etc)
   * @param params Parameters to command
   * @param expectOK Is this operation expected to succeed?
   */
  private void putCmd(String filename, String message, String command,
                      String params, boolean expectOK) throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    String outMsg = message + " (" + command + ")";
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
    if ( expectOK ) {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_OK, resp);
    } else {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_FORBIDDEN, resp);
    }
  }

  /**
   * General-purpose http PUT command to the httpfs server.
   * @param filename The file to operate upon
   * @param message Failure message
   * @param command The command to perform (SETPERMISSION, etc)
   * @param params Parameters to command
   * @param expectOK Is this operation expected to succeed?
   */
  private void deleteCmd(String filename, String message, String command,
                      String params, boolean expectOK) throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    String outMsg = message + " (" + command + ")";
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
    conn.setRequestMethod("DELETE");
    conn.connect();
    int resp = conn.getResponseCode();
    if ( expectOK ) {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_OK, resp);
    } else {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_FORBIDDEN, resp);
    }
  }

  /**
   * General-purpose http POST command to the httpfs server.
   * @param filename The file to operate upon
   * @param message Failure message
   * @param command The command to perform (UNSETSTORAGEPOLICY, etc)
   * @param params Parameters to command"
   * @param expectOK Is this operation expected to succeed?
   */
  private void postCmd(String filename, String message, String command,
                      String params, boolean expectOK) throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    String outMsg = message + " (" + command + ")";
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
    conn.setRequestMethod("POST");
    conn.connect();
    int resp = conn.getResponseCode();
    if ( expectOK ) {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_OK, resp);
    } else {
      Assert.assertEquals(outMsg, HttpURLConnection.HTTP_FORBIDDEN, resp);
    }
  }

  /**
   * Ensure that
   * <ol>
   *   <li>GETFILESTATUS (GET) and LISTSTATUS (GET) work in all modes</li>
   *   <li>GETXATTRS (GET) works in read-write and read-only but write-only throws an exception</li>
   *   <li>SETPERMISSION (PUT) works in read-write and write only but read-only throws an exception</li>
   *   <li>SETPERMISSION (POST) works in read-write and write only but read-only throws an exception</li>
   *   <li>DELETE (DELETE)  works in read-write and write only but read-only throws an exception</li>
   * </ol>
   *
   * @throws Exception
   */
  @Test
  @TestDir
  @TestJetty
  public void testAcessControlledFS() throws Exception {
    final String testRwMsg = "Test read-write ";
    final String testRoMsg = "Test read-only ";
    final String testWoMsg = "Test write-only ";
    final String defUser1 = "default:user:glarch:r-x";
    final String dir = "/testAccess";
    final String pathRW = dir + "/foo-rw";
    final String pathWO = dir + "/foo-wo";
    final String pathRO = dir + "/foo-ro";
    final String setPermSpec = "744";
    final String snapshopSpec = "snapshotname=test-snap";
    startMiniDFS();
    createHttpFSServer();

    FileSystem fs = FileSystem.get(nnConf);
    fs.mkdirs(new Path(dir));
    OutputStream os = fs.create(new Path(pathRW));
    os.write(1);
    os.close();

    os = fs.create(new Path(pathWO));
    os.write(1);
    os.close();

    os = fs.create(new Path(pathRO));
    os.write(1);
    os.close();

    Configuration conf = HttpFSServerWebApp.get().getConfig();

    /* test Read-Write Mode */
    conf.setStrings("httpfs.access.mode", "read-write");
    getCmd(pathRW, testRwMsg + "GET", "GETFILESTATUS", true);
    getCmd(pathRW, testRwMsg + "GET", "LISTSTATUS", true);
    getCmd(pathRW, testRwMsg + "GET", "GETXATTRS", true);
    putCmd(pathRW, testRwMsg + "PUT", "SETPERMISSION", setPermSpec, true);
    postCmd(pathRW, testRwMsg + "POST", "UNSETSTORAGEPOLICY", null, true);
    deleteCmd(pathRW, testRwMsg + "DELETE", "DELETE", null, true);

    /* test Write-Only Mode */
    conf.setStrings("httpfs.access.mode", "write-only");
    getCmd(pathWO, testWoMsg + "GET", "GETFILESTATUS", true);
    getCmd(pathWO, testWoMsg + "GET", "LISTSTATUS", true);
    getCmd(pathWO, testWoMsg + "GET", "GETXATTRS", false);
    putCmd(pathWO, testWoMsg + "PUT", "SETPERMISSION", setPermSpec, true);
    postCmd(pathWO, testWoMsg + "POST", "UNSETSTORAGEPOLICY", null, true);
    deleteCmd(pathWO, testWoMsg + "DELETE", "DELETE", null, true);

    /* test Read-Only Mode */
    conf.setStrings("httpfs.access.mode", "read-only");
    getCmd(pathRO, testRoMsg + "GET",  "GETFILESTATUS", true);
    getCmd(pathRO, testRoMsg + "GET",  "LISTSTATUS", true);
    getCmd(pathRO, testRoMsg + "GET",  "GETXATTRS", true);
    putCmd(pathRO, testRoMsg + "PUT",  "SETPERMISSION", setPermSpec, false);
    postCmd(pathRO, testRoMsg + "POST", "UNSETSTORAGEPOLICY", null, false);
    deleteCmd(pathRO, testRoMsg + "DELETE", "DELETE", null, false);

    conf.setStrings("httpfs.access.mode", "read-write");

    miniDfs.shutdown();
  }
}
