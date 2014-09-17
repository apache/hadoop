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

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;
import org.json.simple.JSONArray;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
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

import com.google.common.collect.Maps;
import java.util.Properties;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.StringSignerSecretProvider;

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
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
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

  /**
   * Talks to the http interface to create a file.
   *
   * @param filename The file to create
   * @param perms The permission field, if any (may be null)
   * @throws Exception
   */
  private void createWithHttp ( String filename, String perms )
          throws Exception {
    String user = HadoopUsersConfTestHelper.getHadoopUsers()[0];
    // Remove leading / from filename
    if ( filename.charAt(0) == '/' ) {
      filename = filename.substring(1);
    }
    String pathOps;
    if ( perms == null ) {
      pathOps = MessageFormat.format(
              "/webhdfs/v1/{0}?user.name={1}&op=CREATE",
              filename, user);
    } else {
      pathOps = MessageFormat.format(
              "/webhdfs/v1/{0}?user.name={1}&permission={2}&op=CREATE",
              filename, user, perms);
    }
    URL url = new URL(TestJettyHelper.getJettyURL(), pathOps);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.addRequestProperty("Content-Type", "application/octet-stream");
    conn.setRequestMethod("PUT");
    conn.connect();
    Assert.assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
  }

  /**
   * Talks to the http interface to get the json output of a *STATUS command
   * on the given file.
   *
   * @param filename The file to query.
   * @param command Either GETFILESTATUS, LISTSTATUS, or ACLSTATUS
   * @return A string containing the JSON output describing the file.
   * @throws Exception
   */
  private String getStatus(String filename, String command)
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
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    BufferedReader reader =
            new BufferedReader(new InputStreamReader(conn.getInputStream()));

    return reader.readLine();
  }

  /**
   * General-purpose http PUT command to the httpfs server.
   * @param filename The file to operate upon
   * @param command The command to perform (SETACL, etc)
   * @param params Parameters, like "aclspec=..."
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
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  /**
   * Given the JSON output from the GETFILESTATUS call, return the
   * 'permission' value.
   *
   * @param statusJson JSON from GETFILESTATUS
   * @return The value of 'permission' in statusJson
   * @throws Exception
   */
  private String getPerms ( String statusJson ) throws Exception {
    JSONParser parser = new JSONParser();
    JSONObject jsonObject = (JSONObject) parser.parse(statusJson);
    JSONObject details = (JSONObject) jsonObject.get("FileStatus");
    return (String) details.get("permission");
  }

  /**
   * Given the JSON output from the GETACLSTATUS call, return the
   * 'entries' value as a List<String>.
   * @param statusJson JSON from GETACLSTATUS
   * @return A List of Strings which are the elements of the ACL entries
   * @throws Exception
   */
  private List<String> getAclEntries ( String statusJson ) throws Exception {
    List<String> entries = new ArrayList<String>();
    JSONParser parser = new JSONParser();
    JSONObject jsonObject = (JSONObject) parser.parse(statusJson);
    JSONObject details = (JSONObject) jsonObject.get("AclStatus");
    JSONArray jsonEntries = (JSONArray) details.get("entries");
    if ( jsonEntries != null ) {
      for (Object e : jsonEntries) {
        entries.add(e.toString());
      }
    }
    return entries;
  }
  
  /**
   * Parse xAttrs from JSON result of GETXATTRS call, return xAttrs Map.
   * @param statusJson JSON from GETXATTRS
   * @return Map<String, byte[]> xAttrs Map
   * @throws Exception
   */
  private Map<String, byte[]> getXAttrs(String statusJson) throws Exception {
    Map<String, byte[]> xAttrs = Maps.newHashMap();
    JSONParser parser = new JSONParser();
    JSONObject jsonObject = (JSONObject) parser.parse(statusJson);
    JSONArray jsonXAttrs = (JSONArray) jsonObject.get("XAttrs");
    if (jsonXAttrs != null) {
      for (Object a : jsonXAttrs) {
        String name = (String) ((JSONObject)a).get("name");
        String value = (String) ((JSONObject)a).get("value");
        xAttrs.put(name, decodeXAttrValue(value));
      }
    }
    return xAttrs;
  }
  
  /** Decode xattr value from string */
  private byte[] decodeXAttrValue(String value) throws IOException {
    if (value != null) {
      return XAttrCodec.decodeValue(value);
    } else {
      return new byte[0];
    }
  }

  /**
   * Validate that files are created with 755 permissions when no
   * 'permissions' attribute is specified, and when 'permissions'
   * is specified, that value is honored.
   */
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testPerms() throws Exception {
    createHttpFSServer(false);

    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path("/perm"));

    createWithHttp("/perm/none", null);
    String statusJson = getStatus("/perm/none", "GETFILESTATUS");
    Assert.assertTrue("755".equals(getPerms(statusJson)));

    createWithHttp("/perm/p-777", "777");
    statusJson = getStatus("/perm/p-777", "GETFILESTATUS");
    Assert.assertTrue("777".equals(getPerms(statusJson)));

    createWithHttp("/perm/p-654", "654");
    statusJson = getStatus("/perm/p-654", "GETFILESTATUS");
    Assert.assertTrue("654".equals(getPerms(statusJson)));

    createWithHttp("/perm/p-321", "321");
    statusJson = getStatus("/perm/p-321", "GETFILESTATUS");
    Assert.assertTrue("321".equals(getPerms(statusJson)));
  }
  
  /**
   * Validate XAttr get/set/remove calls.
   */
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testXAttrs() throws Exception {
    final String name1 = "user.a1";
    final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
    final String name2 = "user.a2";
    final byte[] value2 = new byte[]{0x41, 0x42, 0x43};
    final String dir = "/xattrTest";
    final String path = dir + "/file";
    
    createHttpFSServer(false);
    
    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path(dir));
    
    createWithHttp(path,null);
    String statusJson = getStatus(path, "GETXATTRS");
    Map<String, byte[]> xAttrs = getXAttrs(statusJson);
    Assert.assertEquals(0, xAttrs.size());
    
    // Set two xattrs
    putCmd(path, "SETXATTR", setXAttrParam(name1, value1));
    putCmd(path, "SETXATTR", setXAttrParam(name2, value2));
    statusJson = getStatus(path, "GETXATTRS");
    xAttrs = getXAttrs(statusJson);
    Assert.assertEquals(2, xAttrs.size());
    Assert.assertArrayEquals(value1, xAttrs.get(name1));
    Assert.assertArrayEquals(value2, xAttrs.get(name2));
    
    // Remove one xattr
    putCmd(path, "REMOVEXATTR", "xattr.name=" + name1);
    statusJson = getStatus(path, "GETXATTRS");
    xAttrs = getXAttrs(statusJson);
    Assert.assertEquals(1, xAttrs.size());
    Assert.assertArrayEquals(value2, xAttrs.get(name2));
    
    // Remove another xattr, then there is no xattr
    putCmd(path, "REMOVEXATTR", "xattr.name=" + name2);
    statusJson = getStatus(path, "GETXATTRS");
    xAttrs = getXAttrs(statusJson);
    Assert.assertEquals(0, xAttrs.size());
  }
  
  /** Params for setting an xAttr */
  public static String setXAttrParam(String name, byte[] value) throws IOException {
    return "xattr.name=" + name + "&xattr.value=" + XAttrCodec.encodeValue(
        value, XAttrCodec.HEX) + "&encoding=hex&flag=create"; 
  }

  /**
   * Validate the various ACL set/modify/remove calls.  General strategy is
   * to verify each of the following steps with GETFILESTATUS, LISTSTATUS,
   * and GETACLSTATUS:
   * <ol>
   *   <li>Create a file with no ACLs</li>
   *   <li>Add a user + group ACL</li>
   *   <li>Add another user ACL</li>
   *   <li>Remove the first user ACL</li>
   *   <li>Remove all ACLs</li>
   * </ol>
   */
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testFileAcls() throws Exception {
    final String aclUser1 = "user:foo:rw-";
    final String aclUser2 = "user:bar:r--";
    final String aclGroup1 = "group::r--";
    final String aclSpec = "aclspec=user::rwx," + aclUser1 + ","
            + aclGroup1 + ",other::---";
    final String modAclSpec = "aclspec=" + aclUser2;
    final String remAclSpec = "aclspec=" + aclUser1;
    final String dir = "/aclFileTest";
    final String path = dir + "/test";
    String statusJson;
    List<String> aclEntries;

    createHttpFSServer(false);

    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path(dir));

    createWithHttp(path, null);

    /* getfilestatus and liststatus don't have 'aclBit' in their reply */
    statusJson = getStatus(path, "GETFILESTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(dir, "LISTSTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));

    /* getaclstatus works and returns no entries */
    statusJson = getStatus(path, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 0);

    /*
     * Now set an ACL on the file.  (getfile|list)status have aclBit,
     * and aclstatus has entries that looks familiar.
     */
    putCmd(path, "SETACL", aclSpec);
    statusJson = getStatus(path, "GETFILESTATUS");
    Assert.assertNotEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(dir, "LISTSTATUS");
    Assert.assertNotEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(path, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 2);
    Assert.assertTrue(aclEntries.contains(aclUser1));
    Assert.assertTrue(aclEntries.contains(aclGroup1));

    /* Modify acl entries to add another user acl */
    putCmd(path, "MODIFYACLENTRIES", modAclSpec);
    statusJson = getStatus(path, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 3);
    Assert.assertTrue(aclEntries.contains(aclUser1));
    Assert.assertTrue(aclEntries.contains(aclUser2));
    Assert.assertTrue(aclEntries.contains(aclGroup1));

    /* Remove the first user acl entry and verify */
    putCmd(path, "REMOVEACLENTRIES", remAclSpec);
    statusJson = getStatus(path, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 2);
    Assert.assertTrue(aclEntries.contains(aclUser2));
    Assert.assertTrue(aclEntries.contains(aclGroup1));

    /* Remove all acls and verify */
    putCmd(path, "REMOVEACL", null);
    statusJson = getStatus(path, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 0);
    statusJson = getStatus(path, "GETFILESTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(dir, "LISTSTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));
  }

  /**
   * Test ACL operations on a directory, including default ACLs.
   * General strategy is to use GETFILESTATUS and GETACLSTATUS to verify:
   * <ol>
   *   <li>Initial status with no ACLs</li>
   *   <li>The addition of a default ACL</li>
   *   <li>The removal of default ACLs</li>
   * </ol>
   *
   * @throws Exception
   */
  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testDirAcls() throws Exception {
    final String defUser1 = "default:user:glarch:r-x";
    final String defSpec1 = "aclspec=" + defUser1;
    final String dir = "/aclDirTest";
    String statusJson;
    List<String> aclEntries;

    createHttpFSServer(false);

    FileSystem fs = FileSystem.get(TestHdfsHelper.getHdfsConf());
    fs.mkdirs(new Path(dir));

    /* getfilestatus and liststatus don't have 'aclBit' in their reply */
    statusJson = getStatus(dir, "GETFILESTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));

    /* No ACLs, either */
    statusJson = getStatus(dir, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 0);

    /* Give it a default ACL and verify */
    putCmd(dir, "SETACL", defSpec1);
    statusJson = getStatus(dir, "GETFILESTATUS");
    Assert.assertNotEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(dir, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 5);
    /* 4 Entries are default:(user|group|mask|other):perm */
    Assert.assertTrue(aclEntries.contains(defUser1));

    /* Remove the default ACL and re-verify */
    putCmd(dir, "REMOVEDEFAULTACL", null);
    statusJson = getStatus(dir, "GETFILESTATUS");
    Assert.assertEquals(-1, statusJson.indexOf("aclBit"));
    statusJson = getStatus(dir, "GETACLSTATUS");
    aclEntries = getAclEntries(statusJson);
    Assert.assertTrue(aclEntries.size() == 0);
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
          new KerberosDelegationTokenAuthenticationHandler().getType());
    token.setExpires(System.currentTimeMillis() + 100000000);
    StringSignerSecretProvider secretProvider = new StringSignerSecretProvider();
    Properties secretProviderProps = new Properties();
    secretProviderProps.setProperty(AuthenticationFilter.SIGNATURE_SECRET, "secret");
    secretProvider.init(secretProviderProps, null, -1);
    Signer signer = new Signer(secretProvider);
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
      json.get(DelegationTokenAuthenticator.DELEGATION_TOKEN_JSON);
    String tokenStr = (String)
        json.get(DelegationTokenAuthenticator.DELEGATION_TOKEN_URL_STRING_JSON);

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
