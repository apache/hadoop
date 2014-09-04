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

package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.server.HttpFSServerWebApp;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(value = Parameterized.class)
public abstract class BaseTestHttpFSWith extends HFSTestCase {

  protected abstract Path getProxiedFSTestDir();

  protected abstract String getProxiedFSURI();

  protected abstract Configuration getProxiedFSConf();

  protected boolean isLocalFS() {
    return getProxiedFSURI().startsWith("file://");
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

    //FileSystem being served by HttpFS
    String fsDefaultName = getProxiedFSURI();
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    File hdfsSite = new File(new File(homeDir, "conf"), "hdfs-site.xml");
    OutputStream os = new FileOutputStream(hdfsSite);
    conf.writeXml(os);
    os.close();

    //HTTPFS configuration
    conf = new Configuration(false);
    conf.set("httpfs.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".groups",
             HadoopUsersConfTestHelper.getHadoopProxyUserGroups());
    conf.set("httpfs.proxyuser." + HadoopUsersConfTestHelper.getHadoopProxyUser() + ".hosts",
             HadoopUsersConfTestHelper.getHadoopProxyUserHosts());
    conf.set("httpfs.authentication.signature.secret.file", secretFile.getAbsolutePath());
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
  }

  protected Class getFileSystemClass() {
    return HttpFSFileSystem.class;
  }

  protected String getScheme() {
    return "webhdfs";
  }

  protected FileSystem getHttpFSFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", getFileSystemClass().getName());
    URI uri = new URI(getScheme() + "://" +
                      TestJettyHelper.getJettyURL().toURI().getAuthority());
    return FileSystem.get(uri, conf);
  }

  protected void testGet() throws Exception {
    FileSystem fs = getHttpFSFileSystem();
    Assert.assertNotNull(fs);
    URI uri = new URI(getScheme() + "://" +
                      TestJettyHelper.getJettyURL().toURI().getAuthority());
    Assert.assertEquals(fs.getUri(), uri);
    fs.close();
  }

  private void testOpen() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    OutputStream os = fs.create(path);
    os.write(1);
    os.close();
    fs.close();
    fs = getHttpFSFileSystem();
    InputStream is = fs.open(new Path(path.toUri().getPath()));
    Assert.assertEquals(is.read(), 1);
    is.close();
    fs.close();
  }

  private void testCreate(Path path, boolean override) throws Exception {
    FileSystem fs = getHttpFSFileSystem();
    FsPermission permission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    OutputStream os = fs.create(new Path(path.toUri().getPath()), permission, override, 1024,
                                (short) 2, 100 * 1024 * 1024, null);
    os.write(1);
    os.close();
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    FileStatus status = fs.getFileStatus(path);
    if (!isLocalFS()) {
      Assert.assertEquals(status.getReplication(), 2);
      Assert.assertEquals(status.getBlockSize(), 100 * 1024 * 1024);
    }
    Assert.assertEquals(status.getPermission(), permission);
    InputStream is = fs.open(path);
    Assert.assertEquals(is.read(), 1);
    is.close();
    fs.close();
  }

  private void testCreate() throws Exception {
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    fs.delete(path, true);
    testCreate(path, false);
    testCreate(path, true);
    try {
      testCreate(path, false);
      Assert.fail("the create should have failed because the file exists " +
                  "and override is FALSE");
    } catch (IOException ex) {
System.out.println("#");
    } catch (Exception ex) {
      Assert.fail(ex.toString());
    }
  }

  private void testAppend() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();
      fs = getHttpFSFileSystem();
      os = fs.append(new Path(path.toUri().getPath()));
      os.write(2);
      os.close();
      fs.close();
      fs = FileSystem.get(getProxiedFSConf());
      InputStream is = fs.open(path);
      Assert.assertEquals(is.read(), 1);
      Assert.assertEquals(is.read(), 2);
      Assert.assertEquals(is.read(), -1);
      is.close();
      fs.close();
    }
  }

  private void testConcat() throws Exception {
    Configuration config = getProxiedFSConf();
    config.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(config);
      fs.mkdirs(getProxiedFSTestDir());
      Path path1 = new Path("/test/foo.txt");
      Path path2 = new Path("/test/bar.txt");
      Path path3 = new Path("/test/derp.txt");
      DFSTestUtil.createFile(fs, path1, 1024, (short) 3, 0);
      DFSTestUtil.createFile(fs, path2, 1024, (short) 3, 0);
      DFSTestUtil.createFile(fs, path3, 1024, (short) 3, 0);
      fs.close();
      fs = getHttpFSFileSystem();
      fs.concat(path1, new Path[]{path2, path3});
      fs.close();
      fs = FileSystem.get(config);
      Assert.assertTrue(fs.exists(path1));
      Assert.assertFalse(fs.exists(path2));
      Assert.assertFalse(fs.exists(path3));
      fs.close();
    }
  }

  private void testRename() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo");
    fs.mkdirs(path);
    fs.close();
    fs = getHttpFSFileSystem();
    Path oldPath = new Path(path.toUri().getPath());
    Path newPath = new Path(path.getParent(), "bar");
    fs.rename(oldPath, newPath);
    fs.close();
    fs = FileSystem.get(getProxiedFSConf());
    Assert.assertFalse(fs.exists(oldPath));
    Assert.assertTrue(fs.exists(newPath));
    fs.close();
  }

  private void testDelete() throws Exception {
    Path foo = new Path(getProxiedFSTestDir(), "foo");
    Path bar = new Path(getProxiedFSTestDir(), "bar");
    Path foe = new Path(getProxiedFSTestDir(), "foe");
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    fs.mkdirs(foo);
    fs.mkdirs(new Path(bar, "a"));
    fs.mkdirs(foe);

    FileSystem hoopFs = getHttpFSFileSystem();
    Assert.assertTrue(hoopFs.delete(new Path(foo.toUri().getPath()), false));
    Assert.assertFalse(fs.exists(foo));
    try {
      hoopFs.delete(new Path(bar.toUri().getPath()), false);
      Assert.fail();
    } catch (IOException ex) {
    } catch (Exception ex) {
      Assert.fail();
    }
    Assert.assertTrue(fs.exists(bar));
    Assert.assertTrue(hoopFs.delete(new Path(bar.toUri().getPath()), true));
    Assert.assertFalse(fs.exists(bar));

    Assert.assertTrue(fs.exists(foe));
    Assert.assertTrue(hoopFs.delete(foe, true));
    Assert.assertFalse(fs.exists(foe));

    hoopFs.close();
    fs.close();
  }

  private void testListStatus() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    OutputStream os = fs.create(path);
    os.write(1);
    os.close();
    FileStatus status1 = fs.getFileStatus(path);
    fs.close();

    fs = getHttpFSFileSystem();
    FileStatus status2 = fs.getFileStatus(new Path(path.toUri().getPath()));
    fs.close();

    Assert.assertEquals(status2.getPermission(), status1.getPermission());
    Assert.assertEquals(status2.getPath().toUri().getPath(), status1.getPath().toUri().getPath());
    Assert.assertEquals(status2.getReplication(), status1.getReplication());
    Assert.assertEquals(status2.getBlockSize(), status1.getBlockSize());
    Assert.assertEquals(status2.getAccessTime(), status1.getAccessTime());
    Assert.assertEquals(status2.getModificationTime(), status1.getModificationTime());
    Assert.assertEquals(status2.getOwner(), status1.getOwner());
    Assert.assertEquals(status2.getGroup(), status1.getGroup());
    Assert.assertEquals(status2.getLen(), status1.getLen());

    FileStatus[] stati = fs.listStatus(path.getParent());
    Assert.assertEquals(stati.length, 1);
    Assert.assertEquals(stati[0].getPath().getName(), path.getName());
  }

  private void testWorkingdirectory() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path workingDir = fs.getWorkingDirectory();
    fs.close();

    fs = getHttpFSFileSystem();
    if (isLocalFS()) {
      fs.setWorkingDirectory(workingDir);
    }
    Path httpFSWorkingDir = fs.getWorkingDirectory();
    fs.close();
    Assert.assertEquals(httpFSWorkingDir.toUri().getPath(),
                        workingDir.toUri().getPath());

    fs = getHttpFSFileSystem();
    fs.setWorkingDirectory(new Path("/tmp"));
    workingDir = fs.getWorkingDirectory();
    fs.close();
    Assert.assertEquals(workingDir.toUri().getPath(), new Path("/tmp").toUri().getPath());
  }

  private void testMkdirs() throws Exception {
    Path path = new Path(getProxiedFSTestDir(), "foo");
    FileSystem fs = getHttpFSFileSystem();
    fs.mkdirs(path);
    fs.close();
    fs = FileSystem.get(getProxiedFSConf());
    Assert.assertTrue(fs.exists(path));
    fs.close();
  }

  private void testSetTimes() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      FileStatus status1 = fs.getFileStatus(path);
      fs.close();
      long at = status1.getAccessTime();
      long mt = status1.getModificationTime();

      fs = getHttpFSFileSystem();
      fs.setTimes(path, mt - 10, at - 20);
      fs.close();

      fs = FileSystem.get(getProxiedFSConf());
      status1 = fs.getFileStatus(path);
      fs.close();
      long atNew = status1.getAccessTime();
      long mtNew = status1.getModificationTime();
      Assert.assertEquals(mtNew, mt - 10);
      Assert.assertEquals(atNew, at - 20);
    }
  }

  protected void testSetPermission() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foodir");
    fs.mkdirs(path);

    fs = getHttpFSFileSystem();
    FsPermission permission1 = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    fs.setPermission(path, permission1);
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    FileStatus status1 = fs.getFileStatus(path);
    fs.close();
    FsPermission permission2 = status1.getPermission();
    Assert.assertEquals(permission2, permission1);

    //sticky bit
    fs = getHttpFSFileSystem();
    permission1 = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE, true);
    fs.setPermission(path, permission1);
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    status1 = fs.getFileStatus(path);
    fs.close();
    permission2 = status1.getPermission();
    Assert.assertTrue(permission2.getStickyBit());
    Assert.assertEquals(permission2, permission1);
  }

  private void testSetOwner() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();

      fs = getHttpFSFileSystem();
      String user = HadoopUsersConfTestHelper.getHadoopUsers()[1];
      String group = HadoopUsersConfTestHelper.getHadoopUserGroups(user)[0];
      fs.setOwner(path, user, group);
      fs.close();

      fs = FileSystem.get(getProxiedFSConf());
      FileStatus status1 = fs.getFileStatus(path);
      fs.close();
      Assert.assertEquals(status1.getOwner(), user);
      Assert.assertEquals(status1.getGroup(), group);
    }
  }

  private void testSetReplication() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    OutputStream os = fs.create(path);
    os.write(1);
    os.close();
    fs.close();
    fs.setReplication(path, (short) 2);

    fs = getHttpFSFileSystem();
    fs.setReplication(path, (short) 1);
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    FileStatus status1 = fs.getFileStatus(path);
    fs.close();
    Assert.assertEquals(status1.getReplication(), (short) 1);
  }

  private void testChecksum() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      FileChecksum hdfsChecksum = fs.getFileChecksum(path);
      fs.close();
      fs = getHttpFSFileSystem();
      FileChecksum httpChecksum = fs.getFileChecksum(path);
      fs.close();
      Assert.assertEquals(httpChecksum.getAlgorithmName(), hdfsChecksum.getAlgorithmName());
      Assert.assertEquals(httpChecksum.getLength(), hdfsChecksum.getLength());
      Assert.assertArrayEquals(httpChecksum.getBytes(), hdfsChecksum.getBytes());
    }
  }

  private void testContentSummary() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    OutputStream os = fs.create(path);
    os.write(1);
    os.close();
    ContentSummary hdfsContentSummary = fs.getContentSummary(path);
    fs.close();
    fs = getHttpFSFileSystem();
    ContentSummary httpContentSummary = fs.getContentSummary(path);
    fs.close();
    Assert.assertEquals(httpContentSummary.getDirectoryCount(), hdfsContentSummary.getDirectoryCount());
    Assert.assertEquals(httpContentSummary.getFileCount(), hdfsContentSummary.getFileCount());
    Assert.assertEquals(httpContentSummary.getLength(), hdfsContentSummary.getLength());
    Assert.assertEquals(httpContentSummary.getQuota(), hdfsContentSummary.getQuota());
    Assert.assertEquals(httpContentSummary.getSpaceConsumed(), hdfsContentSummary.getSpaceConsumed());
    Assert.assertEquals(httpContentSummary.getSpaceQuota(), hdfsContentSummary.getSpaceQuota());
  }
  
  /** Set xattr */
  private void testSetXAttr() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();
 
      final String name1 = "user.a1";
      final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
      final String name2 = "user.a2";
      final byte[] value2 = new byte[]{0x41, 0x42, 0x43};
      final String name3 = "user.a3";
      final byte[] value3 = null;
      final String name4 = "trusted.a1";
      final byte[] value4 = new byte[]{0x31, 0x32, 0x33};
      final String name5 = "a1";
      fs = getHttpFSFileSystem();
      fs.setXAttr(path, name1, value1);
      fs.setXAttr(path, name2, value2);
      fs.setXAttr(path, name3, value3);
      fs.setXAttr(path, name4, value4);
      try {
        fs.setXAttr(path, name5, value1);
        Assert.fail("Set xAttr with incorrect name format should fail.");
      } catch (IOException e) {
      } catch (IllegalArgumentException e) {
      }
      fs.close();

      fs = FileSystem.get(getProxiedFSConf());
      Map<String, byte[]> xAttrs = fs.getXAttrs(path);
      fs.close();
      Assert.assertEquals(4, xAttrs.size());
      Assert.assertArrayEquals(value1, xAttrs.get(name1));
      Assert.assertArrayEquals(value2, xAttrs.get(name2));
      Assert.assertArrayEquals(new byte[0], xAttrs.get(name3));
      Assert.assertArrayEquals(value4, xAttrs.get(name4));
    }
  }

  /** Get xattrs */
  private void testGetXAttrs() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();

      final String name1 = "user.a1";
      final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
      final String name2 = "user.a2";
      final byte[] value2 = new byte[]{0x41, 0x42, 0x43};
      final String name3 = "user.a3";
      final byte[] value3 = null;
      final String name4 = "trusted.a1";
      final byte[] value4 = new byte[]{0x31, 0x32, 0x33};
      fs = FileSystem.get(getProxiedFSConf());
      fs.setXAttr(path, name1, value1);
      fs.setXAttr(path, name2, value2);
      fs.setXAttr(path, name3, value3);
      fs.setXAttr(path, name4, value4);
      fs.close();

      // Get xattrs with names parameter
      fs = getHttpFSFileSystem();
      List<String> names = Lists.newArrayList();
      names.add(name1);
      names.add(name2);
      names.add(name3);
      names.add(name4);
      Map<String, byte[]> xAttrs = fs.getXAttrs(path, names);
      fs.close();
      Assert.assertEquals(4, xAttrs.size());
      Assert.assertArrayEquals(value1, xAttrs.get(name1));
      Assert.assertArrayEquals(value2, xAttrs.get(name2));
      Assert.assertArrayEquals(new byte[0], xAttrs.get(name3));
      Assert.assertArrayEquals(value4, xAttrs.get(name4));

      // Get specific xattr
      fs = getHttpFSFileSystem();
      byte[] value = fs.getXAttr(path, name1);
      Assert.assertArrayEquals(value1, value);
      final String name5 = "a1";
      try {
        value = fs.getXAttr(path, name5);
        Assert.fail("Get xAttr with incorrect name format should fail.");
      } catch (IOException e) {
      } catch (IllegalArgumentException e) {
      }
      fs.close();

      // Get all xattrs
      fs = getHttpFSFileSystem();
      xAttrs = fs.getXAttrs(path);
      fs.close();
      Assert.assertEquals(4, xAttrs.size());
      Assert.assertArrayEquals(value1, xAttrs.get(name1));
      Assert.assertArrayEquals(value2, xAttrs.get(name2));
      Assert.assertArrayEquals(new byte[0], xAttrs.get(name3));
      Assert.assertArrayEquals(value4, xAttrs.get(name4));
    }
  }

  /** Remove xattr */
  private void testRemoveXAttr() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();

      final String name1 = "user.a1";
      final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
      final String name2 = "user.a2";
      final byte[] value2 = new byte[]{0x41, 0x42, 0x43};
      final String name3 = "user.a3";
      final byte[] value3 = null;
      final String name4 = "trusted.a1";
      final byte[] value4 = new byte[]{0x31, 0x32, 0x33};
      final String name5 = "a1";
      fs = FileSystem.get(getProxiedFSConf());
      fs.setXAttr(path, name1, value1);
      fs.setXAttr(path, name2, value2);
      fs.setXAttr(path, name3, value3);
      fs.setXAttr(path, name4, value4);
      fs.close();

      fs = getHttpFSFileSystem();
      fs.removeXAttr(path, name1);
      fs.removeXAttr(path, name3);
      fs.removeXAttr(path, name4);
      try {
        fs.removeXAttr(path, name5);
        Assert.fail("Remove xAttr with incorrect name format should fail.");
      } catch (IOException e) {
      } catch (IllegalArgumentException e) {
      }

      fs = FileSystem.get(getProxiedFSConf());
      Map<String, byte[]> xAttrs = fs.getXAttrs(path);
      fs.close();
      Assert.assertEquals(1, xAttrs.size());
      Assert.assertArrayEquals(value2, xAttrs.get(name2));
    }
  }

  /** List xattrs */
  private void testListXAttrs() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path path = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(path);
      os.write(1);
      os.close();
      fs.close();

      final String name1 = "user.a1";
      final byte[] value1 = new byte[]{0x31, 0x32, 0x33};
      final String name2 = "user.a2";
      final byte[] value2 = new byte[]{0x41, 0x42, 0x43};
      final String name3 = "user.a3";
      final byte[] value3 = null;
      final String name4 = "trusted.a1";
      final byte[] value4 = new byte[]{0x31, 0x32, 0x33};
      fs = FileSystem.get(getProxiedFSConf());
      fs.setXAttr(path, name1, value1);
      fs.setXAttr(path, name2, value2);
      fs.setXAttr(path, name3, value3);
      fs.setXAttr(path, name4, value4);
      fs.close();

      fs = getHttpFSFileSystem();
      List<String> names = fs.listXAttrs(path);
      Assert.assertEquals(4, names.size());
      Assert.assertTrue(names.contains(name1));
      Assert.assertTrue(names.contains(name2));
      Assert.assertTrue(names.contains(name3));
      Assert.assertTrue(names.contains(name4));
    }
  }

  /**
   * Runs assertions testing that two AclStatus objects contain the same info
   * @param a First AclStatus
   * @param b Second AclStatus
   * @throws Exception
   */
  private void assertSameAcls(AclStatus a, AclStatus b) throws Exception {
    Assert.assertTrue(a.getOwner().equals(b.getOwner()));
    Assert.assertTrue(a.getGroup().equals(b.getGroup()));
    Assert.assertTrue(a.isStickyBit() == b.isStickyBit());
    Assert.assertTrue(a.getEntries().size() == b.getEntries().size());
    for (AclEntry e : a.getEntries()) {
      Assert.assertTrue(b.getEntries().contains(e));
    }
    for (AclEntry e : b.getEntries()) {
      Assert.assertTrue(a.getEntries().contains(e));
    }
  }

  /**
   * Simple ACL tests on a file:  Set an acl, add an acl, remove one acl,
   * and remove all acls.
   * @throws Exception
   */
  private void testFileAcls() throws Exception {
    if ( isLocalFS() ) {
      return;
    }

    final String aclUser1 = "user:foo:rw-";
    final String aclUser2 = "user:bar:r--";
    final String aclGroup1 = "group::r--";
    final String aclSet = "user::rwx," + aclUser1 + ","
            + aclGroup1 + ",other::---";

    FileSystem proxyFs = FileSystem.get(getProxiedFSConf());
    FileSystem httpfs = getHttpFSFileSystem();

    Path path = new Path(getProxiedFSTestDir(), "testAclStatus.txt");
    OutputStream os = proxyFs.create(path);
    os.write(1);
    os.close();

    AclStatus proxyAclStat = proxyFs.getAclStatus(path);
    AclStatus httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    httpfs.setAcl(path, AclEntry.parseAclSpec(aclSet,true));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    httpfs.modifyAclEntries(path, AclEntry.parseAclSpec(aclUser2, true));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    httpfs.removeAclEntries(path, AclEntry.parseAclSpec(aclUser1, true));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    httpfs.removeAcl(path);
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);
  }

  /**
   * Simple acl tests on a directory: set a default acl, remove default acls.
   * @throws Exception
   */
  private void testDirAcls() throws Exception {
    if ( isLocalFS() ) {
      return;
    }

    final String defUser1 = "default:user:glarch:r-x";

    FileSystem proxyFs = FileSystem.get(getProxiedFSConf());
    FileSystem httpfs = getHttpFSFileSystem();

    Path dir = getProxiedFSTestDir();

    /* ACL Status on a directory */
    AclStatus proxyAclStat = proxyFs.getAclStatus(dir);
    AclStatus httpfsAclStat = httpfs.getAclStatus(dir);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    /* Set a default ACL on the directory */
    httpfs.setAcl(dir, (AclEntry.parseAclSpec(defUser1,true)));
    proxyAclStat = proxyFs.getAclStatus(dir);
    httpfsAclStat = httpfs.getAclStatus(dir);
    assertSameAcls(httpfsAclStat, proxyAclStat);

    /* Remove the default ACL */
    httpfs.removeDefaultAcl(dir);
    proxyAclStat = proxyFs.getAclStatus(dir);
    httpfsAclStat = httpfs.getAclStatus(dir);
    assertSameAcls(httpfsAclStat, proxyAclStat);
  }

  protected enum Operation {
    GET, OPEN, CREATE, APPEND, CONCAT, RENAME, DELETE, LIST_STATUS, WORKING_DIRECTORY, MKDIRS,
    SET_TIMES, SET_PERMISSION, SET_OWNER, SET_REPLICATION, CHECKSUM, CONTENT_SUMMARY,
    FILEACLS, DIRACLS, SET_XATTR, GET_XATTRS, REMOVE_XATTR, LIST_XATTRS
  }

  private void operation(Operation op) throws Exception {
    switch (op) {
      case GET:
        testGet();
        break;
      case OPEN:
        testOpen();
        break;
      case CREATE:
        testCreate();
        break;
      case APPEND:
        testAppend();
        break;
      case CONCAT:
        testConcat();
      case RENAME:
        testRename();
        break;
      case DELETE:
        testDelete();
        break;
      case LIST_STATUS:
        testListStatus();
        break;
      case WORKING_DIRECTORY:
        testWorkingdirectory();
        break;
      case MKDIRS:
        testMkdirs();
        break;
      case SET_TIMES:
        testSetTimes();
        break;
      case SET_PERMISSION:
        testSetPermission();
        break;
      case SET_OWNER:
        testSetOwner();
        break;
      case SET_REPLICATION:
        testSetReplication();
        break;
      case CHECKSUM:
        testChecksum();
        break;
      case CONTENT_SUMMARY:
        testContentSummary();
        break;
      case FILEACLS:
        testFileAcls();
        break;
      case DIRACLS:
        testDirAcls();
        break;
      case SET_XATTR:
        testSetXAttr();
        break;
      case REMOVE_XATTR:
        testRemoveXAttr();
        break;
      case GET_XATTRS:
        testGetXAttrs();
        break;
      case LIST_XATTRS:
        testListXAttrs();
        break;
    }
  }

  @Parameterized.Parameters
  public static Collection operations() {
    Object[][] ops = new Object[Operation.values().length][];
    for (int i = 0; i < Operation.values().length; i++) {
      ops[i] = new Object[]{Operation.values()[i]};
    }
    //To test one or a subset of operations do:
    //return Arrays.asList(new Object[][]{ new Object[]{Operation.APPEND}});
    return Arrays.asList(ops);
  }

  private Operation operation;

  public BaseTestHttpFSWith(Operation operation) {
    this.operation = operation;
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testOperation() throws Exception {
    createHttpFSServer();
    operation(operation);
  }

  @Test
  @TestDir
  @TestJetty
  @TestHdfs
  public void testOperationDoAs() throws Exception {
    createHttpFSServer();
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(HadoopUsersConfTestHelper.getHadoopUsers()[0],
                                                                    UserGroupInformation.getCurrentUser());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        operation(operation);
        return null;
      }
    });
  }

}
