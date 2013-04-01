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

  protected FileSystem getHttpFSFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", getFileSystemClass().getName());
    URI uri = new URI("webhdfs://" +
                      TestJettyHelper.getJettyURL().toURI().getAuthority());
    return FileSystem.get(uri, conf);
  }

  protected void testGet() throws Exception {
    FileSystem fs = getHttpFSFileSystem();
    Assert.assertNotNull(fs);
    URI uri = new URI("webhdfs://" +
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
    testCreate(path, false);
    testCreate(path, true);
    try {
      testCreate(path, false);
      Assert.fail("the create should have failed because the file exists " +
                  "and override is FALSE");
    } catch (IOException ex) {

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

  protected enum Operation {
    GET, OPEN, CREATE, APPEND, CONCAT, RENAME, DELETE, LIST_STATUS, WORKING_DIRECTORY, MKDIRS,
    SET_TIMES, SET_PERMISSION, SET_OWNER, SET_REPLICATION, CHECKSUM, CONTENT_SUMMARY
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
