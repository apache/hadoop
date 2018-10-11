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
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.http.server.HttpFSServerWebApp;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.HFSTestCase;
import org.apache.hadoop.test.HadoopUsersConfTestHelper;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.test.TestHdfs;
import org.apache.hadoop.test.TestHdfsHelper;
import org.apache.hadoop.test.TestJetty;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    assertTrue(new File(homeDir, "conf").mkdir());
    assertTrue(new File(homeDir, "log").mkdir());
    assertTrue(new File(homeDir, "temp").mkdir());
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
    server.setHandler(context);
    server.start();
  }

  protected Class getFileSystemClass() {
    return HttpFSFileSystem.class;
  }

  protected String getScheme() {
    return "webhdfs";
  }

  protected FileSystem getHttpFSFileSystem(Configuration conf) throws
      Exception {
    conf.set("fs.webhdfs.impl", getFileSystemClass().getName());
    URI uri = new URI(getScheme() + "://" +
                      TestJettyHelper.getJettyURL().toURI().getAuthority());
    return FileSystem.get(uri, conf);
  }

  protected FileSystem getHttpFSFileSystem() throws Exception {
    Configuration conf = new Configuration();
    return getHttpFSFileSystem(conf);
  }

  protected void testGet() throws Exception {
    FileSystem fs = getHttpFSFileSystem();
    Assert.assertNotNull(fs);
    URI uri = new URI(getScheme() + "://" +
                      TestJettyHelper.getJettyURL().toURI().getAuthority());
    assertEquals(fs.getUri(), uri);
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
    assertEquals(is.read(), 1);
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
      assertEquals(status.getReplication(), 2);
      assertEquals(status.getBlockSize(), 100 * 1024 * 1024);
    }
    assertEquals(status.getPermission(), permission);
    InputStream is = fs.open(path);
    assertEquals(is.read(), 1);
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
      assertEquals(is.read(), 1);
      assertEquals(is.read(), 2);
      assertEquals(is.read(), -1);
      is.close();
      fs.close();
    }
  }

  private void testTruncate() throws Exception {
    if (!isLocalFS()) {
      final short repl = 3;
      final int blockSize = 1024;
      final int numOfBlocks = 2;
      FileSystem fs = FileSystem.get(getProxiedFSConf());
      fs.mkdirs(getProxiedFSTestDir());
      Path file = new Path(getProxiedFSTestDir(), "foo.txt");
      final byte[] data = FileSystemTestHelper.getFileData(
          numOfBlocks, blockSize);
      FileSystemTestHelper.createFile(fs, file, data, blockSize, repl);

      final int newLength = blockSize;

      boolean isReady = fs.truncate(file, newLength);
      assertTrue("Recovery is not expected.", isReady);

      FileStatus fileStatus = fs.getFileStatus(file);
      assertEquals(fileStatus.getLen(), newLength);
      AppendTestUtil.checkFullFile(fs, file, newLength, data, file.toString());

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
      assertTrue(fs.exists(path1));
      assertFalse(fs.exists(path2));
      assertFalse(fs.exists(path3));
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
    assertFalse(fs.exists(oldPath));
    assertTrue(fs.exists(newPath));
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
    assertTrue(hoopFs.delete(new Path(foo.toUri().getPath()), false));
    assertFalse(fs.exists(foo));
    try {
      hoopFs.delete(new Path(bar.toUri().getPath()), false);
      Assert.fail();
    } catch (IOException ex) {
    } catch (Exception ex) {
      Assert.fail();
    }
    assertTrue(fs.exists(bar));
    assertTrue(hoopFs.delete(new Path(bar.toUri().getPath()), true));
    assertFalse(fs.exists(bar));

    assertTrue(fs.exists(foe));
    assertTrue(hoopFs.delete(foe, true));
    assertFalse(fs.exists(foe));

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

    assertEquals(status2.getPermission(), status1.getPermission());
    assertEquals(status2.getPath().toUri().getPath(),
        status1.getPath().toUri().getPath());
    assertEquals(status2.getReplication(), status1.getReplication());
    assertEquals(status2.getBlockSize(), status1.getBlockSize());
    assertEquals(status2.getAccessTime(), status1.getAccessTime());
    assertEquals(status2.getModificationTime(), status1.getModificationTime());
    assertEquals(status2.getOwner(), status1.getOwner());
    assertEquals(status2.getGroup(), status1.getGroup());
    assertEquals(status2.getLen(), status1.getLen());

    FileStatus[] stati = fs.listStatus(path.getParent());
    assertEquals(1, stati.length);
    assertEquals(stati[0].getPath().getName(), path.getName());

    // The full path should be the path to the file. See HDFS-12139
    FileStatus[] statl = fs.listStatus(path);
    Assert.assertEquals(1, statl.length);
    Assert.assertEquals(status2.getPath(), statl[0].getPath());
    Assert.assertEquals(statl[0].getPath().getName(), path.getName());
    Assert.assertEquals(stati[0].getPath(), statl[0].getPath());
  }

  private void testFileStatusAttr() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory
      Path path = new Path("/tmp/tmp-snap-test");
      DistributedFileSystem distributedFs = (DistributedFileSystem) FileSystem
          .get(path.toUri(), this.getProxiedFSConf());
      distributedFs.mkdirs(path);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      assertFalse("Snapshot should be disallowed by default",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Allow snapshot
      distributedFs.allowSnapshot(path);
      // Check FileStatus
      assertTrue("Snapshot enabled bit is not set in FileStatus",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Disallow snapshot
      distributedFs.disallowSnapshot(path);
      // Check FileStatus
      assertFalse("Snapshot enabled bit is not cleared in FileStatus",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Cleanup
      fs.delete(path, true);
      fs.close();
      distributedFs.close();
    }
  }

  private static void assertSameListing(FileSystem expected, FileSystem
      actual, Path p) throws IOException {
    // Consume all the entries from both iterators
    RemoteIterator<FileStatus> exIt = expected.listStatusIterator(p);
    List<FileStatus> exStatuses = new ArrayList<>();
    while (exIt.hasNext()) {
      exStatuses.add(exIt.next());
    }
    RemoteIterator<FileStatus> acIt = actual.listStatusIterator(p);
    List<FileStatus> acStatuses = new ArrayList<>();
    while (acIt.hasNext()) {
      acStatuses.add(acIt.next());
    }
    assertEquals(exStatuses.size(), acStatuses.size());
    for (int i = 0; i < exStatuses.size(); i++) {
      FileStatus expectedStatus = exStatuses.get(i);
      FileStatus actualStatus = acStatuses.get(i);
      // Path URIs are fully qualified, so compare just the path component
      assertEquals(expectedStatus.getPath().toUri().getPath(),
          actualStatus.getPath().toUri().getPath());
    }
  }

  private void testListStatusBatch() throws Exception {
    // LocalFileSystem writes checksum files next to the data files, which
    // show up when listing via LFS. This makes the listings not compare
    // properly.
    Assume.assumeFalse(isLocalFS());

    FileSystem proxyFs = FileSystem.get(getProxiedFSConf());
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 2);
    FileSystem httpFs = getHttpFSFileSystem(conf);

    // Test an empty directory
    Path dir = new Path(getProxiedFSTestDir(), "dir");
    proxyFs.mkdirs(dir);
    assertSameListing(proxyFs, httpFs, dir);
    // Create and test in a loop
    for (int i = 0; i < 10; i++) {
      proxyFs.create(new Path(dir, "file" + i)).close();
      assertSameListing(proxyFs, httpFs, dir);
    }

    // Test for HDFS-12139
    Path dir1 = new Path(getProxiedFSTestDir(), "dir1");
    proxyFs.mkdirs(dir1);
    Path file1 = new Path(dir1, "file1");
    proxyFs.create(file1).close();

    RemoteIterator<FileStatus> si = proxyFs.listStatusIterator(dir1);
    FileStatus statusl = si.next();
    FileStatus status = proxyFs.getFileStatus(file1);
    Assert.assertEquals(file1.getName(), statusl.getPath().getName());
    Assert.assertEquals(status.getPath(), statusl.getPath());

    si = proxyFs.listStatusIterator(file1);
    statusl = si.next();
    Assert.assertEquals(file1.getName(), statusl.getPath().getName());
    Assert.assertEquals(status.getPath(), statusl.getPath());
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
    assertEquals(httpFSWorkingDir.toUri().getPath(),
                        workingDir.toUri().getPath());

    fs = getHttpFSFileSystem();
    fs.setWorkingDirectory(new Path("/tmp"));
    workingDir = fs.getWorkingDirectory();
    fs.close();
    assertEquals(workingDir.toUri().getPath(),
        new Path("/tmp").toUri().getPath());
  }

  private void testTrashRoot() throws Exception {
    if (!isLocalFS()) {
      FileSystem fs = FileSystem.get(getProxiedFSConf());

      final Path rootDir = new Path("/");
      final Path fooPath = new Path(getProxiedFSTestDir(), "foo.txt");
      OutputStream os = fs.create(fooPath);
      os.write(1);
      os.close();

      Path trashPath = fs.getTrashRoot(rootDir);
      Path fooTrashPath = fs.getTrashRoot(fooPath);
      fs.close();

      fs = getHttpFSFileSystem();
      Path httpFSTrashPath = fs.getTrashRoot(rootDir);
      Path httpFSFooTrashPath = fs.getTrashRoot(fooPath);
      fs.close();

      assertEquals(trashPath.toUri().getPath(),
          httpFSTrashPath.toUri().getPath());
      assertEquals(fooTrashPath.toUri().getPath(),
          httpFSFooTrashPath.toUri().getPath());
      // trash path is related to USER, not path
      assertEquals(trashPath.toUri().getPath(),
          fooTrashPath.toUri().getPath());
    }
  }

  private void testMkdirs() throws Exception {
    Path path = new Path(getProxiedFSTestDir(), "foo");
    FileSystem fs = getHttpFSFileSystem();
    fs.mkdirs(path);
    fs.close();
    fs = FileSystem.get(getProxiedFSConf());
    assertTrue(fs.exists(path));
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
      assertEquals(mtNew, mt - 10);
      assertEquals(atNew, at - 20);
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
    assertEquals(permission2, permission1);

    //sticky bit
    fs = getHttpFSFileSystem();
    permission1 = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE, true);
    fs.setPermission(path, permission1);
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    status1 = fs.getFileStatus(path);
    fs.close();
    permission2 = status1.getPermission();
    assertTrue(permission2.getStickyBit());
    assertEquals(permission2, permission1);
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
      assertEquals(status1.getOwner(), user);
      assertEquals(status1.getGroup(), group);
    }
  }

  private void testSetReplication() throws Exception {
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    Path path = new Path(getProxiedFSTestDir(), "foo.txt");
    OutputStream os = fs.create(path);
    os.write(1);
    os.close();
    fs.setReplication(path, (short) 2);
    fs.close();

    fs = getHttpFSFileSystem();
    fs.setReplication(path, (short) 1);
    fs.close();

    fs = FileSystem.get(getProxiedFSConf());
    FileStatus status1 = fs.getFileStatus(path);
    fs.close();
    assertEquals(status1.getReplication(), (short) 1);
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
      assertEquals(httpChecksum.getAlgorithmName(),
          hdfsChecksum.getAlgorithmName());
      assertEquals(httpChecksum.getLength(), hdfsChecksum.getLength());
      assertArrayEquals(httpChecksum.getBytes(), hdfsChecksum.getBytes());
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
    assertEquals(httpContentSummary.getDirectoryCount(),
        hdfsContentSummary.getDirectoryCount());
    assertEquals(httpContentSummary.getFileCount(),
        hdfsContentSummary.getFileCount());
    assertEquals(httpContentSummary.getLength(),
        hdfsContentSummary.getLength());
    assertEquals(httpContentSummary.getQuota(), hdfsContentSummary.getQuota());
    assertEquals(httpContentSummary.getSpaceConsumed(),
        hdfsContentSummary.getSpaceConsumed());
    assertEquals(httpContentSummary.getSpaceQuota(),
        hdfsContentSummary.getSpaceQuota());
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
      assertEquals(4, xAttrs.size());
      assertArrayEquals(value1, xAttrs.get(name1));
      assertArrayEquals(value2, xAttrs.get(name2));
      assertArrayEquals(new byte[0], xAttrs.get(name3));
      assertArrayEquals(value4, xAttrs.get(name4));
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
      assertEquals(4, xAttrs.size());
      assertArrayEquals(value1, xAttrs.get(name1));
      assertArrayEquals(value2, xAttrs.get(name2));
      assertArrayEquals(new byte[0], xAttrs.get(name3));
      assertArrayEquals(value4, xAttrs.get(name4));

      // Get specific xattr
      fs = getHttpFSFileSystem();
      byte[] value = fs.getXAttr(path, name1);
      assertArrayEquals(value1, value);
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
      assertEquals(4, xAttrs.size());
      assertArrayEquals(value1, xAttrs.get(name1));
      assertArrayEquals(value2, xAttrs.get(name2));
      assertArrayEquals(new byte[0], xAttrs.get(name3));
      assertArrayEquals(value4, xAttrs.get(name4));
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
      assertEquals(1, xAttrs.size());
      assertArrayEquals(value2, xAttrs.get(name2));
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
      assertEquals(4, names.size());
      assertTrue(names.contains(name1));
      assertTrue(names.contains(name2));
      assertTrue(names.contains(name3));
      assertTrue(names.contains(name4));
    }
  }

  /**
   * Runs assertions testing that two AclStatus objects contain the same info
   * @param a First AclStatus
   * @param b Second AclStatus
   * @throws Exception
   */
  private void assertSameAcls(AclStatus a, AclStatus b) throws Exception {
    assertTrue(a.getOwner().equals(b.getOwner()));
    assertTrue(a.getGroup().equals(b.getGroup()));
    assertTrue(a.isStickyBit() == b.isStickyBit());
    assertTrue(a.getEntries().size() == b.getEntries().size());
    for (AclEntry e : a.getEntries()) {
      assertTrue(b.getEntries().contains(e));
    }
    for (AclEntry e : b.getEntries()) {
      assertTrue(a.getEntries().contains(e));
    }
  }

  private static void assertSameAcls(FileSystem expected, FileSystem actual,
      Path path) throws IOException {
    FileStatus expectedFileStatus = expected.getFileStatus(path);
    FileStatus actualFileStatus = actual.getFileStatus(path);
    assertEquals(actualFileStatus.hasAcl(), expectedFileStatus.hasAcl());
    // backwards compat
    assertEquals(actualFileStatus.getPermission().getAclBit(),
        expectedFileStatus.getPermission().getAclBit());
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
    final String rmAclUser1 = "user:foo:";
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
    assertSameAcls(httpfs, proxyFs, path);

    httpfs.setAcl(path, AclEntry.parseAclSpec(aclSet,true));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, path);

    httpfs.modifyAclEntries(path, AclEntry.parseAclSpec(aclUser2, true));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, path);

    httpfs.removeAclEntries(path, AclEntry.parseAclSpec(rmAclUser1, false));
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, path);

    httpfs.removeAcl(path);
    proxyAclStat = proxyFs.getAclStatus(path);
    httpfsAclStat = httpfs.getAclStatus(path);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, path);
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
    assertSameAcls(httpfs, proxyFs, dir);

    /* Set a default ACL on the directory */
    httpfs.setAcl(dir, (AclEntry.parseAclSpec(defUser1,true)));
    proxyAclStat = proxyFs.getAclStatus(dir);
    httpfsAclStat = httpfs.getAclStatus(dir);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, dir);

    /* Remove the default ACL */
    httpfs.removeDefaultAcl(dir);
    proxyAclStat = proxyFs.getAclStatus(dir);
    httpfsAclStat = httpfs.getAclStatus(dir);
    assertSameAcls(httpfsAclStat, proxyAclStat);
    assertSameAcls(httpfs, proxyFs, dir);
  }

  private void testEncryption() throws Exception {
    if (isLocalFS()) {
      return;
    }
    FileSystem proxyFs = FileSystem.get(getProxiedFSConf());
    FileSystem httpFs = getHttpFSFileSystem();
    FileStatus proxyStatus = proxyFs.getFileStatus(TestHdfsHelper
        .ENCRYPTED_FILE);
    assertTrue(proxyStatus.isEncrypted());
    FileStatus httpStatus = httpFs.getFileStatus(TestHdfsHelper
        .ENCRYPTED_FILE);
    assertTrue(httpStatus.isEncrypted());
    proxyStatus = proxyFs.getFileStatus(new Path("/"));
    httpStatus = httpFs.getFileStatus(new Path("/"));
    assertFalse(proxyStatus.isEncrypted());
    assertFalse(httpStatus.isEncrypted());
  }

  private void testErasureCoding() throws Exception {
    Assume.assumeFalse("Assume its not a local FS!", isLocalFS());
    FileSystem proxyFs = FileSystem.get(getProxiedFSConf());
    FileSystem httpFS = getHttpFSFileSystem();
    Path filePath = new Path(getProxiedFSTestDir(), "foo.txt");
    proxyFs.create(filePath).close();

    ContractTestUtils.assertNotErasureCoded(httpFS, getProxiedFSTestDir());
    ContractTestUtils.assertNotErasureCoded(httpFS, filePath);
    ContractTestUtils.assertErasureCoded(httpFS,
        TestHdfsHelper.ERASURE_CODING_DIR);
    ContractTestUtils.assertErasureCoded(httpFS,
        TestHdfsHelper.ERASURE_CODING_FILE);

    proxyFs.close();
    httpFS.close();
  }

  private void testStoragePolicy() throws Exception {
    Assume.assumeFalse("Assume its not a local FS", isLocalFS());
    FileSystem fs = FileSystem.get(getProxiedFSConf());
    fs.mkdirs(getProxiedFSTestDir());
    Path path = new Path(getProxiedFSTestDir(), "policy.txt");
    FileSystem httpfs = getHttpFSFileSystem();
    // test getAllStoragePolicies
    Assert.assertArrayEquals(
        "Policy array returned from the DFS and HttpFS should be equals",
        fs.getAllStoragePolicies().toArray(), httpfs.getAllStoragePolicies().toArray());

    // test get/set/unset policies
    DFSTestUtil.createFile(fs, path, 0, (short) 1, 0L);
    // get defaultPolicy
   BlockStoragePolicySpi defaultdfsPolicy = fs.getStoragePolicy(path);
    // set policy through webhdfs
    httpfs.setStoragePolicy(path, HdfsConstants.COLD_STORAGE_POLICY_NAME);
    // get policy from dfs
    BlockStoragePolicySpi dfsPolicy = fs.getStoragePolicy(path);
    // get policy from webhdfs
    BlockStoragePolicySpi httpFsPolicy = httpfs.getStoragePolicy(path);
    Assert
       .assertEquals(
            "Storage policy returned from the get API should"
            + " be same as set policy",
            HdfsConstants.COLD_STORAGE_POLICY_NAME.toString(),
            httpFsPolicy.getName());
    Assert.assertEquals(
        "Storage policy returned from the DFS and HttpFS should be equals",
        httpFsPolicy, dfsPolicy);
    // unset policy
    httpfs.unsetStoragePolicy(path);
    Assert
       .assertEquals(
            "After unset storage policy, the get API shoudld"
            + " return the default policy",
            defaultdfsPolicy, httpfs.getStoragePolicy(path));
    fs.close();
  }

  protected enum Operation {
    GET, OPEN, CREATE, APPEND, TRUNCATE, CONCAT, RENAME, DELETE, LIST_STATUS,
    WORKING_DIRECTORY, MKDIRS, SET_TIMES, SET_PERMISSION, SET_OWNER,
    SET_REPLICATION, CHECKSUM, CONTENT_SUMMARY, FILEACLS, DIRACLS, SET_XATTR,
    GET_XATTRS, REMOVE_XATTR, LIST_XATTRS, ENCRYPTION, LIST_STATUS_BATCH,
    GETTRASHROOT, STORAGEPOLICY, ERASURE_CODING,
    CREATE_SNAPSHOT, RENAME_SNAPSHOT, DELETE_SNAPSHOT,
    ALLOW_SNAPSHOT, DISALLOW_SNAPSHOT, DISALLOW_SNAPSHOT_EXCEPTION,
    FILE_STATUS_ATTR, GET_SNAPSHOT_DIFF, GET_SNAPSHOTTABLE_DIRECTORY_LIST
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
    case TRUNCATE:
      testTruncate();
      break;
    case CONCAT:
      testConcat();
      break;
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
    case ENCRYPTION:
      testEncryption();
      break;
    case LIST_STATUS_BATCH:
      testListStatusBatch();
      break;
    case GETTRASHROOT:
      testTrashRoot();
      break;
    case STORAGEPOLICY:
      testStoragePolicy();
      break;
    case ERASURE_CODING:
      testErasureCoding();
      break;
    case CREATE_SNAPSHOT:
      testCreateSnapshot();
      break;
    case RENAME_SNAPSHOT:
      testRenameSnapshot();
      break;
    case DELETE_SNAPSHOT:
      testDeleteSnapshot();
      break;
    case ALLOW_SNAPSHOT:
      testAllowSnapshot();
      break;
    case DISALLOW_SNAPSHOT:
      testDisallowSnapshot();
      break;
    case DISALLOW_SNAPSHOT_EXCEPTION:
      testDisallowSnapshotException();
      break;
    case FILE_STATUS_ATTR:
      testFileStatusAttr();
      break;
    case GET_SNAPSHOT_DIFF:
      testGetSnapshotDiff();
      testGetSnapshotDiffIllegalParam();
      break;
    case GET_SNAPSHOTTABLE_DIRECTORY_LIST:
      testGetSnapshottableDirListing();
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

  private void testCreateSnapshot(String snapshotName) throws Exception {
    if (!this.isLocalFS()) {
      Path snapshottablePath = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(snapshottablePath);
      //Now get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      if (snapshotName == null) {
        fs.createSnapshot(snapshottablePath);
      } else {
        fs.createSnapshot(snapshottablePath, snapshotName);
      }
      Path snapshotsDir = new Path("/tmp/tmp-snap-test/.snapshot");
      FileStatus[] snapshotItems = fs.listStatus(snapshotsDir);
      assertTrue("Should have exactly one snapshot.",
          snapshotItems.length == 1);
      String resultingSnapName = snapshotItems[0].getPath().getName();
      if (snapshotName == null) {
        assertTrue("Snapshot auto generated name not matching pattern",
            Pattern.matches("(s)(\\d{8})(-)(\\d{6})(\\.)(\\d{3})",
                resultingSnapName));
      } else {
        assertTrue("Snapshot name is not same as passed name.",
            snapshotName.equals(resultingSnapName));
      }
      cleanSnapshotTests(snapshottablePath, resultingSnapName);
    }
  }

  private void testCreateSnapshot() throws Exception {
    testCreateSnapshot(null);
    testCreateSnapshot("snap-with-name");
  }

  private void createSnapshotTestsPreconditions(Path snapshottablePath,
      Boolean allowSnapshot) throws Exception {
    //Needed to get a DistributedFileSystem instance, in order to
    //call allowSnapshot on the newly created directory
    DistributedFileSystem distributedFs = (DistributedFileSystem)
        FileSystem.get(snapshottablePath.toUri(), this.getProxiedFSConf());
    distributedFs.mkdirs(snapshottablePath);
    if (allowSnapshot) {
      distributedFs.allowSnapshot(snapshottablePath);
    }
    Path subdirPath = new Path("/tmp/tmp-snap-test/subdir");
    distributedFs.mkdirs(subdirPath);
  }

  private void createSnapshotTestsPreconditions(Path snapshottablePath)
      throws Exception {
    // Allow snapshot by default for snapshot test
    createSnapshotTestsPreconditions(snapshottablePath, true);
  }

  private void cleanSnapshotTests(Path snapshottablePath,
                                  String resultingSnapName) throws Exception {
    DistributedFileSystem distributedFs = (DistributedFileSystem)
        FileSystem.get(snapshottablePath.toUri(), this.getProxiedFSConf());
    distributedFs.deleteSnapshot(snapshottablePath, resultingSnapName);
    distributedFs.delete(snapshottablePath, true);
  }

  private void testRenameSnapshot() throws Exception {
    if (!this.isLocalFS()) {
      Path snapshottablePath = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(snapshottablePath);
      //Now get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      fs.createSnapshot(snapshottablePath, "snap-to-rename");
      fs.renameSnapshot(snapshottablePath, "snap-to-rename",
          "snap-new-name");
      Path snapshotsDir = new Path("/tmp/tmp-snap-test/.snapshot");
      FileStatus[] snapshotItems = fs.listStatus(snapshotsDir);
      assertTrue("Should have exactly one snapshot.",
          snapshotItems.length == 1);
      String resultingSnapName = snapshotItems[0].getPath().getName();
      assertTrue("Snapshot name is not same as passed name.",
          "snap-new-name".equals(resultingSnapName));
      cleanSnapshotTests(snapshottablePath, resultingSnapName);
    }
  }

  private void testDeleteSnapshot() throws Exception {
    if (!this.isLocalFS()) {
      Path snapshottablePath = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(snapshottablePath);
      //Now get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      fs.createSnapshot(snapshottablePath, "snap-to-delete");
      Path snapshotsDir = new Path("/tmp/tmp-snap-test/.snapshot");
      FileStatus[] snapshotItems = fs.listStatus(snapshotsDir);
      assertTrue("Should have exactly one snapshot.",
          snapshotItems.length == 1);
      fs.deleteSnapshot(snapshottablePath, "snap-to-delete");
      snapshotItems = fs.listStatus(snapshotsDir);
      assertTrue("There should be no snapshot anymore.",
          snapshotItems.length == 0);
      fs.delete(snapshottablePath, true);
    }
  }

  private void testAllowSnapshot() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory with snapshot disallowed
      Path path = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(path, false);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      assertFalse("Snapshot should be disallowed by default",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Allow snapshot
      if (fs instanceof HttpFSFileSystem) {
        HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
        httpFS.allowSnapshot(path);
      } else if (fs instanceof WebHdfsFileSystem) {
        WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
        webHdfsFileSystem.allowSnapshot(path);
      } else {
        Assert.fail(fs.getClass().getSimpleName() +
            " doesn't support allowSnapshot");
      }
      // Check FileStatus
      assertTrue("allowSnapshot failed",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Cleanup
      fs.delete(path, true);
    }
  }

  private void testDisallowSnapshot() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory with snapshot allowed
      Path path = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(path);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      assertTrue("Snapshot should be allowed by DFS",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Disallow snapshot
      if (fs instanceof HttpFSFileSystem) {
        HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
        httpFS.disallowSnapshot(path);
      } else if (fs instanceof WebHdfsFileSystem) {
        WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
        webHdfsFileSystem.disallowSnapshot(path);
      } else {
        Assert.fail(fs.getClass().getSimpleName() +
            " doesn't support disallowSnapshot");
      }
      // Check FileStatus
      assertFalse("disallowSnapshot failed",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Cleanup
      fs.delete(path, true);
    }
  }

  private void testDisallowSnapshotException() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory with snapshot allowed
      Path path = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(path);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      assertTrue("Snapshot should be allowed by DFS",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Create some snapshots
      fs.createSnapshot(path, "snap-01");
      fs.createSnapshot(path, "snap-02");
      // Disallow snapshot
      boolean disallowSuccess = false;
      if (fs instanceof HttpFSFileSystem) {
        HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
        try {
          httpFS.disallowSnapshot(path);
          disallowSuccess = true;
        } catch (SnapshotException e) {
          // Expect SnapshotException
        }
      } else if (fs instanceof WebHdfsFileSystem) {
        WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
        try {
          webHdfsFileSystem.disallowSnapshot(path);
          disallowSuccess = true;
        } catch (SnapshotException e) {
          // Expect SnapshotException
        }
      } else {
        Assert.fail(fs.getClass().getSimpleName() +
            " doesn't support disallowSnapshot");
      }
      if (disallowSuccess) {
        Assert.fail("disallowSnapshot doesn't throw SnapshotException when "
            + "disallowing snapshot on a directory with at least one snapshot");
      }
      // Check FileStatus, should still be enabled since
      // disallow snapshot should fail
      assertTrue("disallowSnapshot should not have succeeded",
          fs.getFileStatus(path).isSnapshotEnabled());
      // Cleanup
      fs.deleteSnapshot(path, "snap-02");
      fs.deleteSnapshot(path, "snap-01");
      fs.delete(path, true);
    }
  }

  private void testGetSnapshotDiff() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory with snapshot allowed
      Path path = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(path);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      Assert.assertTrue(fs.getFileStatus(path).isSnapshotEnabled());
      // Create a file and take a snapshot
      Path file1 = new Path(path, "file1");
      testCreate(file1, false);
      fs.createSnapshot(path, "snap1");
      // Create another file and take a snapshot
      Path file2 = new Path(path, "file2");
      testCreate(file2, false);
      fs.createSnapshot(path, "snap2");
      // Get snapshot diff
      SnapshotDiffReport diffReport = null;
      if (fs instanceof HttpFSFileSystem) {
        HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
        diffReport = httpFS.getSnapshotDiffReport(path, "snap1", "snap2");
      } else if (fs instanceof WebHdfsFileSystem) {
        WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
        diffReport = webHdfsFileSystem.getSnapshotDiffReport(path,
            "snap1", "snap2");
      } else {
        Assert.fail(fs.getClass().getSimpleName() +
            " doesn't support getSnapshotDiff");
      }
      // Verify result with DFS
      DistributedFileSystem dfs = (DistributedFileSystem)
          FileSystem.get(path.toUri(), this.getProxiedFSConf());
      SnapshotDiffReport dfsDiffReport =
          dfs.getSnapshotDiffReport(path, "snap1", "snap2");
      Assert.assertEquals(diffReport.toString(), dfsDiffReport.toString());
      // Cleanup
      fs.deleteSnapshot(path, "snap2");
      fs.deleteSnapshot(path, "snap1");
      fs.delete(path, true);
    }
  }

  private void testGetSnapshotDiffIllegalParamCase(FileSystem fs, Path path,
      String oldsnapshotname, String snapshotname) throws IOException {
    try {
      if (fs instanceof HttpFSFileSystem) {
        HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
        httpFS.getSnapshotDiffReport(path, oldsnapshotname, snapshotname);
      } else if (fs instanceof WebHdfsFileSystem) {
        WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
        webHdfsFileSystem.getSnapshotDiffReport(path, oldsnapshotname,
            snapshotname);
      } else {
        Assert.fail(fs.getClass().getSimpleName() +
            " doesn't support getSnapshotDiff");
      }
    } catch (SnapshotException|IllegalArgumentException|RemoteException e) {
      // Expect SnapshotException, IllegalArgumentException
      // or RemoteException(IllegalArgumentException)
      if (e instanceof RemoteException) {
        // Check RemoteException class name, should be IllegalArgumentException
        Assert.assertEquals(((RemoteException) e).getClassName()
            .compareTo(java.lang.IllegalArgumentException.class.getName()), 0);
      }
      return;
    }
    Assert.fail("getSnapshotDiff illegal param didn't throw Exception");
  }

  private void testGetSnapshotDiffIllegalParam() throws Exception {
    if (!this.isLocalFS()) {
      // Create a directory with snapshot allowed
      Path path = new Path("/tmp/tmp-snap-test");
      createSnapshotTestsPreconditions(path);
      // Get the FileSystem instance that's being tested
      FileSystem fs = this.getHttpFSFileSystem();
      // Check FileStatus
      assertTrue("Snapshot should be allowed by DFS",
          fs.getFileStatus(path).isSnapshotEnabled());
      Assert.assertTrue(fs.getFileStatus(path).isSnapshotEnabled());
      // Get snapshot diff
      testGetSnapshotDiffIllegalParamCase(fs, path, "", "");
      testGetSnapshotDiffIllegalParamCase(fs, path, "snap1", "");
      testGetSnapshotDiffIllegalParamCase(fs, path, "", "snap2");
      testGetSnapshotDiffIllegalParamCase(fs, path, "snap1", "snap2");
      // Cleanup
      fs.delete(path, true);
    }
  }

  private void verifyGetSnapshottableDirListing(
      FileSystem fs, DistributedFileSystem dfs) throws Exception {
    // Get snapshottable directory list
    SnapshottableDirectoryStatus[] sds = null;
    if (fs instanceof HttpFSFileSystem) {
      HttpFSFileSystem httpFS = (HttpFSFileSystem) fs;
      sds = httpFS.getSnapshottableDirectoryList();
    } else if (fs instanceof WebHdfsFileSystem) {
      WebHdfsFileSystem webHdfsFileSystem = (WebHdfsFileSystem) fs;
      sds = webHdfsFileSystem.getSnapshottableDirectoryList();
    } else {
      Assert.fail(fs.getClass().getSimpleName() +
          " doesn't support getSnapshottableDirListing");
    }
    // Verify result with DFS
    SnapshottableDirectoryStatus[] dfssds = dfs.getSnapshottableDirListing();
    Assert.assertEquals(JsonUtil.toJsonString(sds),
        JsonUtil.toJsonString(dfssds));
  }

  private void testGetSnapshottableDirListing() throws Exception {
    if (!this.isLocalFS()) {
      FileSystem fs = this.getHttpFSFileSystem();
      // Create directories with snapshot allowed
      Path path1 = new Path("/tmp/tmp-snap-dirlist-test-1");
      DistributedFileSystem dfs = (DistributedFileSystem)
          FileSystem.get(path1.toUri(), this.getProxiedFSConf());
      // Verify response when there is no snapshottable directory
      verifyGetSnapshottableDirListing(fs, dfs);
      createSnapshotTestsPreconditions(path1);
      Assert.assertTrue(fs.getFileStatus(path1).isSnapshotEnabled());
      // Verify response when there is one snapshottable directory
      verifyGetSnapshottableDirListing(fs, dfs);
      Path path2 = new Path("/tmp/tmp-snap-dirlist-test-2");
      createSnapshotTestsPreconditions(path2);
      Assert.assertTrue(fs.getFileStatus(path2).isSnapshotEnabled());
      // Verify response when there are two snapshottable directories
      verifyGetSnapshottableDirListing(fs, dfs);

      // Clean up and verify
      fs.delete(path2, true);
      verifyGetSnapshottableDirListing(fs, dfs);
      fs.delete(path1, true);
      verifyGetSnapshottableDirListing(fs, dfs);
    }
  }
}
