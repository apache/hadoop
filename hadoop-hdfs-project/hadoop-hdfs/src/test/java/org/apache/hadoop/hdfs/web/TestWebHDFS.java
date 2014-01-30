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

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertArrayEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestDFSClientRetries;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/** Test WebHDFS */
public class TestWebHDFS {
  static final Log LOG = LogFactory.getLog(TestWebHDFS.class);
  
  static final Random RANDOM = new Random();
  
  static final long systemStartTime = System.nanoTime();

  private static MiniDFSCluster testCluster;
  private static Configuration testConf;
  private static FileSystem testFs;
  private static int pathCount = 0;
  private static Path tmpPath;

  @BeforeClass
  public static void init() throws Exception {
    testConf = WebHdfsTestUtil.createConf();

    testCluster = new MiniDFSCluster.Builder(testConf).numDataNodes(1).build();
    testCluster.waitActive();
    testFs = WebHdfsTestUtil.getWebHdfsFileSystem(testConf, WebHdfsFileSystem.SCHEME);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, testFs);
    if (testCluster != null) {
      testCluster.shutdown();
    }
  }

  @Before
  public void setUp() {
    pathCount += 1;
    tmpPath = new Path("/p" + pathCount);
  }

  /** A timer for measuring performance. */
  static class Ticker {
    final String name;
    final long startTime = System.nanoTime();
    private long previousTick = startTime;

    Ticker(final String name, String format, Object... args) {
      this.name = name;
      LOG.info(String.format("\n\n%s START: %s\n",
          name, String.format(format, args)));
    }

    void tick(final long nBytes, String format, Object... args) {
      final long now = System.nanoTime();
      if (now - previousTick > 10000000000L) {
        previousTick = now;
        final double mintues = (now - systemStartTime)/60000000000.0;
        LOG.info(String.format("\n\n%s %.2f min) %s %s\n", name, mintues,
            String.format(format, args), toMpsString(nBytes, now)));
      }
    }
    
    void end(final long nBytes) {
      final long now = System.nanoTime();
      final double seconds = (now - startTime)/1000000000.0;
      LOG.info(String.format("\n\n%s END: duration=%.2fs %s\n",
          name, seconds, toMpsString(nBytes, now)));
    }
    
    String toMpsString(final long nBytes, final long now) {
      final double mb = nBytes/(double)(1<<20);
      final double mps = mb*1000000000.0/(now - startTime);
      return String.format("[nBytes=%.2fMB, speed=%.2fMB/s]", mb, mps);
    }
  }

  @Test(timeout=300000)
  public void testLargeFile() throws Exception {
    largeFileTest(200L << 20); //200MB file length
  }

  /** Test read and write large files. */
  static void largeFileTest(final long fileLength) throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    try {
      cluster.waitActive();

      final FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME);
      final Path dir = new Path("/test/largeFile");
      Assert.assertTrue(fs.mkdirs(dir));

      final byte[] data = new byte[1 << 20];
      RANDOM.nextBytes(data);

      final byte[] expected = new byte[2 * data.length];
      System.arraycopy(data, 0, expected, 0, data.length);
      System.arraycopy(data, 0, expected, data.length, data.length);

      final Path p = new Path(dir, "file");
      final Ticker t = new Ticker("WRITE", "fileLength=" + fileLength);
      final FSDataOutputStream out = fs.create(p);
      try {
        long remaining = fileLength;
        for(; remaining > 0;) {
          t.tick(fileLength - remaining, "remaining=%d", remaining);
          
          final int n = (int)Math.min(remaining, data.length);
          out.write(data, 0, n);
          remaining -= n;
        }
      } finally {
        out.close();
      }
      t.end(fileLength);
  
      Assert.assertEquals(fileLength, fs.getFileStatus(p).getLen());

      final long smallOffset = RANDOM.nextInt(1 << 20) + (1 << 20);
      final long largeOffset = fileLength - smallOffset;
      final byte[] buf = new byte[data.length];

      verifySeek(fs, p, largeOffset, fileLength, buf, expected);
      verifySeek(fs, p, smallOffset, fileLength, buf, expected);
  
      verifyPread(fs, p, largeOffset, fileLength, buf, expected);
    } finally {
      cluster.shutdown();
    }
  }

  static void checkData(long offset, long remaining, int n,
      byte[] actual, byte[] expected) {
    if (RANDOM.nextInt(100) == 0) {
      int j = (int)(offset % actual.length);
      for(int i = 0; i < n; i++) {
        if (expected[j] != actual[i]) {
          Assert.fail("expected[" + j + "]=" + expected[j]
              + " != actual[" + i + "]=" + actual[i]
              + ", offset=" + offset + ", remaining=" + remaining + ", n=" + n);
        }
        j++;
      }
    }
  }

  /** test seek */
  static void verifySeek(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException { 
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX SEEK: offset=" + offset + ", remaining=" + remaining);

    final Ticker t = new Ticker("SEEK", "offset=%d, remaining=%d",
        offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    in.seek(offset);
    for(; remaining > 0; ) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int)Math.min(remaining, buf.length);
      in.readFully(buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  static void verifyPread(FileSystem fs, Path p, long offset, long length,
      byte[] buf, byte[] expected) throws IOException {
    long remaining = length - offset;
    long checked = 0;
    LOG.info("XXX PREAD: offset=" + offset + ", remaining=" + remaining);

    final Ticker t = new Ticker("PREAD", "offset=%d, remaining=%d",
        offset, remaining);
    final FSDataInputStream in = fs.open(p, 64 << 10);
    for(; remaining > 0; ) {
      t.tick(checked, "offset=%d, remaining=%d", offset, remaining);
      final int n = (int)Math.min(remaining, buf.length);
      in.readFully(offset, buf, 0, n);
      checkData(offset, remaining, n, buf, expected);

      offset += n;
      remaining -= n;
      checked += n;
    }
    in.close();
    t.end(checked);
  }

  /** Test client retry with namenode restarting. */
  @Test(timeout=300000)
  public void testNamenodeRestart() throws Exception {
    ((Log4JLogger)NamenodeWebHdfsMethods.LOG).getLogger().setLevel(Level.ALL);
    final Configuration conf = WebHdfsTestUtil.createConf();
    TestDFSClientRetries.namenodeRestartTest(conf, true);
  }
  
  @Test(timeout=300000)
  public void testLargeDirectory() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    final int listLimit = 2;
    // force small chunking of directory listing
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, listLimit);
    // force paths to be only owner-accessible to ensure ugi isn't changing
    // during listStatus
    FsPermission.setUMask(conf, new FsPermission((short)0077));
    
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      // trick the NN into not believing it's not the superuser so we can
      // tell if the correct user is used by listStatus
      UserGroupInformation.setLoginUser(
          UserGroupInformation.createUserForTesting(
              "not-superuser", new String[]{"not-supergroup"}));

      UserGroupInformation.createUserForTesting("me", new String[]{"my-group"})
        .doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException, URISyntaxException {
              FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                  WebHdfsFileSystem.SCHEME);
              Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            for (int i=0; i < listLimit*3; i++) {
              Path p = new Path(d, "file-"+i);
              Assert.assertTrue(fs.createNewFile(p));
            }
            Assert.assertEquals(listLimit*3, fs.listStatus(d).length);
            return null;
          }
        });
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=300000)
  public void testNumericalUserName() throws Exception {
    final Configuration conf = WebHdfsTestUtil.createConf();
    conf.set(DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY, "^[A-Za-z0-9_][A-Za-z0-9._-]*[$]?$");
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      cluster.waitActive();
      WebHdfsTestUtil.getWebHdfsFileSystem(conf, WebHdfsFileSystem.SCHEME)
          .setPermission(new Path("/"),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      UserGroupInformation.createUserForTesting("123", new String[]{"my-group"})
        .doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException, URISyntaxException {
            FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(conf,
                WebHdfsFileSystem.SCHEME);
            Path d = new Path("/my-dir");
            Assert.assertTrue(fs.mkdirs(d));
            return null;
          }
        });
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * WebHdfs should be enabled by default after HDFS-5532
   * 
   * @throws Exception
   */
  @Test
  public void testWebHdfsEnabledByDefault() throws Exception {
    Configuration conf = new HdfsConfiguration();
    Assert.assertTrue(conf.getBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY,
        false));
  }
  
  @Test
  public void testModifyAclEntries() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    
    
  }

  @Test
  public void testModifyAclEntriesOnlyAccess() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
  }

  @Test
  public void testModifyAclEntriesOnlyDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testModifyAclEntriesMinimal() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_WRITE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testModifyAclEntriesMinimalDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testModifyAclEntriesCustomMask() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, MASK, NONE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testModifyAclEntriesStickyBit() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test(expected=FileNotFoundException.class)
  public void testModifyAclEntriesPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.modifyAclEntries(tmpPath, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testModifyAclEntriesDefaultOnFile() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.modifyAclEntries(tmpPath, aclSpec);
  }

  @Test
  public void testRemoveAclEntries() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(DEFAULT, USER, "foo"));
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testRemoveAclEntriesOnlyAccess() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, USER, "bar", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"));
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bar", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_WRITE) }, returned);
  }

  @Test
  public void testRemoveAclEntriesOnlyDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, USER, "bar", READ_EXECUTE));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo"));
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bar", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testRemoveAclEntriesMinimal() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(ACCESS, MASK));
    System.out.println("AclSpec :"+aclSpec);
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }


  @Test
  public void testRemoveAclEntriesMinimalDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(ACCESS, MASK),
      aclEntry(DEFAULT, USER, "foo"),
      aclEntry(DEFAULT, MASK));
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testRemoveAclEntriesStickyBit() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(DEFAULT, USER, "foo"));
    testFs.removeAclEntries(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclEntriesPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"));
    testFs.removeAclEntries(tmpPath, aclSpec);
  }

  @Test
  public void testRemoveDefaultAcl() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeDefaultAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
  }

  @Test
  public void testRemoveDefaultAclOnlyAccess() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeDefaultAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
  }

  @Test
  public void testRemoveDefaultAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeDefaultAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test
  public void testRemoveDefaultAclMinimal() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    testFs.removeDefaultAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test
  public void testRemoveDefaultAclStickyBit() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeDefaultAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveDefaultAclPathNotFound() throws IOException {
    // Path has not been created.
    testFs.removeDefaultAcl(tmpPath);
  }

  @Test
  public void testRemoveAcl() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test
  public void testRemoveAclMinimalAcl() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    testFs.removeAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test
  public void testRemoveAclStickyBit() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.removeAcl(tmpPath);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclPathNotFound() throws IOException {
    // Path has not been created.
    testFs.removeAcl(tmpPath);
  }

  @Test
  public void testSetAcl() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testSetAclOnlyAccess() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testSetAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testSetAclMinimal() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  @Test
  public void testSetAclMinimalDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testSetAclCustomMask() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testSetAclStickyBit() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test(expected=FileNotFoundException.class)
  public void testSetAclPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testSetAclDefaultOnFile() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
  }

  @Test
  public void testSetPermission() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short)0700));
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }

  @Test
  public void testSetPermissionOnlyAccess() throws IOException {
    testFs.create(tmpPath).close();
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short) 0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short)0600));
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testSetPermissionOnlyDefault() throws IOException {
    FileSystem.mkdirs(testFs, tmpPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    testFs.setAcl(tmpPath, aclSpec);
    testFs.setPermission(tmpPath, FsPermission.createImmutable((short)0700));
    AclStatus s = testFs.getAclStatus(tmpPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
  }
}
