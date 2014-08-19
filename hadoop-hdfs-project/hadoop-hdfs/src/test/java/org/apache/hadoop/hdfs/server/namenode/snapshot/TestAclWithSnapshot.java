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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.AclTestHelpers;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

/**
 * Tests interaction of ACLs with snapshots.
 */
public class TestAclWithSnapshot {
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { });

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fsAsBruce, fsAsDiana;
  private static DistributedFileSystem hdfs;
  private static int pathCount = 0;
  private static Path path, snapshotPath;
  private static String snapshotName;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    initCluster(true);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, hdfs, fsAsBruce, fsAsDiana);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() {
    ++pathCount;
    path = new Path("/p" + pathCount);
    snapshotName = "snapshot" + pathCount;
    snapshotPath = new Path(path, new Path(".snapshot", snapshotName));
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotRootAfterChange()
      throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);

    assertDirPermissionGranted(fsAsBruce, BRUCE, path);
    assertDirPermissionDenied(fsAsDiana, DIANA, path);

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    // Both original and snapshot still have same ACL.
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, path);

    s = hdfs.getAclStatus(snapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, snapshotPath);

    assertDirPermissionGranted(fsAsBruce, BRUCE, snapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, snapshotPath);

    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_EXECUTE),
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);

    // Original has changed, but snapshot still has old ACL.
    doSnapshotRootChangeAssertions(path, snapshotPath);
    restart(false);
    doSnapshotRootChangeAssertions(path, snapshotPath);
    restart(true);
    doSnapshotRootChangeAssertions(path, snapshotPath);
  }

  private static void doSnapshotRootChangeAssertions(Path path,
      Path snapshotPath) throws Exception {
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010550, path);

    s = hdfs.getAclStatus(snapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, snapshotPath);

    assertDirPermissionDenied(fsAsBruce, BRUCE, path);
    assertDirPermissionGranted(fsAsDiana, DIANA, path);
    assertDirPermissionGranted(fsAsBruce, BRUCE, snapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, snapshotPath);
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotContentsAfterChange()
      throws Exception {
    Path filePath = new Path(path, "file1");
    Path subdirPath = new Path(path, "subdir1");
    Path fileSnapshotPath = new Path(snapshotPath, "file1");
    Path subdirSnapshotPath = new Path(snapshotPath, "subdir1");
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0777));
    FileSystem.create(hdfs, filePath, FsPermission.createImmutable((short)0600))
      .close();
    FileSystem.mkdirs(hdfs, subdirPath, FsPermission.createImmutable(
      (short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_EXECUTE),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(filePath, aclSpec);
    hdfs.setAcl(subdirPath, aclSpec);

    assertFilePermissionGranted(fsAsBruce, BRUCE, filePath);
    assertFilePermissionDenied(fsAsDiana, DIANA, filePath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirPath);

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    // Both original and snapshot still have same ACL.
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) };
    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, filePath);

    s = hdfs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirPath);

    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, fileSnapshotPath);
    assertFilePermissionGranted(fsAsBruce, BRUCE, fileSnapshotPath);
    assertFilePermissionDenied(fsAsDiana, DIANA, fileSnapshotPath);

    s = hdfs.getAclStatus(subdirSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirSnapshotPath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirSnapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirSnapshotPath);

    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_EXECUTE),
      aclEntry(ACCESS, USER, "diana", ALL),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(filePath, aclSpec);
    hdfs.setAcl(subdirPath, aclSpec);

    // Original has changed, but snapshot still has old ACL.
    doSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
    restart(false);
    doSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
    restart(true);
    doSnapshotContentsChangeAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
  }

  private static void doSnapshotContentsChangeAssertions(Path filePath,
      Path fileSnapshotPath, Path subdirPath, Path subdirSnapshotPath)
      throws Exception {
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "diana", ALL),
      aclEntry(ACCESS, GROUP, NONE) };
    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010570, filePath);
    assertFilePermissionDenied(fsAsBruce, BRUCE, filePath);
    assertFilePermissionGranted(fsAsDiana, DIANA, filePath);

    s = hdfs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010570, subdirPath);
    assertDirPermissionDenied(fsAsBruce, BRUCE, subdirPath);
    assertDirPermissionGranted(fsAsDiana, DIANA, subdirPath);

    expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) };
    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, fileSnapshotPath);
    assertFilePermissionGranted(fsAsBruce, BRUCE, fileSnapshotPath);
    assertFilePermissionDenied(fsAsDiana, DIANA, fileSnapshotPath);

    s = hdfs.getAclStatus(subdirSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirSnapshotPath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirSnapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirSnapshotPath);
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotRootAfterRemoval()
      throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);

    assertDirPermissionGranted(fsAsBruce, BRUCE, path);
    assertDirPermissionDenied(fsAsDiana, DIANA, path);

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    // Both original and snapshot still have same ACL.
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, path);

    s = hdfs.getAclStatus(snapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, snapshotPath);

    assertDirPermissionGranted(fsAsBruce, BRUCE, snapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, snapshotPath);

    hdfs.removeAcl(path);

    // Original has changed, but snapshot still has old ACL.
    doSnapshotRootRemovalAssertions(path, snapshotPath);
    restart(false);
    doSnapshotRootRemovalAssertions(path, snapshotPath);
    restart(true);
    doSnapshotRootRemovalAssertions(path, snapshotPath);
  }

  private static void doSnapshotRootRemovalAssertions(Path path,
      Path snapshotPath) throws Exception {
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0700, path);

    s = hdfs.getAclStatus(snapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010750, snapshotPath);

    assertDirPermissionDenied(fsAsBruce, BRUCE, path);
    assertDirPermissionDenied(fsAsDiana, DIANA, path);
    assertDirPermissionGranted(fsAsBruce, BRUCE, snapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, snapshotPath);
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotContentsAfterRemoval()
      throws Exception {
    Path filePath = new Path(path, "file1");
    Path subdirPath = new Path(path, "subdir1");
    Path fileSnapshotPath = new Path(snapshotPath, "file1");
    Path subdirSnapshotPath = new Path(snapshotPath, "subdir1");
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0777));
    FileSystem.create(hdfs, filePath, FsPermission.createImmutable((short)0600))
      .close();
    FileSystem.mkdirs(hdfs, subdirPath, FsPermission.createImmutable(
      (short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_EXECUTE),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(filePath, aclSpec);
    hdfs.setAcl(subdirPath, aclSpec);

    assertFilePermissionGranted(fsAsBruce, BRUCE, filePath);
    assertFilePermissionDenied(fsAsDiana, DIANA, filePath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirPath);

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    // Both original and snapshot still have same ACL.
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) };
    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, filePath);

    s = hdfs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirPath);

    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, fileSnapshotPath);
    assertFilePermissionGranted(fsAsBruce, BRUCE, fileSnapshotPath);
    assertFilePermissionDenied(fsAsDiana, DIANA, fileSnapshotPath);

    s = hdfs.getAclStatus(subdirSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirSnapshotPath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirSnapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirSnapshotPath);

    hdfs.removeAcl(filePath);
    hdfs.removeAcl(subdirPath);

    // Original has changed, but snapshot still has old ACL.
    doSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
    restart(false);
    doSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
    restart(true);
    doSnapshotContentsRemovalAssertions(filePath, fileSnapshotPath, subdirPath,
      subdirSnapshotPath);
  }

  private static void doSnapshotContentsRemovalAssertions(Path filePath,
      Path fileSnapshotPath, Path subdirPath, Path subdirSnapshotPath)
      throws Exception {
    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)0500, filePath);
    assertFilePermissionDenied(fsAsBruce, BRUCE, filePath);
    assertFilePermissionDenied(fsAsDiana, DIANA, filePath);

    s = hdfs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)0500, subdirPath);
    assertDirPermissionDenied(fsAsBruce, BRUCE, subdirPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirPath);

    expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) };
    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, fileSnapshotPath);
    assertFilePermissionGranted(fsAsBruce, BRUCE, fileSnapshotPath);
    assertFilePermissionDenied(fsAsDiana, DIANA, fileSnapshotPath);

    s = hdfs.getAclStatus(subdirSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010550, subdirSnapshotPath);
    assertDirPermissionGranted(fsAsBruce, BRUCE, subdirSnapshotPath);
    assertDirPermissionDenied(fsAsDiana, DIANA, subdirSnapshotPath);
  }

  @Test
  public void testModifyReadsCurrentState() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", ALL));
    hdfs.modifyAclEntries(path, aclSpec);

    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE));
    hdfs.modifyAclEntries(path, aclSpec);

    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", ALL),
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE) };
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)010770, path);
    assertDirPermissionGranted(fsAsBruce, BRUCE, path);
    assertDirPermissionGranted(fsAsDiana, DIANA, path);
  }

  @Test
  public void testRemoveReadsCurrentState() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", ALL));
    hdfs.modifyAclEntries(path, aclSpec);

    hdfs.removeAcl(path);

    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission((short)0700, path);
    assertDirPermissionDenied(fsAsBruce, BRUCE, path);
    assertDirPermissionDenied(fsAsDiana, DIANA, path);
  }

  @Test
  public void testDefaultAclNotCopiedToAccessAclOfNewSnapshot()
      throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    hdfs.modifyAclEntries(path, aclSpec);

    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);

    AclStatus s = hdfs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, NONE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010700, path);

    s = hdfs.getAclStatus(snapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, NONE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010700, snapshotPath);

    assertDirPermissionDenied(fsAsBruce, BRUCE, snapshotPath);
  }

  @Test
  public void testModifyAclEntriesSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.modifyAclEntries(snapshotPath, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce"));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeAclEntries(snapshotPath, aclSpec);
  }

  @Test
  public void testRemoveDefaultAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeDefaultAcl(snapshotPath);
  }

  @Test
  public void testRemoveAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeAcl(snapshotPath);
  }

  @Test
  public void testSetAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce"));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.setAcl(snapshotPath, aclSpec);
  }

  @Test
  public void testChangeAclExceedsQuota() throws Exception {
    Path filePath = new Path(path, "file1");
    Path fileSnapshotPath = new Path(snapshotPath, "file1");
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0755));
    hdfs.allowSnapshot(path);
    hdfs.setQuota(path, 3, HdfsConstants.QUOTA_DONT_SET);
    FileSystem.create(hdfs, filePath, FsPermission.createImmutable((short)0600))
      .close();
    hdfs.setPermission(filePath, FsPermission.createImmutable((short)0600));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_WRITE));
    hdfs.modifyAclEntries(filePath, aclSpec);

    hdfs.createSnapshot(path, snapshotName);

    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010660, filePath);

    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010660, filePath);

    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ));
    exception.expect(NSQuotaExceededException.class);
    hdfs.modifyAclEntries(filePath, aclSpec);
  }

  @Test
  public void testRemoveAclExceedsQuota() throws Exception {
    Path filePath = new Path(path, "file1");
    Path fileSnapshotPath = new Path(snapshotPath, "file1");
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0755));
    hdfs.allowSnapshot(path);
    hdfs.setQuota(path, 3, HdfsConstants.QUOTA_DONT_SET);
    FileSystem.create(hdfs, filePath, FsPermission.createImmutable((short)0600))
      .close();
    hdfs.setPermission(filePath, FsPermission.createImmutable((short)0600));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ_WRITE));
    hdfs.modifyAclEntries(filePath, aclSpec);

    hdfs.createSnapshot(path, snapshotName);

    AclStatus s = hdfs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010660, filePath);

    s = hdfs.getAclStatus(fileSnapshotPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bruce", READ_WRITE),
      aclEntry(ACCESS, GROUP, NONE) }, returned);
    assertPermission((short)010660, filePath);

    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "bruce", READ));
    exception.expect(NSQuotaExceededException.class);
    hdfs.removeAcl(filePath);
  }

  @Test
  public void testGetAclStatusDotSnapshotPath() throws Exception {
    hdfs.mkdirs(path);
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    AclStatus s = hdfs.getAclStatus(new Path(path, ".snapshot"));
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
  }

  /**
   * Asserts that permission is denied to the given fs/user for the given
   * directory.
   *
   * @param fs FileSystem to check
   * @param user UserGroupInformation owner of fs
   * @param pathToCheck Path directory to check
   * @throws Exception if there is an unexpected error
   */
  private static void assertDirPermissionDenied(FileSystem fs,
      UserGroupInformation user, Path pathToCheck) throws Exception {
    try {
      fs.listStatus(pathToCheck);
      fail("expected AccessControlException for user " + user + ", path = " +
        pathToCheck);
    } catch (AccessControlException e) {
      // expected
    }

    try {
      fs.access(pathToCheck, FsAction.READ);
      fail("The access call should have failed for "+pathToCheck);
    } catch (AccessControlException e) {
      // expected
    }
  }

  /**
   * Asserts that permission is granted to the given fs/user for the given
   * directory.
   *
   * @param fs FileSystem to check
   * @param user UserGroupInformation owner of fs
   * @param pathToCheck Path directory to check
   * @throws Exception if there is an unexpected error
   */
  private static void assertDirPermissionGranted(FileSystem fs,
      UserGroupInformation user, Path pathToCheck) throws Exception {
    try {
      fs.listStatus(pathToCheck);
      fs.access(pathToCheck, FsAction.READ);
    } catch (AccessControlException e) {
      fail("expected permission granted for user " + user + ", path = " +
        pathToCheck);
    }
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of the test path.
   *
   * @param perm short expected permission bits
   * @param pathToCheck Path to check
   * @throws Exception thrown if there is an unexpected error
   */
  private static void assertPermission(short perm, Path pathToCheck)
      throws Exception {
    AclTestHelpers.assertPermission(hdfs, pathToCheck, perm);
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem
   * instances for our test users.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @throws Exception if any step fails
   */
  private static void initCluster(boolean format) throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
    fsAsBruce = DFSTestUtil.getFileSystemAs(BRUCE, conf);
    fsAsDiana = DFSTestUtil.getFileSystemAs(DIANA, conf);
  }

  /**
   * Restart the cluster, optionally saving a new checkpoint.
   *
   * @param checkpoint boolean true to save a new checkpoint
   * @throws Exception if restart fails
   */
  private static void restart(boolean checkpoint) throws Exception {
    NameNode nameNode = cluster.getNameNode();
    if (checkpoint) {
      NameNodeAdapter.enterSafeMode(nameNode, false);
      NameNodeAdapter.saveNamespace(nameNode);
    }
    shutdown();
    initCluster(false);
  }
}
