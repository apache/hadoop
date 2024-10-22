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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.assertPermission;

public class TestSerialFSImage {
  static final long SEED = 0;
  static final int BLOCK_SIZE = 1024;
  private static final String NAME1 = "user.a1";
  private static final byte[] VALUE1 = {0x31, 0x32, 0x33};
  private static final byte[] NEW_VALUE1 = {0x31, 0x31, 0x31};
  private static final String NAME2 = "user.a2";
  private static final byte[] VALUE2 = {0x37, 0x38, 0x39};
  private static final String NAME3 = "user.a3";
  private static final byte[] VALUE3 = {};
  private final Path snapshotDir = new Path("/TestSnapshot");
  private final Path aclFile = new Path("/testAcl");
  private final Path xattrFile = new Path("/testXAttr");
  private final String testDir = GenericTestUtils.getTestDir().getAbsolutePath();
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem fsn;
  private DistributedFileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_SAVE_STRING_TABLE_STRUCTURE_KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Restart the NameNode, and saving a new checkpoint.
   * @param hdfs DistributedFileSystem used for saving namespace
   * @throws IOException if restart fails
   */
  private void restart(DistributedFileSystem hdfs) throws IOException {
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);

    cluster.restartNameNode();
    cluster.waitActive();
  }

  /**
   * Create a temp file for dumping the fsdir.
   * @param dir directory for the temp file
   * @param suffix suffix of the temp file
   * @return the temp file
   */
  private File getDumpTreeFile(String dir, String suffix) {
    return new File(dir, String.format("dumpTree_%s", suffix));
  }

  /**
   * Dump the fsdir tree to a temp file.
   * @param fileSuffix suffix of the temp file for dumping
   * @return the temp file
   */
  private File dumpTree2File(String fileSuffix) throws IOException {
    File file = getDumpTreeFile(testDir, fileSuffix);
    SnapshotTestHelper.dumpTree2File(fsn.getFSDirectory(), file);
    return file;
  }

  /**
   * Create a temp fsimage file for testing.
   * @param dir The directory where the fsimage file resides
   * @param imageTxId The transaction id of the fsimage
   * @return The file of the image file
   */
  private File getImageFile(String dir, long imageTxId) {
    return new File(dir, String.format("%s_%019d", NNStorage.NameNodeFile.IMAGE, imageTxId));
  }

  /**
   * Load the fsimage from a temp file.
   * @param imageFile the fsimage to load
   */
  private void loadFSImageFromTempFile(File imageFile) throws IOException {
    FSImageFormat.LoaderDelegator loader = FSImageFormat.newLoader(conf, fsn);
    fsn.writeLock();
    fsn.getFSDirectory().writeLock();
    try {
      loader.load(imageFile, false);
      fsn.getFSDirectory().updateCountForQuota();
    } finally {
      fsn.getFSDirectory().writeUnlock();
      fsn.writeUnlock();
    }
  }

  /**
   * Save the fsimage to a temp file.
   * @return the temp file
   */
  private File saveFSImageToTempFile() throws IOException {
    SaveNamespaceContext context = new SaveNamespaceContext(fsn, 1, new Canceler());
    FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context, conf);
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    File imageFile = getImageFile(testDir, 1);
    fsn.readLock();
    try {
      saver.save(imageFile, compression);
    } finally {
      fsn.readUnlock();
    }
    return imageFile;
  }

  void checkImage(int s) throws IOException {
    final String name = "s" + s;

    // dump the fsdir tree
    File fsnBefore = dumpTree2File(name + "_before");

    // save the namesystem to a temp file
    File imageFile = saveFSImageToTempFile();

    long numSdirBefore = fsn.getNumSnapshottableDirs();
    long numSnapshotBefore = fsn.getNumSnapshots();
    SnapshottableDirectoryStatus[] dirBefore = fs.getSnapshottableDirListing();

    // shutdown the cluster
    cluster.shutdown();

    // dump the fsdir tree
    File fsnBetween = dumpTree2File(name + "_between");
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnBetween, true);

    // restart the cluster, and format the cluster
    cluster = new MiniDFSCluster.Builder(conf).format(true)
            .numDataNodes(1).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    fs = cluster.getFileSystem();

    // load the namesystem from the temp file
    loadFSImageFromTempFile(imageFile);

    // dump the fsdir tree again
    File fsnAfter = dumpTree2File(name + "_after");

    // compare two dumped tree
    SnapshotTestHelper.compareDumpedTreeInFile(fsnBefore, fsnAfter, true);

    long numSdirAfter = fsn.getNumSnapshottableDirs();
    long numSnapshotAfter = fsn.getNumSnapshots();
    SnapshottableDirectoryStatus[] dirAfter = fs.getSnapshottableDirListing();

    Assert.assertEquals(numSdirBefore, numSdirAfter);
    Assert.assertEquals(numSnapshotBefore, numSnapshotAfter);
    Assert.assertEquals(dirBefore.length, dirAfter.length);
    List<String> pathListBefore = new ArrayList<String>();
    for (SnapshottableDirectoryStatus sBefore : dirBefore) {
      pathListBefore.add(sBefore.getFullPath().toString());
    }
    for (SnapshottableDirectoryStatus sAfter : dirAfter) {
      Assert.assertTrue(pathListBefore.contains(sAfter.getFullPath().toString()));
    }
  }

  /**
   * Testing steps:
   * <pre>
   * 1. Creating/modifying directories/files while snapshots are being taken.
   * 2. Dump the FSDirectory tree of the namesystem.
   * 3. Save the namesystem to a temp file (FSImage saving).
   * 4. Restart the cluster and format the namesystem.
   * 5. Load the namesystem from the temp file (FSImage loading).
   * 6. Dump the FSDirectory again and compare the two dumped string.
   * </pre>
   */
  @Test
  public void testSaveAndLoadSerialImage() throws Exception {
    int s = 0;
    // make changes to the namesystem
    fs.mkdirs(snapshotDir);
    SnapshotTestHelper.createSnapshot(fs, snapshotDir, "s" + ++s);
    Path sub1 = new Path(snapshotDir, "sub1");
    fs.mkdirs(sub1);
    fs.setPermission(sub1, new FsPermission((short)0777));
    Path sub11 = new Path(sub1, "sub11");
    fs.mkdirs(sub11);
    checkImage(s);

    fs.createSnapshot(snapshotDir, "s" + ++s);
    Path sub1File1 = new Path(sub1, "sub1file1");
    Path sub1File2 = new Path(sub1, "sub1file2");
    DFSTestUtil.createFile(fs, sub1File1, BLOCK_SIZE, (short) 1, SEED);
    DFSTestUtil.createFile(fs, sub1File2, BLOCK_SIZE, (short) 1, SEED);
    checkImage(s);

    fs.createSnapshot(snapshotDir, "s" + ++s);
    Path sub2 = new Path(snapshotDir, "sub2");
    Path sub2File1 = new Path(sub2, "sub2file1");
    Path sub2File2 = new Path(sub2, "sub2file2");
    DFSTestUtil.createFile(fs, sub2File1, BLOCK_SIZE, (short) 1, SEED);
    DFSTestUtil.createFile(fs, sub2File2, BLOCK_SIZE, (short) 1, SEED);
    checkImage(s);

    fs.createSnapshot(snapshotDir, "s" + ++s);
    fs.setReplication(sub1File1, (short) 1);
    fs.delete(sub1File2, true);
    fs.setOwner(sub2, "dr.who", "unknown");
    fs.delete(sub2File1, true);
    checkImage(s);

    fs.createSnapshot(snapshotDir, "s" + ++s);
    Path sub1Sub2File2 = new Path(sub1, "sub2file2");
    fs.rename(sub2File2, sub1Sub2File2);

    fs.rename(sub1File1, sub2File1);
    checkImage(s);

    fs.rename(sub2File1, sub2File2);
    checkImage(s);
  }

  private void testPersistAcl() throws IOException {
    fs.create(aclFile).close();
    fs.mkdirs(new Path("/HDFS-17463"));

    AclEntry e = new AclEntry.Builder().setName("foo")
        .setPermission(READ_EXECUTE).setScope(ACCESS).setType(USER).build();
    fs.modifyAclEntries(aclFile, Lists.newArrayList(e));

    restart(fs);

    AclStatus s = cluster.getNamesystem().getAclStatus(aclFile.toString());
    AclEntry[] returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ) }, returned);

    fs.removeAcl(aclFile);
    restart(fs);

    s = cluster.getNamesystem().getAclStatus(aclFile.toString());
    returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {}, returned);

    fs.modifyAclEntries(aclFile, Lists.newArrayList(e));
    s = cluster.getNamesystem().getAclStatus(aclFile.toString());
    returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  private void testDefaultAclNewChildren() throws IOException {
    Path dirPath = new Path("/dir");
    Path filePath = new Path(dirPath, "file1");
    Path subdirPath = new Path(dirPath, "subdir1");
    fs.mkdirs(dirPath);
    List<AclEntry> aclSpec = Lists.newArrayList(
            aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(dirPath, aclSpec);

    fs.create(filePath).close();
    fs.mkdirs(subdirPath);

    AclEntry[] fileExpected = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    AclEntry[] subdirExpected = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE) };

    short permExpected = (short)010775;

    AclEntry[] fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    AclEntry[] subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);

    restart(fs);

    fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);

    aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "foo", READ_WRITE));
    fs.modifyAclEntries(dirPath, aclSpec);

    fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);

    restart(fs);

    fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);

    fs.removeAcl(dirPath);

    fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);

    restart(fs);

    fileReturned = fs.getAclStatus(filePath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries().toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, permExpected);
  }

  private void testRootACLAfterLoadingFsImage() throws IOException {
    Path rootDir = new Path("/");
    AclEntry e1 = new AclEntry.Builder().setName("foo")
            .setPermission(ALL).setScope(ACCESS).setType(GROUP).build();
    AclEntry e2 = new AclEntry.Builder().setName("bar")
            .setPermission(READ).setScope(ACCESS).setType(GROUP).build();
    fs.modifyAclEntries(rootDir, Lists.newArrayList(e1, e2));

    AclStatus s = cluster.getNamesystem().getAclStatus(rootDir.toString());
    AclEntry[] returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "bar", READ),
        aclEntry(ACCESS, GROUP, "foo", ALL) },
        returned);

    // restart - hence save and load from fsimage
    restart(fs);

    s = cluster.getNamesystem().getAclStatus(rootDir.toString());
    returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "bar", READ),
        aclEntry(ACCESS, GROUP, "foo", ALL) },
        returned);
  }

  @Test
  public void testAcl() throws IOException {
    testPersistAcl();
    testDefaultAclNewChildren();
    testRootACLAfterLoadingFsImage();
  }

  private void testPersistXAttr() throws IOException {
    fs.create(xattrFile).close();

    fs.setXAttr(xattrFile, NAME1, VALUE1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(xattrFile, NAME2, VALUE2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(xattrFile, NAME3, null, EnumSet.of(XAttrSetFlag.CREATE));

    restart(fs);

    Map<String, byte[]> xattrs = fs.getXAttrs(xattrFile);
    Assert.assertEquals(xattrs.size(), 3);
    Assert.assertArrayEquals(VALUE1, xattrs.get(NAME1));
    Assert.assertArrayEquals(VALUE2, xattrs.get(NAME2));
    Assert.assertArrayEquals(VALUE3, xattrs.get(NAME3));

    fs.setXAttr(xattrFile, NAME1, NEW_VALUE1, EnumSet.of(XAttrSetFlag.REPLACE));

    restart(fs);

    xattrs = fs.getXAttrs(xattrFile);
    Assert.assertEquals(xattrs.size(), 3);
    Assert.assertArrayEquals(NEW_VALUE1, xattrs.get(NAME1));
    Assert.assertArrayEquals(VALUE2, xattrs.get(NAME2));
    Assert.assertArrayEquals(VALUE3, xattrs.get(NAME3));

    fs.removeXAttr(xattrFile, NAME1);
    fs.removeXAttr(xattrFile, NAME2);
    fs.removeXAttr(xattrFile, NAME3);

    restart(fs);
    xattrs = fs.getXAttrs(xattrFile);
    Assert.assertEquals(xattrs.size(), 0);
  }

  @Test
  public void testXAttr() throws IOException {
    testPersistXAttr();
  }
}
