/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SNAPSHOT_DELETED;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.junit.Assert.assertTrue;

/**
 * Test ordered snapshot deletion.
 */
public class TestOrderedSnapshotDeletion {
  static final String xattrName = "user.a1";
  static final byte[] xattrValue = {0x31, 0x32, 0x33};
  private final Path snapshottableDir
      = new Path("/" + getClass().getSimpleName());

  private MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, true);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testOrderedSnapshotDeletion() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");

    final Path sub2 = new Path(snapshottableDir, "sub2");
    hdfs.mkdirs(sub2);
    hdfs.createSnapshot(snapshottableDir, "s2");

    assertXAttrSet("s1", hdfs, null);
    assertXAttrSet("s2", hdfs, null);
    hdfs.deleteSnapshot(snapshottableDir, "s0");
    assertXAttrSet("s2", hdfs, null);
    hdfs.deleteSnapshot(snapshottableDir,
        getDeletedSnapshotName(hdfs, snapshottableDir, "s1"));
    hdfs.deleteSnapshot(snapshottableDir,
        getDeletedSnapshotName(hdfs, snapshottableDir, "s2"));
  }

  static void assertMarkedAsDeleted(Path snapshotRoot, Path snapshottableDir,
      MiniDFSCluster cluster) throws IOException {
    final String snapName =
        getDeletedSnapshotName(cluster.getFileSystem(), snapshottableDir,
            snapshotRoot.getName());
    final Path snapPathNew =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapName);
    // Check if the path exists
    Assert.assertNotNull(cluster.getFileSystem().getFileStatus(snapPathNew));

    // Check xAttr for snapshotRoot
    final INode inode = cluster.getNamesystem().getFSDirectory()
        .getINode(snapPathNew.toString());
    final XAttrFeature f = inode.getXAttrFeature();
    final XAttr xAttr = f.getXAttr(XATTR_SNAPSHOT_DELETED);
    Assert.assertNotNull(xAttr);
    Assert.assertEquals(XATTR_SNAPSHOT_DELETED.substring("system.".length()),
        xAttr.getName());
    Assert.assertEquals(XAttr.NameSpace.SYSTEM, xAttr.getNameSpace());
    Assert.assertNull(xAttr.getValue());

    // Check inode
    Assert.assertTrue(inode instanceof Snapshot.Root);
    Assert.assertTrue(((Snapshot.Root) inode).isMarkedAsDeleted());
  }

  static void assertNotMarkedAsDeleted(Path snapshotRoot,
      MiniDFSCluster cluster) throws IOException {
    // Check if the path exists
    Assert.assertNotNull(cluster.getFileSystem().getFileStatus(snapshotRoot));

    // Check xAttr for snapshotRoot
    final INode inode = cluster.getNamesystem().getFSDirectory()
        .getINode(snapshotRoot.toString());
    final XAttrFeature f = inode.getXAttrFeature();
    if (f != null) {
      final XAttr xAttr = f.getXAttr(XATTR_SNAPSHOT_DELETED);
      Assert.assertNull(xAttr);
    }

    // Check inode
    Assert.assertTrue(inode instanceof Snapshot.Root);
    Assert.assertFalse(((Snapshot.Root)inode).isMarkedAsDeleted());
  }

  void assertXAttrSet(String snapshot,
                      DistributedFileSystem hdfs, XAttr newXattr)
      throws IOException {
    String snapName = getDeletedSnapshotName(hdfs, snapshottableDir, snapshot);
    hdfs.deleteSnapshot(snapshottableDir, snapName);
    // Check xAttr for parent directory
    Path snapshotRoot =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapshot);
    assertMarkedAsDeleted(snapshotRoot, snapshottableDir, cluster);
    // Check xAttr for parent directory
    snapName = getDeletedSnapshotName(hdfs, snapshottableDir, snapshot);
    snapshotRoot =
        SnapshotTestHelper.getSnapshotRoot(snapshottableDir, snapName);
    // Make sure its not user visible
    if (cluster.getNameNode().getConf().getBoolean(DFSConfigKeys.
            DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT)) {
      Map<String, byte[]> xattrMap = hdfs.getXAttrs(snapshotRoot);
      assertTrue(newXattr == null ? xattrMap.isEmpty() :
          Arrays.equals(newXattr.getValue(), xattrMap.get(xattrName)));
    }
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrPersistence() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    assertXAttrSet("s1", hdfs, null);
    cluster.restartNameNodes();
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrWithSaveNameSpace() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    hdfs.setSafeMode(SafeModeAction.ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(SafeModeAction.LEAVE);
    cluster.restartNameNodes();
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 6000000)
  public void testOrderedDeletionWithRestart() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    assertXAttrSet("s1", hdfs, null);
    cluster.getNameNode().getConf().
        setBoolean(DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED, false);
    cluster.restartNameNodes();
  }

  @Test(timeout = 60000)
  public void testSnapshotXattrWithDisablingXattr() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);

    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, null);
    cluster.getNameNode().getConf().setBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, false);
    cluster.restartNameNodes();
    // ensure xAttr feature is disabled
    try {
      hdfs.getXAttrs(snapshottableDir);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("The XAttr operation has been " +
          "rejected.  Support for XAttrs has been disabled by " +
          "setting dfs.namenode.xattrs.enabled to false"));
    }
    // try deleting snapshot and verify it still sets the snapshot XAttr
    assertXAttrSet("s1", hdfs, null);
  }

  @Test(timeout = 60000)
  public void testSnapshotXAttrWithPreExistingXattrs() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);
    hdfs.setXAttr(snapshottableDir, xattrName, xattrValue,
        EnumSet.of(XAttrSetFlag.CREATE));
    XAttr newXAttr = XAttrHelper.buildXAttr(xattrName, xattrValue);
    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, newXAttr);
  }

  public static String getDeletedSnapshotName(DistributedFileSystem hdfs,
      Path snapshottableDir, String snapshot) throws IOException {
    return Arrays.stream(hdfs.getSnapshotListing(snapshottableDir))
        .filter(p -> p.getFullPath().getName().startsWith(snapshot)).findFirst()
        .get().getFullPath().getName();
  }
}
