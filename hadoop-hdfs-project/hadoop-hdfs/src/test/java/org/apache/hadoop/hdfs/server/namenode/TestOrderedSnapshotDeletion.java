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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test ordered snapshot deletion.
 */
public class TestOrderedSnapshotDeletion {
  static final String name1 = "user.a1";
  static final byte[] value1 = {0x31, 0x32, 0x33};
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
  public void testConf() throws Exception {
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
    hdfs.deleteSnapshot(snapshottableDir, "s1");
    hdfs.deleteSnapshot(snapshottableDir, "s2");
  }

  void assertXAttrSet(String snapshot,
                      DistributedFileSystem hdfs, XAttr newXattr)
      throws IOException {
    hdfs.deleteSnapshot(snapshottableDir, snapshot);
    // Check xAttr for parent directory
    FSNamesystem namesystem = cluster.getNamesystem();
    INode inode = namesystem.getFSDirectory().getINode(
        snapshottableDir.toString());
    XAttrFeature f = inode.getXAttrFeature();
    XAttr xAttr = f.getXAttr(FSDirSnapshotOp.buildXAttrName(snapshot));
    assertTrue("Snapshot xAttr should exist", xAttr != null);
    assertTrue(xAttr.getName().contains(snapshot));
    assertTrue(xAttr.getNameSpace().equals(XAttr.NameSpace.SYSTEM));
    assertNull(xAttr.getValue());

    // Make sure its not user visible
    Map<String, byte[]> xattrMap = hdfs.getXAttrs(snapshottableDir);
    assertTrue(newXattr == null ? xattrMap.isEmpty() :
        Arrays.equals(newXattr.getValue(), xattrMap.get(name1)));
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
    hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
    hdfs.saveNamespace();
    hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNodes();
    assertXAttrSet("s1", hdfs, null);
  }
  
  @Test(timeout = 60000)
  public void testSnapshotXAttrWithPreExistingXattrs() throws Exception {
    DistributedFileSystem hdfs = cluster.getFileSystem();
    hdfs.mkdirs(snapshottableDir);
    hdfs.allowSnapshot(snapshottableDir);
    hdfs.setXAttr(snapshottableDir, name1, value1,
        EnumSet.of(XAttrSetFlag.CREATE));
    XAttr newXAttr = XAttrHelper.buildXAttr(name1, value1);
    final Path sub0 = new Path(snapshottableDir, "sub0");
    hdfs.mkdirs(sub0);
    hdfs.createSnapshot(snapshottableDir, "s0");

    final Path sub1 = new Path(snapshottableDir, "sub1");
    hdfs.mkdirs(sub1);
    hdfs.createSnapshot(snapshottableDir, "s1");
    assertXAttrSet("s1", hdfs, newXAttr);
  }
}
