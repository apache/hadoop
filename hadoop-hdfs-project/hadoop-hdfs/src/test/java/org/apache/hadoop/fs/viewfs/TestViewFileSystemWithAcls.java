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
package org.apache.hadoop.fs.viewfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Verify ACL through ViewFileSystem functionality.
 */
public class TestViewFileSystemWithAcls {

  private static MiniDFSCluster cluster;
  private static Configuration clusterConf = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem fHdfs2;
  private FileSystem fsView;
  private Configuration fsViewConf;
  private FileSystem fsTarget, fsTarget2;
  private Path targetTestRoot, targetTestRoot2, mountOnNn1, mountOnNn2;
  private FileSystemTestHelper fileSystemTestHelper =
      new FileSystemTestHelper("/tmp/TestViewFileSystemWithAcls");

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException {
    clusterConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(clusterConf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .numDataNodes(2)
        .build();
    cluster.waitClusterUp();

    fHdfs = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = fHdfs;
    fsTarget2 = fHdfs2;
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    targetTestRoot2 = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget2);

    fsTarget.delete(targetTestRoot, true);
    fsTarget2.delete(targetTestRoot2, true);
    fsTarget.mkdirs(targetTestRoot);
    fsTarget2.mkdirs(targetTestRoot2);

    fsViewConf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, fsViewConf);
  }

  private void setupMountPoints() {
    mountOnNn1 = new Path("/mountOnNn1");
    mountOnNn2 = new Path("/mountOnNn2");
    ConfigUtil.addLink(fsViewConf, mountOnNn1.toString(), targetTestRoot.toUri());
    ConfigUtil.addLink(fsViewConf, mountOnNn2.toString(), targetTestRoot2.toUri());
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    fsTarget2.delete(fileSystemTestHelper.getTestRootPath(fsTarget2), true);
  }

  /**
   * Verify a ViewFs wrapped over multiple federated NameNodes will
   * dispatch the ACL operations to the correct NameNode.
   */
  @Test
  public void testAclOnMountEntry() throws Exception {
    // Set ACLs on the first namespace and verify they are correct
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, "foo", READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE));
    fsView.setAcl(mountOnNn1, aclSpec);

    AclEntry[] expected = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ),
        aclEntry(ACCESS, GROUP, READ) };
    assertArrayEquals(expected,  aclEntryArray(fsView.getAclStatus(mountOnNn1)));
    // Double-check by getting ACL status using FileSystem
    // instead of ViewFs
    assertArrayEquals(expected, aclEntryArray(fHdfs.getAclStatus(targetTestRoot)));

    // Modify the ACL entries on the first namespace
    aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", READ));
    fsView.modifyAclEntries(mountOnNn1, aclSpec);
    expected = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, "foo", READ),
        aclEntry(DEFAULT, GROUP, READ),
        aclEntry(DEFAULT, MASK, READ),
        aclEntry(DEFAULT, OTHER, NONE) };
    assertArrayEquals(expected, aclEntryArray(fsView.getAclStatus(mountOnNn1)));

    fsView.removeDefaultAcl(mountOnNn1);
    expected = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ),
        aclEntry(ACCESS, GROUP, READ) };
    assertArrayEquals(expected, aclEntryArray(fsView.getAclStatus(mountOnNn1)));
    assertArrayEquals(expected, aclEntryArray(fHdfs.getAclStatus(targetTestRoot)));

    // Paranoid check: verify the other namespace does not
    // have ACLs set on the same path.
    assertEquals(0, fsView.getAclStatus(mountOnNn2).getEntries().size());
    assertEquals(0, fHdfs2.getAclStatus(targetTestRoot2).getEntries().size());

    // Remove the ACL entries on the first namespace
    fsView.removeAcl(mountOnNn1);
    assertEquals(0, fsView.getAclStatus(mountOnNn1).getEntries().size());
    assertEquals(0, fHdfs.getAclStatus(targetTestRoot).getEntries().size());

    // Now set ACLs on the second namespace
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, "bar", READ));
    fsView.modifyAclEntries(mountOnNn2, aclSpec);
    expected = new AclEntry[] {
        aclEntry(ACCESS, USER, "bar", READ),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    assertArrayEquals(expected, aclEntryArray(fsView.getAclStatus(mountOnNn2)));
    assertArrayEquals(expected, aclEntryArray(fHdfs2.getAclStatus(targetTestRoot2)));

    // Remove the ACL entries on the second namespace
    fsView.removeAclEntries(mountOnNn2, Lists.newArrayList(
        aclEntry(ACCESS, USER, "bar", READ)
    ));
    expected = new AclEntry[] { aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    assertArrayEquals(expected, aclEntryArray(fHdfs2.getAclStatus(targetTestRoot2)));
    fsView.removeAcl(mountOnNn2);
    assertEquals(0, fsView.getAclStatus(mountOnNn2).getEntries().size());
    assertEquals(0, fHdfs2.getAclStatus(targetTestRoot2).getEntries().size());
  }

  private AclEntry[] aclEntryArray(AclStatus aclStatus) {
    return aclStatus.getEntries().toArray(new AclEntry[0]);
  }

}
