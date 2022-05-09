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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test view file system with nested mount point.
 */
public class TestViewFileSystemWithNestedMountPoint extends ViewFileSystemBaseTest {
  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static Configuration clusterConfig;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final String TEST_BASE_PATH =
      "/tmp/TestViewFileSystemWithNestedMountPoint";

  @Override
  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper(TEST_BASE_PATH);
  }

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException {
    SupportsBlocks = true;
    clusterConfig = ViewFileSystemTestSetup.createConfig();
    clusterConfig.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(clusterConfig).nnTopology(
        MiniDFSNNTopology.simpleFederatedTopology(NAME_SPACES_COUNT))
        .numDataNodes(DATA_NODES_COUNT).build();
    cluster.waitClusterUp();

    for (int i = 0; i < NAME_SPACES_COUNT; i++) {
      FS_HDFS[i] = cluster.getFileSystem(i);
    }
    fsDefault = FS_HDFS[FS_INDEX_DEFAULT];
  }

  @Override
  @Before
  public void setUp() throws Exception {
    this.fsTarget = fsDefault;
    super.setUp();
  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Override this so that we don't set the targetTestRoot to any path under the
   * root of the FS, and so that we don't try to delete the test dir, but rather
   * only its contents.
   */
  @Override
  void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    for (FileStatus status : fsDefault.listStatus(targetTestRoot)) {
      fsDefault.delete(status.getPath(), true);
    }
  }

  @Override
  void setupMountPoints() {
    ConfigUtil.addLink(conf, "/targetRoot", targetTestRoot.toUri());
    ConfigUtil.addLink(conf, "/user", new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/user2", new Path(targetTestRoot,"user").toUri());
    ConfigUtil.addLink(conf, "/data", new Path(targetTestRoot,"data").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2",
        new Path(targetTestRoot,"dir2").toUri());
    ConfigUtil.addLink(conf, "/internalDir/internalDir2/linkToDir3",
        new Path(targetTestRoot,"dir3").toUri());
    ConfigUtil.addLink(conf, "/danglingLink",
        new Path(targetTestRoot, "missingTarget").toUri());
    ConfigUtil.addLink(conf, "/linkToAFile",
        new Path(targetTestRoot, "aFile").toUri());

    // Enable nested mount point, ViewFilesystem should support both non-nested and nested mount points
    ConfigUtil.setIsNestedMountPointSupported(conf, true);
    ConfigUtil.addLink(conf, "/user/userA",
        new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/user/userB",
        new Path(targetTestRoot, "userB").toUri());
    ConfigUtil.addLink(conf, "/data/dataA",
        new Path(targetTestRoot, "dataA").toUri());
    ConfigUtil.addLink(conf, "/data/dataB",
        new Path(targetTestRoot, "user").toUri());
    ConfigUtil.addLink(conf, "/internalDir/linkToDir2/linkToDir2",
        new Path(targetTestRoot,"linkToDir2").toUri());
  }

  @Override
  int getExpectedMountPoints() {
    return 13;
  }

  @Override
  int getExpectedDelegationTokenCount() {
    return 1; // all point to the same fs so 1 unique token
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 1;
  }

  @Test
  public void testOperationsThroughMountLinksInternal()
      throws IOException {
    // Create file with nested mp
    fileSystemTestHelper.createFile(fsView, "/user/userB/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.getFileStatus(new Path("/user/userB/foo")).isFile());
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.getFileStatus(new Path(targetTestRoot,"userB/foo")).isFile());

    // Delete the created file with nested mp
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/user/userB/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/user/userB/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"userB/foo")));

    // Create file with a 2 component dirs with nested mp
    fileSystemTestHelper.createFile(fsView, "/internalDir/linkToDir2/linkToDir2/foo");
    Assert.assertTrue("Created file should be type file",
        fsView.getFileStatus(new Path("/internalDir/linkToDir2/linkToDir2/foo")).isFile());
    Assert.assertTrue("Target of created file should be type file",
        fsTarget.getFileStatus(new Path(targetTestRoot,"linkToDir2/foo")).isFile());

    // Delete the created file with nested mp
    Assert.assertTrue("Delete should succeed",
        fsView.delete(new Path("/internalDir/linkToDir2/linkToDir2/foo"), false));
    Assert.assertFalse("File should not exist after delete",
        fsView.exists(new Path("/internalDir/linkToDir2/linkToDir2/foo")));
    Assert.assertFalse("Target File should not exist after delete",
        fsTarget.exists(new Path(targetTestRoot,"linkToDir2/foo")));
  }

  // rename across nested mount points that point to same target also fail
  @Test
  public void testRenameAcrossNestedMountPointSameTarget() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/user/foo");
    try {
      // Nested mount points point to the same target should fail
      // /user -> /user
      // /user/userA -> /user
      // Rename strategy: SAME_MOUNTPOINT
      fsView.rename(new Path("/user/foo"), new Path("/user/userA/foo"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }


  // rename across nested mount points fail if the mount link targets are different
  // even if the targets are part of the same target FS
  @Test
  public void testRenameAcrossMountPointDifferentTarget() throws IOException {
    fileSystemTestHelper.createFile(fsView, "/data/foo");
    // /data -> /data
    // /data/dataA -> /dataA
    // Rename strategy: SAME_MOUNTPOINT
    try {
      fsView.rename(new Path("/data/foo"), new Path("/data/dataA/fooBar"));
      ContractTestUtils.fail("IOException is not thrown on rename operation");
    } catch (IOException e) {
      GenericTestUtils
          .assertExceptionContains("Renames across Mount points not supported",
              e);
    }
  }

  // RenameStrategy SAME_TARGET_URI_ACROSS_MOUNTPOINT enabled
  // to rename across nested mount points that point to same target URI
  @Test
  public void testRenameAcrossNestedMountPointSameTargetUriAcrossMountPoint() throws IOException {
    //  /user/foo -> /user
    // /user/userA/fooBarBar -> /user
    // Rename strategy: SAME_TARGET_URI_ACROSS_MOUNTPOINT
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/user/foo");
    fsView2.rename(new Path("/user/foo"), new Path("/user/userA/fooBarBar"));
    ContractTestUtils.assertPathDoesNotExist(fsView2, "src should not exist after rename",
        new Path("/user/foo"));
    ContractTestUtils.assertPathDoesNotExist(fsTarget, "src should not exist after rename",
        new Path(targetTestRoot, "user/foo"));
    ContractTestUtils.assertIsFile(fsView2, fileSystemTestHelper.getTestRootPath(fsView2, "/user/userA/fooBarBar"));
    ContractTestUtils.assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBarBar"));
  }

  // RenameStrategy SAME_FILESYSTEM_ACROSS_MOUNTPOINT enabled
  // to rename across mount points where the mount link targets are different
  // but are part of the same target FS
  @Test
  public void testRenameAcrossNestedMountPointSameFileSystemAcrossMountPoint() throws IOException {
    // /data/foo -> /data
    // /data/dataA/fooBar -> /dataA
    // Rename strategy: SAME_FILESYSTEM_ACROSS_MOUNTPOINT
    Configuration conf2 = new Configuration(conf);
    conf2.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
        ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT
            .toString());
    FileSystem fsView2 = FileSystem.newInstance(FsConstants.VIEWFS_URI, conf2);
    fileSystemTestHelper.createFile(fsView2, "/data/foo");
    fsView2.rename(new Path("/data/foo"), new Path("/data/dataB/fooBar"));
    ContractTestUtils
        .assertPathDoesNotExist(fsView2, "src should not exist after rename",
            new Path("/data/foo"));
    ContractTestUtils
        .assertPathDoesNotExist(fsTarget, "src should not exist after rename",
            new Path(targetTestRoot, "data/foo"));
    ContractTestUtils.assertIsFile(fsView2,
        fileSystemTestHelper.getTestRootPath(fsView2, "/user/fooBar"));
    ContractTestUtils
        .assertIsFile(fsTarget, new Path(targetTestRoot, "user/fooBar"));
  }
}
