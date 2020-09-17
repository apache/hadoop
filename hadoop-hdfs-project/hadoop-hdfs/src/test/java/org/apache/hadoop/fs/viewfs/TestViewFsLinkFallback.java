/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for viewfs with LinkFallback mount table entries.
 */
public class TestViewFsLinkFallback {
  private static FileSystem fsDefault;
  private FileSystem fsTarget;
  private static MiniDFSCluster cluster;
  private static URI viewFsDefaultClusterUri;
  private Path targetTestRoot;

  @BeforeClass
  public static void clusterSetupAtBeginning()
      throws IOException, URISyntaxException {
    int nameSpacesCount = 3;
    int dataNodesCount = 3;
    int fsIndexDefault = 0;
    Configuration conf = new Configuration();
    FileSystem[] fsHdfs = new FileSystem[nameSpacesCount];
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(
            nameSpacesCount))
        .numDataNodes(dataNodesCount)
        .build();
    cluster.waitClusterUp();

    for (int i = 0; i < nameSpacesCount; i++) {
      fsHdfs[i] = cluster.getFileSystem(i);
    }
    fsDefault = fsHdfs[fsIndexDefault];
    viewFsDefaultClusterUri = new URI(FsConstants.VIEWFS_SCHEME,
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE, "/", null, null);

  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = fsDefault;
    initializeTargetTestRoot();
  }

  private void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    for (FileStatus status : fsDefault.listStatus(targetTestRoot)) {
      fsDefault.delete(status.getPath(), true);
    }
  }

  /**
   * Tests that directory making should be successful when the parent directory
   * is same as the existent fallback directory. The new dir should be created
   * in fallback instead failing.
   */
  @Test
  public void testMkdirOfLinkParentWithFallbackLinkWithSameMountDirectoryTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    Path dir1 = new Path(targetTestRoot,
        "fallbackDir/user1/hive/warehouse/partition-0");
    fsTarget.mkdirs(dir1);
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path p = new Path("/user1/hive/warehouse/test");
    Path test = Path.mergePaths(fallbackTarget, p);
    assertFalse(fsTarget.exists(test));
    vfs.mkdir(p, null, true);
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests that directory making should be successful when attempting to create
   * the root directory as it's already exist.
   */
  @Test
  public void testMkdirOfRootWithFallbackLinkAndMountWithSameDirTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil
        .addLink(conf, "/user1", new Path(targetTestRoot.toString()).toUri());
    Path dir1 = new Path(targetTestRoot, "fallbackDir/user1");
    fsTarget.mkdirs(dir1);
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path p = new Path("/");
    Path test = Path.mergePaths(fallbackTarget, p);
    assertTrue(fsTarget.exists(test));
    vfs.mkdir(p, null, true);
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests the making of a new directory which is not matching to any of
   * internal directory under the root.
   */
  @Test
  public void testMkdirOfNewDirWithOutMatchingToMountOrFallbackDirTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    // user2 does not exist in fallback
    Path p = new Path("/user2");
    Path test = Path.mergePaths(fallbackTarget, p);
    assertFalse(fsTarget.exists(test));
    vfs.mkdir(p, null, true);
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests that when the parent dirs does not exist in fallback but the parent
   * dir is same as mount internal directory, then we create parent structure
   * (mount internal directory tree structure) in fallback.
   */
  @Test
  public void testMkdirWithFallbackLinkWithMountPathMatchingDirExist()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    //user1 does not exist in fallback
    Path immediateLevelToInternalDir = new Path("/user1/test");
    Path test = Path.mergePaths(fallbackTarget, immediateLevelToInternalDir);
    assertFalse(fsTarget.exists(test));
    vfs.mkdir(immediateLevelToInternalDir, null, true);
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests that when the parent dirs does not exist in fallback but the
   * immediate parent dir is not same as mount internal directory, then we
   * create parent structure (mount internal directory tree structure) in
   * fallback.
   */
  @Test
  public void testMkdirOfDeepTreeWithFallbackLinkAndMountPathMatchingDirExist()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    //user1 does not exist in fallback
    Path multipleLevelToInternalDir = new Path("/user1/test/test");
    Path test = Path.mergePaths(fallbackTarget, multipleLevelToInternalDir);
    assertFalse(fsTarget.exists(test));
    vfs.mkdir(multipleLevelToInternalDir, null, true);
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests that mkdir with createParent false should still create parent in
   * fallback when the same mount dir exist.
   */
  @Test
  public void testMkdirShouldCreateParentDirInFallbackWhenMountDirExist()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/test",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs = AbstractFileSystem.get(viewFsDefaultClusterUri,
        conf);
    //user1/hive/test1 does not exist in fallback
    Path multipleLevelToInternalDir = new Path("/user1/hive/test1");
    Path test = Path.mergePaths(fallbackTarget, multipleLevelToInternalDir);
    assertFalse(fsTarget.exists(test));
    // user1/hive exist in viewFS.
    assertNotNull(vfs.getFileStatus(new Path("/user1/hive")));
    // user1/hive does not exists in fallback.
    assertFalse(fsTarget.exists(test.getParent()));

    vfs.mkdir(multipleLevelToInternalDir, FsPermission.getDirDefault(), false);
    assertTrue(fsTarget.exists(test));

  }

  /**
   * Tests that mkdir should fail with IOE when there is a problem with
   * fallbackfs.
   */
  @Test
  public void testMkdirShouldFailWhenFallbackFSNotAvailable()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/test",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs = AbstractFileSystem.get(viewFsDefaultClusterUri,
        conf);
    //user1/test1 does not exist in fallback
    Path nextLevelToInternalDir = new Path("/user1/test1");
    Path test = Path.mergePaths(fallbackTarget, nextLevelToInternalDir);
    assertFalse(fsTarget.exists(test));
    // user1 exists in viewFS mount.
    assertNotNull(vfs.getFileStatus(new Path("/user1")));
    // user1 does not exists in fallback.
    assertFalse(fsTarget.exists(test.getParent()));
    cluster.shutdownNameNodes();
    try {
      // /user1/test1 does not exist in mount internal dir tree, it would
      // attempt to create in fallback.
      vfs.mkdir(nextLevelToInternalDir, FsPermission.getDirDefault(),
          false);
      Assert.fail("It should throw IOE when fallback fs not available.");
    } catch (IOException e) {
      cluster.restartNameNodes();
      // should succeed when fallback fs is back to normal.
      vfs.mkdir(nextLevelToInternalDir, FsPermission.getDirDefault(),
          false);
    }
    assertTrue(fsTarget.exists(test));
  }

  /**
   * Tests that the create file should be successful when the parent directory
   * is same as the existent fallback directory. The new file should be created
   * in fallback.
   */
  @Test
  public void testCreateFileOnInternalMountDirWithSameDirTreeExistInFallback()
      throws Exception {
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    Path dir1 = new Path(fallbackTarget, "user1/hive/warehouse/partition-0");
    fsTarget.mkdirs(dir1);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestFile = new Path("/user1/hive/warehouse/test.file");
    Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
    assertFalse(fsTarget.exists(testFileInFallback));
    assertTrue(fsTarget.exists(testFileInFallback.getParent()));
    vfs.create(vfsTestFile, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    assertTrue(fsTarget.exists(testFileInFallback));

  }

  /**
   * Tests the making of a new directory which is not matching to any of
   * internal directory.
   */
  @Test
  public void testCreateNewFileWithOutMatchingToMountDirOrFallbackDirPath()
      throws Exception {
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestFile = new Path("/user2/test.file");
    Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
    assertFalse(fsTarget.exists(testFileInFallback));
    // user2 does not exist in fallback
    assertFalse(fsTarget.exists(testFileInFallback.getParent()));
    vfs.create(vfsTestFile, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault()),
        Options.CreateOpts.createParent()).close();
    // /user2/test.file should be created in fallback
    assertTrue(fsTarget.exists(testFileInFallback));
  }

  /**
   * Tests the making of a new file on root which is not matching to any of
   * fallback files on root.
   */
  @Test
  public void testCreateFileOnRootWithFallbackEnabled()
      throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);

    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestFile = new Path("/test.file");
    Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
    assertFalse(fsTarget.exists(testFileInFallback));
    vfs.create(vfsTestFile, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    // /test.file should be created in fallback
    assertTrue(fsTarget.exists(testFileInFallback));

  }

  /**
   * Tests the create of a file on root where the path is matching to an
   * existing file on fallback's file on root.
   */
  @Test (expected = FileAlreadyExistsException.class)
  public void testCreateFileOnRootWithFallbackWithFileAlreadyExist()
      throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    Path testFile = new Path(fallbackTarget, "test.file");
    // pre-creating test file in fallback.
    fsTarget.create(testFile).close();

    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestFile = new Path("/test.file");
    assertTrue(fsTarget.exists(testFile));
    vfs.create(vfsTestFile, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
  }

  /**
   * Tests the creating of a file where the path is same as mount link path.
   */
  @Test(expected= FileAlreadyExistsException.class)
  public void testCreateFileWhereThePathIsSameAsItsMountLinkPath()
      throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);

    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestDir = new Path("/user1/hive");
    assertFalse(fsTarget.exists(Path.mergePaths(fallbackTarget, vfsTestDir)));
    vfs.create(vfsTestDir, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
  }

  /**
   * Tests the create of a file where the path is same as one of of the internal
   * dir path should fail.
   */
  @Test(expected = FileAlreadyExistsException.class)
  public void testCreateFileSameAsInternalDirPath()
      throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    AbstractFileSystem vfs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    Path vfsTestDir = new Path("/user1");
    assertFalse(fsTarget.exists(Path.mergePaths(fallbackTarget, vfsTestDir)));
    vfs.create(vfsTestDir, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
  }

  /**
   * Tests that, when fallBack has files under matching internal dir, listFiles
   * should work.
   */
  @Test
  public void testListFiles() throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    Path fileInFallBackRoot = new Path(fallbackTarget, "GetFileBlockLocations");
    fsTarget.create(fileInFallBackRoot).close();
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    FileContext fc = FileContext.getFileContext(viewFsDefaultClusterUri, conf);
    RemoteIterator<LocatedFileStatus> iterator =
        fc.util().listFiles(new Path("/"), false);
    assertTrue(iterator.hasNext());
    assertEquals(fileInFallBackRoot.getName(),
        iterator.next().getPath().getName());
  }

  @Test
  public void testRenameOnInternalDirWithFallback() throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLink(conf, "/user1",
        new Path(targetTestRoot.toString() + "/user1").toUri());
    ConfigUtil.addLink(conf, "/NewHDFSUser/next",
        new Path(targetTestRoot.toString() + "/newUser1").toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    //Make sure target fs has parent dir structures
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(fsDefault.getUri(), conf);
      dfs.mkdirs(new Path(targetTestRoot.toString() + "/user1"));
      dfs.mkdirs(new Path(fallbackTarget.toString() + "/newUser1"));
    }

    final AbstractFileSystem fs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);

    Path src = new Path("/newFileOnRoot");
    Path dst = new Path("/newFileOnRoot1");
    fs.create(src, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    verifyRename(fs, src, dst);

    src = new Path("/newFileOnRoot1");
    dst = new Path("/newUser1/newFileOnRoot");
    fs.mkdir(dst.getParent(), FsPermission.getDefault(), true);
    verifyRename(fs, src, dst);

    src = new Path("/newUser1/newFileOnRoot");
    dst = new Path("/newUser1/newFileOnRoot1");
    verifyRename(fs, src, dst);

    src = new Path("/newUser1/newFileOnRoot1");
    dst = new Path("/newFileOnRoot");
    verifyRename(fs, src, dst);

    src = new Path("/user1/newFileOnRoot1");
    dst = new Path("/user1/newFileOnRoot");
    fs.create(src, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    verifyRename(fs, src, dst);
  }

  @Test
  public void testRenameWhenDstOnInternalDirWithFallback() throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLink(conf, "/InternalDirDoesNotExistInFallback/test",
        new Path(targetTestRoot.toString() + "/user1").toUri());
    ConfigUtil.addLink(conf, "/NewHDFSUser/next/next1",
        new Path(targetTestRoot.toString() + "/newUser1").toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(fsDefault.getUri(), conf);
      dfs.mkdirs(new Path(targetTestRoot.toString() + "/newUser1"));
      dfs.mkdirs(
          new Path(fallbackTarget.toString() + "/NewHDFSUser/next/next1"));
    }

    final AbstractFileSystem fs =
        AbstractFileSystem.get(viewFsDefaultClusterUri, conf);
    final Path src = new Path("/newFileOnRoot");
    final Path dst = new Path("/NewHDFSUser/next");
    fs.mkdir(src, FsPermission.getDefault(), true);
    // src and dst types are must be either  same dir or files
    LambdaTestUtils.intercept(IOException.class,
        () -> fs.rename(src, dst, Options.Rename.OVERWRITE));

    final Path src1 = new Path("/newFileOnRoot1");
    final Path dst1 = new Path("/NewHDFSUser/next/file");
    fs.create(src1, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    verifyRename(fs, src1, dst1);

    final Path src2 = new Path("/newFileOnRoot2");
    final Path dst2 = new Path("/InternalDirDoesNotExistInFallback/file");
    fs.create(src2, EnumSet.of(CREATE),
        Options.CreateOpts.perms(FsPermission.getDefault())).close();
    // If fallback does not have same structure as internal, rename will fail.
    LambdaTestUtils.intercept(FileNotFoundException.class,
        () -> fs.rename(src2, dst2, Options.Rename.OVERWRITE));
  }

  private void verifyRename(AbstractFileSystem fs, Path src, Path dst)
      throws Exception {
    fs.rename(src, dst, Options.Rename.OVERWRITE);
    LambdaTestUtils
        .intercept(FileNotFoundException.class, () -> fs.getFileStatus(src));
    Assert.assertNotNull(fs.getFileStatus(dst));
  }

}
