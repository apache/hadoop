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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.HashSet;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for viewfs with LinkFallback mount table entries.
 */
public class TestViewFileSystemLinkFallback extends ViewFileSystemBaseTest {

  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final String LINK_FALLBACK_CLUSTER_1_NAME = "Cluster1";
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final Configuration CONF = new Configuration();
  private static final File TEST_DIR = GenericTestUtils.getTestDir(
      TestViewFileSystemLinkFallback.class.getSimpleName());
  private static final String TEST_BASE_PATH =
      "/tmp/TestViewFileSystemLinkFallback";
  private final static Logger LOG = LoggerFactory.getLogger(
      TestViewFileSystemLinkFallback.class);
  private static URI viewFsDefaultClusterUri;

  @Override
  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper(TEST_BASE_PATH);
  }

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException,
      LoginException, URISyntaxException {
    SupportsBlocks = true;
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(CONF)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(
            NAME_SPACES_COUNT))
        .numDataNodes(DATA_NODES_COUNT)
        .build();
    cluster.waitClusterUp();

    for (int i = 0; i < NAME_SPACES_COUNT; i++) {
      FS_HDFS[i] = cluster.getFileSystem(i);
    }
    fsDefault = FS_HDFS[FS_INDEX_DEFAULT];
    viewFsDefaultClusterUri = new URI(FsConstants.VIEWFS_SCHEME,
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE, "/", null, null);
  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    fsTarget = fsDefault;
    super.setUp();
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
    super.setupMountPoints();
    ConfigUtil.addLinkFallback(conf, LINK_FALLBACK_CLUSTER_1_NAME,
        targetTestRoot.toUri());
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
  public void testConfLinkFallback() throws Exception {
    Path testBasePath = new Path(TEST_BASE_PATH);
    Path testLevel2Dir = new Path(TEST_BASE_PATH, "dir1/dirA");
    Path testBaseFile = new Path(testBasePath, "testBaseFile.log");
    Path testBaseFileRelative = new Path(testLevel2Dir,
        "../../testBaseFile.log");
    Path testLevel2File = new Path(testLevel2Dir, "testLevel2File.log");
    fsTarget.mkdirs(testLevel2Dir);

    fsTarget.createNewFile(testBaseFile);
    FSDataOutputStream dataOutputStream = fsTarget.append(testBaseFile);
    dataOutputStream.write(1);
    dataOutputStream.close();

    fsTarget.createNewFile(testLevel2File);
    dataOutputStream = fsTarget.append(testLevel2File);
    dataOutputStream.write("test link fallback".toString().getBytes());
    dataOutputStream.close();

    String clusterName = "ClusterFallback";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);

    Configuration conf = new Configuration();
    ConfigUtil.addLinkFallback(conf, clusterName, fsTarget.getUri());

    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    FileStatus baseFileStat = vfs.getFileStatus(new Path(viewFsUri.toString()
        + testBaseFile.toUri().toString()));
    LOG.info("BaseFileStat: " + baseFileStat);
    FileStatus baseFileRelStat = vfs.getFileStatus(new Path(viewFsUri.toString()
        + testBaseFileRelative.toUri().toString()));
    LOG.info("BaseFileRelStat: " + baseFileRelStat);
    Assert.assertEquals("Unexpected file length for " + testBaseFile,
        1, baseFileStat.getLen());
    Assert.assertEquals("Unexpected file length for " + testBaseFileRelative,
        baseFileStat.getLen(), baseFileRelStat.getLen());
    FileStatus level2FileStat = vfs.getFileStatus(new Path(viewFsUri.toString()
        + testLevel2File.toUri().toString()));
    LOG.info("Level2FileStat: " + level2FileStat);
    vfs.close();
  }

  @Test
  public void testConfLinkFallbackWithRegularLinks() throws Exception {
    Path testBasePath = new Path(TEST_BASE_PATH);
    Path testLevel2Dir = new Path(TEST_BASE_PATH, "dir1/dirA");
    Path testBaseFile = new Path(testBasePath, "testBaseFile.log");
    Path testLevel2File = new Path(testLevel2Dir, "testLevel2File.log");
    fsTarget.mkdirs(testLevel2Dir);

    fsTarget.createNewFile(testBaseFile);
    fsTarget.createNewFile(testLevel2File);
    FSDataOutputStream dataOutputStream = fsTarget.append(testLevel2File);
    dataOutputStream.write("test link fallback".toString().getBytes());
    dataOutputStream.close();

    String clusterName = "ClusterFallback";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);

    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, clusterName,
        "/internalDir/linkToDir2",
        new Path(targetTestRoot, "dir2").toUri());
    ConfigUtil.addLink(conf, clusterName,
        "/internalDir/internalDirB/linkToDir3",
        new Path(targetTestRoot, "dir3").toUri());
    ConfigUtil.addLink(conf, clusterName,
        "/danglingLink",
        new Path(targetTestRoot, "missingTarget").toUri());
    ConfigUtil.addLink(conf, clusterName,
        "/linkToAFile",
        new Path(targetTestRoot, "aFile").toUri());
    System.out.println("ViewFs link fallback " + fsTarget.getUri());
    ConfigUtil.addLinkFallback(conf, clusterName, targetTestRoot.toUri());

    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    FileStatus baseFileStat = vfs.getFileStatus(
        new Path(viewFsUri.toString() + testBaseFile.toUri().toString()));
    LOG.info("BaseFileStat: " + baseFileStat);
    Assert.assertEquals("Unexpected file length for " + testBaseFile,
        0, baseFileStat.getLen());
    FileStatus level2FileStat = vfs.getFileStatus(new Path(viewFsUri.toString()
        + testLevel2File.toUri().toString()));
    LOG.info("Level2FileStat: " + level2FileStat);

    dataOutputStream = vfs.append(testLevel2File);
    dataOutputStream.write("Writing via viewfs fallback path".getBytes());
    dataOutputStream.close();

    FileStatus level2FileStatAfterWrite = vfs.getFileStatus(
        new Path(viewFsUri.toString() + testLevel2File.toUri().toString()));
    Assert.assertTrue("Unexpected file length for " + testLevel2File,
        level2FileStatAfterWrite.getLen() > level2FileStat.getLen());

    vfs.close();
  }

  @Test
  public void testConfLinkFallbackWithMountPoint() throws Exception {
    TEST_DIR.mkdirs();
    Configuration conf = new Configuration();
    String clusterName = "ClusterX";
    String mountPoint = "/user";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String expectedErrorMsg =  "Invalid linkFallback entry in config: " +
        "linkFallback./user";
    String mountTableEntry = Constants.CONFIG_VIEWFS_PREFIX + "."
        + clusterName + "." + Constants.CONFIG_VIEWFS_LINK_FALLBACK
        + "." + mountPoint;
    conf.set(mountTableEntry, TEST_DIR.toURI().toString());

    try {
      FileSystem.get(viewFsUri, conf);
      fail("Shouldn't allow linkMergeSlash to take extra mount points!");
    } catch (IOException e) {
      assertTrue("Unexpected error: " + e.getMessage(),
          e.getMessage().contains(expectedErrorMsg));
    }
  }

  /**
   * This tests whether the fallback link gets listed for list operation
   * of root directory of mount table.
   * @throws Exception
   */
  @Test
  public void testListingWithFallbackLink() throws Exception {
    Path dir1 = new Path(targetTestRoot, "fallbackDir/dir1");
    fsTarget.mkdirs(dir1);
    String clusterName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);

    HashSet<Path> beforeFallback = new HashSet<>();
    try(FileSystem vfs = FileSystem.get(viewFsUri, conf)) {
      for (FileStatus stat : vfs.listStatus(new Path(viewFsUri.toString()))) {
        beforeFallback.add(stat.getPath());
      }
    }

    ConfigUtil.addLinkFallback(conf, clusterName,
        new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsUri, conf)) {
      HashSet<Path> afterFallback = new HashSet<>();
      for (FileStatus stat : vfs.listStatus(new Path(viewFsUri.toString()))) {
        afterFallback.add(stat.getPath());
      }
      afterFallback.removeAll(beforeFallback);
      assertTrue("Listing didn't include fallback link",
          afterFallback.size() == 1);
      Path[] fallbackArray = new Path[afterFallback.size()];
      afterFallback.toArray(fallbackArray);
      Path expected = new Path(viewFsUri.toString(), "dir1");
      assertEquals("Path did not match",
          expected, fallbackArray[0]);

      // Create a directory using the returned fallback path and verify
      Path childDir = new Path(fallbackArray[0], "child");
      vfs.mkdirs(childDir);
      FileStatus status = fsTarget.getFileStatus(new Path(dir1, "child"));
      assertTrue(status.isDirectory());
      assertTrue(vfs.getFileStatus(childDir).isDirectory());
    }
  }

  /**
   * This tests whether fallback directory gets shaded during list operation
   * of root directory of mount table when the same directory name exists as
   * mount point as well as in the fallback linked directory.
   * @throws Exception
   */
  @Test
  public void testListingWithFallbackLinkWithSameMountDirectories()
      throws Exception {
    // Creating two directories under the fallback directory.
    // "user" directory already exists as configured mount point.
    Path dir1 = new Path(targetTestRoot, "fallbackDir/user");
    Path dir2 = new Path(targetTestRoot, "fallbackDir/user1");
    fsTarget.mkdirs(dir1);
    fsTarget.mkdirs(dir2);
    String clusterName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    HashSet<Path> beforeFallback = new HashSet<>();
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      for (FileStatus stat : vfs
          .listStatus(new Path(viewFsDefaultClusterUri.toString()))) {
        beforeFallback.add(stat.getPath());
      }
    }
    ConfigUtil.addLinkFallback(conf, clusterName,
        new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      HashSet<Path> afterFallback = new HashSet<>();
      for (FileStatus stat : vfs
          .listStatus(new Path(viewFsDefaultClusterUri.toString()))) {
        afterFallback.add(stat.getPath());
      }
      afterFallback.removeAll(beforeFallback);
      assertEquals("The same directory name in fallback link should be shaded",
          1, afterFallback.size());
      Path[] fallbackArray = new Path[afterFallback.size()];
      // Only user1 should be listed as fallback link
      Path expected = new Path(viewFsDefaultClusterUri.toString(), "user1");
      assertEquals("Path did not match",
          expected, afterFallback.toArray(fallbackArray)[0]);

      // Create a directory using the returned fallback path and verify
      Path childDir = new Path(fallbackArray[0], "child");
      vfs.mkdirs(childDir);
      FileStatus status = fsTarget.getFileStatus(new Path(dir2, "child"));
      assertTrue(status.isDirectory());
      assertTrue(vfs.getFileStatus(childDir).isDirectory());
    }
  }

  /**
   * Tests ListStatus on non-link parent with fallback configured.
   * =============================Example.======================================
   * ===== Fallback path tree =============== Mount Path Tree ==================
   * ===========================================================================
   * *             /            *****               /          *****************
   * *            /             *****              /           *****************
   * *          user1           *****          user1           *****************
   * *           /              *****          /               *****************
   * *         hive             *****        hive              *****************
   * *       /      \           *****       /                  *****************
   * * warehouse    warehouse1  *****  warehouse               *****************
   * * (-rwxr--r--)             ***** (-r-xr--r--)             *****************
   * *     /                    *****    /                     *****************
   * * partition-0              ***** partition-0              *****************
   * ===========================================================================
   * ===========================================================================
   * ***         ls /user1/hive                                        *********
   * ***         viewfs://default/user1/hive/warehouse (-rwxr--r--)    *********
   * ***         viewfs://default/user1/hive/warehouse1                *********
   * ===========================================================================
   */
  @Test
  public void testListingWithFallbackLinkWithSameMountDirectoryTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    // Creating multiple directories path under the fallback directory.
    // "/user1/hive/warehouse/partition-0" directory already exists as
    // configured mount point.
    Path dir1 = new Path(targetTestRoot,
        "fallbackDir/user1/hive/warehouse/partition-0");
    Path dir2 = new Path(targetTestRoot, "fallbackDir/user1/hive/warehouse1");
    fsTarget.mkdirs(dir1);
    fsTarget.mkdirs(dir2);
    fsTarget.setPermission(new Path(targetTestRoot, "fallbackDir/user1/hive/"),
        FsPermission.valueOf("-rwxr--r--"));

    HashSet<Path> beforeFallback = new HashSet<>();
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      for (FileStatus stat : vfs.listStatus(
          new Path(viewFsDefaultClusterUri.toString(), "/user1/hive/"))) {
        beforeFallback.add(stat.getPath());
      }
    }
    ConfigUtil
        .addLinkFallback(conf, new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      HashSet<Path> afterFallback = new HashSet<>();
      for (FileStatus stat : vfs.listStatus(
          new Path(viewFsDefaultClusterUri.toString(), "/user1/hive/"))) {
        afterFallback.add(stat.getPath());
        if (dir1.getName().equals(stat.getPath().getName())) {
          // make sure fallback dir listed out with correct permissions, but not
          // with link permissions.
          assertEquals(FsPermission.valueOf("-rwxr--r--"),
              stat.getPermission());
        }
      }
      //viewfs://default/user1/hive/warehouse
      afterFallback.removeAll(beforeFallback);
      assertEquals("The same directory name in fallback link should be shaded",
          1, afterFallback.size());
    }
  }

  /**
   * Tests ListStatus on link parent with fallback configured.
   * =============================Example.======================================
   * ===== Fallback path tree =============== Mount Path Tree ==================
   * ===========================================================================
   * *             /            *****               /                 **********
   * *            /             *****              /                  **********
   * *          user1           *****          user1                  **********
   * *           /              *****          /                      **********
   * *         hive             *****        hive                     **********
   * *       /      \           *****       /                         **********
   * * warehouse    warehouse1  *****  warehouse                      **********
   * * (-rwxr--r--)             ***** (-r-xr--r--)                    **********
   * *     /                    *****    /                            **********
   * * partition-0              ***** partition-0 ---> targetTestRoot **********
   * *                          ***** (-r-xr--r--)      (-rwxr--rw-)  **********
   * ===========================================================================
   * ===========================================================================
   * ***       ls /user1/hive/warehouse                                       **
   * ***       viewfs://default/user1/hive/warehouse/partition-0 (-rwxr--rw-) **
   * ===========================================================================
   */
  @Test
  public void testLSOnLinkParentWithFallbackLinkWithSameMountDirectoryTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    // Creating multiple directories path under the fallback directory.
    // "/user1/hive/warehouse/partition-0" directory already exists as
    // configured mount point.
    Path dir1 = new Path(targetTestRoot,
        "fallbackDir/user1/hive/warehouse/partition-0");
    Path dir2 = new Path(targetTestRoot, "fallbackDir/user1/hive/warehouse1");
    fsTarget.mkdirs(dir1);
    fsTarget.mkdirs(dir2);
    fsTarget.setPermission(new Path(targetTestRoot,
            "fallbackDir/user1/hive/warehouse/partition-0"),
        FsPermission.valueOf("-rwxr--r--"));
    fsTarget.setPermission(targetTestRoot, FsPermission.valueOf("-rwxr--rw-"));

    HashSet<Path> beforeFallback = new HashSet<>();
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      for (FileStatus stat : vfs.listStatus(
          new Path(viewFsDefaultClusterUri.toString(),
              "/user1/hive/warehouse/"))) {
        beforeFallback.add(stat.getPath());
      }
    }
    ConfigUtil
        .addLinkFallback(conf, new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      HashSet<Path> afterFallback = new HashSet<>();
      for (FileStatus stat : vfs.listStatus(
          new Path(viewFsDefaultClusterUri.toString(),
              "/user1/hive/warehouse/"))) {
        afterFallback.add(stat.getPath());
        if (dir1.getName().equals(stat.getPath().getName())) {
          // make sure fallback dir listed out with correct permissions, but not
          // with link permissions.
          assertEquals(FsPermission.valueOf("-rwxr--rw-"),
              stat.getPermission());
        }
      }
      afterFallback.removeAll(beforeFallback);
      assertEquals("Just to make sure paths are same.", 0,
          afterFallback.size());
    }
  }

  /**
   * Tests ListStatus on root with fallback configured.
   * =============================Example.======================================
   * ===== Fallback path tree =============== Mount Path Tree ==================
   * ===========================================================================
   * *          /       /          *****               /                     ***
   * *         /       /           *****              /                      ***
   * *      user1    user2         *****           user1 ---> targetTestRoot ***
   * *(-r-xr--r--)   (-r-xr--r--)  *****                      (-rwxr--rw-)   ***
   * ===========================================================================
   * ===========================================================================
   * ***       ls /user1/hive/warehouse                                       **
   * ***       viewfs://default/user1(-rwxr--rw-)                             **
   * ***       viewfs://default/user2(-r-xr--r--)                             **
   * ===========================================================================
   */
  @Test
  public void testLSOnRootWithFallbackLinkWithSameMountDirectories()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil
        .addLink(conf, "/user1", new Path(targetTestRoot.toString()).toUri());
    // Creating multiple directories path under the fallback directory.
    // "/user1" directory already exists as configured mount point.
    Path dir1 = new Path(targetTestRoot, "fallbackDir/user1");
    Path dir2 = new Path(targetTestRoot, "fallbackDir/user2");
    fsTarget.mkdirs(dir1);
    fsTarget.mkdirs(dir2, FsPermission.valueOf("-rwxr--r--"));
    fsTarget.setPermission(targetTestRoot, FsPermission.valueOf("-rwxr--rw-"));

    HashSet<Path> beforeFallback = new HashSet<>();
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      for (FileStatus stat : vfs
          .listStatus(new Path(viewFsDefaultClusterUri.toString(), "/"))) {
        beforeFallback.add(stat.getPath());
      }
    }
    ConfigUtil
        .addLinkFallback(conf, new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      HashSet<Path> afterFallback = new HashSet<>();
      for (FileStatus stat : vfs
          .listStatus(new Path(viewFsDefaultClusterUri.toString(), "/"))) {
        afterFallback.add(stat.getPath());
        if (dir1.getName().equals(stat.getPath().getName())) {
          // make sure fallback dir listed out with correct permissions, but not
          // with link permissions.
          assertEquals(FsPermission.valueOf("-rwxr--rw-"),
              stat.getPermission());
        } else {
          assertEquals("Path is: " + stat.getPath(),
              FsPermission.valueOf("-rwxr--r--"), stat.getPermission());
        }
      }
      afterFallback.removeAll(beforeFallback);
      assertEquals(1, afterFallback.size());
      assertEquals("/user2 dir from fallback should be listed.", "user2",
          afterFallback.iterator().next().getName());
    }
  }

  @Test
  public void testLSOnLinkParentWhereMountLinkMatchesWithAFileUnderFallback()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, true);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/part-0",
        new Path(targetTestRoot.toString()).toUri());
    // Create a file path in fallback matching to the path of mount link.
    Path file1 =
        new Path(targetTestRoot, "fallbackDir/user1/hive/warehouse/part-0");
    fsTarget.createNewFile(file1);
    Path dir2 = new Path(targetTestRoot, "fallbackDir/user1/hive/warehouse1");
    fsTarget.mkdirs(dir2);

    ConfigUtil
        .addLinkFallback(conf, new Path(targetTestRoot, "fallbackDir").toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      for (FileStatus stat : vfs.listStatus(
          new Path(viewFsDefaultClusterUri.toString(),
              "/user1/hive/warehouse/"))) {
        if (file1.getName().equals(stat.getPath().getName())) {
          // Link represents as symlink.
          assertFalse(stat.isFile());
          assertFalse(stat.isDirectory());
          assertTrue(stat.isSymlink());
          Path fileUnderDir = new Path(stat.getPath(), "check");
          assertTrue(vfs.mkdirs(fileUnderDir)); // Creating dir under target
          assertTrue(fsTarget
              .exists(new Path(targetTestRoot, fileUnderDir.getName())));
        }
      }
    }
  }

  /**
   * Tests that directory making should be successful when the parent directory
   * is same as the existent fallback directory. The new dir should be created
   * in fallback instead failing.
   */
  @Test
  public void testMkdirsOfLinkParentWithFallbackLinkWithSameMountDirectoryTree()
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

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path p = new Path("/user1/hive/warehouse/test");
      Path test = Path.mergePaths(fallbackTarget, p);
      assertFalse(fsTarget.exists(test));
      assertTrue(vfs.mkdirs(p));
      assertTrue(fsTarget.exists(test));
    }
  }

  /**
   * Tests that directory making should be successful when attempting to create
   * the root directory as it's already exist.
   */
  @Test
  public void testMkdirsOfRootWithFallbackLinkAndMountWithSameDirTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil
        .addLink(conf, "/user1", new Path(targetTestRoot.toString()).toUri());
    Path dir1 = new Path(targetTestRoot, "fallbackDir/user1");
    fsTarget.mkdirs(dir1);
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path p = new Path("/");
      Path test = Path.mergePaths(fallbackTarget, p);
      assertTrue(fsTarget.exists(test));
      assertTrue(vfs.mkdirs(p));
      assertTrue(fsTarget.exists(test));
    }
  }

  /**
   * Tests the making of a new directory which is not matching to any of
   * internal directory under the root.
   */
  @Test
  public void testMkdirsOfNewDirWithOutMatchingToMountOrFallbackDirTree()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive/warehouse/partition-0",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      // user2 does not exist in fallback
      Path p = new Path("/user2");
      Path test = Path.mergePaths(fallbackTarget, p);
      assertFalse(fsTarget.exists(test));
      assertTrue(vfs.mkdirs(p));
      assertTrue(fsTarget.exists(test));
    }
  }

  /**
   * Tests that when the parent dirs does not exist in fallback but the parent
   * dir is same as mount internal directory, then we create parent structure
   * (mount internal directory tree structure) in fallback.
   */
  @Test
  public void testMkdirsWithFallbackLinkWithMountPathMatchingDirExist()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      //user1 does not exist in fallback
      Path immediateLevelToInternalDir = new Path("/user1/test");
      Path test = Path.mergePaths(fallbackTarget, immediateLevelToInternalDir);
      assertFalse(fsTarget.exists(test));
      assertTrue(vfs.mkdirs(immediateLevelToInternalDir));
      assertTrue(fsTarget.exists(test));
    }
  }

  /**
   * Tests that when the parent dirs does not exist in fallback but the
   * immediate parent dir is not same as mount internal directory, then we
   * create parent structure (mount internal directory tree structure) in
   * fallback.
   */
  @Test
  public void testMkdirsOfDeepTreeWithFallbackLinkAndMountPathMatchingDirExist()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/hive",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      //user1 does not exist in fallback
      Path multipleLevelToInternalDir = new Path("/user1/test/test");
      Path test = Path.mergePaths(fallbackTarget, multipleLevelToInternalDir);
      assertFalse(fsTarget.exists(test));
      assertTrue(vfs.mkdirs(multipleLevelToInternalDir));
      assertTrue(fsTarget.exists(test));
    }
  }

  /**
   * Tests that mkdirs should return false when there is a problem with
   * fallbackfs.
   */
  @Test
  public void testMkdirsShouldReturnFalseWhenFallbackFSNotAvailable()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(Constants.CONFIG_VIEWFS_MOUNT_LINKS_AS_SYMLINKS, false);
    ConfigUtil.addLink(conf, "/user1/test",
        new Path(targetTestRoot.toString()).toUri());
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      //user1/test1 does not exist in fallback
      Path nextLevelToInternalDir = new Path("/user1/test1");
      Path test = Path.mergePaths(fallbackTarget, nextLevelToInternalDir);
      assertFalse(fsTarget.exists(test));
      // user1 exists in viewFS mount.
      assertNotNull(vfs.getFileStatus(new Path("/user1")));
      // user1 does not exists in fallback.
      assertFalse(fsTarget.exists(test.getParent()));
      cluster.shutdownNameNodes(); // Stopping fallback server
      // /user1/test1 does not exist in mount internal dir tree, it would
      // attempt to create in fallback.
      intercept(IOException.class, () -> {
        vfs.mkdirs(nextLevelToInternalDir);
      });
      cluster.restartNameNodes();
      // should return true succeed when fallback fs is back to normal.
      assertTrue(vfs.mkdirs(nextLevelToInternalDir));
      assertTrue(fsTarget.exists(test));
    }
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

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestFile = new Path("/user1/hive/warehouse/test.file");
      Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
      assertFalse(fsTarget.exists(testFileInFallback));
      assertTrue(fsTarget.exists(testFileInFallback.getParent()));
      vfs.create(vfsTestFile).close();
      assertTrue(fsTarget.exists(testFileInFallback));
    }
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
    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestFile = new Path("/user2/test.file");
      Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
      assertFalse(fsTarget.exists(testFileInFallback));
      // user2 does not exist in fallback
      assertFalse(fsTarget.exists(testFileInFallback.getParent()));
      vfs.create(vfsTestFile).close();
      // /user2/test.file should be created in fallback
      assertTrue(fsTarget.exists(testFileInFallback));
    }
  }

  /**
   * Tests the making of a new file on root which is not matching to any of
   * fallback files on root.
   */
  @Test
  public void testCreateFileOnRootWithFallbackEnabled() throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);

    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestFile = new Path("/test.file");
      Path testFileInFallback = Path.mergePaths(fallbackTarget, vfsTestFile);
      assertFalse(fsTarget.exists(testFileInFallback));
      vfs.create(vfsTestFile).close();
      // /test.file should be created in fallback
      assertTrue(fsTarget.exists(testFileInFallback));
    }
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

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestFile = new Path("/test.file");
      assertTrue(fsTarget.exists(testFile));
      vfs.create(vfsTestFile, false).close();
    }
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

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestDir = new Path("/user1/hive");
      assertFalse(fsTarget.exists(Path.mergePaths(fallbackTarget, vfsTestDir)));
      vfs.create(vfsTestDir).close();
    }
  }

  /**
   * Tests the create of a file where the path is same as one of of the internal
   * dir path should fail.
   */
  @Test
  public void testCreateFileSameAsInternalDirPath() throws Exception {
    Configuration conf = new Configuration();
    Path fallbackTarget = new Path(targetTestRoot, "fallbackDir");
    fsTarget.mkdirs(fallbackTarget);
    ConfigUtil.addLink(conf, "/user1/hive/",
        new Path(targetTestRoot.toString()).toUri());
    ConfigUtil.addLinkFallback(conf, fallbackTarget.toUri());

    try (FileSystem vfs = FileSystem.get(viewFsDefaultClusterUri, conf)) {
      Path vfsTestDir = new Path("/user1");
      assertFalse(fsTarget.exists(Path.mergePaths(fallbackTarget, vfsTestDir)));
      try {
        vfs.create(vfsTestDir);
        Assert.fail("Should fail to create file as this is an internal dir.");
      } catch (NotInMountpointException e){
        // This tree is part of internal tree. The above exception will be
        // thrown from getDefaultReplication, getDefaultBlockSize APIs which was
        // called in create API.
      }
    }
  }
}
