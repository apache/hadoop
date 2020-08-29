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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.PathUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;;


/**
 * Tests ViewFsOverloadScheme with configured mount links.
 */
public class TestViewFsOverloadSchemeWithHdfsScheme {
  private static final String FS_IMPL_PATTERN_KEY =
      "fs.AbstractFileSystem.%s.impl";
  private static final String HDFS_SCHEME = "hdfs";
  private Configuration conf = null;
  private static MiniDFSCluster cluster = null;
  private URI defaultFSURI;
  private File localTargetDir;
  private static final String TEST_ROOT_DIR = PathUtils
      .getTestDirName(TestViewFsOverloadSchemeWithHdfsScheme.class);
  private static final String HDFS_USER_FOLDER = "/HDFSUser";
  private static final String LOCAL_FOLDER = "/local";

  @BeforeClass
  public static void init() throws IOException {
    cluster =
        new MiniDFSCluster.Builder(new Configuration()).numDataNodes(2).build();
    cluster.waitClusterUp();
  }

  /**
   * Sets up the configurations and starts the MiniDFSCluster.
   */
  @Before
  public void startCluster() throws IOException {
    conf = new Configuration(cluster.getConfiguration(0));
    conf.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFsOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_ABSTRACTFS_IMPL_PATTERN,
        HDFS_SCHEME), Hdfs.class.getName());
    defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    localTargetDir = new File(TEST_ROOT_DIR, "/root/");
    Assert.assertEquals(HDFS_SCHEME, defaultFSURI.getScheme()); // hdfs scheme.
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Adds the given mount links to config. sources contains mount link src and
   * the respective index location in targets contains the target uri.
   */
  void addMountLinks(String mountTable, String[] sources, String[] targets,
      Configuration config) throws IOException, URISyntaxException {
    ViewFsTestSetup.addMountLinksToConf(mountTable, sources, targets, config);
  }

  /**
   * Create mount links as follows.
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   *
   * create file /HDFSUser/testfile should create in hdfs
   * create file /local/test should create directory in local fs
   */
  @Test(timeout = 30000)
  public void testMountLinkWithLocalAndHDFS() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path localTargetPath = new Path(localTargetDir.toURI());

    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    // /HDFSUser/testfile
    Path hdfsFile = new Path(HDFS_USER_FOLDER + "/testfile");
    // /local/test
    Path localDir = new Path(LOCAL_FOLDER + "/test");

    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    fc.mkdir(localDir, FileContext.DEFAULT_PERM, true);
    FileContextTestHelper.createFile(fc, hdfsFile);

    // Initialize HDFS and test files exist in ls or not
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(defaultFSURI, conf);
      Assert.assertTrue(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should be in hdfs.
      Assert.assertFalse(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTargetPath),
              localDir.getName()))); // should not be in local fs.
    }

    try (RawLocalFileSystem lfs = new RawLocalFileSystem()) {
      lfs.initialize(localTargetPath.toUri(), conf);
      Assert.assertFalse(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should not be in hdfs.
      Assert.assertTrue(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTargetPath),
              localDir.getName()))); // should be in local fs.
    }
  }

  /**
   * Create mount links as follows.
   * hdfs://localhost:xxx/HDFSUser --> nonexistent://NonExistent/User/
   * It should fail to add non existent fs link.
   */
  @Test(expected = IOException.class, timeout = 30000)
  public void testMountLinkWithNonExistentLink() throws Exception {
    final String userFolder = "/User";
    final Path nonExistTargetPath =
        new Path("nonexistent://NonExistent" + userFolder);

    /**
     * Below addLink will create following mount points
     * hdfs://localhost:xxx/User --> nonexistent://NonExistent/User/
     */
    addMountLinks(defaultFSURI.getAuthority(), new String[] {userFolder },
        new String[] {nonExistTargetPath.toUri().toString() }, conf);
    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    Assert.fail("Expected to fail with non existent link");
  }

  /**
   * Create mount links as follows.
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * ListStatus on / should list the mount links.
   */
  @Test(timeout = 30000)
  public void testListStatusOnRootShouldListAllMountLinks() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    RemoteIterator<FileStatus> dirContents = fc.listStatus(new Path("/"));
    List<FileStatus> ls = new ArrayList<>();
    while (dirContents.hasNext()) {
      FileStatus fileStatus = dirContents.next();
      ls.add(fileStatus);
    }
    Assert.assertEquals(2, ls.size());
    String path1 =
        Path.getPathWithoutSchemeAndAuthority(ls.get(0).getPath()).toString();
    String path2 =
        Path.getPathWithoutSchemeAndAuthority(ls.get(1).getPath()).toString();
    Assert.assertTrue(
        HDFS_USER_FOLDER.equals(path1) || LOCAL_FOLDER.equals(path1));
    Assert.assertTrue(
        HDFS_USER_FOLDER.equals(path2) || LOCAL_FOLDER.equals(path2));
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * ListStatus non mount directory should fail.
   */
  @Test(expected = IOException.class, timeout = 30000)
  public void testListStatusOnNonMountedPath() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    RemoteIterator<FileStatus> ls = fc.listStatus(new Path("/nonMount"));
    Assert.fail("It should fail as no mount link with /nonMount");
  }

  /**
   * Create mount links as follows hdfs://localhost:xxx/HDFSUser -->
   * hdfs://localhost:xxx/HDFSUser/ hdfs://localhost:xxx/local -->
   * file://TEST_ROOT_DIR/root/ fallback --> hdfs://localhost:xxx/HDFSUser/
   * Creating file or directory at non root level should succeed with fallback
   * links.
   */
  @Test(timeout = 30000)
  public void testWithLinkFallBack() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER,
            Constants.CONFIG_VIEWFS_LINK_FALLBACK },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);

    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    FileContextTestHelper.createFile(fc, new Path("/nonMount/myfile"));
    RemoteIterator<FileStatus> dirContents =
        fc.listStatus(new Path("/nonMount"));
    List<FileStatus> ls = new ArrayList<>();
    while (dirContents.hasNext()) {
      FileStatus fileStatus = dirContents.next();
      ls.add(fileStatus);
    }
    Assert.assertEquals(1, ls.size());
    Assert.assertEquals(
        Path.getPathWithoutSchemeAndAuthority(ls.get(0).getPath()).getName(),
        "myfile");
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   *
   * It cannot find any mount link. ViewFS expects a mount point from root.
   */
  @Test(expected = AccessControlException.class, timeout = 30000)
  public void testCreateOnRootShouldFailWhenMountLinkConfigured()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);
    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    FileContextTestHelper.createFile(fc, new Path("/newFileOnRoot"));
    Assert.fail("It should fail as root is read only in viewFS.");
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * fallback --> hdfs://localhost:xxx/HDFSUser/
   *
   * Note: Above links created because to make fs initialization success.
   * Otherwise will not proceed if no mount links.
   *
   * Don't set fs.viewfs.overload.scheme.target.hdfs.impl property.
   * So, OverloadScheme target fs initialization will fail.
   */
  @Test(expected = UnsupportedFileSystemException.class, timeout = 30000)
  public void testInvalidOverloadSchemeTargetFS() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    conf = new Configuration();
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER,
            Constants.CONFIG_VIEWFS_LINK_FALLBACK },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        defaultFSURI.toString());
    conf.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFsOverloadScheme.class.getName());

    FileContext fc = FileContext.getFileContext(defaultFSURI, conf);
    FileContextTestHelper.createFile(fc, new Path("/onRootWhenFallBack"));
    Assert.fail("OverloadScheme target fs should be valid.");
  }
}
