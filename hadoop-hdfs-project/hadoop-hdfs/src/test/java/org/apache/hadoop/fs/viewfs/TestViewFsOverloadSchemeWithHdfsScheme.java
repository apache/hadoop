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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests ViewFsOverloadScheme with configured mount links.
 */
public class TestViewFsOverloadSchemeWithHdfsScheme {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private URI defaultFSURI;
  private File localTargetDir;
  private static final String TEST_ROOT_DIR =
      PathUtils.getTestDirName(TestViewFsOverloadSchemeWithHdfsScheme.class);
  private static String HDFS_USER_FOLDER = "/HDFSUser";
  private static String LOCAL_FOLDER = "/local";

  @Before
  public void startCluster() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    conf.set(
        String.format(FsConstants.FS_IMPL_PATTERN_KEY,
            FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT),
        ViewFsOverloadScheme.class.getName());
    conf.set(
        String.format(
            FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN_KEY,
            FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT),
        DistributedFileSystem.class.getName());
    conf.set(FsConstants.VIEWFS_OVERLOAD_SCHEME_KEY,
        FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitClusterUp();
    defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    localTargetDir = new File(TEST_ROOT_DIR, "/root/");
    Assert.assertEquals(FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT,
        defaultFSURI.getScheme()); // hdfs scheme.
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      FileSystem.closeAll();
      cluster.shutdown();
    }
  }

  private void createLinks(boolean needFalbackLink, Path hdfsTargetPath,
      Path localTragetPath) {
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), HDFS_USER_FOLDER,
        hdfsTargetPath.toUri());
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), LOCAL_FOLDER,
        localTragetPath.toUri());
    if (needFalbackLink) {
      ConfigUtil.addLinkFallback(conf, defaultFSURI.getAuthority(),
          hdfsTargetPath.toUri());
    }
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
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(false, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    Assert.assertEquals(2, fs.getMountPoints().length);

    // /HDFSUser/testfile
    Path hdfsFile = new Path(HDFS_USER_FOLDER + "/testfile");
    // /local/test
    Path localDir = new Path(LOCAL_FOLDER + "/test");

    fs.create(hdfsFile); // /HDFSUser/testfile
    fs.mkdirs(localDir); // /local/test

    // Initialize HDFS and test files exist in ls or not
    DistributedFileSystem dfs = new DistributedFileSystem();
    dfs.initialize(defaultFSURI, conf);
    try {
      Assert.assertTrue(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should be in hdfs.
      Assert.assertFalse(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTragetPath),
              localDir.getName()))); // should not be in local fs.
    } finally {
      dfs.close();
    }

    RawLocalFileSystem lfs = new RawLocalFileSystem();
    lfs.initialize(localTragetPath.toUri(), conf);
    try {
      Assert.assertFalse(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should not be in hdfs.
      Assert.assertTrue(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTragetPath),
              localDir.getName()))); // should be in local fs.
    } finally {
      lfs.close();
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
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), userFolder,
        nonExistTargetPath.toUri());
    FileSystem.get(conf);
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
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(false, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      FileStatus[] ls = fs.listStatus(new Path("/"));
      Assert.assertEquals(2, ls.length);
      String lsPath1 =
          Path.getPathWithoutSchemeAndAuthority(ls[0].getPath()).toString();
      String lsPath2 =
          Path.getPathWithoutSchemeAndAuthority(ls[1].getPath()).toString();
      Assert.assertTrue(
          HDFS_USER_FOLDER.equals(lsPath1) || LOCAL_FOLDER.equals(lsPath1));
      Assert.assertTrue(
          HDFS_USER_FOLDER.equals(lsPath2) || LOCAL_FOLDER.equals(lsPath2));
    } finally {
      fs.close();
    }
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
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(false, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      fs.listStatus(new Path("/nonMount"));
      Assert.fail("It should fail as no mount link with /nonMount");
    } finally {
      fs.close();
    }
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
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(true, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      fs.create(new Path("/nonMount/myfile"));
      FileStatus[] ls = fs.listStatus(new Path("/nonMount"));
      Assert.assertEquals(1, ls.length);
      Assert.assertEquals(
          Path.getPathWithoutSchemeAndAuthority(ls[0].getPath()).getName(),
          "myfile");
    } finally {
      fs.close();
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   *
   * It can not find any mount link. ViewFS expects a mount point from root.
   */
  @Test(expected = NotInMountpointException.class, timeout = 30000)
  public void testCreateOnRootShouldFailWhenMountLinkConfigured()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(false, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      fs.create(new Path("/newFileOnRoot"));
      Assert.fail("It should fail as root is read only in viewFS.");
    } finally {
      fs.close();
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * fallback --> hdfs://localhost:xxx/HDFSUser/
   *
   * It will find fallback link, but root is not accessible and read only.
   */
  @Test(expected = AccessControlException.class, timeout = 30000)
  public void testCreateOnRootShouldFailEvenFallBackMountLinkConfigured()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path localTragetPath = new Path(localTargetDir.toURI());

    createLinks(true, hdfsTargetPath, localTragetPath);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      fs.create(new Path("/onRootWhenFallBack"));
      Assert.fail(
          "It should fail as root is read only in viewFS, even when configured"
              + " with fallback.");
    } finally {
      fs.close();
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * fallback --> hdfs://localhost:xxx/HDFSUser/
   *
   * It will find fallback link, but root is not accessible and read only.
   */
  @Test(expected = UnsupportedFileSystemException.class, timeout = 30000)
  public void testInvalidOverloadSchemeTargetFS() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path localTragetPath = new Path(localTargetDir.toURI());
    conf = new Configuration();
    createLinks(true, hdfsTargetPath, localTragetPath);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        defaultFSURI.toString());
    conf.set(
        String.format(FsConstants.FS_IMPL_PATTERN_KEY,
            FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT),
        ViewFsOverloadScheme.class.getName());
    conf.set(FsConstants.VIEWFS_OVERLOAD_SCHEME_KEY,
        FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT);

    ViewFsOverloadScheme fs = (ViewFsOverloadScheme) FileSystem.get(conf);
    try {
      fs.create(new Path("/onRootWhenFallBack"));
      Assert.fail("OverloadScheme target fs should be valid.");
    } finally {
      fs.close();
    }
  }
}