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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT;
import static org.junit.Assert.*;


/**
 * Tests ViewFileSystemOverloadScheme with configured mount links.
 */
public class TestViewFileSystemOverloadSchemeWithHdfsScheme {
  private static final String TEST_STRING = "Hello ViewFSOverloadedScheme!";
  private static final String FS_IMPL_PATTERN_KEY = "fs.%s.impl";
  private static final String HDFS_SCHEME = "hdfs";
  private Configuration conf = null;
  private static MiniDFSCluster cluster = null;
  private URI defaultFSURI;
  private File localTargetDir;
  private static final String TEST_ROOT_DIR = PathUtils
      .getTestDirName(TestViewFileSystemOverloadSchemeWithHdfsScheme.class);
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
  public void setUp() throws IOException {
    Configuration config = getNewConf();
    config.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    config.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFileSystemOverloadScheme.class.getName());
    config.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
        CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT);
    setConf(config);
    defaultFSURI =
        URI.create(config.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    localTargetDir = new File(TEST_ROOT_DIR, "/root/");
    localTargetDir.mkdirs();
    Assert.assertEquals(HDFS_SCHEME, defaultFSURI.getScheme()); // hdfs scheme.
  }

  @After
  public void cleanUp() throws IOException {
    if (cluster != null) {
      FileSystem fs = new DistributedFileSystem();
      fs.initialize(defaultFSURI, conf);
      try {
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus st : statuses) {
          Assert.assertTrue(fs.delete(st.getPath(), true));
        }
      } finally {
        fs.close();
      }
      FileSystem.closeAll();
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      FileSystem.closeAll();
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
    final Path localTragetPath = new Path(localTargetDir.toURI());

    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    // /HDFSUser/testfile
    Path hdfsFile = new Path(HDFS_USER_FOLDER + "/testfile");
    // /local/test
    Path localDir = new Path(LOCAL_FOLDER + "/test");

    try (FileSystem fs
        =  FileSystem.get(conf)) {
      Assert.assertEquals(2, fs.getChildFileSystems().length);
      fs.createNewFile(hdfsFile); // /HDFSUser/testfile
      fs.mkdirs(localDir); // /local/test
    }

    // Initialize HDFS and test files exist in ls or not
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(defaultFSURI, conf);
      Assert.assertTrue(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should be in hdfs.
      Assert.assertFalse(dfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTragetPath),
              localDir.getName()))); // should not be in local fs.
    }

    try (RawLocalFileSystem lfs = new RawLocalFileSystem()) {
      lfs.initialize(localTragetPath.toUri(), conf);
      Assert.assertFalse(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(hdfsTargetPath),
              hdfsFile.getName()))); // should not be in hdfs.
      Assert.assertTrue(lfs.exists(
          new Path(Path.getPathWithoutSchemeAndAuthority(localTragetPath),
              localDir.getName()))); // should be in local fs.
    }
  }

  /**
   * Create mount links as follows.
   * hdfs://localhost:xxx/HDFSUser --> nonexistent://NonExistent/User/
   * It should fail to add non existent fs link.
   */
  @Test(timeout = 30000)
  public void testMountLinkWithNonExistentLink() throws Exception {
    testMountLinkWithNonExistentLink(true);
  }

  public void testMountLinkWithNonExistentLink(boolean expectFsInitFailure)
      throws Exception {
    final String userFolder = "/User";
    final Path nonExistTargetPath =
        new Path("nonexistent://NonExistent" + userFolder);

    /**
     * Below addLink will create following mount points
     * hdfs://localhost:xxx/User --> nonexistent://NonExistent/User/
     */
    addMountLinks(defaultFSURI.getAuthority(), new String[] {userFolder},
        new String[] {nonExistTargetPath.toUri().toString()}, conf);
    if (expectFsInitFailure) {
      LambdaTestUtils.intercept(IOException.class, () -> {
        FileSystem.get(conf);
      });
    } else {
      try (FileSystem fs = FileSystem.get(conf)) {
        Assert.assertEquals("hdfs", fs.getScheme());
      }
    }
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
    try (DistributedFileSystem dfs = new DistributedFileSystem()) {
      dfs.initialize(defaultFSURI, conf);
      dfs.mkdirs(hdfsTargetPath);
    }

    try (RawLocalFileSystem lfs = new RawLocalFileSystem()) {
      lfs.initialize(localTargetDir.toURI(), conf);
      lfs.mkdirs(new Path(localTargetDir.toURI()));
    }
    try (FileSystem fs = FileSystem.get(conf)) {
      fs.mkdirs(hdfsTargetPath);
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
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    try (FileSystem fs = FileSystem.get(conf)) {
      fs.listStatus(new Path("/nonMount"));
      Assert.fail("It should fail as no mount link with /nonMount");
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   * Check that "viewfs:/" paths without authority can work when the
   * default mount table name is set correctly.
   */
  @Test
  public void testAccessViewFsPathWithoutAuthority() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString() },
        conf);

    // /HDFSUser/test
    Path hdfsDir = new Path(HDFS_USER_FOLDER, "test");
    // /local/test
    Path localDir = new Path(LOCAL_FOLDER, "test");
    FileStatus[] expectedStatus;

    try (FileSystem fs = FileSystem.get(conf)) {
      fs.mkdirs(hdfsDir); // /HDFSUser/test
      fs.mkdirs(localDir); // /local/test
      expectedStatus = fs.listStatus(new Path("/"));
    }

    // check for viewfs path without authority
    Path viewFsRootPath = new Path("viewfs:/");
    LambdaTestUtils.intercept(IOException.class,
        "Empty Mount table in config for viewfs://default", () -> {
          viewFsRootPath.getFileSystem(conf);
        });

    // set the name of the default mount table here and
    // subsequent calls should succeed.
    conf.set(Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE_NAME_KEY,
        defaultFSURI.getAuthority());

    try (FileSystem fs = viewFsRootPath.getFileSystem(conf)) {
      FileStatus[] status = fs.listStatus(viewFsRootPath);
      // compare only the final components of the paths as
      // full paths have different schemes (hdfs:/ vs. viewfs:/).
      List<String> expectedPaths = Arrays.stream(expectedStatus)
          .map(s -> s.getPath().getName()).sorted()
          .collect(Collectors.toList());
      List<String> paths = Arrays.stream(status)
          .map(s -> s.getPath().getName()).sorted()
          .collect(Collectors.toList());
      assertEquals(expectedPaths, paths);
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
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER,
            Constants.CONFIG_VIEWFS_LINK_FALLBACK },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);

    try (FileSystem fs = FileSystem.get(conf)) {
      fs.createNewFile(new Path("/nonMount/myfile"));
      FileStatus[] ls = fs.listStatus(new Path("/nonMount"));
      Assert.assertEquals(1, ls.length);
      Assert.assertEquals(
          Path.getPathWithoutSchemeAndAuthority(ls[0].getPath()).getName(),
          "myfile");
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   *
   * It cannot find any mount link. ViewFS expects a mount point from root.
   */
  @Test(timeout = 30000)
  public void testCreateOnRoot() throws Exception {
    testCreateOnRoot(false);
  }

  public void testCreateOnRoot(boolean fallbackExist) throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER},
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString()}, conf);
    try (FileSystem fs = FileSystem.get(conf)) {
      if (fallbackExist) {
        Assert.assertTrue(fs.createNewFile(new Path("/newFileOnRoot")));
      } else {
        LambdaTestUtils.intercept(NotInMountpointException.class, () -> {
          fs.createNewFile(new Path("/newFileOnRoot"));
        });
      }
    }
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
   * Unset fs.viewfs.overload.scheme.target.hdfs.impl property.
   * So, OverloadScheme target fs initialization will fail.
   */
  @Test(expected = UnsupportedFileSystemException.class, timeout = 30000)
  public void testInvalidOverloadSchemeTargetFS() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    String mountTableIfSet = conf.get(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH);
    conf = new Configuration();
    if (mountTableIfSet != null) {
      conf.set(Constants.CONFIG_VIEWFS_MOUNTTABLE_PATH, mountTableIfSet);
    }
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER,
            Constants.CONFIG_VIEWFS_LINK_FALLBACK },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        defaultFSURI.toString());
    conf.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFileSystemOverloadScheme.class.getName());
    conf.unset(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
        HDFS_SCHEME));

    try (FileSystem fs = FileSystem.get(conf)) {
      fs.createNewFile(new Path("/onRootWhenFallBack"));
      Assert.fail("OverloadScheme target fs should be valid.");
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/local --> file://TEST_ROOT_DIR/root/
   *
   * It should be able to create file using ViewFileSystemOverloadScheme.
   */
  @Test(timeout = 30000)
  public void testViewFsOverloadSchemeWhenInnerCacheDisabled()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER },
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(), },
        conf);
    conf.setBoolean(Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    try (FileSystem fs = FileSystem.get(conf)) {
      Path testFile = new Path(HDFS_USER_FOLDER + "/testFile");
      fs.createNewFile(testFile);
      Assert.assertTrue(fs.exists(testFile));
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser0 --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/HDFSUser1 --> hdfs://localhost:xxx/HDFSUser/
   *
   * 1. With cache, only one hdfs child file system instance should be there.
   * 2. Without cache, there should 2 hdfs instances.
   */
  @Test(timeout = 30000)
  public void testViewFsOverloadSchemeWithInnerCache()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER + 0, HDFS_USER_FOLDER + 1 },
        new String[] {hdfsTargetPath.toUri().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);

    // 1. Only 1 hdfs child file system should be there with cache.
    try (FileSystem vfs = FileSystem.get(conf)) {
      Assert.assertEquals(1, vfs.getChildFileSystems().length);
    }

    // 2. Two hdfs file systems should be there if no cache.
    conf.setBoolean(Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    try (FileSystem vfs = FileSystem.get(conf)) {
      Assert.assertEquals(isFallBackExist(conf) ? 3 : 2,
          vfs.getChildFileSystems().length);
    }
  }

  // HDFS-15529: if any extended tests added fallback, then getChildFileSystems
  // will include fallback as well.
  private boolean isFallBackExist(Configuration config) {
    return config.get(ConfigUtil.getConfigViewFsPrefix(defaultFSURI
        .getAuthority()) + "." + Constants.CONFIG_VIEWFS_LINK_FALLBACK) != null;
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/HDFSUser0 --> hdfs://localhost:xxx/HDFSUser/
   * hdfs://localhost:xxx/HDFSUser1 --> hdfs://localhost:xxx/HDFSUser/
   *
   * When InnerCache disabled, all matching ViewFileSystemOverloadScheme
   * initialized scheme file systems would not use FileSystem cache.
   */
  @Test(timeout = 3000)
  public void testViewFsOverloadSchemeWithNoInnerCacheAndHdfsTargets()
      throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {HDFS_USER_FOLDER + 0, HDFS_USER_FOLDER + 1 },
        new String[] {hdfsTargetPath.toUri().toString(),
            hdfsTargetPath.toUri().toString() },
        conf);

    conf.setBoolean(Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    // Two hdfs file systems should be there if no cache.
    try (FileSystem vfs = FileSystem.get(conf)) {
      Assert.assertEquals(isFallBackExist(conf) ? 3 : 2,
          vfs.getChildFileSystems().length);
    }
  }

  /**
   * Create mount links as follows
   * hdfs://localhost:xxx/local0 --> file://localPath/
   * hdfs://localhost:xxx/local1 --> file://localPath/
   *
   * When InnerCache disabled, all non matching ViewFileSystemOverloadScheme
   * initialized scheme file systems should continue to take advantage of
   * FileSystem cache.
   */
  @Test(timeout = 3000)
  public void testViewFsOverloadSchemeWithNoInnerCacheAndLocalSchemeTargets()
      throws Exception {
    final Path localTragetPath = new Path(localTargetDir.toURI());
    addMountLinks(defaultFSURI.getAuthority(),
        new String[] {LOCAL_FOLDER + 0, LOCAL_FOLDER + 1 },
        new String[] {localTragetPath.toUri().toString(),
            localTragetPath.toUri().toString() },
        conf);

    // Only one local file system should be there if no InnerCache, but fs
    // cache should work.
    conf.setBoolean(Constants.CONFIG_VIEWFS_ENABLE_INNER_CACHE, false);
    try (FileSystem vfs = FileSystem.get(conf)) {
      Assert.assertEquals(isFallBackExist(conf) ? 2 : 1,
          vfs.getChildFileSystems().length);
    }
  }

  /**
   * Tests the rename with nfly mount link.
   */
  @Test(timeout = 3000)
  public void testNflyRename() throws Exception {
    final Path hdfsTargetPath1 = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path hdfsTargetPath2 = new Path(defaultFSURI + HDFS_USER_FOLDER + 1);
    final URI uri1 = hdfsTargetPath1.toUri();
    final URI uri2 = hdfsTargetPath2.toUri();
    final Path nflyRoot = new Path("/nflyroot");

    final String nflyLinkKey = Constants.CONFIG_VIEWFS_LINK_NFLY
        + ".minReplication=2." + nflyRoot.toString();
    addMountLinks(defaultFSURI.getAuthority(), new String[] {nflyLinkKey },
        new String[] {uri1.toString() + "," + uri2.toString() }, conf);
    final FileSystem nfly = FileSystem.get(defaultFSURI, conf);

    final Path testDir = new Path("/nflyroot/testdir1/sub1/sub3");
    final Path testDirTmp = new Path("/nflyroot/testdir1/sub1/sub3_temp");
    assertTrue(testDir + ": Failed to create!", nfly.mkdirs(testDir));

    // Test renames
    assertTrue(nfly.rename(testDir, testDirTmp));
    assertTrue(nfly.rename(testDirTmp, testDir));

    final URI[] testUris = new URI[] {uri1, uri2 };
    for (final URI testUri : testUris) {
      final FileSystem fs = FileSystem.get(testUri, conf);
      assertTrue(testDir + " should exist!", fs.exists(testDir));
    }
  }

  /**
   * Tests the write and read contents with nfly mount link.
   */
  @Test(timeout = 3000)
  public void testNflyWriteRead() throws Exception {
    final Path hdfsTargetPath1 = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path hdfsTargetPath2 = new Path(defaultFSURI + HDFS_USER_FOLDER + 1);
    final URI uri1 = hdfsTargetPath1.toUri();
    final URI uri2 = hdfsTargetPath2.toUri();
    final Path nflyRoot = new Path("/nflyroot");
    final String nflyLinkKey = Constants.CONFIG_VIEWFS_LINK_NFLY
        + ".minReplication=2." + nflyRoot.toString();
    addMountLinks(defaultFSURI.getAuthority(), new String[] {nflyLinkKey },
        new String[] {uri1.toString() + "," + uri2.toString() }, conf);
    final FileSystem nfly = FileSystem.get(defaultFSURI, conf);
    final Path testFile = new Path("/nflyroot/test.txt");
    writeString(nfly, TEST_STRING, testFile);
    final URI[] testUris = new URI[] {uri1, uri2 };
    for (final URI testUri : testUris) {
      try (FileSystem fs = FileSystem.get(testUri, conf)) {
        readString(fs, testFile, TEST_STRING, testUri);
      }
    }
  }

  /**
   * 1. Writes contents with nfly link having two target uris. 2. Deletes one
   * target file. 3. Tests the read works with repairOnRead flag. 4. Tests that
   * previously deleted file fully recovered and exists.
   */
  @Test(timeout = 3000)
  public void testNflyRepair() throws Exception {
    final NflyFSystem.NflyKey repairKey = NflyFSystem.NflyKey.repairOnRead;
    final Path hdfsTargetPath1 = new Path(defaultFSURI + HDFS_USER_FOLDER);
    final Path hdfsTargetPath2 = new Path(defaultFSURI + HDFS_USER_FOLDER + 1);
    final URI uri1 = hdfsTargetPath1.toUri();
    final URI uri2 = hdfsTargetPath2.toUri();
    final Path nflyRoot = new Path("/nflyroot");
    final String nflyLinkKey = Constants.CONFIG_VIEWFS_LINK_NFLY
        + ".minReplication=2," + repairKey + "=true." + nflyRoot.toString();
    addMountLinks(defaultFSURI.getAuthority(), new String[] {nflyLinkKey },
        new String[] {uri1.toString() + "," + uri2.toString() }, conf);
    try (FileSystem nfly = FileSystem.get(defaultFSURI, conf)) {
      // write contents to nfly
      final Path testFilePath = new Path("/nflyroot/test.txt");
      writeString(nfly, TEST_STRING, testFilePath);

      final URI[] testUris = new URI[] {uri1, uri2 };
      // both nodes are up again, test repair
      FsGetter getter = new ViewFileSystemOverloadScheme.ChildFsGetter("hdfs");
      try (FileSystem fs1 = getter.getNewInstance(testUris[0], conf)) {
        // Delete a file from one target URI
        String testFile = "/test.txt";
        assertTrue(
            fs1.delete(new Path(testUris[0].toString() + testFile), false));
        assertFalse(fs1.exists(new Path(testUris[0].toString() + testFile)));

        // Verify read success.
        readString(nfly, testFilePath, TEST_STRING, testUris[0]);
        // Verify file recovered.
        assertTrue(fs1.exists(new Path(testUris[0].toString() + testFile)));
      }
    }
  }

  /**
   * Tests that the fs initialization should ignore the port number when it's
   * extracting the mount table name from uri.
   */
  @Test(timeout = 30000)
  public void testMountTableNameShouldIgnorePortFromURI() throws Exception {
    final Path hdfsTargetPath = new Path(defaultFSURI + HDFS_USER_FOLDER);
    conf = new Configuration(getConf());
    addMountLinks(defaultFSURI.getHost(),
        new String[] {HDFS_USER_FOLDER, LOCAL_FOLDER,
            Constants.CONFIG_VIEWFS_LINK_FALLBACK},
        new String[] {hdfsTargetPath.toUri().toString(),
            localTargetDir.toURI().toString(),
            hdfsTargetPath.toUri().toString()}, conf);
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        defaultFSURI.toString());
    conf.set(String.format(FS_IMPL_PATTERN_KEY, HDFS_SCHEME),
        ViewFileSystemOverloadScheme.class.getName());
    conf.set(String
        .format(FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
            HDFS_SCHEME), DistributedFileSystem.class.getName());
    conf.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME, true);

    Path testDirOnRoot = new Path("/test");
    URI uriWithoutPort = new URI("hdfs://" + defaultFSURI.getHost());
    //Initialize with out port
    try (FileSystem fs = FileSystem
        .get(uriWithoutPort, conf)) {
      fs.mkdirs(testDirOnRoot);
      fs.delete(testDirOnRoot, true);
    }

    //Initialize with port
    try (FileSystem fs = FileSystem.get(defaultFSURI, conf)) {
      fs.mkdirs(testDirOnRoot);
      fs.delete(testDirOnRoot, true);
    }
  }

  private void writeString(final FileSystem nfly, final String testString,
      final Path testFile) throws IOException {
    try (FSDataOutputStream fsDos = nfly.create(testFile)) {
      fsDos.writeUTF(testString);
    }
  }

  private void readString(final FileSystem nfly, final Path testFile,
      final String testString, final URI testUri) throws IOException {
    try (FSDataInputStream fsDis = nfly.open(testFile)) {
      assertEquals("Wrong file content", testString, fsDis.readUTF());
    }
  }

  /**
   * @return configuration.
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * @return configuration.
   */
  public Configuration getNewConf() {
    return new Configuration(cluster.getConfiguration(0));
  }

  /**
   * sets configuration.
   */
  public void setConf(Configuration config) {
    conf = config;
  }
}