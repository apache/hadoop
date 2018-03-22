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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.security.auth.login.LoginException;

/**
 * Test for viewfs with LinkMergeSlash mount table entries.
 */
public class TestViewFileSystemLinkMergeSlash extends ViewFileSystemBaseTest {

  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final String LINK_MERGE_SLASH_CLUSTER_1_NAME = "ClusterLMS1";
  private static final String LINK_MERGE_SLASH_CLUSTER_2_NAME = "ClusterLMS2";
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final Configuration CONF = new Configuration();
  private static final File TEST_DIR = GenericTestUtils.getTestDir(
      TestViewFileSystemLinkMergeSlash.class.getSimpleName());
  private static final String TEST_TEMP_PATH =
      "/tmp/TestViewFileSystemLinkMergeSlash";
  private final static Logger LOG = LoggerFactory.getLogger(
      TestViewFileSystemLinkMergeSlash.class);

  @Override
  protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper(TEST_TEMP_PATH);
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
    ConfigUtil.addLinkMergeSlash(conf, LINK_MERGE_SLASH_CLUSTER_1_NAME,
        targetTestRoot.toUri());
    ConfigUtil.addLinkMergeSlash(conf, LINK_MERGE_SLASH_CLUSTER_2_NAME,
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
  public void testConfLinkMergeSlash() throws Exception {
    TEST_DIR.mkdirs();
    String clusterName = "ClusterMerge";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String testFileName = "testLinkMergeSlash";

    File infile = new File(TEST_DIR, testFileName);
    final byte[] content = "HelloWorld".getBytes();
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(infile);
      fos.write(content);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
    assertEquals((long)content.length, infile.length());

    Configuration conf = new Configuration();
    ConfigUtil.addLinkMergeSlash(conf, clusterName, TEST_DIR.toURI());

    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    FileStatus stat = vfs.getFileStatus(new Path(viewFsUri.toString() +
        testFileName));

    LOG.info("File stat: " + stat);
    vfs.close();
  }

  @Test
  public void testConfLinkMergeSlashWithRegularLinks() throws Exception {
    TEST_DIR.mkdirs();
    String clusterName = "ClusterMerge";
    String expectedErrorMsg1 = "Mount table ClusterMerge has already been " +
        "configured with a merge slash link";
    String expectedErrorMsg2 = "Mount table ClusterMerge has already been " +
        "configured with regular links";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    Configuration conf = new Configuration();
    ConfigUtil.addLinkMergeSlash(conf, clusterName, TEST_DIR.toURI());
    ConfigUtil.addLink(conf, clusterName, "testDir", TEST_DIR.toURI());

    try {
      FileSystem.get(viewFsUri, conf);
      fail("Shouldn't allow both merge slash link and regular link on same "
          + "mount table.");
    } catch (IOException e) {
      assertTrue("Unexpected error message: " + e.getMessage(),
          e.getMessage().contains(expectedErrorMsg1) || e.getMessage()
              .contains(expectedErrorMsg2));
    }
  }

  @Test
  public void testConfLinkMergeSlashWithMountPoint() throws Exception {
    TEST_DIR.mkdirs();
    Configuration conf = new Configuration();
    String clusterName = "ClusterX";
    String mountPoint = "/user";
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME, clusterName,
        "/", null, null);
    String expectedErrorMsg =  "Invalid linkMergeSlash entry in config: " +
        "linkMergeSlash./user";
    String mountTableEntry = Constants.CONFIG_VIEWFS_PREFIX + "."
        + clusterName + "." + Constants.CONFIG_VIEWFS_LINK_MERGE_SLASH
        + "." + mountPoint;
    conf.set(mountTableEntry, TEST_DIR.toURI().toString());

    try {
      FileSystem.get(viewFsUri, conf);
      fail("Shouldn't allow linkMergeSlash to take extra mount points!");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(expectedErrorMsg));
    }
  }

  @Test
  public void testChildFileSystems() throws Exception {
    URI viewFsUri = new URI(FsConstants.VIEWFS_SCHEME,
        LINK_MERGE_SLASH_CLUSTER_1_NAME, "/", null, null);
    FileSystem fs = FileSystem.get(viewFsUri, conf);
    FileSystem[] childFs = fs.getChildFileSystems();
    Assert.assertEquals("Unexpected number of child filesystems!",
        1, childFs.length);
    Assert.assertEquals("Unexpected child filesystem!",
        DistributedFileSystem.class, childFs[0].getClass());
  }
}
