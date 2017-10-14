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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
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
}
