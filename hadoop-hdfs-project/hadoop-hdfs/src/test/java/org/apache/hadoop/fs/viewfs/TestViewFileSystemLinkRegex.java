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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

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

import static org.apache.hadoop.fs.viewfs.RegexMountPoint.INTERCEPTOR_INTERNAL_SEP;

/**
 * Test linkRegex node type for view file system.
 */
public class TestViewFileSystemLinkRegex extends ViewFileSystemBaseTest {
  public static final Logger LOGGER =
      LoggerFactory.getLogger(TestViewFileSystemLinkRegex.class);

  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final Configuration CONF = new Configuration();
  private static final String CLUSTER_NAME =
      "TestViewFileSystemLinkRegexCluster";
  private static final File TEST_DIR = GenericTestUtils
      .getTestDir(TestViewFileSystemLinkRegex.class.getSimpleName());
  private static final String TEST_BASE_PATH =
      "/tmp/TestViewFileSystemLinkRegex";

  @Override protected FileSystemTestHelper createFileSystemHelper() {
    return new FileSystemTestHelper(TEST_BASE_PATH);
  }

  @BeforeClass public static void clusterSetupAtBeginning() throws IOException {
    SupportsBlocks = true;
    CONF.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(CONF).nnTopology(
        MiniDFSNNTopology.simpleFederatedTopology(NAME_SPACES_COUNT))
        .numDataNodes(DATA_NODES_COUNT).build();
    cluster.waitClusterUp();

    for (int i = 0; i < NAME_SPACES_COUNT; i++) {
      FS_HDFS[i] = cluster.getFileSystem(i);
    }
    fsDefault = FS_HDFS[FS_INDEX_DEFAULT];
  }

  @AfterClass public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override @Before public void setUp() throws Exception {
    fsTarget = fsDefault;
    super.setUp();
  }

  /**
   * Override this so that we don't set the targetTestRoot to any path under the
   * root of the FS, and so that we don't try to delete the test dir, but rather
   * only its contents.
   */
  @Override void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    for (FileStatus status : fsDefault.listStatus(targetTestRoot)) {
      fsDefault.delete(status.getPath(), true);
    }
  }

  @Override void setupMountPoints() {
    super.setupMountPoints();
  }

  @Override int getExpectedDelegationTokenCount() {
    return 1; // all point to the same fs so 1 unique token
  }

  @Override
  int getExpectedDelegationTokenCountWithCredentials() {
    return 1;
  }

  public String buildReplaceInterceptorSettingString(String srcRegex,
      String replaceString) {
    return
        RegexMountPointInterceptorType.REPLACE_RESOLVED_DST_PATH.getConfigName()
            + INTERCEPTOR_INTERNAL_SEP + srcRegex + INTERCEPTOR_INTERNAL_SEP
            + replaceString;
  }

  public String linkInterceptorSettings(
      List<String> interceptorSettingStrList) {
    StringBuilder stringBuilder = new StringBuilder();
    int listSize = interceptorSettingStrList.size();
    for (int i = 0; i < listSize; ++i) {
      stringBuilder.append(interceptorSettingStrList.get(i));
      if (i < listSize - 1) {
        stringBuilder.append(RegexMountPoint.INTERCEPTOR_SEP);
      }
    }
    return stringBuilder.toString();
  }

  @Test
  public void testConfLinkRegexIndexMapping() throws Exception {
    // (^/(\w+),/targetTestRoot/$1)
    // => /targetTestRoot/testConfLinkRegexIndexMapping1
    URI viewFsUri =
        new URI(FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/(\\w+)";
    String dstPathStr = targetTestRoot + "$1";
    Path srcPath = new Path("/testConfLinkRegexIndexMapping1");
    Path expectedResolveResult =
        new Path(dstPathStr.replace("$1", "testConfLinkRegexIndexMapping1"));
    FSDataOutputStream outputStream = fsTarget.create((expectedResolveResult));
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());

    // Test ${1} format
    // ^/(\w+, /targetTestRoot/${1})
    // => /targetTestRoot/testConfLinkRegexIndexMapping2
    dstPathStr = targetTestRoot + "${1}";
    srcPath = new Path("/testConfLinkRegexIndexMapping2");
    expectedResolveResult =
        new Path(dstPathStr.replace("${1}", "testConfLinkRegexIndexMapping2"));
    outputStream = fsTarget.create(expectedResolveResult);
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());

    //(^/(\w+)/file1, /targetTestRoot/$1)
    // = > /targetTestRoot/testConfLinkRegexIndexMapping3/file1
    dstPathStr = targetTestRoot + "$1";
    srcPath = new Path("/testConfLinkRegexIndexMapping3/file1");
    expectedResolveResult = new Path(
        dstPathStr.replace("$1", "testConfLinkRegexIndexMapping3/file1"));
    outputStream = fsTarget.create(expectedResolveResult);
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());

    //(^/(\w+)/file1, /targetTestRoot/$1/)
    // = > /targetTestRoot/testConfLinkRegexIndexMapping4/file1
    dstPathStr = targetTestRoot + "$1/";
    srcPath = new Path("/testConfLinkRegexIndexMapping4/file1");
    expectedResolveResult = new Path(
        dstPathStr.replace("$1", "testConfLinkRegexIndexMapping4/file1"));
    outputStream = fsTarget.create(expectedResolveResult);
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());
  }

  @Test
  public void testConfLinkRegexNamedGroupMapping() throws Exception {
    // ^/(?<firstDir>\\w+) = > /targetTestRoot/$firstDir
    URI viewFsUri =
        new URI(FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/(?<firstDir>\\w+)";
    String dstPathStr = targetTestRoot + "$firstDir";
    Path srcPath = new Path("/testConfLinkRegexNamedGroupMapping1");
    Path expectedResolveResult = new Path(
        dstPathStr.replace("$firstDir", "testConfLinkRegexNamedGroupMapping1"));
    FSDataOutputStream outputStream = fsTarget.create((expectedResolveResult));
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());

    // Test ${1} format
    dstPathStr = targetTestRoot + "${firstDir}";
    srcPath = new Path("/testConfLinkRegexNamedGroupMapping2");
    expectedResolveResult = new Path(dstPathStr
        .replace("${firstDir}", "testConfLinkRegexNamedGroupMapping2"));
    outputStream = fsTarget.create(expectedResolveResult);
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());
  }

  @Test
  public void testConfLinkRegexFixedDestMapping() throws Exception {
    URI viewFsUri =
        new URI(FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/\\w+";
    String dstPathStr =
        targetTestRoot + "testConfLinkRegexFixedDestMappingFile";
    Path expectedResolveResult = new Path(dstPathStr);
    FSDataOutputStream outputStream = fsTarget.create((expectedResolveResult));
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil.addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, null);
    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(
        expectedResolveResult.equals(vfs.resolvePath(new Path("/misc1"))));
    Assert.assertTrue(
        expectedResolveResult.equals(vfs.resolvePath(new Path("/misc2"))));
  }

  @Test
  public void testConfLinkRegexWithSingleInterceptor() throws Exception {
    URI viewFsUri =
        new URI(FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/user/(?<username>\\w+)";
    String dstPathStr = targetTestRoot + "$username";
    // Replace "_" with "-"
    String settingString = buildReplaceInterceptorSettingString("_", "-");
    Path srcPath = new Path("/user/hadoop_user1/hadoop_file1");
    Path expectedResolveResult =
        new Path(targetTestRoot, "hadoop-user1/hadoop_file1");
    FSDataOutputStream outputStream = fsTarget.create((expectedResolveResult));
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil
        .addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, settingString);
    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());
  }

  @Test
  public void testConfLinkRegexWithInterceptors() throws Exception {
    URI viewFsUri =
        new URI(FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/user/(?<username>\\w+)/";
    String dstPathStr = targetTestRoot + "$username";
    // Replace "_" with "-"
    String interceptor1 = buildReplaceInterceptorSettingString("_", "-");
    // Replace "hadoop" with "hdfs"
    String interceptor2 =
        buildReplaceInterceptorSettingString("hadoop", "hdfs");
    String interceptors =
        linkInterceptorSettings(Arrays.asList(interceptor1, interceptor2));
    Path srcPath = new Path("/user/hadoop_user1/hadoop_file1");
    Path expectedResolveResult =
        new Path(targetTestRoot, "hdfs-user1/hadoop_file1");
    FSDataOutputStream outputStream = fsTarget.create((expectedResolveResult));
    fsTarget.listStatus(expectedResolveResult);
    outputStream.close();
    ConfigUtil
        .addLinkRegex(conf, CLUSTER_NAME, regexStr, dstPathStr, interceptors);
    FileSystem vfs = FileSystem.get(viewFsUri, conf);
    Assert.assertTrue(expectedResolveResult.equals(vfs.resolvePath(srcPath)));
    Assert.assertEquals(0L, vfs.getFileStatus(srcPath).getLen());
  }
}
