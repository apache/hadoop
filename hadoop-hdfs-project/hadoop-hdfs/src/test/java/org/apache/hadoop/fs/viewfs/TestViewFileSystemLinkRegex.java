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
import java.net.URISyntaxException;
import java.util.ArrayList;
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
import static org.junit.Assert.assertSame;

/**
 * Test linkRegex node type for view file system.
 */
public class TestViewFileSystemLinkRegex extends ViewFileSystemBaseTest {
  public static final Logger LOGGER =
      LoggerFactory.getLogger(TestViewFileSystemLinkRegex.class);

  private static FileSystem fsDefault;
  private static MiniDFSCluster cluster;
  private static Configuration clusterConfig;
  private static final int NAME_SPACES_COUNT = 3;
  private static final int DATA_NODES_COUNT = 3;
  private static final int FS_INDEX_DEFAULT = 0;
  private static final FileSystem[] FS_HDFS = new FileSystem[NAME_SPACES_COUNT];
  private static final String CLUSTER_NAME =
      "TestViewFileSystemLinkRegexCluster";
  private static final File TEST_DIR = GenericTestUtils
      .getTestDir(TestViewFileSystemLinkRegex.class.getSimpleName());
  private static final String TEST_BASE_PATH =
      "/tmp/TestViewFileSystemLinkRegex";

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
  }

  @Override
  int getExpectedDelegationTokenCount() {
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

  private void createDirWithChildren(
      FileSystem fileSystem, Path dir, List<Path> childrenFiles)
      throws IOException {
    Assert.assertTrue(fileSystem.mkdirs(dir));
    int index = 0;
    for (Path childFile : childrenFiles) {
      createFile(fileSystem, childFile, index, true);
    }
  }

  private void createFile(
      FileSystem fileSystem, Path file, int dataLenToWrite, boolean overwrite)
      throws IOException {
    FSDataOutputStream outputStream = null;
    try {
      outputStream = fileSystem.create(file, overwrite);
      for (int i = 0; i < dataLenToWrite; ++i) {
        outputStream.writeByte(i);
      }
      outputStream.close();
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }

  private void createDirWithChildren(
      FileSystem fileSystem, Path dir, int childrenFilesCnt)
      throws IOException {
    List<Path> childrenFiles = new ArrayList<>(childrenFilesCnt);
    for (int i = 0; i < childrenFilesCnt; ++i) {
      childrenFiles.add(new Path(dir, "file" + i));
    }
    createDirWithChildren(fileSystem, dir, childrenFiles);
  }

  /**
   * The function used to test regex mountpoints.
   * @param config - get mountable config from this conf
   * @param regexStr - the src path regex expression that applies to this config
   * @param dstPathStr - the string of target path
   * @param interceptorSettings - the serialized interceptor string to be
   *                           applied while resolving the mapping
   * @param dirPathBeforeMountPoint - the src path user passed in to be mapped.
   * @param expectedResolveResult - the expected path after resolve
   *                             dirPathBeforeMountPoint via regex mountpint.
   * @param childrenFilesCnt - the child files under dirPathBeforeMountPoint to
   *                         be created
   * @throws IOException
   * @throws URISyntaxException
   */
  private void testRegexMountpoint(
      Configuration config,
      String regexStr,
      String dstPathStr,
      String interceptorSettings,
      Path dirPathBeforeMountPoint,
      Path expectedResolveResult,
      int childrenFilesCnt)
      throws IOException, URISyntaxException {
      // Set up test env
    createDirWithChildren(
        fsTarget, expectedResolveResult, childrenFilesCnt);
    ConfigUtil.addLinkRegex(
        config, CLUSTER_NAME, regexStr, dstPathStr, interceptorSettings);
    // Asserts
    URI viewFsUri = new URI(
        FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    try (FileSystem vfs = FileSystem.get(viewFsUri, config)) {
      Assert.assertEquals(expectedResolveResult.toString(),
          vfs.resolvePath(dirPathBeforeMountPoint).toString());
      Assert.assertTrue(
          vfs.getFileStatus(dirPathBeforeMountPoint).isDirectory());
      Assert.assertEquals(
          childrenFilesCnt, vfs.listStatus(dirPathBeforeMountPoint).length);

      // Test Inner cache, the resolved result's filesystem should be the same.
      ViewFileSystem viewFileSystem = (ViewFileSystem) vfs;
      ChRootedFileSystem target1 = (ChRootedFileSystem) viewFileSystem.fsState
          .resolve(viewFileSystem.getUriPath(dirPathBeforeMountPoint), true)
          .targetFileSystem;
      ChRootedFileSystem target2 = (ChRootedFileSystem) viewFileSystem.fsState
          .resolve(viewFileSystem.getUriPath(dirPathBeforeMountPoint), true)
          .targetFileSystem;
      assertSame(target1.getMyFs(), target2.getMyFs());
    }
  }
  /**
   * Test regex mount points which use capture group index for mapping.
   *
   * @throws Exception
   */
  @Test
  public void testConfLinkRegexIndexMapping() throws Exception {
    //  Config:
    //   <property>
    //     <name>
    //     fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(\w+)</name>
    //     <value>/targetTestRoot/$1</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexIndexMapping1
    // Expect path: /targetTestRoot/testConfLinkRegexIndexMapping1
    String regexStr = "^/(\\w+)";
    String dstPathStr = targetTestRoot + "$1";
    Path srcPath = new Path("/testConfLinkRegexIndexMapping1");
    Path expectedResolveResult = new Path(dstPathStr.replace(
          "$1", "testConfLinkRegexIndexMapping1"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 3);

    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(\w+)</name>
    //     <value>/targetTestRoot/${1}</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexIndexMapping2
    // Expect path: /targetTestRoot/testConfLinkRegexIndexMapping2

    dstPathStr = targetTestRoot + "${1}";
    srcPath = new Path("/testConfLinkRegexIndexMapping2");
    expectedResolveResult =
        new Path(
            dstPathStr.replace("${1}", "testConfLinkRegexIndexMapping2"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 4);

    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(\w+)</name>
    //     <value>/targetTestRoot/$1</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexIndexMapping3/dir1
    // Expect path: /targetTestRoot/testConfLinkRegexIndexMapping3/dir1
    dstPathStr = targetTestRoot + "$1";
    srcPath = new Path("/testConfLinkRegexIndexMapping3/dir1");
    expectedResolveResult = new Path(
        dstPathStr.replace("$1", "testConfLinkRegexIndexMapping3/dir1"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 5);

    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(\w+)</name>
    //     <value>/targetTestRoot/${1}/</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexIndexMapping4/dir1
    // Expect path: /targetTestRoot/testConfLinkRegexIndexMapping4/dir1
    dstPathStr = targetTestRoot + "${1}/";
    srcPath = new Path("/testConfLinkRegexIndexMapping4/dir1");
    expectedResolveResult = new Path(
          dstPathStr.replace("${1}", "testConfLinkRegexIndexMapping4/dir1"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 6);
  }

  /**
   * Test regex mount pointes with named capture group.
   * @throws Exception
   */
  @Test
  public void testConfLinkRegexNamedGroupMapping() throws Exception {
    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(?<firstDir>\w+)</name>
    //     <value>/targetTestRoot/$firstDir</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexNamedGroupMapping1
    // Expect path: /targetTestRoot/testConfLinkRegexNamedGroupMapping1
    URI viewFsUri = new URI(
        FsConstants.VIEWFS_SCHEME, CLUSTER_NAME, "/", null, null);
    String regexStr = "^/(?<firstDir>\\w+)";
    String dstPathStr = targetTestRoot + "$firstDir";
    Path srcPath = new Path("/testConfLinkRegexNamedGroupMapping1");
    Path expectedResolveResult = new Path(
        dstPathStr.replace("$firstDir", "testConfLinkRegexNamedGroupMapping1"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 3);

    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(?<firstDir>\w+)</name>
    //     <value>/targetTestRoot/${firstDir}</value>
    //   </property>
    // Dir path to test: /testConfLinkRegexNamedGroupMapping2
    // Expect path: /targetTestRoot/testConfLinkRegexNamedGroupMapping2
    dstPathStr = targetTestRoot + "${firstDir}";
    srcPath = new Path("/testConfLinkRegexNamedGroupMapping2");
    expectedResolveResult = new Path(
        dstPathStr.replace(
            "${firstDir}", "testConfLinkRegexNamedGroupMapping2"));
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        srcPath, expectedResolveResult, 5);
  }

  /**
   * Test cases when the destination is fixed paths.
   * @throws Exception
   */
  @Test
  public void testConfLinkRegexFixedDestMapping() throws Exception {
    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.^/(?<firstDir>\w+)</name>
    //     <value>/targetTestRoot/${firstDir}</value>
    //   </property>
    // Dir path to test: /misc1
    // Expect path: /targetTestRoot/testConfLinkRegexFixedDestMappingFile
    // Dir path to test: /misc2
    // Expect path: /targetTestRoot/testConfLinkRegexFixedDestMappingFile
    String regexStr = "^/\\w+";
    String dstPathStr =
        targetTestRoot + "testConfLinkRegexFixedDestMappingFile";
    Path expectedResolveResult = new Path(dstPathStr);
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        new Path("/misc1"), expectedResolveResult, 5);
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, null,
        new Path("/misc2"), expectedResolveResult, 6);
  }

  /**
   * Test regex mount point config with a single interceptor.
   *
   */
  @Test
  public void testConfLinkRegexWithSingleInterceptor() throws Exception {
    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex.replaceresolveddstpath:_:-#.^/user/(?<username>\w+)</name>
    //     <value>/targetTestRoot/$username</value>
    //   </property>
    // Dir path to test: /user/hadoop_user1/hadoop_dir1
    // Expect path: /targetTestRoot/hadoop-user1/hadoop_dir1

    String regexStr = "^/user/(?<username>\\w+)";
    String dstPathStr = targetTestRoot + "$username";
    // Replace "_" with "-"
    String settingString = buildReplaceInterceptorSettingString("_", "-");
    Path srcPath = new Path("/user/hadoop_user1/hadoop_dir1");
    Path expectedResolveResult = new Path(
        targetTestRoot, "hadoop-user1/hadoop_dir1");
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, settingString,
        srcPath, expectedResolveResult, 2);
  }

  /**
   * Test regex mount point config with multiple interceptors.
   *
   */
  @Test
  public void testConfLinkRegexWithInterceptors() throws Exception {
    // Config:
    //   <property>
    //     <name>fs.viewfs.mounttable.TestViewFileSystemLinkRegexCluster
    //     .linkRegex
    //     .replaceresolveddstpath:_:-;
    //     replaceresolveddstpath:hadoop:hdfs#.^/user/(?<username>\w+)</name>
    //     <value>/targetTestRoot/$username</value>
    //   </property>
    // Dir path to test: /user/hadoop_user1/hadoop_dir1
    // Expect path: /targetTestRoot/hdfs-user1/hadoop_dir1
    String regexStr = "^/user/(?<username>\\w+)/";
    String dstPathStr = targetTestRoot + "$username";
    // Replace "_" with "-"
    String interceptor1 = buildReplaceInterceptorSettingString("_", "-");
    // Replace "hadoop" with "hdfs"
    String interceptor2 =
        buildReplaceInterceptorSettingString("hadoop", "hdfs");
    String interceptors =
        linkInterceptorSettings(Arrays.asList(interceptor1, interceptor2));
    Path srcPath = new Path("/user/hadoop_user1/hadoop_dir1");
    Path expectedResolveResult =
        new Path(targetTestRoot, "hdfs-user1/hadoop_dir1");
    testRegexMountpoint(
        new Configuration(conf),
        regexStr, dstPathStr, interceptors,
        srcPath, expectedResolveResult, 2);

  }
}
