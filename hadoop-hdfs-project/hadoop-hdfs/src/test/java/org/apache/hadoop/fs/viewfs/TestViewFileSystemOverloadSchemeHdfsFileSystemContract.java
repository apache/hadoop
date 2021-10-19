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

import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME;
import static org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestHDFSFileSystemContract;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests ViewFileSystemOverloadScheme with file system contract tests.
 */
public class TestViewFileSystemOverloadSchemeHdfsFileSystemContract
    extends TestHDFSFileSystemContract {

  private static MiniDFSCluster cluster;
  private static String defaultWorkingDirectory;
  private static Configuration conf = new HdfsConfiguration();

  @BeforeClass
  public static void init() throws IOException {
    final File basedir = GenericTestUtils.getRandomizedTestDir();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        FileSystemContractBaseTest.TEST_UMASK);
    cluster = new MiniDFSCluster.Builder(conf, basedir)
        .numDataNodes(2)
        .build();
    defaultWorkingDirectory =
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @Before
  public void setUp() throws Exception {
    conf.set(String.format("fs.%s.impl", "hdfs"),
        ViewFileSystemOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN,
        "hdfs"),
        DistributedFileSystem.class.getName());
    conf.setBoolean(CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME,
        CONFIG_VIEWFS_IGNORE_PORT_IN_MOUNT_TABLE_NAME_DEFAULT);
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), "/user",
        defaultFSURI);
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), "/append",
        defaultFSURI);
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(),
        "/FileSystemContractBaseTest/",
        new URI(defaultFSURI.toString() + "/FileSystemContractBaseTest/"));
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDownAfter() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Override
  @Test
  public void testAppend() throws IOException {
    AppendTestUtil.testAppend(fs, new Path("/append/f"));
  }

  @Override
  @Test(expected = AccessControlException.class)
  public void testRenameRootDirForbidden() throws Exception {
    super.testRenameRootDirForbidden();
  }

  @Override
  @Test
  public void testListStatusRootDir() throws Throwable {
    assumeTrue(rootDirTestEnabled());
    Path dir = path("/");
    Path child = path("/FileSystemContractBaseTest");
    try (FileSystem dfs = ((ViewFileSystemOverloadScheme) fs).getRawFileSystem(
        new Path(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY), "/"),
        conf)) {
      dfs.mkdirs(child);
    }
    assertListStatusFinds(dir, child);
  }

  @Override
  @Ignore // This test same as above in this case.
  public void testLSRootDir() throws Throwable {
  }
}
