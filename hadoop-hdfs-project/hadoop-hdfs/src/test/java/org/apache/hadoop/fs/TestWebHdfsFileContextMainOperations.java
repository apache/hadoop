/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.FileContextTestHelper.getDefaultBlockSize;
import static org.apache.hadoop.fs.FileContextTestHelper.getFileData;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of FileContext apis on Webhdfs.
 */
public class TestWebHdfsFileContextMainOperations
    extends FileContextMainOperationsBaseTest {

  protected static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  protected static URI webhdfsUrl;

  protected static int numBlocks = 2;

  protected static final byte[] data = getFileData(numBlocks,
      getDefaultBlockSize());
  protected static final HdfsConfiguration CONF = new HdfsConfiguration();

  @Override
  public Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Override
  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper();
  }

  public URI getWebhdfsUrl() {
    return webhdfsUrl;
  }

  @BeforeClass
  public static void clusterSetupAtBeginning()
      throws IOException, LoginException, URISyntaxException {

    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    webhdfsUrl = new URI(WebHdfs.SCHEME + "://" + cluster.getConfiguration(0)
        .get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY));
    fc = FileContext.getFileContext(webhdfsUrl, CONF);
    defaultWorkingDirectory = fc.makeQualified(new Path(
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  @Before
  public void setUp() throws Exception {
    URI webhdfsUrlReal = getWebhdfsUrl();
    Path testBuildData = new Path(
        webhdfsUrlReal + "/" + GenericTestUtils.DEFAULT_TEST_DATA_PATH
            + RandomStringUtils.randomAlphanumeric(10));
    Path rootPath = new Path(testBuildData, "root-uri");

    localFsRootPath = rootPath.makeQualified(webhdfsUrlReal, null);
    fc.mkdir(getTestRootPath(fc, "test"), FileContext.DEFAULT_PERM, true);
  }

  private Path getTestRootPath(FileContext fc, String path) {
    return fileContextTestHelper.getTestRootPath(fc, path);
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  /**
   * Test FileContext APIs when symlinks are not supported
   * TODO: Open separate JIRA for full support of the Symlink in webhdfs
   */
  @Test
  public void testUnsupportedSymlink() throws IOException {
    /**
     * WebHdfs client Partially supports the Symlink.
     * creation of Symlink is supported, but the getLinkTargetPath() api is not supported currently,
     * Implement the test case once the full support is available.
     */
  }

  /**
   * TODO: Open JIRA for the idiosyncrasies between hdfs and webhdfs
   */
  public void testSetVerifyChecksum() throws IOException {
    final Path rootPath = getTestRootPath(fc, "test");
    final Path path = new Path(rootPath, "zoo");

    FSDataOutputStream out = fc
        .create(path, EnumSet.of(CREATE), Options.CreateOpts.createParent());
    try {
      out.write(data, 0, data.length);
    } finally {
      out.close();
    }

    //In webhdfs scheme fc.setVerifyChecksum() can be called only after
    // writing first few bytes but in case of the hdfs scheme we can call
    // immediately after the creation call.
    // instruct FS to verify checksum through the FileContext:
    fc.setVerifyChecksum(true, path);

    FileStatus fileStatus = fc.getFileStatus(path);
    final long len = fileStatus.getLen();
    assertTrue(len == data.length);
    byte[] bb = new byte[(int) len];
    FSDataInputStream fsdis = fc.open(path);
    try {
      fsdis.readFully(bb);
    } finally {
      fsdis.close();
    }
    assertArrayEquals(data, bb);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
