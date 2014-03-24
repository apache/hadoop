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

package org.apache.hadoop.fs;

import java.io.File;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests whether FileContext can resolve an hdfs path that has a symlink to
 * local file system. Also tests getDelegationTokens API in file context with
 * underlying file system as Hdfs.
 */
public class TestResolveHdfsSymlink {
  private static final FileContextTestHelper helper = new FileContextTestHelper();
  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setUp() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests resolution of an hdfs symlink to the local file system.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFcResolveAfs() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    FileContext fcLocal = FileContext.getLocalFSFileContext();
    FileContext fcHdfs = FileContext.getFileContext(cluster.getFileSystem()
        .getUri());

    final String localTestRoot = helper.getAbsoluteTestRootDir(fcLocal);
    Path alphaLocalPath = new Path(fcLocal.getDefaultFileSystem().getUri()
        .toString(), new File(localTestRoot, "alpha").getAbsolutePath());
    DFSTestUtil.createFile(FileSystem.getLocal(conf), alphaLocalPath, 16,
        (short) 1, 2);

    Path linkTarget = new Path(fcLocal.getDefaultFileSystem().getUri()
        .toString(), localTestRoot);
    Path hdfsLink = new Path(fcHdfs.getDefaultFileSystem().getUri().toString(),
        "/tmp/link");
    fcHdfs.createSymlink(linkTarget, hdfsLink, true);

    Path alphaHdfsPathViaLink = new Path(fcHdfs.getDefaultFileSystem().getUri()
        .toString()
        + "/tmp/link/alpha");

    Set<AbstractFileSystem> afsList = fcHdfs
        .resolveAbstractFileSystems(alphaHdfsPathViaLink);
    Assert.assertEquals(2, afsList.size());
    for (AbstractFileSystem afs : afsList) {
      if ((!afs.equals(fcHdfs.getDefaultFileSystem()))
          && (!afs.equals(fcLocal.getDefaultFileSystem()))) {
        Assert.fail("Failed to resolve AFS correctly");
      }
    }
  }
  
  /**
   * Tests delegation token APIs in FileContext for Hdfs; and renew and cancel
   * APIs in Hdfs.
   * 
   * @throws UnsupportedFileSystemException
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test
  public void testFcDelegationToken() throws UnsupportedFileSystemException,
      IOException, InterruptedException {
    FileContext fcHdfs = FileContext.getFileContext(cluster.getFileSystem()
        .getUri());
    final AbstractFileSystem afs = fcHdfs.getDefaultFileSystem();
    final List<Token<?>> tokenList =
        afs.getDelegationTokens(UserGroupInformation.getCurrentUser()
            .getUserName());
    ((Hdfs) afs).renewDelegationToken((Token<DelegationTokenIdentifier>) tokenList
        .get(0));
    ((Hdfs) afs).cancelDelegationToken(
        (Token<? extends AbstractDelegationTokenIdentifier>) tokenList.get(0));
  }

  /**
   * Verifies that attempting to resolve a non-symlink results in client
   * exception
   */
  @Test
  public void testLinkTargetNonSymlink() throws UnsupportedFileSystemException,
      IOException {
    FileContext fc = null;
    Path notSymlink = new Path("/notasymlink");
    try {
      fc = FileContext.getFileContext(cluster.getFileSystem().getUri());
      fc.create(notSymlink, EnumSet.of(CreateFlag.CREATE));
      DFSClient client = new DFSClient(cluster.getFileSystem().getUri(),
          cluster.getConfiguration(0));
      try {
        client.getLinkTarget(notSymlink.toString());
        fail("Expected exception for resolving non-symlink");
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains("is not a symbolic link", e);
      }
    } finally {
      if (fc != null) {
        fc.delete(notSymlink, false);
      }
    }
  }

  /**
   * Tests that attempting to resolve a non-existent-file
   */
  @Test
  public void testLinkTargetNonExistent() throws IOException {
    Path doesNotExist = new Path("/filethatdoesnotexist");
    DFSClient client = new DFSClient(cluster.getFileSystem().getUri(),
        cluster.getConfiguration(0));
    try {
      client.getLinkTarget(doesNotExist.toString());
      fail("Expected exception for resolving non-existent file");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("File does not exist: "
          + doesNotExist.toString(), e);
    }
  }
}
