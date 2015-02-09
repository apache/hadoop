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

import static org.apache.hadoop.fs.FileContextTestHelper.exists;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHDFSFileContextMainOperations extends
    FileContextMainOperationsBaseTest {
  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static final HdfsConfiguration CONF = new HdfsConfiguration();
  
  @Override
  protected FileContextTestHelper createFileContextHelper() {
    return new FileContextTestHelper("/tmp/TestHDFSFileContextMainOperations");
  }

  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    URI uri0 = cluster.getURI(0);
    fc = FileContext.getFileContext(uri0, CONF);
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  private static void restartCluster() throws IOException, LoginException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(1)
                                              .format(false).build();
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getURI(0), CONF);
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }
      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }    
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  } 
  
  @Override
   protected IOException unwrapException(IOException e) {
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    }
    return e;
  }
  
  private Path getTestRootPath(FileContext fc, String path) {
    return fileContextTestHelper.getTestRootPath(fc, path);
  }

  @Test
  public void testTruncate() throws Exception {
    final short repl = 3;
    final int blockSize = 1024;
    final int numOfBlocks = 2;
    DistributedFileSystem fs = cluster.getFileSystem();
    Path dir = getTestRootPath(fc, "test/hadoop");
    Path file = getTestRootPath(fc, "test/hadoop/file");

    final byte[] data = FileSystemTestHelper.getFileData(
        numOfBlocks, blockSize);
    FileSystemTestHelper.createFile(fs, file, data, blockSize, repl);

    final int newLength = blockSize;

    boolean isReady = fc.truncate(file, newLength);

    Assert.assertTrue("Recovery is not expected.", isReady);

    FileStatus fileStatus = fc.getFileStatus(file);
    Assert.assertEquals(fileStatus.getLen(), newLength);
    AppendTestUtil.checkFullFile(fs, file, newLength, data, file.toString());

    ContentSummary cs = fs.getContentSummary(dir);
    Assert.assertEquals("Bad disk space usage", cs.getSpaceConsumed(),
        newLength * repl);
    Assert.assertTrue(fs.delete(dir, true));
  }

  @Test
  public void testOldRenameWithQuota() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath(fc, "test/testOldRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath(fc, "test/testOldRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);

    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    /* 
     * Test1: src does not exceed quota and dst has no quota check and hence 
     * accommodates rename
     */
    oldRename(src1, dst1, true, false);

    /*
     * Test2: src does not exceed quota and dst has *no* quota to accommodate 
     * rename. 
     */
    // dstDir quota = 1 and dst1 already uses it
    oldRename(src2, dst2, false, true);

    /*
     * Test3: src exceeds quota and dst has *no* quota to accommodate rename
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    oldRename(dst1, src1, false, true);
  }
  
  @Test
  public void testRenameWithQuota() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "test/testRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath(fc, "test/testRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath(fc, "test/testRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);

    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    /* 
     * Test1: src does not exceed quota and dst has no quota check and hence 
     * accommodates rename
     */
    // rename uses dstdir quota=1
    rename(src1, dst1, false, true, false, Rename.NONE);
    // rename reuses dstdir quota=1
    rename(src2, dst1, true, true, false, Rename.OVERWRITE);

    /*
     * Test2: src does not exceed quota and dst has *no* quota to accommodate 
     * rename. 
     */
    // dstDir quota = 1 and dst1 already uses it
    createFile(src2);
    rename(src2, dst2, false, false, true, Rename.NONE);

    /*
     * Test3: src exceeds quota and dst has *no* quota to accommodate rename
     * rename to a destination that does not exist
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    rename(dst1, src1, false, false, true, Rename.NONE);
    
    /*
     * Test4: src exceeds quota and dst has *no* quota to accommodate rename
     * rename to a destination that exists and quota freed by deletion of dst
     * is same as quota needed by src.
     */
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 100, HdfsConstants.QUOTA_DONT_SET);
    createFile(src1);
    fs.setQuota(src1.getParent(), 1, HdfsConstants.QUOTA_DONT_SET);
    rename(dst1, src1, true, true, false, Rename.OVERWRITE);
  }
  
  @Test
  public void testRenameRoot() throws Exception {
    Path src = getTestRootPath(fc, "test/testRenameRoot/srcdir/src1");
    Path dst = new Path("/");
    createFile(src);
    rename(src, dst, true, false, true, Rename.OVERWRITE);
    rename(dst, src, true, false, true, Rename.OVERWRITE);
  }
  
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  @Test
  public void testEditsLogOldRename() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
    Path dst1 = getTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
    createFile(src1);
    fs.mkdirs(dst1.getParent());
    createFile(dst1);
    
    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    // Free up quota for a subsequent rename
    fs.delete(dst1, true);
    oldRename(src1, dst1, true, false);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = cluster.getFileSystem();
    src1 = getTestRootPath(fc, "testEditsLogOldRename/srcdir/src1");
    dst1 = getTestRootPath(fc, "testEditsLogOldRename/dstdir/dst1");
    Assert.assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    Assert.assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }
  
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  @Test
  public void testEditsLogRename() throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    Path src1 = getTestRootPath(fc, "testEditsLogRename/srcdir/src1");
    Path dst1 = getTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
    createFile(src1);
    fs.mkdirs(dst1.getParent());
    createFile(dst1);
    
    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, HdfsConstants.QUOTA_DONT_SET);
    // Free up quota for a subsequent rename
    fs.delete(dst1, true);
    rename(src1, dst1, true, true, false, Rename.OVERWRITE);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = cluster.getFileSystem();
    src1 = getTestRootPath(fc, "testEditsLogRename/srcdir/src1");
    dst1 = getTestRootPath(fc, "testEditsLogRename/dstdir/dst1");
    Assert.assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    Assert.assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }

  @Test
  public void testIsValidNameInvalidNames() {
    String[] invalidNames = {
      "/foo/../bar",
      "/foo/./bar",
      "/foo/:/bar",
      "/foo:bar"
    };

    for (String invalidName: invalidNames) {
      Assert.assertFalse(invalidName + " is not valid",
        fc.getDefaultFileSystem().isValidName(invalidName));
    }
  }

  private void oldRename(Path src, Path dst, boolean renameSucceeds,
      boolean exception) throws Exception {
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      Assert.assertEquals(renameSucceeds, fs.rename(src, dst));
    } catch (Exception ex) {
      Assert.assertTrue(exception);
    }
    Assert.assertEquals(renameSucceeds, !exists(fc, src));
    Assert.assertEquals(renameSucceeds, exists(fc, dst));
  }
  
  private void rename(Path src, Path dst, boolean dstExists,
      boolean renameSucceeds, boolean exception, Options.Rename... options)
      throws Exception {
    try {
      fc.rename(src, dst, options);
      Assert.assertTrue(renameSucceeds);
    } catch (Exception ex) {
      Assert.assertTrue(exception);
    }
    Assert.assertEquals(renameSucceeds, !exists(fc, src));
    Assert.assertEquals((dstExists||renameSucceeds), exists(fc, dst));
  }
  
  @Override
  protected boolean listCorruptedBlocksSupported() {
    return true;
  }
  
  @Test
  public void testCrossFileSystemRename() throws IOException {
    try {
      fc.rename(
        new Path("hdfs://127.0.0.1/aaa/bbb/Foo"), 
        new Path("file://aaa/bbb/Moo"), 
        Options.Rename.OVERWRITE);
      fail("IOexception expected.");
    } catch (IOException ioe) {
      // okay
    }
  }
  
}
