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

import java.io.IOException;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.security.UnixUserGroupInformation;
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
  private static HdfsConfiguration CONF = new HdfsConfiguration();
  
  @BeforeClass
  public static void clusterSetupAtBegining() throws IOException,
      LoginException, URISyntaxException {
    cluster = new MiniDFSCluster(CONF, 2, true, null);
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getFileSystem());
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UnixUserGroupInformation.login().getUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  private static void restartCluster() throws IOException, LoginException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    cluster = new MiniDFSCluster(CONF, 1, false, null);
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getFileSystem());
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UnixUserGroupInformation.login().getUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }
      
  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    cluster.shutdown();   
  }
  
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
  @Test
  public void testRenameFileAsExistingFile() throws Exception {
    // ignore base class test till hadoop-6240 is fixed
  }
  
  @Test
  public void testOldRenameWithQuota() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath("test/testOldRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath("test/testOldRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath("test/testOldRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath("test/testOldRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), FSConstants.QUOTA_DONT_SET,
        FSConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);

    fs.setQuota(dst1.getParent(), 2, FSConstants.QUOTA_DONT_SET);
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
    fs.setQuota(src1.getParent(), 1, FSConstants.QUOTA_DONT_SET);
    oldRename(dst1, src1, false, true);

  }
  
    
  /**
   * Perform operations such as setting quota, deletion of files, rename and
   * ensure system can apply edits log during startup.
   */
  @Test
  public void testEditsLogOldRename() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath("testEditsLogOldRename/srcdir/src1");
    Path dst1 = getTestRootPath("testEditsLogOldRename/dstdir/dst1");
    createFile(src1);
    fs.mkdirs(dst1.getParent());
    createFile(dst1);
    
    // Set quota so that dst1 parent cannot allow under it new files/directories 
    fs.setQuota(dst1.getParent(), 2, FSConstants.QUOTA_DONT_SET);
    // Free up quota for a subsequent rename
    fs.delete(dst1, true);
    oldRename(src1, dst1, true, false);
    
    // Restart the cluster and ensure the above operations can be
    // loaded from the edits log
    restartCluster();
    fs = (DistributedFileSystem)cluster.getFileSystem();
    src1 = getTestRootPath("testEditsLogOldRename/srcdir/src1");
    dst1 = getTestRootPath("testEditsLogOldRename/dstdir/dst1");
    Assert.assertFalse(fs.exists(src1));   // ensure src1 is already renamed
    Assert.assertTrue(fs.exists(dst1));    // ensure rename dst exists
  }
  

  private void oldRename(Path src, Path dst, boolean renameSucceeds,
      boolean exception) throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    try {
      Assert.assertEquals(renameSucceeds, fs.rename(src, dst));
    } catch (Exception ex) {
      Assert.assertTrue(exception);
    }
    Assert.assertEquals(renameSucceeds, !fc.exists(src));
    Assert.assertEquals(renameSucceeds, fc.exists(dst));
  }
}
