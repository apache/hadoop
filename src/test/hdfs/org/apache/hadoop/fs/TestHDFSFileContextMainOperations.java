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

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
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
  
  @BeforeClass
  public static void clusterSetupAtBegining()
                                    throws IOException, LoginException  {
    cluster = new MiniDFSCluster(new HdfsConfiguration(), 2, true, null);
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
  public void testRenameWithQuota() throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    Path src1 = getTestRootPath("testRenameWithQuota/srcdir/src1");
    Path src2 = getTestRootPath("testRenameWithQuota/srcdir/src2");
    Path dst1 = getTestRootPath("testRenameWithQuota/dstdir/dst1");
    Path dst2 = getTestRootPath("testRenameWithQuota/dstdir/dst2");
    createFile(src1);
    createFile(src2);
    fs.setQuota(src1.getParent(), FSConstants.QUOTA_DONT_SET,
        FSConstants.QUOTA_DONT_SET);
    fc.mkdir(dst1.getParent(), FileContext.DEFAULT_PERM, true);
    fs.setQuota(dst1.getParent(), FSConstants.QUOTA_DONT_SET,
        FSConstants.QUOTA_DONT_SET);

    // Test1: src does not exceed quota and dst has no quota check and hence 
    // accommodates rename
    rename(src1, dst1, true, false);

    // Test2: src does not exceed quota and dst has *no* quota to accommodate
    // rename. 

    // testRenameWithQuota/dstDir now has quota = 1 and dst1 already uses it
    fs.setQuota(dst1.getParent(), 1, FSConstants.QUOTA_DONT_SET);
    rename(src2, dst2, false, true);

    // Test3: src exceeds quota and dst has *no* quota to accommodate rename
    
    // src1 has no quota to accommodate new rename node
    fs.setQuota(src1.getParent(), 1, FSConstants.QUOTA_DONT_SET);
    rename(dst1, src1, false, true);
  }
  
  private void rename(Path src, Path dst, boolean renameSucceeds,
      boolean quotaException) throws Exception {
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    try {
      Assert.assertEquals(renameSucceeds, fs.rename(src, dst));
    } catch (QuotaExceededException ex) {
      Assert.assertTrue(quotaException);
    }
    Assert.assertEquals(renameSucceeds, !fc.exists(src));
    Assert.assertEquals(renameSucceeds, fc.exists(dst));
  }
}
