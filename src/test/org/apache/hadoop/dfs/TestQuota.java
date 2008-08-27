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
package org.apache.hadoop.dfs;

import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UnixUserGroupInformation;

import junit.framework.TestCase;

/** A class for testing quota-related commands */
public class TestQuota extends TestCase {
  private void runCommand(DFSAdmin admin, String args[], boolean expectEror)
  throws Exception {
    int val = admin.run(args);
    if (expectEror) {
      assertEquals(val, -1);
    } else {
      assertTrue(val>=0);
    }
  }
  
  /** Test quota related commands: setQuota, clrQuota, and count */
  public void testQuotaCommands() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    DFSAdmin admin = new DFSAdmin(conf);
    
    try {
      // 1: create a directory /test and set its quota to be 3
      final Path parent = new Path("/test");
      assertTrue(dfs.mkdirs(parent));
      String[] args = new String[]{"-setQuota", "3", parent.toString()};
      runCommand(admin, args, false);
      
      // 2: create directory /test/data0
      final Path childDir0 = new Path(parent, "data0");
      assertTrue(dfs.mkdirs(childDir0));

      // 3: create a file /test/datafile0
      final Path childFile0 = new Path(parent, "datafile0");
      OutputStream fout = dfs.create(childFile0);
      fout.close();
      
      // 4: count -q /test
      ContentSummary c = dfs.getContentSummary(parent);
      assertEquals(c.getFileCount()+c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 3);
      
      // 5: count -q /test/data0
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getFileCount()+c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), -1);

      // 6: create a directory /test/data1
      final Path childDir1 = new Path(parent, "data1");
      boolean hasException = false;
      try {
        assertFalse(dfs.mkdirs(childDir1));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      // 7: create a file /test/datafile1
      final Path childFile1 = new Path(parent, "datafile1");
      hasException = false;
      try {
        fout = dfs.create(childFile1);
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      
      // 8: clear quota /test
      runCommand(admin, new String[]{"-clrQuota", parent.toString()}, false);
      c = dfs.getContentSummary(parent);
      assertEquals(c.getQuota(), -1);
      
      // 9: clear quota /test/data0
      runCommand(admin, new String[]{"-clrQuota", childDir0.toString()}, false);
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getQuota(), -1);
      
      // 10: create a file /test/datafile1
      fout = dfs.create(childFile1);
      fout.close();
      
      // 11: set the quota of /test to be 1
      args = new String[]{"-setQuota", "1", parent.toString()};
      runCommand(admin, args, true);
      
      // 12: set the quota of /test/data0 to be 1
      args = new String[]{"-setQuota", "1", childDir0.toString()};
      runCommand(admin, args, false);
      
      // 13: not able create a directory under data0
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(new Path(childDir0, "in")));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      c = dfs.getContentSummary(childDir0);
      assertEquals(c.getDirectoryCount()+c.getFileCount(), 1);
      assertEquals(c.getQuota(), 1);
      
      // 14a: set quota on a non-existent directory
      Path nonExistentPath = new Path("/test1");
      assertFalse(dfs.exists(nonExistentPath));
      args = new String[]{"-setQuota", "1", nonExistentPath.toString()};
      runCommand(admin, args, true);
      
      // 14b: set quota on a file
      assertTrue(dfs.isFile(childFile0));
      args[1] = childFile0.toString();
      runCommand(admin, args, true);
      
      // 15a: clear quota on a file
      args[0] = "-clrQuota";
      runCommand(admin, args, true);
      
      // 15b: clear quota on a non-existent directory
      args[1] = nonExistentPath.toString();
      runCommand(admin, args, true);

      // 16a: set the quota of /test to be 0
      args = new String[]{"-setQuota", "0", parent.toString()};
      runCommand(admin, args, true);
      
      // 16b: set the quota of /test to be -1
      args[1] = "-1";
      runCommand(admin, args, true);
      
      // 16c: set the quota of /test to be Long.MAX_VALUE+1
      args[1] = String.valueOf(Long.MAX_VALUE+1L);
      runCommand(admin, args, true);
      
      // 16d: set the quota of /test to be a non integer
      args[1] = "33aa1.5";
      runCommand(admin, args, true);
      
      // 17:  setQuota by a non-administrator
      UnixUserGroupInformation.saveToConf(conf, 
          UnixUserGroupInformation.UGI_PROPERTY_NAME, 
          new UnixUserGroupInformation(new String[]{"userxx\n", "groupyy\n"}));
      DFSAdmin userAdmin = new DFSAdmin(conf);
      args[1] = "100";
      runCommand(userAdmin, args, true);
      
      // 18: clrQuota by a non-administrator
      args = new String[] {"-clrQuota", parent.toString()};
      runCommand(userAdmin, args, true);
    } finally {
      cluster.shutdown();
    }
  }
  
  /** Test commands that change the size of the name space:
   *  mkdirs, rename, and delete */
  public void testNamespaceCommands() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
                fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    
    try {
      // 1: create directory /nqdir0/qdir1/qdir20/nqdir30
      assertTrue(dfs.mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")));

      // 2: set the quota of /nqdir0/qdir1 to be 6
      final Path quotaDir1 = new Path("/nqdir0/qdir1");
      dfs.setQuota(quotaDir1, 6);
      ContentSummary c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // 3: set the quota of /nqdir0/qdir1/qdir20 to be 7
      final Path quotaDir2 = new Path("/nqdir0/qdir1/qdir20");
      dfs.setQuota(quotaDir2, 7);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);

      // 4: Create directory /nqdir0/qdir1/qdir21 and set its quota to 2
      final Path quotaDir3 = new Path("/nqdir0/qdir1/qdir21");
      assertTrue(dfs.mkdirs(quotaDir3));
      dfs.setQuota(quotaDir3, 2);
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
      Path tempPath = new Path(quotaDir3, "nqdir32");
      assertTrue(dfs.mkdirs(tempPath));
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // 6: Create directory /nqdir0/qdir1/qdir21/nqdir33
      tempPath = new Path(quotaDir3, "nqdir33");
      boolean hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      c = dfs.getContentSummary(quotaDir3);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 2);

      // 7: Create directory /nqdir0/qdir1/qdir20/nqdir31
      tempPath = new Path(quotaDir2, "nqdir31");
      assertTrue(dfs.mkdirs(tempPath));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // 8: Create directory /nqdir0/qdir1/qdir20/nqdir33
      tempPath = new Path(quotaDir2, "nqdir33");
      hasException = false;
      try {
        assertFalse(dfs.mkdirs(tempPath));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // 9: Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
      tempPath = new Path(quotaDir2, "nqdir30");
      dfs.rename(new Path(quotaDir3, "nqdir32"), tempPath);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);

      // 10: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, quotaDir3));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir30")));
      
      // 10.a: Rename /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/nqdir32
      hasException = false;
      try {
        assertFalse(dfs.rename(tempPath, new Path(quotaDir3, "nqdir32")));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);
      assertTrue(dfs.exists(tempPath));
      assertFalse(dfs.exists(new Path(quotaDir3, "nqdir32")));

      // 11: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0
      assertTrue(dfs.rename(tempPath, new Path("/nqdir0")));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);

      // 12: Create directory /nqdir0/nqdir30/nqdir33
      assertTrue(dfs.mkdirs(new Path("/nqdir0/nqdir30/nqdir33")));

      // 13: Move /nqdir0/nqdir30 /nqdir0/qdir1/qdir20/qdir30
      hasException = false;
      try {
        assertFalse(dfs.rename(new Path("/nqdir0/nqdir30"), tempPath));
      } catch (QuotaExceededException e) {
        hasException = true;
      }
      assertTrue(hasException);

      // 14: Move /nqdir0/qdir1/qdir21 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(quotaDir3, quotaDir2));
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 4);
      assertEquals(c.getQuota(), 6);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 7);
      tempPath = new Path(quotaDir2, "qdir21");
      c = dfs.getContentSummary(tempPath);
      assertEquals(c.getDirectoryCount(), 1);
      assertEquals(c.getQuota(), 2);

      // 15: Delete /nqdir0/qdir1/qdir20/qdir21
      dfs.delete(tempPath, true);
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 2);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 3);
      assertEquals(c.getQuota(), 6);

      // 16: Move /nqdir0/qdir30 /nqdir0/qdir1/qdir20
      assertTrue(dfs.rename(new Path("/nqdir0/nqdir30"), quotaDir2));
      c = dfs.getContentSummary(quotaDir2);
      assertEquals(c.getDirectoryCount(), 5);
      assertEquals(c.getQuota(), 7);
      c = dfs.getContentSummary(quotaDir1);
      assertEquals(c.getDirectoryCount(), 6);
      assertEquals(c.getQuota(), 6);
    } finally {
      cluster.shutdown();
    }
  }
}
