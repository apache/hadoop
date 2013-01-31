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
package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.fs.FileSystemTestHelper.exists;
import static org.apache.hadoop.fs.FileSystemTestHelper.getTestRootPath;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.web.resources.DatanodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFSMainOperationsWebHdfs extends FSMainOperationsBaseTest {
  {
    ((Log4JLogger)ExceptionHandler.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DatanodeWebHdfsMethods.LOG).getLogger().setLevel(Level.ALL);
  }

  private static MiniDFSCluster cluster = null;
  private static Path defaultWorkingDirectory;

  @BeforeClass
  public static void setupCluster() {
    // Initialize the test root directory to a DFS like path
    // since we are testing based on the MiniDFSCluster.
    FileSystemTestHelper.TEST_ROOT_DIR = "/tmp/TestFSMainOperationsWebHdfs";

    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      //change root permission to 777
      cluster.getFileSystem().setPermission(
          new Path("/"), new FsPermission((short)0777));

      final String uri = WebHdfsFileSystem.SCHEME  + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

      //get file system as a non-superuser
      final UserGroupInformation current = UserGroupInformation.getCurrentUser();
      final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          current.getShortUserName() + "x", new String[]{"user"});
      fSys = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return FileSystem.get(new URI(uri), conf);
        }
      });

      defaultWorkingDirectory = fSys.getWorkingDirectory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected Path getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Test
  public void testConcat() throws Exception {
    Path[] paths = {new Path("/test/hadoop/file1"),
                    new Path("/test/hadoop/file2"),
                    new Path("/test/hadoop/file3")};

    DFSTestUtil.createFile(fSys, paths[0], 1024, (short) 3, 0);
    DFSTestUtil.createFile(fSys, paths[1], 1024, (short) 3, 0);
    DFSTestUtil.createFile(fSys, paths[2], 1024, (short) 3, 0);

    Path catPath = new Path("/test/hadoop/catFile");
    DFSTestUtil.createFile(fSys, catPath, 1024, (short) 3, 0);
    Assert.assertTrue(exists(fSys, catPath));

    fSys.concat(catPath, paths);

    Assert.assertFalse(exists(fSys, paths[0]));
    Assert.assertFalse(exists(fSys, paths[1]));
    Assert.assertFalse(exists(fSys, paths[2]));

    FileStatus fileStatus = fSys.getFileStatus(catPath);
    Assert.assertEquals(1024*4, fileStatus.getLen());
  }

  @Override
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assert.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    try {
      fSys.mkdirs(testSubDir);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      Assert.assertFalse(exists(fSys, testSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    try {
      fSys.mkdirs(testDeepSubDir);
      Assert.fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      Assert.assertFalse(exists(fSys, testDeepSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
  }
}
