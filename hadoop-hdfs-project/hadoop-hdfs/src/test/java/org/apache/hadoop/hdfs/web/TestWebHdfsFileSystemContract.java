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

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class TestWebHdfsFileSystemContract extends FileSystemContractBaseTest {
  private static final Configuration conf = new Configuration();
  private static final MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  static {
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      //change root permission to 777
      cluster.getFileSystem().setPermission(
          new Path("/"), new FsPermission((short)0777));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setUp() throws Exception {
    final String uri = WebHdfsFileSystem.SCHEME  + "://"
        + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    //get file system as a non-superuser
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI(uri), conf);
      }
    });

    defaultWorkingDirectory = fs.getWorkingDirectory().toUri().getPath();
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  /** HDFS throws AccessControlException
   * when calling exist(..) on a path /foo/bar/file
   * but /foo/bar is indeed a file in HDFS.
   */
  @Override
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = path("/test/hadoop");
    assertFalse(fs.exists(testDir));
    assertTrue(fs.mkdirs(testDir));
    assertTrue(fs.exists(testDir));
    
    createFile(path("/test/hadoop/file"));
    
    Path testSubDir = path("/test/hadoop/file/subdir");
    try {
      fs.mkdirs(testSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(fs.exists(testSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
    
    Path testDeepSubDir = path("/test/hadoop/file/deep/sub/dir");
    try {
      fs.mkdirs(testDeepSubDir);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // expected
    }
    try {
      assertFalse(fs.exists(testDeepSubDir));
    } catch(AccessControlException e) {
      // also okay for HDFS.
    }    
  }
}
