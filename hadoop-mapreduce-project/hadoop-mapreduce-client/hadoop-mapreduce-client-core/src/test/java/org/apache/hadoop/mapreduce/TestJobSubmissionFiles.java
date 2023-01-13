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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.HdfsConfiguration;
/**
 * Tests for JobSubmissionFiles Utility class.
 */
public class TestJobSubmissionFiles {
  final private static String USER_1 = "user1@HADOOP.APACHE.ORG";
  final private static String USER_1_SHORT_NAME = "user1";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String GROUP3_NAME = "group3";
  final private static String[] GROUP_NAMES = new String[] {GROUP1_NAME,
      GROUP2_NAME, GROUP3_NAME };

  @Test
  public void testGetStagingDirWhenFullFileOwnerNameAndFullUserName()
      throws IOException, InterruptedException {
    Cluster cluster = mock(Cluster.class);
    Configuration conf = new Configuration();
    Path stagingPath = mock(Path.class);
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(USER_1, GROUP_NAMES);
    assertEquals(USER_1, user.getUserName());
    FileSystem fs = new FileSystemTestHelper.MockFileSystem();
    when(cluster.getStagingAreaDir()).thenReturn(stagingPath);
    when(stagingPath.getFileSystem(conf)).thenReturn(fs);

    //Staging directory owner full principal name is in lower case.
    String stagingDirOwner = USER_1.toLowerCase();
    FileStatus fileStatus = new FileStatus(1, true, 1, 1, 100L, 100L,
        FsPermission.getDefault(), stagingDirOwner, stagingDirOwner,
        stagingPath);
    when(fs.getFileStatus(stagingPath)).thenReturn(fileStatus);
    assertEquals(stagingPath,
        JobSubmissionFiles.getStagingDir(cluster, conf, user));

    //Staging directory owner full principal name in upper and lower case
    stagingDirOwner = USER_1;
    fileStatus = new FileStatus(1, true, 1, 1, 100L, 100L,
        FsPermission.getDefault(), stagingDirOwner, stagingDirOwner,
        stagingPath);
    when(fs.getFileStatus(stagingPath)).thenReturn(fileStatus);
    assertEquals(stagingPath,
        JobSubmissionFiles.getStagingDir(cluster, conf, user));
  }

  @Test(expected = IOException.class)
  public void testGetStagingWhenFileOwnerNameAndCurrentUserNameDoesNotMatch()
      throws IOException, InterruptedException {
    Cluster cluster = mock(Cluster.class);
    Configuration conf = new Configuration();
    String stagingDirOwner = "someuser";
    Path stagingPath = mock(Path.class);
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(USER_1, GROUP_NAMES);
    assertEquals(USER_1, user.getUserName());
    FileSystem fs = new FileSystemTestHelper.MockFileSystem();
    FileStatus fileStatus = new FileStatus(1, true, 1, 1, 100L, 100L,
        FsPermission.getDefault(), stagingDirOwner, stagingDirOwner,
        stagingPath);
    when(stagingPath.getFileSystem(conf)).thenReturn(fs);
    when(fs.getFileStatus(stagingPath)).thenReturn(fileStatus);
    when(cluster.getStagingAreaDir()).thenReturn(stagingPath);
    assertEquals(stagingPath,
        JobSubmissionFiles.getStagingDir(cluster, conf, user));
  }

  @Test
  public void testGetStagingDirWhenShortFileOwnerNameAndFullUserName()
      throws IOException, InterruptedException {
    Cluster cluster = mock(Cluster.class);
    Configuration conf = new Configuration();
    String stagingDirOwner = USER_1_SHORT_NAME;
    Path stagingPath = mock(Path.class);
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(USER_1, GROUP_NAMES);
    assertEquals(USER_1, user.getUserName());
    FileSystem fs = new FileSystemTestHelper.MockFileSystem();
    FileStatus fileStatus = new FileStatus(1, true, 1, 1, 100L, 100L,
        FsPermission.getDefault(), stagingDirOwner, stagingDirOwner,
        stagingPath);
    when(stagingPath.getFileSystem(conf)).thenReturn(fs);
    when(fs.getFileStatus(stagingPath)).thenReturn(fileStatus);
    when(cluster.getStagingAreaDir()).thenReturn(stagingPath);
    assertEquals(stagingPath,
        JobSubmissionFiles.getStagingDir(cluster, conf, user));
  }

  @Test
  public void testGetStagingDirWhenShortFileOwnerNameAndShortUserName()
      throws IOException, InterruptedException {
    Cluster cluster = mock(Cluster.class);
    Configuration conf = new Configuration();
    String stagingDirOwner = USER_1_SHORT_NAME;
    Path stagingPath = mock(Path.class);
    UserGroupInformation user = UserGroupInformation
        .createUserForTesting(USER_1_SHORT_NAME, GROUP_NAMES);
    assertEquals(USER_1_SHORT_NAME, user.getUserName());
    FileSystem fs = new FileSystemTestHelper.MockFileSystem();
    FileStatus fileStatus = new FileStatus(1, true, 1, 1, 100L, 100L,
        FsPermission.getDefault(), stagingDirOwner, stagingDirOwner,
        stagingPath);
    when(stagingPath.getFileSystem(conf)).thenReturn(fs);
    when(fs.getFileStatus(stagingPath)).thenReturn(fileStatus);
    when(cluster.getStagingAreaDir()).thenReturn(stagingPath);
    assertEquals(stagingPath,
        JobSubmissionFiles.getStagingDir(cluster, conf, user));
  }

  @Test
  public void testDirPermission() throws Exception {
    Cluster cluster = mock(Cluster.class);
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "700");
    MiniDFSCluster dfsCluster = null;
    try {
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fs = dfsCluster.getFileSystem();
      UserGroupInformation user = UserGroupInformation
          .createUserForTesting(USER_1_SHORT_NAME, GROUP_NAMES);
      Path stagingPath = new Path(fs.getUri().toString() + "/testDirPermission");
      when(cluster.getStagingAreaDir()).thenReturn(stagingPath);
      Path res = JobSubmissionFiles.getStagingDir(cluster, conf, user);
      assertEquals(new FsPermission(0700), fs.getFileStatus(res).getPermission());
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
  }
}
