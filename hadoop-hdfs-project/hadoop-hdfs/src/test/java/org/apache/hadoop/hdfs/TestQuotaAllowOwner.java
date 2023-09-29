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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQuotaAllowOwner {
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_KEY,
        true);
    restartCluster();
  }

  @AfterClass
  public static void tearDownClass() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private static void restartCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
  }

  private void createDirssAndSetOwner(
      String parentDir, String subDir, String owner, String group)
      throws Exception {
    Path parentPath = new Path(parentDir);
    assertTrue(dfs.mkdirs(parentPath));
    dfs.setOwner(parentPath, owner, group);
    assertTrue(dfs.mkdirs(new Path(subDir)));
  }

  /**
   * Test the owner(not admin) of directory can set the quota of
   * it's sub-directories.
   */
  @Test
  public void testOwnerCanSetSubDirQuota() throws Exception {
    final String userName = "user1";
    final String groupName = "hadoop";
    final String parentDir = "/parent_owner";
    final String subDir = parentDir + "/subdir";

    createDirssAndSetOwner(parentDir, subDir, userName, groupName);

    final DFSAdmin admin = new DFSAdmin(conf);
    // set quota with superuser.
    String[] args = new String[]{"-setQuota", "10", parentDir.toString()};
    TestQuota.runCommand(admin, args, false);
    args = new String[]{"-setSpaceQuota", "128", parentDir.toString()};
    TestQuota.runCommand(admin, args, false);

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        userName,  new String[]{groupName});
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      assertEquals("Not running as new user", userName,
          UserGroupInformation.getCurrentUser().getShortUserName());
      DFSAdmin userAdmin = new DFSAdmin(conf);

      String[] args2 = new String[]{"-setQuota", "5", subDir};
      TestQuota.runCommand(userAdmin, args2, false);
      args2 = new String[]{"-setSpaceQuota", "64", subDir};
      TestQuota.runCommand(userAdmin, args2, false);

      ContentSummary c = dfs.getContentSummary(new Path(subDir));
      assertEquals("Not same with setting quota",
          5, c.getQuota());
      assertEquals("Not same with setting space quota",
          64, c.getSpaceQuota());
      args2 = new String[]{"-clrQuota", subDir};
      TestQuota.runCommand(userAdmin, args2, false);
      args2 = new String[]{"-clrSpaceQuota", subDir};
      TestQuota.runCommand(userAdmin, args2, false);
      c = dfs.getContentSummary(new Path(subDir));
      assertEquals("Not clean quota", -1, c.getQuota());
      assertEquals("Not clean space quota",
          -1, c.getSpaceQuota());
      return null;
    });
  }

  /**
   * Test the owner(not admin) of directory can set the quota of
   * it's sub-directories even the admin never set quota to it.
   */
  @Test
  public void testOwnerCanSetSubDirQuotaWithoutAdminDone() throws Exception {
    final String userName = "user1";
    final String groupName = "hadoop";
    final String parentDir = "/parent_owner_without_Admin";
    final String subDir = parentDir + "/subdir";

    createDirssAndSetOwner(parentDir, subDir, userName, groupName);

    UserGroupInformation ugi2 = UserGroupInformation.createUserForTesting(
        userName,  new String[]{groupName});
    ugi2.doAs((PrivilegedExceptionAction<Object>) () -> {
      assertEquals("Not running as new user", userName,
          UserGroupInformation.getCurrentUser().getShortUserName());
      DFSAdmin userAdmin = new DFSAdmin(conf);

      String[] args2 = new String[]{"-setQuota", "5", subDir};
      TestQuota.runCommand(userAdmin, args2, false);
      args2 = new String[]{"-setSpaceQuota", "64", subDir};
      TestQuota.runCommand(userAdmin, args2, false);
      return null;
    });
  }

  /**
   * Test other user(not admin) can NOT set the quota of sub-directories.
   */
  @Test
  public void testOtherCanNotSetSubDirQuota() throws Exception {
    final String userName = "user1";
    final String groupName = "hadoop";
    final String userOther = "otherUser";
    final String groupOther = "otherGroup";
    final String parentDir = "/parent_other_user";
    final String subDir = parentDir + "/subdir";

    createDirssAndSetOwner(parentDir, subDir, userName, groupName);

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        userOther, new String[]{groupOther});
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      assertEquals("Not running as new user", userOther,
          UserGroupInformation.getCurrentUser().getShortUserName());
      DFSAdmin userAdmin = new DFSAdmin(conf);

      String[] args2 = new String[]{"-setQuota", "5", subDir.toString()};
      TestQuota.runCommand(userAdmin, args2, true);
      args2 = new String[]{"-setSpaceQuota", "64", subDir.toString()};
      TestQuota.runCommand(userAdmin, args2, true);
      return null;
    });
  }

  /**
   * Test other user(not admin) with same group can NOT set the quota of
   * sub-directories.
   */
  @Test
  public void testOtherInSameGroupCanNotSetSubDirQuota() throws Exception {
    final String userName = "user1";
    final String groupName = "hadoop";
    final String userOther = "otherUser";
    final String parentDir = "/parent_other_user_in_same_group";
    final String subDir = parentDir + "/subdir";

    createDirssAndSetOwner(parentDir, subDir, userName, groupName);

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        userOther, new String[]{groupName});
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      assertEquals("Not running as new user", userOther,
          UserGroupInformation.getCurrentUser().getShortUserName());
      DFSAdmin userAdmin = new DFSAdmin(conf);

      String[] args2 = new String[]{"-setQuota", "5", subDir.toString()};
      TestQuota.runCommand(userAdmin, args2, true);
      args2 = new String[]{"-setSpaceQuota", "64", subDir.toString()};
      TestQuota.runCommand(userAdmin, args2, true);
      return null;
    });
  }

  /**
   * Test the owner(not admin) of directory can NOT set the quota of
   * sub-directories if this feature is not enabled.
   */
  @Test
  public void testOwnerCanNotSetIfNotEanbled() throws Exception {
    final String userName = "user1";
    final String groupName = "hadoop";
    final String parentDir = "/parent_owner_without_Admin";
    final String subDir = parentDir + "/subdir";
    try {
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_KEY,
          false);
      restartCluster();

      createDirssAndSetOwner(parentDir, subDir, userName, groupName);

      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          userName, new String[]{groupName});
      ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
        assertEquals("Not running as new user", userName,
            UserGroupInformation.getCurrentUser().getShortUserName());

        DFSAdmin userAdmin = new DFSAdmin(conf);

        String[] args2 = new String[]{"-setQuota", "5", subDir};
        TestQuota.runCommand(userAdmin, args2, true);
        args2 = new String[]{"-setSpaceQuota", "64", subDir};
        TestQuota.runCommand(userAdmin, args2, true);
        return null;
      });
    } finally {
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ALLOW_OWNER_SET_QUOTA_KEY,
          true);
      restartCluster();
    }
  }
}
