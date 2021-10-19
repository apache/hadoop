/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStoragePolicyPermissionSettings {

  private static final short REPL = 1;
  private static final int SIZE = 128;

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static BlockStoragePolicySuite suite;
  private static BlockStoragePolicy cold;
  private static UserGroupInformation nonAdmin;
  private static UserGroupInformation admin;

  @BeforeClass
  public static void clusterSetUp() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    suite = BlockStoragePolicySuite.createDefaultSuite();
    cold = suite.getPolicy("COLD");
    nonAdmin = UserGroupInformation.createUserForTesting(
        "user1", new String[] {"test"});
    admin = UserGroupInformation.createUserForTesting("user2",
        new String[]{"supergroup"});
  }

  @AfterClass
  public static void clusterShutdown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setFSNameSystemFinalField(String field, boolean value)
      throws NoSuchFieldException, IllegalAccessException {
    Field f = FSNamesystem.class.getDeclaredField(field);
    f.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
    f.set(cluster.getNamesystem(), value);
  }

  private void setStoragePolicyPermissions(boolean isStoragePolicyEnabled,
                                           boolean isStoragePolicySuperuserOnly)
      throws NoSuchFieldException, IllegalAccessException {
    setFSNameSystemFinalField("isStoragePolicyEnabled", isStoragePolicyEnabled);
    setFSNameSystemFinalField("isStoragePolicySuperuserOnly",
        isStoragePolicySuperuserOnly);
  }

  @Test
  public void testStoragePolicyPermissionDefault() throws Exception {
    Path foo = new Path("/foo");
    DFSTestUtil.createFile(fs, foo, SIZE, REPL, 0);
    setStoragePolicyPermissions(true, false);
    // Test default user fails
    final FileSystem fileSystemNonAdmin =
        DFSTestUtil.getFileSystemAs(nonAdmin, conf);
    LambdaTestUtils.intercept(AccessControlException.class,
        "Permission denied: user=user1",
        "Only super user can set storage policy.",
        () -> fileSystemNonAdmin.setStoragePolicy(foo, cold.getName()));
    // widen privilege
    fs.setPermission(foo, new FsPermission("777"));
    assertNotEquals(fs.getStoragePolicy(foo), cold);
    LambdaTestUtils.eval(
        () -> fileSystemNonAdmin.setStoragePolicy(foo, cold.getName()));
    assertEquals(fs.getStoragePolicy(foo), cold);
  }

  @Test
  public void testStoragePolicyPermissionAdmins() throws Exception {
    Path foo = new Path("/foo");
    DFSTestUtil.createFile(fs, foo, SIZE, REPL, 0);
    fs.setPermission(foo, new FsPermission("777"));
    // Test only super can set storage policies
    setStoragePolicyPermissions(true, true);

    final FileSystem fileSystemNonAdmin =
        DFSTestUtil.getFileSystemAs(nonAdmin, conf);

    LambdaTestUtils.intercept(AccessControlException.class,
        "Access denied for user user1. Superuser privilege is required",
        "Only super user can set storage policy.",
        () -> fileSystemNonAdmin.setStoragePolicy(foo, cold.getName()));

    final FileSystem fileSystemAdmin =
        DFSTestUtil.getFileSystemAs(admin, conf);
    assertNotEquals(fs.getStoragePolicy(foo), cold);
    LambdaTestUtils.eval(
        () -> fileSystemAdmin.setStoragePolicy(foo, cold.getName()));
    assertEquals(fs.getStoragePolicy(foo), cold);
  }

  @Test
  public void testStoragePolicyPermissionDisabled() throws Exception {
    Path foo = new Path("/foo");
    DFSTestUtil.createFile(fs, foo, SIZE, REPL, 0);
    fs.setPermission(foo, new FsPermission("777"));
    setStoragePolicyPermissions(false, false);
    final FileSystem fileSystemAdmin =
        DFSTestUtil.getFileSystemAs(admin, conf);
    LambdaTestUtils.intercept(IOException.class,
        "Failed to set storage policy " +
            "since dfs.storage.policy.enabled is set to false.",
        "Storage policy settings are disabled.",
        () -> fileSystemAdmin.setStoragePolicy(foo, cold.getName()));
    assertEquals(suite.getDefaultPolicy(), fs.getStoragePolicy(foo));
  }
}
