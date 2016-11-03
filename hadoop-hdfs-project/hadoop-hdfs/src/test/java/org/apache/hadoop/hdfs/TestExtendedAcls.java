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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;

import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A class for testing the behavior of HDFS directory and file ACL.
 */
public class TestExtendedAcls {

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private static final short REPLICATION = 3;

  private static DistributedFileSystem hdfs;

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Set default ACL to a directory.
   * Create subdirectory, it must have default acls set.
   * Create sub file and it should have default acls.
   * @throws IOException
   */
  @Test
  public void testDefaultAclNewChildDirFile() throws IOException {
    Path parent = new Path("/testDefaultAclNewChildDirFile");
    List<AclEntry> acls = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));

    hdfs.mkdirs(parent);
    hdfs.setAcl(parent, acls);

    // create sub directory
    Path childDir = new Path(parent, "childDir");
    hdfs.mkdirs(childDir);
    // the sub directory should have the default acls
    AclEntry[] childDirExpectedAcl = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());

    // create sub file
    Path childFile = new Path(parent, "childFile");
    hdfs.create(childFile).close();
    // the sub file should have the default acls
    AclEntry[] childFileExpectedAcl = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE)
    };
    AclStatus childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    hdfs.delete(parent, true);
  }

  /**
   * Set default ACL to a directory and make sure existing sub dirs/files
   * does not have default acl.
   * @throws IOException
   */
  @Test
  public void testDefaultAclExistingDirFile() throws Exception {
    Path parent = new Path("/testDefaultAclExistingDirFile");
    hdfs.mkdirs(parent);
    // the old acls
    List<AclEntry> acls1 = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL));
    // the new acls
    List<AclEntry> acls2 = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    // set parent to old acl
    hdfs.setAcl(parent, acls1);

    Path childDir = new Path(parent, "childDir");
    hdfs.mkdirs(childDir);
    // the sub directory should also have the old acl
    AclEntry[] childDirExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());

    Path childFile = new Path(childDir, "childFile");
    // the sub file should also have the old acl
    hdfs.create(childFile).close();
    AclEntry[] childFileExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE)
    };
    AclStatus childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    // now change parent to new acls
    hdfs.setAcl(parent, acls2);

    // sub directory and sub file should still have the old acls
    childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());
    childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    // now remove the parent acls
    hdfs.removeAcl(parent);

    // sub directory and sub file should still have the old acls
    childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());
    childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    // check changing the access mode of the file
    // mask out the access of group other for testing
    hdfs.setPermission(childFile, new FsPermission((short)0640));
    boolean canAccess =
        tryAccess(childFile, "other", new String[]{"other"}, READ);
    assertFalse(canAccess);
    hdfs.delete(parent, true);
  }

  /**
   * Verify that access acl does not get inherited on newly created subdir/file.
   * @throws IOException
   */
  @Test
  public void testAccessAclNotInherited() throws IOException {
    Path parent = new Path("/testAccessAclNotInherited");
    hdfs.mkdirs(parent);
    // parent have both access acl and default acl
    List<AclEntry> acls = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, READ),
        aclEntry(ACCESS, USER, "bar", ALL));
    hdfs.setAcl(parent, acls);

    Path childDir = new Path(parent, "childDir");
    hdfs.mkdirs(childDir);
    // subdirectory should only have the default acl inherited
    AclEntry[] childDirExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, READ)
    };
    AclStatus childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());

    Path childFile = new Path(parent, "childFile");
    hdfs.create(childFile).close();
    // sub file should only have the default acl inherited
    AclEntry[] childFileExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ)
    };
    AclStatus childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    hdfs.delete(parent, true);
  }

  /**
   * Create a parent dir and set default acl to allow foo read/write access.
   * Create a sub dir and set default acl to allow bar group read/write access.
   * parent dir/file can not be viewed/appended by bar group.
   * parent dir/child dir/file can be viewed/appended by bar group.
   * @throws Exception
   */
  @Test
  public void testGradSubdirMoreAccess() throws Exception {
    Path parent = new Path("/testGradSubdirMoreAccess");
    hdfs.mkdirs(parent);
    List<AclEntry> aclsParent = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    List<AclEntry> aclsChild = Lists.newArrayList(
        aclEntry(DEFAULT, GROUP, "bar", READ_WRITE));

    hdfs.setAcl(parent, aclsParent);
    AclEntry[] parentDirExpectedAcl = new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus parentAcl = hdfs.getAclStatus(parent);
    assertArrayEquals(parentDirExpectedAcl, parentAcl.getEntries().toArray());

    Path childDir = new Path(parent, "childDir");
    hdfs.mkdirs(childDir);
    hdfs.modifyAclEntries(childDir, aclsChild);
    // child dir should inherit the default acls from parent, plus bar group
    AclEntry[] childDirExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "bar", READ_WRITE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());

    Path parentFile = new Path(parent, "parentFile");
    hdfs.create(parentFile).close();
    hdfs.setPermission(parentFile, new FsPermission((short)0640));
    // parent dir/parent file allows foo to access but not bar group
    AclEntry[] parentFileExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE)
    };
    AclStatus parentFileAcl = hdfs.getAclStatus(parentFile);
    assertArrayEquals(parentFileExpectedAcl,
        parentFileAcl.getEntries().toArray());

    Path childFile = new Path(childDir, "childFile");
    hdfs.create(childFile).close();
    hdfs.setPermission(childFile, new FsPermission((short)0640));
    // child dir/child file allows foo user and bar group to access
    AclEntry[] childFileExpectedAcl = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", READ_WRITE)
    };
    AclStatus childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    // parent file should not be accessible for bar group
    assertFalse(tryAccess(parentFile, "barUser", new String[]{"bar"}, READ));
    // child file should be accessible for bar group
    assertTrue(tryAccess(childFile, "barUser", new String[]{"bar"}, READ));
    // parent file should be accessible for foo user
    assertTrue(tryAccess(parentFile, "foo", new String[]{"fooGroup"}, READ));
    // child file should be accessible for foo user
    assertTrue(tryAccess(childFile, "foo", new String[]{"fooGroup"}, READ));

    hdfs.delete(parent, true);
  }

  /**
   * Verify that sub directory can restrict acl with acl inherited from parent.
   * Create a parent dir and set default to allow foo and bar full access
   * Create a sub dir and set default to restrict bar to empty access
   *
   * parent dir/file can be viewed by foo
   * parent dir/child dir/file can be viewed by foo
   * parent dir/child dir/file can not be viewed by bar
   *
   * @throws IOException
   */
  @Test
  public void testRestrictAtSubDir() throws Exception {
    Path parent = new Path("/testRestrictAtSubDir");
    hdfs.mkdirs(parent);
    List<AclEntry> aclsParent = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, "bar", ALL)
    );
    hdfs.setAcl(parent, aclsParent);
    AclEntry[] parentDirExpectedAcl = new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "bar", ALL),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus parentAcl = hdfs.getAclStatus(parent);
    assertArrayEquals(parentDirExpectedAcl, parentAcl.getEntries().toArray());

    Path parentFile = new Path(parent, "parentFile");
    hdfs.create(parentFile).close();
    hdfs.setPermission(parentFile, new FsPermission((short)0640));
    AclEntry[] parentFileExpectedAcl = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", ALL),
    };
    AclStatus parentFileAcl = hdfs.getAclStatus(parentFile);
    assertArrayEquals(
        parentFileExpectedAcl, parentFileAcl.getEntries().toArray());

    Path childDir = new Path(parent, "childDir");
    hdfs.mkdirs(childDir);
    List<AclEntry> newAclsChild = Lists.newArrayList(
        aclEntry(DEFAULT, GROUP, "bar", NONE)
    );
    hdfs.modifyAclEntries(childDir, newAclsChild);
    AclEntry[] childDirExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "bar", ALL),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, "foo", ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, "bar", NONE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE)
    };
    AclStatus childDirAcl = hdfs.getAclStatus(childDir);
    assertArrayEquals(childDirExpectedAcl, childDirAcl.getEntries().toArray());

    Path childFile = new Path(childDir, "childFile");
    hdfs.create(childFile).close();
    hdfs.setPermission(childFile, new FsPermission((short)0640));
    AclEntry[] childFileExpectedAcl = new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, "bar", NONE)
    };
    AclStatus childFileAcl = hdfs.getAclStatus(childFile);
    assertArrayEquals(
        childFileExpectedAcl, childFileAcl.getEntries().toArray());

    // child file should not be accessible for bar group
    assertFalse(tryAccess(childFile, "barUser", new String[]{"bar"}, READ));
    // child file should be accessible for foo user
    assertTrue(tryAccess(childFile, "foo", new String[]{"fooGroup"}, READ));
    // parent file should be accessible for bar group
    assertTrue(tryAccess(parentFile, "barUser", new String[]{"bar"}, READ));
    // parent file should be accessible for foo user
    assertTrue(tryAccess(parentFile, "foo", new String[]{"fooGroup"}, READ));

    hdfs.delete(parent, true);
  }

  private boolean tryAccess(Path path, String user,
      String[] group, FsAction action) throws Exception {
    UserGroupInformation testUser =
        UserGroupInformation.createUserForTesting(
            user, group);
    FileSystem fs = testUser.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(conf);
      }
    });

    boolean canAccess;
    try {
      fs.access(path, action);
      canAccess = true;
    } catch (AccessControlException e) {
      canAccess = false;
    }
    return canAccess;
  }
}
