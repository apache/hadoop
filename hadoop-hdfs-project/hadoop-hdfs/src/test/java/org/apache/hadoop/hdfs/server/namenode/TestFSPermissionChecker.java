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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Unit tests covering FSPermissionChecker.  All tests in this suite have been
 * cross-validated against Linux setfacl/getfacl to check for consistency of the
 * HDFS implementation.
 */
public class TestFSPermissionChecker {
  private static final long PREFERRED_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final short REPLICATION = 3;
  private static final String SUPERGROUP = "supergroup";
  private static final String SUPERUSER = "superuser";
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { "sales" });
  private static final UserGroupInformation CLARK =
    UserGroupInformation.createUserForTesting("clark", new String[] { "execs" });

  private INodeDirectory inodeRoot;

  @Before
  public void setUp() {
    PermissionStatus permStatus = PermissionStatus.createImmutable(SUPERUSER,
      SUPERGROUP, FsPermission.createImmutable((short)0755));
    inodeRoot = new INodeDirectory(INodeId.ROOT_INODE_ID,
      INodeDirectory.ROOT_NAME, permStatus, 0L);
  }

  @Test
  public void testAclOwner() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ);
    assertPermissionGranted(BRUCE, "/file1", WRITE);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionDenied(BRUCE, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
  }

  @Test
  public void testAclNamedUser() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedUserDeny() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", NONE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
  }

  @Test
  public void testAclNamedUserTraverseDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile(inodeDir, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", NONE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, MASK, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  @Test
  public void testAclNamedUserMask() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0620);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, WRITE),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclGroup() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclGroupDeny() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "sales",
      (short)0604);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, MASK, NONE),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclGroupTraverseDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile(inodeDir, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, MASK, NONE),
      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }

  @Test
  public void testAclGroupTraverseDenyOnlyDefaultEntries() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile(inodeDir, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, "sales", NONE),
      aclEntry(DEFAULT, GROUP, NONE),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/dir1/file1", ALL);
  }

  @Test
  public void testAclGroupMask() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroup() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "sales", READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupDeny() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "sales",
      (short)0644);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "execs", NONE),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  @Test
  public void testAclNamedGroupTraverseDeny() throws IOException {
    INodeDirectory inodeDir = createINodeDirectory(inodeRoot, "dir1", "bruce",
      "execs", (short)0755);
    INodeFile inodeFile = createINodeFile(inodeDir, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeDir,
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "sales", NONE),
      aclEntry(ACCESS, MASK, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, READ_EXECUTE));
    assertPermissionGranted(BRUCE, "/dir1/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", READ);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/dir1/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/dir1/file1", ALL);
  }

  @Test
  public void testAclNamedGroupMask() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0644);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, GROUP, "sales", READ_WRITE),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
    assertPermissionDenied(DIANA, "/file1", EXECUTE);
    assertPermissionDenied(DIANA, "/file1", READ_WRITE);
    assertPermissionDenied(DIANA, "/file1", READ_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(DIANA, "/file1", ALL);
  }

  @Test
  public void testAclOther() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "sales",
      (short)0774);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", ALL),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, READ));
    assertPermissionGranted(BRUCE, "/file1", ALL);
    assertPermissionGranted(DIANA, "/file1", ALL);
    assertPermissionGranted(CLARK, "/file1", READ);
    assertPermissionDenied(CLARK, "/file1", WRITE);
    assertPermissionDenied(CLARK, "/file1", EXECUTE);
    assertPermissionDenied(CLARK, "/file1", READ_WRITE);
    assertPermissionDenied(CLARK, "/file1", READ_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", WRITE_EXECUTE);
    assertPermissionDenied(CLARK, "/file1", ALL);
  }

  private void addAcl(INodeWithAdditionalFields inode, AclEntry... acl)
      throws IOException {
    AclStorage.updateINodeAcl(inode,
      Arrays.asList(acl), Snapshot.CURRENT_STATE_ID);
  }

  private void assertPermissionGranted(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
      inodeRoot, false, null, null, access, null, true);
  }

  private void assertPermissionDenied(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    try {
      new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
        inodeRoot, false, null, null, access, null, true);
      fail("expected AccessControlException for user + " + user + ", path = " +
        path + ", access = " + access);
    } catch (AccessControlException e) {
      // expected
    }
  }

  private static INodeDirectory createINodeDirectory(INodeDirectory parent,
      String name, String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeDirectory inodeDirectory = new INodeDirectory(
      INodeId.GRANDFATHER_INODE_ID, name.getBytes("UTF-8"), permStatus, 0L);
    parent.addChild(inodeDirectory);
    return inodeDirectory;
  }

  private static INodeFile createINodeFile(INodeDirectory parent, String name,
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeFile inodeFile = new INodeFile(INodeId.GRANDFATHER_INODE_ID,
      name.getBytes("UTF-8"), permStatus, 0L, 0L, null, REPLICATION,
      PREFERRED_BLOCK_SIZE);
    parent.addChild(inodeFile);
    return inodeFile;
  }
}
