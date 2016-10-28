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

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.NONE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.fs.permission.FsAction.WRITE;
import static org.apache.hadoop.fs.permission.FsAction.WRITE_EXECUTE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

  private FSDirectory dir;
  private INodeDirectory inodeRoot;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    FSNamesystem fsn = mock(FSNamesystem.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        FsPermission perm = (FsPermission) args[0];
        return new PermissionStatus(SUPERUSER, SUPERGROUP, perm);
      }
    }).when(fsn).createFsOwnerPermissions(any(FsPermission.class));
    dir = new FSDirectory(fsn, conf);
    inodeRoot = dir.getRoot();
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
    INodesInPath iip = dir.getINodesInPath(path, DirOp.READ);
    dir.getPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(iip,
      false, null, null, access, null, false);
  }

  private void assertPermissionDenied(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    try {
      INodesInPath iip = dir.getINodesInPath(path, DirOp.READ);
      dir.getPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(iip,
        false, null, null, access, null, false);
      fail("expected AccessControlException for user + " + user + ", path = " +
        path + ", access = " + access);
    } catch (AccessControlException e) {
      assertTrue("Permission denied messages must carry the username",
              e.getMessage().contains(user.getUserName().toString()));
      assertTrue("Permission denied messages must carry the path parent",
              e.getMessage().contains(
                  new Path(path).getParent().toUri().getPath()));
    }
  }

  private static INodeDirectory createINodeDirectory(INodeDirectory parent,
      String name, String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeDirectory inodeDirectory = new INodeDirectory(
      HdfsConstants.GRANDFATHER_INODE_ID, name.getBytes("UTF-8"), permStatus, 0L);
    parent.addChild(inodeDirectory);
    return inodeDirectory;
  }

  private static INodeFile createINodeFile(INodeDirectory parent, String name,
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeFile inodeFile = new INodeFile(HdfsConstants.GRANDFATHER_INODE_ID,
      name.getBytes("UTF-8"), permStatus, 0L, 0L, null, REPLICATION,
      PREFERRED_BLOCK_SIZE);
    parent.addChild(inodeFile);
    return inodeFile;
  }
}
