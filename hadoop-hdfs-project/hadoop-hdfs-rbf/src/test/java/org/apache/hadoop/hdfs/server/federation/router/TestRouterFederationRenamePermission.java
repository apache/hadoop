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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test permission check of router federation rename.
 */
public class TestRouterFederationRenamePermission
    extends TestRouterFederationRenameBase {

  private String srcNs; // the source namespace.
  private String dstNs; // the dst namespace.
  // the source path.
  private String srcStr;
  private Path srcPath;
  // the dst path.
  private String dstStr;
  private Path dstPath;
  private UserGroupInformation foo;
  private MiniRouterDFSCluster.RouterContext router;
  private FileSystem routerFS;
  private MiniRouterDFSCluster cluster;

  @BeforeClass
  public static void before() throws Exception {
    globalSetUp();
  }

  @AfterClass
  public static void after() {
    tearDown();
  }

  @Before
  public void testSetup() throws Exception {
    setup();
    cluster = getCluster();
    List<String> nss = cluster.getNameservices();
    srcNs = nss.get(0);
    dstNs = nss.get(1);
    srcStr = cluster.getFederatedTestDirectoryForNS(srcNs) + "/d0/"
        + getMethodName();
    dstStr = cluster.getFederatedTestDirectoryForNS(dstNs) + "/d0/"
        + getMethodName();
    srcPath = new Path(srcStr);
    dstPath = new Path(dstStr);
    foo = UserGroupInformation.createRemoteUser("foo");
    router = getRouterContext();
    routerFS = getRouterFileSystem();
  }

  @Test
  public void testRenameSnapshotPath() throws Exception {
    LambdaTestUtils.intercept(IOException.class,
        "Router federation rename can't rename snapshot path",
        "Expect IOException.", () -> RouterFederationRename.checkSnapshotPath(
            new RemoteLocation(srcNs, "/foo/.snapshot/src", "/src"),
            new RemoteLocation(dstNs, "/foo/dst", "/dst")));
    LambdaTestUtils.intercept(IOException.class,
        "Router federation rename can't rename snapshot path",
        "Expect IOException.", () -> RouterFederationRename
            .checkSnapshotPath(new RemoteLocation(srcNs, "/foo/src", "/src"),
                new RemoteLocation(dstNs, "/foo/.snapshot/dst", "/dst")));
  }

  // Case1: the source path doesn't exist.
  @Test
  public void testPermission1() throws Exception {
    LambdaTestUtils.intercept(RemoteException.class, "FileNotFoundException",
        "Expect FileNotFoundException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case2: the source path parent without any permission.
  @Test
  public void testPermission2() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setPermission(srcPath.getParent(),
        FsPermission.createImmutable((short) 0));
    LambdaTestUtils.intercept(RemoteException.class, "AccessControlException",
        "Expect AccessControlException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case3: the source path with rwxr-xr-x permission.
  @Test
  public void testPermission3() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setPermission(srcPath.getParent(),
        FsPermission.createImmutable((short) 493));
    LambdaTestUtils.intercept(RemoteException.class, "AccessControlException",
        "Expect AccessControlException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case4: the source path with unrelated acl user:not-foo:rwx.
  @Test
  public void testPermission4() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setAcl(srcPath.getParent(), buildAcl("not-foo", ALL));
    LambdaTestUtils.intercept(RemoteException.class, "AccessControlException",
        "Expect AccessControlException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case5: the source path with user:foo:rwx. And the dst path doesn't exist.
  @Test
  public void testPermission5() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setAcl(srcPath.getParent(), buildAcl("foo", ALL));
    assertFalse(routerFS.exists(dstPath.getParent()));
    LambdaTestUtils.intercept(RemoteException.class, "FileNotFoundException",
        "Expect FileNotFoundException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case6: the src path with correct permission and the dst path with bad
  //        permission.
  @Test
  public void testPermission6() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setAcl(srcPath.getParent(), buildAcl("foo", ALL));
    assertTrue(routerFS.mkdirs(dstPath.getParent()));
    LambdaTestUtils.intercept(RemoteException.class, "AccessControlException",
        "Expect AccessControlException.", () -> {
          DFSClient client = router.getClient(foo);
          ClientProtocol clientProtocol = client.getNamenode();
          clientProtocol.rename(srcStr, dstStr);
        });
  }

  // Case7: successful rename.
  @Test
  public void testPermission7() throws Exception {
    createDir(routerFS, srcStr);
    routerFS.setAcl(srcPath.getParent(), buildAcl("foo", ALL));
    assertTrue(routerFS.mkdirs(dstPath.getParent()));
    routerFS.setOwner(dstPath.getParent(), "foo", "foogroup");
    DFSClient client = router.getClient(foo);
    ClientProtocol clientProtocol = client.getNamenode();
    clientProtocol.rename(srcStr, dstStr);
    assertFalse(verifyFileExists(routerFS, srcStr));
    assertTrue(
        verifyFileExists(routerFS, dstStr + "/file"));
  }

  /**
   * Build acl list.
   *
   * user::rwx
   * group::rwx
   * user:input_user:input_permission
   * other::r-x
   * @param user the input user.
   * @param permission the input fs action.
   */
  private List<AclEntry> buildAcl(String user, FsAction permission) {
    List<AclEntry> aclEntryList = Lists.newArrayList();
    aclEntryList.add(
        new AclEntry.Builder()
            .setName(user)
            .setPermission(permission)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.ALL)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(FsAction.ALL)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.GROUP)
            .build());
    aclEntryList.add(
        new AclEntry.Builder()
            .setPermission(READ_EXECUTE)
            .setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.OTHER)
            .build());
    return aclEntryList;
  }
}
