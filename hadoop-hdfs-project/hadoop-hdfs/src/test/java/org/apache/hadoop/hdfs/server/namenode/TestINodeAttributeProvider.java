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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

public class TestINodeAttributeProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestINodeAttributeProvider.class);

  private MiniDFSCluster miniDFS;
  private static final Set<String> CALLED = new HashSet<String>();
  private static final short HDFS_PERMISSION = 0777;
  private static final short PROVIDER_PERMISSION = 0770;
  private static boolean runPermissionCheck = false;

  public static class MyAuthorizationProvider extends INodeAttributeProvider {

    public static class MyAccessControlEnforcer implements AccessControlEnforcer {
      AccessControlEnforcer ace;

      public MyAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
        this.ace = defaultEnforcer;
      }

      @Override
      public void checkPermission(String fsOwner, String supergroup,
          UserGroupInformation ugi, INodeAttributes[] inodeAttrs,
          INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
          int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
          FsAction parentAccess, FsAction access, FsAction subAccess,
          boolean ignoreEmptyDir) throws AccessControlException {
        if ((ancestorIndex > 1
            && inodes[1].getLocalName().equals("user")
            && inodes[2].getLocalName().equals("acl")) || runPermissionCheck) {
          this.ace.checkPermission(fsOwner, supergroup, ugi, inodeAttrs, inodes,
              pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
              ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
        }
        CALLED.add("checkPermission|" + ancestorAccess + "|" + parentAccess + "|" + access);
        CALLED.add("checkPermission|" + path);
      }

      @Override
      public void checkPermissionWithContext(
          AuthorizationContext authzContext) throws AccessControlException {
        if (authzContext.getAncestorIndex() > 1
            && authzContext.getInodes()[1].getLocalName().equals("user")
            && authzContext.getInodes()[2].getLocalName().equals("acl")) {
          this.ace.checkPermissionWithContext(authzContext);
        }
        CALLED.add("checkPermission|" + authzContext.getAncestorAccess()
            + "|" + authzContext.getParentAccess() + "|" + authzContext
            .getAccess());
        CALLED.add("checkPermission|" + authzContext.getPath());
      }
    }

    @Override
    public void start() {
      CALLED.add("start");
    }

    @Override
    public void stop() {
      CALLED.add("stop");
    }

    @Override
    public INodeAttributes getAttributes(String[] pathElements,
        final INodeAttributes inode) {
      String fullPath = String.join("/", pathElements);
      if (!fullPath.startsWith("/")) {
        fullPath = "/" + fullPath;
      }
      CALLED.add("getAttributes");
      CALLED.add("getAttributes|"+fullPath);
      final boolean useDefault = useDefault(pathElements);
      final boolean useNullAcl = useNullAclFeature(pathElements);
      return new INodeAttributes() {
        @Override
        public boolean isDirectory() {
          return inode.isDirectory();
        }

        @Override
        public byte[] getLocalNameBytes() {
          return inode.getLocalNameBytes();
        }

        @Override
        public String getUserName() {
          return (useDefault) ? inode.getUserName() : "foo";
        }

        @Override
        public String getGroupName() {
          return (useDefault) ? inode.getGroupName() : "bar";
        }

        @Override
        public FsPermission getFsPermission() {
          return (useDefault) ? inode.getFsPermission()
                              : new FsPermission(getFsPermissionShort());
        }

        @Override
        public short getFsPermissionShort() {
          return (useDefault) ? inode.getFsPermissionShort()
                              : (short) getPermissionLong();
        }

        @Override
        public long getPermissionLong() {
          return (useDefault) ? inode.getPermissionLong() :
            (long)PROVIDER_PERMISSION;
        }

        @Override
        public AclFeature getAclFeature() {
          AclFeature f;
          if (useNullAcl) {
            int[] entries = new int[0];
            f = new AclFeature(entries);
          } else if (useDefault) {
            f = inode.getAclFeature();
          } else {
            AclEntry acl = new AclEntry.Builder().setType(AclEntryType.GROUP).
                setPermission(FsAction.ALL).setName("xxx").build();
            f = new AclFeature(AclEntryStatusFormat.toInt(
                Lists.newArrayList(acl)));
          }
          return f;
        }

        @Override
        public XAttrFeature getXAttrFeature() {
          XAttrFeature x;
          if (useDefault) {
            x = inode.getXAttrFeature();
          } else {
            x = new XAttrFeature(ImmutableList.copyOf(
                    Lists.newArrayList(
                            new XAttr.Builder().setName("test")
                                    .setValue(new byte[] {1, 2})
                                    .build())));
          }
          return x;
        }

        @Override
        public long getModificationTime() {
          return (useDefault) ? inode.getModificationTime() : 0;
        }

        @Override
        public long getAccessTime() {
          return (useDefault) ? inode.getAccessTime() : 0;
        }
      };

    }

    @Override
    public AccessControlEnforcer getExternalAccessControlEnforcer(
        AccessControlEnforcer defaultEnforcer) {
      return new MyAccessControlEnforcer(defaultEnforcer);
    }

    private boolean useDefault(String[] pathElements) {
      return !Arrays.stream(pathElements).anyMatch("authz"::equals);
    }

    private boolean useNullAclFeature(String[] pathElements) {
      return (pathElements.length > 2)
          && pathElements[1].equals("user")
          && pathElements[2].equals("acl");
    }
  }

  @Before
  public void setUp() throws IOException {
    CALLED.clear();
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        MyAuthorizationProvider.class.getName());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_BYPASS_USERS_KEY,
        " u2,, ,u3, ");
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
  }

  @After
  public void cleanUp() throws IOException {
    CALLED.clear();
    if (miniDFS != null) {
      miniDFS.shutdown();
      miniDFS = null;
    }
    runPermissionCheck = false;
    Assert.assertTrue(CALLED.contains("stop"));
  }

  @Test
  public void testDelegationToProvider() throws Exception {
    Assert.assertTrue(CALLED.contains("start"));
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    final Path tmpPath = new Path("/tmp");
    final Path fooPath = new Path("/tmp/foo");

    fs.mkdirs(tmpPath);
    fs.setPermission(tmpPath, new FsPermission(HDFS_PERMISSION));
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("u1",
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        CALLED.clear();
        fs.mkdirs(fooPath);
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(CALLED.contains("checkPermission|null|null|null"));
        Assert.assertTrue(CALLED.contains("checkPermission|WRITE|null|null"));

        CALLED.clear();
        fs.listStatus(fooPath);
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(
            CALLED.contains("checkPermission|null|null|READ_EXECUTE"));

        CALLED.clear();
        fs.getAclStatus(fooPath);
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(CALLED.contains("checkPermission|null|null|null"));
        return null;
      }
    });
  }

  private class AssertHelper {
    private boolean bypass = true;
    AssertHelper(boolean bp) {
      bypass = bp;
    }
    public void doAssert(boolean x) {
      if (bypass) {
        Assert.assertFalse(x);
      } else {
        Assert.assertTrue(x);
      }
    }
  }

  private void testBypassProviderHelper(final String[] users,
      final short expectedPermission, final boolean bypass) throws Exception {
    final AssertHelper asserter = new AssertHelper(bypass);

    Assert.assertTrue(CALLED.contains("start"));

    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    final Path userPath = new Path("/user");
    final Path authz = new Path("/user/authz");
    final Path authzChild = new Path("/user/authz/child2");

    fs.mkdirs(userPath);
    fs.setPermission(userPath, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authz);
    fs.setPermission(authz, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authzChild);
    fs.setPermission(authzChild, new FsPermission(HDFS_PERMISSION));
    for(String user : users) {
      UserGroupInformation ugiBypass =
          UserGroupInformation.createUserForTesting(user,
              new String[]{"g1"});
      ugiBypass.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
          Assert.assertEquals(expectedPermission,
              fs.getFileStatus(authzChild).getPermission().toShort());
          asserter.doAssert(CALLED.contains("getAttributes"));
          asserter.doAssert(CALLED.contains("checkPermission|null|null|null"));

          CALLED.clear();
          Assert.assertEquals(expectedPermission,
              fs.listStatus(userPath)[0].getPermission().toShort());
          asserter.doAssert(CALLED.contains("getAttributes"));
          asserter.doAssert(
              CALLED.contains("checkPermission|null|null|READ_EXECUTE"));

          CALLED.clear();
          fs.getAclStatus(authzChild);
          asserter.doAssert(CALLED.contains("getAttributes"));
          asserter.doAssert(CALLED.contains("checkPermission|null|null|null"));
          return null;
        }
      });
    }
  }

  @Test
  public void testAuthzDelegationToProvider() throws Exception {
    LOG.info("Test not bypassing provider");
    String[] users = {"u1"};
    testBypassProviderHelper(users, PROVIDER_PERMISSION, false);
  }

  @Test
  public void testAuthzBypassingProvider() throws Exception {
    LOG.info("Test bypassing provider");
    String[] users = {"u2", "u3"};
    testBypassProviderHelper(users, HDFS_PERMISSION, true);
  }

  private void verifyFileStatus(UserGroupInformation ugi) throws IOException {
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));

    FileStatus status = fs.getFileStatus(new Path("/"));
    LOG.info("Path '/' is owned by: "
        + status.getOwner() + ":" + status.getGroup());

    Path userDir = new Path("/user/" + ugi.getShortUserName());
    fs.mkdirs(userDir);
    status = fs.getFileStatus(userDir);
    Assert.assertEquals(ugi.getShortUserName(), status.getOwner());
    Assert.assertEquals("supergroup", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0755), status.getPermission());

    Path authzDir = new Path("/user/authz");
    fs.mkdirs(authzDir);
    status = fs.getFileStatus(authzDir);
    Assert.assertEquals("foo", status.getOwner());
    Assert.assertEquals("bar", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0770), status.getPermission());

    AclStatus aclStatus = fs.getAclStatus(authzDir);
    Assert.assertEquals(1, aclStatus.getEntries().size());
    Assert.assertEquals(AclEntryType.GROUP,
        aclStatus.getEntries().get(0).getType());
    Assert.assertEquals("xxx",
        aclStatus.getEntries().get(0).getName());
    Assert.assertEquals(FsAction.ALL,
        aclStatus.getEntries().get(0).getPermission());
    Map<String, byte[]> xAttrs = fs.getXAttrs(authzDir);
    Assert.assertTrue(xAttrs.containsKey("user.test"));
    Assert.assertEquals(2, xAttrs.get("user.test").length);
  }

  /**
   * With the custom provider configured, verify file status attributes.
   * A superuser can bypass permission check while resolving paths. So,
   * verify file status for both superuser and non-superuser.
   */
  @Test
  public void testCustomProvider() throws Exception {
    final UserGroupInformation[] users = new UserGroupInformation[]{
        UserGroupInformation.createUserForTesting(
            System.getProperty("user.name"), new String[]{"supergroup"}),
        UserGroupInformation.createUserForTesting(
            "normaluser", new String[]{"normalusergroup"}),
    };

    for (final UserGroupInformation user : users) {
      user.doAs((PrivilegedExceptionAction<Object>) () -> {
        verifyFileStatus(user);
        return null;
      });
    }
  }

  @Test
  public void testAclFeature() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
            "testuser", new String[]{"testgroup"});
    ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
      FileSystem fs = miniDFS.getFileSystem();
      Path aclDir = new Path("/user/acl");
      fs.mkdirs(aclDir);
      Path aclChildDir = new Path(aclDir, "subdir");
      fs.mkdirs(aclChildDir);
      AclStatus aclStatus = fs.getAclStatus(aclDir);
      Assert.assertEquals(0, aclStatus.getEntries().size());
      return null;
    });
  }

  @Test
  // HDFS-14389 - Ensure getAclStatus returns the owner, group and permissions
  // from the Attribute Provider, and not from HDFS.
  public void testGetAclStatusReturnsProviderOwnerPerms() throws Exception {
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    final Path userPath = new Path("/user");
    final Path authz = new Path("/user/authz");
    final Path authzChild = new Path("/user/authz/child2");

    fs.mkdirs(userPath);
    fs.setPermission(userPath, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authz);
    fs.setPermission(authz, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authzChild);
    fs.setPermission(authzChild, new FsPermission(HDFS_PERMISSION));
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("u1",
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        Assert.assertEquals(PROVIDER_PERMISSION,
            fs.getFileStatus(authzChild).getPermission().toShort());

        Assert.assertEquals("foo", fs.getAclStatus(authzChild).getOwner());
        Assert.assertEquals("bar", fs.getAclStatus(authzChild).getGroup());
        Assert.assertEquals(PROVIDER_PERMISSION,
            fs.getAclStatus(authzChild).getPermission().toShort());
        return null;
      }
    });
  }


  @Test
  // HDFS-15165 - ContentSummary calls should use the provider permissions(if
  // attribute provider is configured) and not the underlying HDFS permissions.
  public void testContentSummary() throws Exception {
    runPermissionCheck = true;
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    final Path userPath = new Path("/user");
    final Path authz = new Path("/user/authz");
    final Path authzChild = new Path("/user/authz/child2");
    // Create the path /user/authz/child2 where the HDFS permissions are
    // 777, 700, 700.
    // The permission provider will give permissions 770 to authz and child2
    // with the owner foo, group bar.
    fs.mkdirs(userPath);
    fs.setPermission(userPath, new FsPermission(0777));
    fs.mkdirs(authz);
    fs.setPermission(authz, new FsPermission(0700));
    fs.mkdirs(authzChild);
    fs.setPermission(authzChild, new FsPermission(0700));
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("foo",
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        fs.getContentSummary(authz);
        return null;
      }
    });
  }

  @Test
  // HDFS-15372 - Attribute provider should not see the snapshot path as it
  // should be resolved into the original path name before it hits the provider.
  public void testAttrProviderSeesResolvedSnapahotPaths() throws Exception {
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    DistributedFileSystem hdfs = miniDFS.getFileSystem();
    final Path userPath = new Path("/user");
    final Path authz = new Path("/user/authz");
    final Path authzChild = new Path("/user/authz/child2");

    fs.mkdirs(userPath);
    fs.setPermission(userPath, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authz);
    hdfs.allowSnapshot(userPath);
    fs.setPermission(authz, new FsPermission(HDFS_PERMISSION));
    fs.mkdirs(authzChild);
    fs.setPermission(authzChild, new FsPermission(HDFS_PERMISSION));
    fs.createSnapshot(userPath, "snapshot_1");
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("u1",
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        final Path snapChild =
            new Path("/user/.snapshot/snapshot_1/authz/child2");
        // Run various methods on the path to access the attributes etc.
        fs.getAclStatus(snapChild);
        fs.getContentSummary(snapChild);
        fs.getFileStatus(snapChild);
        Assert.assertFalse(CALLED.contains("getAttributes|" +
            snapChild.toString()));
        Assert.assertTrue(CALLED.contains("getAttributes|/user/authz/child2"));
        // The snapshot path should be seen by the permission checker, but when
        // it checks access, the paths will be resolved so the attributeProvider
        // only sees the resolved path.
        Assert.assertTrue(
            CALLED.contains("checkPermission|" + snapChild.toString()));
        CALLED.clear();
        fs.getAclStatus(new Path("/"));
        Assert.assertTrue(CALLED.contains("checkPermission|/"));
        Assert.assertTrue(CALLED.contains("getAttributes|/"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user"));
        Assert.assertTrue(CALLED.contains("checkPermission|/user"));
        Assert.assertTrue(CALLED.contains("getAttributes|/user"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot"));
        Assert.assertTrue(CALLED.contains("checkPermission|/user/.snapshot"));
        // attribute provider never sees the .snapshot path directly.
        Assert.assertFalse(CALLED.contains("getAttributes|/user/.snapshot"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot/snapshot_1"));
        Assert.assertTrue(
            CALLED.contains("checkPermission|/user/.snapshot/snapshot_1"));
        Assert.assertTrue(
            CALLED.contains("getAttributes|/user/.snapshot/snapshot_1"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot/snapshot_1/authz"));
        Assert.assertTrue(CALLED
            .contains("checkPermission|/user/.snapshot/snapshot_1/authz"));
        Assert.assertTrue(CALLED.contains("getAttributes|/user/authz"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/authz"));
        Assert.assertTrue(CALLED.contains("checkPermission|/user/authz"));
        Assert.assertTrue(CALLED.contains("getAttributes|/user/authz"));
        return null;
      }
    });
    // Delete the files / folders covered by the snapshot, then re-check they
    // are all readable correctly.
    fs.delete(authz, true);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot"));
        Assert.assertTrue(CALLED.contains("checkPermission|/user/.snapshot"));
        // attribute provider never sees the .snapshot path directly.
        Assert.assertFalse(CALLED.contains("getAttributes|/user/.snapshot"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot/snapshot_1"));
        Assert.assertTrue(
            CALLED.contains("checkPermission|/user/.snapshot/snapshot_1"));
        Assert.assertTrue(
            CALLED.contains("getAttributes|/user/.snapshot/snapshot_1"));

        CALLED.clear();
        fs.getFileStatus(new Path("/user/.snapshot/snapshot_1/authz"));
        Assert.assertTrue(CALLED
            .contains("checkPermission|/user/.snapshot/snapshot_1/authz"));
        Assert.assertTrue(CALLED.contains("getAttributes|/user/authz"));

        return null;
      }
    });

  }
}
