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
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPermissionSymlinks {

  private static final Log LOG = LogFactory.getLog(TestPermissionSymlinks.class);
  private static final Configuration conf = new HdfsConfiguration();
  // Non-super user to run commands with
  private static final UserGroupInformation user = UserGroupInformation
      .createRemoteUser("myuser");
  
  private static final Path linkParent = new Path("/symtest1");
  private static final Path targetParent = new Path("/symtest2");
  private static final Path link = new Path(linkParent, "link");
  private static final Path target = new Path(targetParent, "target");

  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static FileSystemTestWrapper wrapper;
  
  @BeforeClass
  public static void beforeClassSetUp() throws Exception {
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.set(FsPermission.UMASK_LABEL, "000");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    wrapper = new FileSystemTestWrapper(fs);
  }

  @AfterClass
  public static void afterClassTearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Create initial test files
    fs.mkdirs(linkParent);
    fs.mkdirs(targetParent);
    DFSTestUtil.createFile(fs, target, 1024, (short)3, 0xBEEFl);
    wrapper.createSymlink(target, link, false);
  }

  @After
  public void tearDown() throws Exception {
    // Wipe out everything
    fs.delete(linkParent, true);
    fs.delete(targetParent, true);
  }

  @Test(timeout = 5000)
  public void testDelete() throws Exception {
    fs.setPermission(linkParent, new FsPermission((short) 0555));
    doDeleteLinkParentNotWritable();

    fs.setPermission(linkParent, new FsPermission((short) 0777));
    fs.setPermission(targetParent, new FsPermission((short) 0555));
    fs.setPermission(target, new FsPermission((short) 0555));
    doDeleteTargetParentAndTargetNotWritable();
  }

  @Test
  public void testAclDelete() throws Exception {
    fs.setAcl(linkParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doDeleteLinkParentNotWritable();

    fs.setAcl(linkParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    fs.setAcl(targetParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    fs.setAcl(target, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doDeleteTargetParentAndTargetNotWritable();
  }

  private void doDeleteLinkParentNotWritable() throws Exception {
    // Try to delete where the symlink's parent dir is not writable
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException {
          FileContext myfc = FileContext.getFileContext(conf);
          myfc.delete(link, false);
          return null;
        }
      });
      fail("Deleted symlink without write permissions on parent!");
    } catch (AccessControlException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
  }

  private void doDeleteTargetParentAndTargetNotWritable() throws Exception {
    // Try a delete where the symlink parent dir is writable,
    // but the target's parent and target are not
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        FileContext myfc = FileContext.getFileContext(conf);
        myfc.delete(link, false);
        return null;
      }
    });
    // Make sure only the link was deleted
    assertTrue("Target should not have been deleted!",
        wrapper.exists(target));
    assertFalse("Link should have been deleted!",
        wrapper.exists(link));
  }

  @Test(timeout = 5000)
  public void testReadWhenTargetNotReadable() throws Exception {
    fs.setPermission(target, new FsPermission((short) 0000));
    doReadTargetNotReadable();
  }

  @Test
  public void testAclReadTargetNotReadable() throws Exception {
    fs.setAcl(target, Arrays.asList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, user.getUserName(), NONE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ)));
    doReadTargetNotReadable();
  }

  private void doReadTargetNotReadable() throws Exception {
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException {
          FileContext myfc = FileContext.getFileContext(conf);
          myfc.open(link).read();
          return null;
        }
      });
      fail("Read link target even though target does not have"
          + " read permissions!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
  }

  @Test(timeout = 5000)
  public void testFileStatus() throws Exception {
    fs.setPermission(target, new FsPermission((short) 0000));
    doGetFileLinkStatusTargetNotReadable();
  }

  @Test
  public void testAclGetFileLinkStatusTargetNotReadable() throws Exception {
    fs.setAcl(target, Arrays.asList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, user.getUserName(), NONE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ)));
    doGetFileLinkStatusTargetNotReadable();
  }

  private void doGetFileLinkStatusTargetNotReadable() throws Exception {
    // Try to getFileLinkStatus the link when the target is not readable
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        FileContext myfc = FileContext.getFileContext(conf);
        FileStatus stat = myfc.getFileLinkStatus(link);
        assertEquals("Expected link's FileStatus path to match link!",
            link.makeQualified(fs.getUri(), fs.getWorkingDirectory()), stat.getPath());
        Path linkTarget = myfc.getLinkTarget(link);
        assertEquals("Expected link's target to match target!",
            target, linkTarget);
        return null;
      }
    });
  }

  @Test(timeout = 5000)
  public void testRenameLinkTargetNotWritableFC() throws Exception {
    fs.setPermission(target, new FsPermission((short) 0555));
    fs.setPermission(targetParent, new FsPermission((short) 0555));
    doRenameLinkTargetNotWritableFC();
  }

  @Test
  public void testAclRenameTargetNotWritableFC() throws Exception {
    fs.setAcl(target, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    fs.setAcl(targetParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doRenameLinkTargetNotWritableFC();
  }

  private void doRenameLinkTargetNotWritableFC() throws Exception {
    // Rename the link when the target and parent are not writable
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        // First FileContext
        FileContext myfc = FileContext.getFileContext(conf);
        Path newlink = new Path(linkParent, "newlink");
        myfc.rename(link, newlink, Rename.NONE);
        Path linkTarget = myfc.getLinkTarget(newlink);
        assertEquals("Expected link's target to match target!",
            target, linkTarget);
        return null;
      }
    });
    assertTrue("Expected target to exist", wrapper.exists(target));
  }

  @Test(timeout = 5000)
  public void testRenameSrcNotWritableFC() throws Exception {
    fs.setPermission(linkParent, new FsPermission((short) 0555));
    doRenameSrcNotWritableFC();
  }

  @Test
  public void testAclRenameSrcNotWritableFC() throws Exception {
    fs.setAcl(linkParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doRenameSrcNotWritableFC();
  }

  private void doRenameSrcNotWritableFC() throws Exception {
    // Rename the link when the target and parent are not writable
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException {
          FileContext myfc = FileContext.getFileContext(conf);
          Path newlink = new Path(targetParent, "newlink");
          myfc.rename(link, newlink, Rename.NONE);
          return null;
        }
      });
      fail("Renamed link even though link's parent is not writable!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
  }

  // Need separate FileSystem tests since the server-side impl is different
  // See {@link ClientProtocol#rename} and {@link ClientProtocol#rename2}.

  @Test(timeout = 5000)
  public void testRenameLinkTargetNotWritableFS() throws Exception {
    fs.setPermission(target, new FsPermission((short) 0555));
    fs.setPermission(targetParent, new FsPermission((short) 0555));
    doRenameLinkTargetNotWritableFS();
  }

  @Test
  public void testAclRenameTargetNotWritableFS() throws Exception {
    fs.setAcl(target, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    fs.setAcl(targetParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doRenameLinkTargetNotWritableFS();
  }

  private void doRenameLinkTargetNotWritableFS() throws Exception {
    // Rename the link when the target and parent are not writable
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException {
        // First FileContext
        FileSystem myfs = FileSystem.get(conf);
        Path newlink = new Path(linkParent, "newlink");
        myfs.rename(link, newlink);
        Path linkTarget = myfs.getLinkTarget(newlink);
        assertEquals("Expected link's target to match target!",
            target, linkTarget);
        return null;
      }
    });
    assertTrue("Expected target to exist", wrapper.exists(target));
  }

  @Test(timeout = 5000)
  public void testRenameSrcNotWritableFS() throws Exception {
    fs.setPermission(linkParent, new FsPermission((short) 0555));
    doRenameSrcNotWritableFS();
  }

  @Test
  public void testAclRenameSrcNotWritableFS() throws Exception {
    fs.setAcl(linkParent, Arrays.asList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, user.getUserName(), READ_EXECUTE),
      aclEntry(ACCESS, GROUP, ALL),
      aclEntry(ACCESS, OTHER, ALL)));
    doRenameSrcNotWritableFS();
  }

  private void doRenameSrcNotWritableFS() throws Exception {
    // Rename the link when the target and parent are not writable
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException {
          FileSystem myfs = FileSystem.get(conf);
          Path newlink = new Path(targetParent, "newlink");
          myfs.rename(link, newlink);
          return null;
        }
      });
      fail("Renamed link even though link's parent is not writable!");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }
  }

  @Test
  public void testAccess() throws Exception {
    fs.setPermission(target, new FsPermission((short) 0002));
    fs.setAcl(target, Arrays.asList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, NONE),
        aclEntry(ACCESS, USER, user.getShortUserName(), WRITE),
        aclEntry(ACCESS, OTHER, WRITE)));
    FileContext myfc = user.doAs(new PrivilegedExceptionAction<FileContext>() {
      @Override
      public FileContext run() throws IOException {
        return FileContext.getFileContext(conf);
      }
    });

    // Path to targetChild via symlink
    myfc.access(link, FsAction.WRITE);
    try {
      myfc.access(link, FsAction.ALL);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      // expected
    }

    Path badPath = new Path(link, "bad");
    try {
      myfc.access(badPath, FsAction.READ);
      fail("The access call should have failed");
    } catch (AccessControlException ace) {
      // expected
      String message = ace.getMessage();
      assertTrue(message, message.contains("is not a directory"));
      assertTrue(message.contains(target.toString()));
      assertFalse(message.contains(badPath.toString()));
    }
  }
}
