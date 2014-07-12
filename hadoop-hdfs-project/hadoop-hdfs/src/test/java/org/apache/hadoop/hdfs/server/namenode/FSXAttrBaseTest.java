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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests NameNode interaction for all XAttr APIs.
 * This test suite covers restarting the NN, saving a new checkpoint. 
 */
public class FSXAttrBaseTest {

  private static final int MAX_SIZE = 16;

  protected static MiniDFSCluster dfsCluster;
  protected static Configuration conf;
  private static int pathCount = 0;
  protected static Path path;
  
  // XAttrs
  protected static final String name1 = "user.a1";
  protected static final byte[] value1 = {0x31, 0x32, 0x33};
  protected static final byte[] newValue1 = {0x31, 0x31, 0x31};
  protected static final String name2 = "user.a2";
  protected static final byte[] value2 = {0x37, 0x38, 0x39};
  protected static final String name3 = "user.a3";
  protected static final String name4 = "user.a4";

  protected FileSystem fs;

  private static final UserGroupInformation BRUCE =
      UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
      UserGroupInformation.createUserForTesting("diana", new String[] { });

  @BeforeClass
  public static void init() throws Exception {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, MAX_SIZE);
    initCluster(true);
  }

  @AfterClass
  public static void shutdown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    pathCount += 1;
    path = new Path("/p" + pathCount);
    initFileSystem();
  }

  @After
  public void destroyFileSystems() {
    IOUtils.cleanup(null, fs);
    fs = null;
  }
  
  /**
   * Tests for creating xattr
   * 1. Create an xattr using XAttrSetFlag.CREATE.
   * 2. Create an xattr which already exists and expect an exception.
   * 3. Create multiple xattrs.
   * 4. Restart NN and save checkpoint scenarios.
   */
  @Test(timeout = 120000)
  public void testCreateXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 0);
    
    // Create xattr which already exists.
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    try {
      fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      Assert.fail("Creating xattr which already exists should fail.");
    } catch (IOException e) {
    }
    fs.removeXAttr(path, name1);
    
    // Create two xattrs
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, null, EnumSet.of(XAttrSetFlag.CREATE));
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    restart(false);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    restart(true);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for replacing xattr
   * 1. Replace an xattr using XAttrSetFlag.REPLACE.
   * 2. Replace an xattr which doesn't exist and expect an exception.
   * 3. Create multiple xattrs and replace some.
   * 4. Restart NN and save checkpoint scenarios.
   */
  @Test(timeout = 120000)
  public void testReplaceXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.REPLACE));
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(newValue1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    // Replace xattr which does not exist.
    try {
      fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.REPLACE));
      Assert.fail("Replacing xattr which does not exist should fail.");
    } catch (IOException e) {
    }
    
    // Create two xattrs, then replace one
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, null, EnumSet.of(XAttrSetFlag.REPLACE));
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    restart(false);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    restart(true);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for setting xattr
   * 1. Set xattr with XAttrSetFlag.CREATE|XAttrSetFlag.REPLACE flag.
   * 2. Set xattr with illegal name.
   * 3. Set xattr without XAttrSetFlag.
   * 4. Set xattr and total number exceeds max limit.
   * 5. Set xattr and name is too long.
   * 6. Set xattr and value is too long.
   */
  @Test(timeout = 120000)
  public void testSetXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE, 
        XAttrSetFlag.REPLACE));
        
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    fs.removeXAttr(path, name1);
    
    // Set xattr with null name
    try {
      fs.setXAttr(path, null, value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with null name should fail.");
    } catch (NullPointerException e) {
      GenericTestUtils.assertExceptionContains("XAttr name cannot be null", e);
    } catch (RemoteException e) {
      GenericTestUtils.assertExceptionContains("XAttr name cannot be null", e);
    }
    
    // Set xattr with empty name: "user."
    try {
      fs.setXAttr(path, "user.", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with empty name should fail.");
    } catch (HadoopIllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("XAttr name cannot be empty", e);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Invalid value: \"user.\" does " + 
          "not belong to the domain ^(user\\.|trusted\\.|system\\.|security\\.).+", e);
    }
    
    // Set xattr with invalid name: "a1"
    try {
      fs.setXAttr(path, "a1", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with invalid name prefix or without " +
          "name prefix should fail.");
    } catch (HadoopIllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("XAttr name must be prefixed", e);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Invalid value: \"a1\" does " + 
          "not belong to the domain ^(user\\.|trusted\\.|system\\.|security\\.).+", e);
    }
    
    // Set xattr without XAttrSetFlag
    fs.setXAttr(path, name1, value1);
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    fs.removeXAttr(path, name1);
    
    // XAttr exists, and replace it using CREATE|REPLACE flag.
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.CREATE, 
        XAttrSetFlag.REPLACE));
    
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(newValue1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    // Total number exceeds max limit
    fs.setXAttr(path, name1, value1);
    fs.setXAttr(path, name2, value2);
    fs.setXAttr(path, name3, null);
    try {
      fs.setXAttr(path, name4, null);
      Assert.fail("Setting xattr should fail if total number of xattrs " +
          "for inode exceeds max limit.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot add additional XAttr", e);
    }
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    fs.removeXAttr(path, name3);
    
    // Name length exceeds max limit
    String longName = "user.0123456789abcdefX";
    try {
      fs.setXAttr(path, longName, null);
      Assert.fail("Setting xattr should fail if name is too long.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("XAttr is too big", e);
      GenericTestUtils.assertExceptionContains("total size is 17", e);
    }

    // Value length exceeds max limit
    byte[] longValue = new byte[MAX_SIZE];
    try {
      fs.setXAttr(path, "user.a", longValue);
      Assert.fail("Setting xattr should fail if value is too long.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("XAttr is too big", e);
      GenericTestUtils.assertExceptionContains("total size is 17", e);
    }

    // Name + value exactly equal the limit
    String name = "user.111";
    byte[] value = new byte[MAX_SIZE-3];
    fs.setXAttr(path, name, value);
  }
  
  /**
   * Tests for getting xattr
   * 1. To get xattr which does not exist.
   * 2. To get multiple xattrs.
   */
  @Test(timeout = 120000)
  public void testGetXAttrs() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    
    // XAttr does not exist.
    byte[] value = fs.getXAttr(path, name3);
    Assert.assertEquals(value, null);
    
    List<String> names = Lists.newArrayList();
    names.add(name1);
    names.add(name2);
    names.add(name3);
    Map<String, byte[]> xattrs = fs.getXAttrs(path, names);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for removing xattr
   * 1. Remove xattr.
   * 2. Restart NN and save checkpoint scenarios.
   */
  @Test(timeout = 120000)
  public void testRemoveXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name3, null, EnumSet.of(XAttrSetFlag.CREATE));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    restart(false);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    restart(true);
    initFileSystem();
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    fs.removeXAttr(path, name3);
  }

  @Test(timeout = 120000)
  public void testRenameFileWithXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    Path renamePath = new Path(path.toString() + "-rename");
    fs.rename(path, renamePath);
    Map<String, byte[]> xattrs = fs.getXAttrs(renamePath);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    fs.removeXAttr(renamePath, name1);
    fs.removeXAttr(renamePath, name2);
  }

  /**
   * Test the listXAttrs api.
   * listXAttrs on a path that doesn't exist.
   * listXAttrs on a path with no XAttrs
   * Check basic functionality.
   * Check that read access to parent dir is not enough to get xattr names
   * Check that write access to the parent dir is not enough to get names
   * Check that execute/scan access to the parent dir is sufficient to get
   *  xattr names.
   */
  @Test(timeout = 120000)
  public void testListXAttrs() throws Exception {
    final UserGroupInformation user = UserGroupInformation.
      createUserForTesting("user", new String[] {"mygroup"});

    /* listXAttrs in a path that doesn't exist. */
    try {
      fs.listXAttrs(path);
      fail("expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      GenericTestUtils.assertExceptionContains("cannot find", e);
    }

    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 0750));

    /* listXAttrs on a path with no XAttrs.*/
    final List<String> noXAttrs = fs.listXAttrs(path);
    assertTrue("XAttrs were found?", noXAttrs.size() == 0);

    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));

    /** Check basic functionality. */
    final List<String> xattrNames = fs.listXAttrs(path);
    assertTrue(xattrNames.contains(name1));
    assertTrue(xattrNames.contains(name2));
    assertTrue(xattrNames.size() == 2);

    /* Check that read access to parent dir is not enough to get xattr names. */
    fs.setPermission(path, new FsPermission((short) 0704));
    final Path childDir = new Path(path, "child" + pathCount);
    FileSystem.mkdirs(fs, childDir, FsPermission.createImmutable((short) 0700));
    fs.setXAttr(childDir, name1, "1234".getBytes());
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.listXAttrs(childDir);
            return null;
          }
        });
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that write access to the parent dir is not enough to get names.
     */
    fs.setPermission(path, new FsPermission((short) 0702));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.listXAttrs(childDir);
            return null;
          }
        });
      fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that execute/scan access to the parent dir is sufficient to get
     * xattr names.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          userFs.listXAttrs(childDir);
          return null;
        }
      });

    /*
     * Test that xattrs in the "trusted" namespace are filtered correctly.
     */
    fs.setXAttr(childDir, "trusted.myxattr", "1234".getBytes());
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          assertTrue(userFs.listXAttrs(childDir).size() == 1);
          return null;
        }
      });

    assertTrue(fs.listXAttrs(childDir).size() == 2);
  }
  
  /**
   * Steps:
   * 1) Set xattrs on a file.
   * 2) Remove xattrs from that file.
   * 3) Save a checkpoint and restart NN.
   * 4) Set xattrs again on the same file.
   * 5) Remove xattrs from that file.
   * 6) Restart NN without saving a checkpoint.
   * 7) Set xattrs again on the same file.
   */
  @Test(timeout = 120000)
  public void testCleanupXAttrs() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    
    restart(true);
    initFileSystem();
    
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    
    restart(false);
    initFileSystem();
    
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
  }

  @Test(timeout = 120000)
  public void testXAttrAcl() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 0750));
    fs.setOwner(path, BRUCE.getUserName(), null);
    FileSystem fsAsBruce = createFileSystem(BRUCE);
    FileSystem fsAsDiana = createFileSystem(DIANA);
    fsAsBruce.setXAttr(path, name1, value1);

    Map<String, byte[]> xattrs;
    try {
      xattrs = fsAsDiana.getXAttrs(path);
      Assert.fail("Diana should not have read access to get xattrs");
    } catch (AccessControlException e) {
      // Ignore
    }

    // Give Diana read permissions to the path
    fsAsBruce.modifyAclEntries(path, Lists.newArrayList(
        aclEntry(ACCESS, USER, DIANA.getUserName(), READ)));
    xattrs = fsAsDiana.getXAttrs(path);
    Assert.assertArrayEquals(value1, xattrs.get(name1));

    try {
      fsAsDiana.removeXAttr(path, name1);
      Assert.fail("Diana should not have write access to remove xattrs");
    } catch (AccessControlException e) {
      // Ignore
    }

    try {
      fsAsDiana.setXAttr(path, name2, value2);
      Assert.fail("Diana should not have write access to set xattrs");
    } catch (AccessControlException e) {
      // Ignore
    }

    fsAsBruce.modifyAclEntries(path, Lists.newArrayList(
        aclEntry(ACCESS, USER, DIANA.getUserName(), ALL)));
    fsAsDiana.setXAttr(path, name2, value2);
    Assert.assertArrayEquals(value2, fsAsDiana.getXAttrs(path).get(name2));
    fsAsDiana.removeXAttr(path, name1);
    fsAsDiana.removeXAttr(path, name2);
  }
  
  /**
   * Creates a FileSystem for the super-user.
   *
   * @return FileSystem for super-user
   * @throws Exception if creation fails
   */
  protected FileSystem createFileSystem() throws Exception {
    return dfsCluster.getFileSystem();
  }

  /**
   * Creates a FileSystem for a specific user.
   *
   * @param user UserGroupInformation specific user
   * @return FileSystem for specific user
   * @throws Exception if creation fails
   */
  protected FileSystem createFileSystem(UserGroupInformation user)
      throws Exception {
    return DFSTestUtil.getFileSystemAs(user, conf);
  }
  
  /**
   * Initializes all FileSystem instances used in the tests.
   *
   * @throws Exception if initialization fails
   */
  private void initFileSystem() throws Exception {
    fs = createFileSystem();
  }
  
  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem
   * instances for our test users.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @throws Exception if any step fails
   */
  protected static void initCluster(boolean format) throws Exception {
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    dfsCluster.waitActive();
  }
  
  /**
   * Restart the cluster, optionally saving a new checkpoint.
   *
   * @param checkpoint boolean true to save a new checkpoint
   * @throws Exception if restart fails
   */
  protected static void restart(boolean checkpoint) throws Exception {
    NameNode nameNode = dfsCluster.getNameNode();
    if (checkpoint) {
      NameNodeAdapter.enterSafeMode(nameNode, false);
      NameNodeAdapter.saveNamespace(nameNode);
    }
    shutdown();
    initCluster(false);
  }
}
