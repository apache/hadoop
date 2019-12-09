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
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
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
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests NameNode interaction for all XAttr APIs.
 * This test suite covers restarting the NN, saving a new checkpoint. 
 */
public class FSXAttrBaseTest {

  protected static MiniDFSCluster dfsCluster;
  protected static Configuration conf;
  private static int pathCount = 0;
  protected static Path path;
  protected static Path filePath;
  protected static Path rawPath;
  protected static Path rawFilePath;

  // XAttrs
  protected static final String name1 = "user.a1";
  protected static final byte[] value1 = {0x31, 0x32, 0x33};
  protected static final byte[] newValue1 = {0x31, 0x31, 0x31};
  protected static final String name2 = "user.a2";
  protected static final byte[] value2 = {0x37, 0x38, 0x39};
  protected static final String name3 = "user.a3";
  protected static final String name4 = "user.a4";
  protected static final String raw1 = "raw.a1";
  protected static final String raw2 = "raw.a2";
  protected static final String security1 =
      SECURITY_XATTR_UNREADABLE_BY_SUPERUSER;

  private static final int MAX_SIZE = security1.length();

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
    filePath = new Path(path, "file");
    rawPath = new Path("/.reserved/raw/p" + pathCount);
    rawFilePath = new Path(rawPath, "file");
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
    Map<String, byte[]> expectedXAttrs = Maps.newHashMap();
    expectedXAttrs.put(name1, value1);
    expectedXAttrs.put(name2, null);
    expectedXAttrs.put(security1, null);
    doTestCreateXAttr(filePath, expectedXAttrs);
    expectedXAttrs.put(raw1, value1);
    doTestCreateXAttr(rawFilePath, expectedXAttrs);
  }

  private void doTestCreateXAttr(Path usePath, Map<String,
      byte[]> expectedXAttrs) throws Exception {
    DFSTestUtil.createFile(fs, usePath, 8192, (short) 1, 0xFEED);
    fs.setXAttr(usePath, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));

    Map<String, byte[]> xattrs = fs.getXAttrs(usePath);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    
    fs.removeXAttr(usePath, name1);
    
    xattrs = fs.getXAttrs(usePath);
    Assert.assertEquals(xattrs.size(), 0);
    
    // Create xattr which already exists.
    fs.setXAttr(usePath, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    try {
      fs.setXAttr(usePath, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      Assert.fail("Creating xattr which already exists should fail.");
    } catch (IOException e) {
    }
    fs.removeXAttr(usePath, name1);
    
    // Create the xattrs
    for (Map.Entry<String, byte[]> ent : expectedXAttrs.entrySet()) {
      fs.setXAttr(usePath, ent.getKey(), ent.getValue(),
          EnumSet.of(XAttrSetFlag.CREATE));
    }
    xattrs = fs.getXAttrs(usePath);
    Assert.assertEquals(xattrs.size(), expectedXAttrs.size());
    for (Map.Entry<String, byte[]> ent : expectedXAttrs.entrySet()) {
      final byte[] val =
          (ent.getValue() == null) ? new byte[0] : ent.getValue();
      Assert.assertArrayEquals(val, xattrs.get(ent.getKey()));
    }
    
    restart(false);
    initFileSystem();
    xattrs = fs.getXAttrs(usePath);
    Assert.assertEquals(xattrs.size(), expectedXAttrs.size());
    for (Map.Entry<String, byte[]> ent : expectedXAttrs.entrySet()) {
      final byte[] val =
          (ent.getValue() == null) ? new byte[0] : ent.getValue();
      Assert.assertArrayEquals(val, xattrs.get(ent.getKey()));
    }
    
    restart(true);
    initFileSystem();
    xattrs = fs.getXAttrs(usePath);
    Assert.assertEquals(xattrs.size(), expectedXAttrs.size());
    for (Map.Entry<String, byte[]> ent : expectedXAttrs.entrySet()) {
      final byte[] val =
          (ent.getValue() == null) ? new byte[0] : ent.getValue();
      Assert.assertArrayEquals(val, xattrs.get(ent.getKey()));
    }

    fs.delete(usePath, false);
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
      GenericTestUtils.assertExceptionContains("Required param xattr.name for "
          + "op: SETXATTR is null or empty", e);
    }
    
    // Set xattr with empty name: "user."
    try {
      fs.setXAttr(path, "user.", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with empty name should fail.");
    } catch (RemoteException e) {
      assertEquals("Unexpected RemoteException: " + e, e.getClassName(),
          HadoopIllegalArgumentException.class.getCanonicalName());
      GenericTestUtils.assertExceptionContains("XAttr name cannot be empty", e);
    } catch (HadoopIllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("XAttr name cannot be empty", e);
    }
    
    // Set xattr with invalid name: "a1"
    try {
      fs.setXAttr(path, "a1", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with invalid name prefix or without " +
          "name prefix should fail.");
    } catch (RemoteException e) {
      assertEquals("Unexpected RemoteException: " + e, e.getClassName(),
          HadoopIllegalArgumentException.class.getCanonicalName());
      GenericTestUtils.assertExceptionContains("XAttr name must be prefixed", e);
    } catch (HadoopIllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("XAttr name must be prefixed", e);
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
    String longName = "user.0123456789abcdefX0123456789abcdefX0123456789abcdef";
    try {
      fs.setXAttr(path, longName, null);
      Assert.fail("Setting xattr should fail if name is too long.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("XAttr is too big", e);
      GenericTestUtils.assertExceptionContains("total size is 50", e);
    }

    // Value length exceeds max limit
    byte[] longValue = new byte[MAX_SIZE];
    try {
      fs.setXAttr(path, "user.a", longValue);
      Assert.fail("Setting xattr should fail if value is too long.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("XAttr is too big", e);
      GenericTestUtils.assertExceptionContains("total size is 38", e);
    }

    // Name + value exactly equal the limit
    String name = "user.111";
    byte[] value = new byte[MAX_SIZE-3];
    fs.setXAttr(path, name, value);
  }
  
  /**
   * getxattr tests. Test that getxattr throws an exception if any of
   * the following are true:
   * an xattr that was requested doesn't exist
   * the caller specifies an unknown namespace
   * the caller doesn't have access to the namespace
   * the caller doesn't have permission to get the value of the xattr
   * the caller does not have search access to the parent directory
   * the caller has only read access to the owning directory
   * the caller has only search access to the owning directory and
   * execute/search access to the actual entity
   * the caller does not have search access to the owning directory and read
   * access to the actual entity
   */
  @Test(timeout = 120000)
  public void testGetXAttrs() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));

    final byte[] theValue = fs.getXAttr(path, "USER.a2");
    Assert.assertArrayEquals(value2, theValue);

    /* An XAttr that was requested does not exist. */
    try {
      final byte[] value = fs.getXAttr(path, name3);
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "At least one of the attributes provided was not found.", e);
    }
    
    /* Throw an exception if an xattr that was requested does not exist. */
    {
      final List<String> names = Lists.newArrayList();
      names.add(name1);
      names.add(name2);
      names.add(name3);
      try {
        final Map<String, byte[]> xattrs = fs.getXAttrs(path, names);
        Assert.fail("expected IOException");
      } catch (IOException e) {
        GenericTestUtils.assertExceptionContains(
            "At least one of the attributes provided was not found.", e);
      }
    }
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);

    /* Unknown namespace should throw an exception. */
    try {
      final byte[] xattr = fs.getXAttr(path, "wackynamespace.foo");
      Assert.fail("expected IOException");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains
          ("An XAttr name must be prefixed with " +
           "user/trusted/security/system/raw, " +
           "followed by a '.'",
          e);
    }

    /*
     * The 'trusted' namespace should not be accessible and should throw an
     * exception.
     */
    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    fs.setXAttr(path, "trusted.foo", "1234".getBytes());
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            final byte[] xattr = userFs.getXAttr(path, "trusted.foo");
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("User doesn't have permission", e);
    }

    fs.setXAttr(path, name1, "1234".getBytes());

    /*
     * Test that an exception is thrown if the caller doesn't have permission to
     * get the value of the xattr.
     */

    /* Set access so that only the owner has access. */
    fs.setPermission(path, new FsPermission((short) 0700));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            final byte[] xattr = userFs.getXAttr(path, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * The caller must have search access to the parent directory.
     */
    final Path childDir = new Path(path, "child" + pathCount);
    /* Set access to parent so that only the owner has access. */
    FileSystem.mkdirs(fs, childDir, FsPermission.createImmutable((short)0700));
    fs.setXAttr(childDir, name1, "1234".getBytes());
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            final byte[] xattr = userFs.getXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /* Check that read access to the owning directory is not good enough. */
    fs.setPermission(path, new FsPermission((short) 0704));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            final byte[] xattr = userFs.getXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that search access to the owning directory and search/execute
     * access to the actual entity with extended attributes is not good enough.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    fs.setPermission(childDir, new FsPermission((short) 0701));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            final byte[] xattr = userFs.getXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that search access to the owning directory and read access to
     * the actual entity with the extended attribute is good enough.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    fs.setPermission(childDir, new FsPermission((short) 0704));
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          final byte[] xattr = userFs.getXAttr(childDir, name1);
          return null;
        }
      });
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

  /**
   * removexattr tests. Test that removexattr throws an exception if any of
   * the following are true:
   * an xattr that was requested doesn't exist
   * the caller specifies an unknown namespace
   * the caller doesn't have access to the namespace
   * the caller doesn't have permission to get the value of the xattr
   * the caller does not have "execute" (scan) access to the parent directory
   * the caller has only read access to the owning directory
   * the caller has only execute access to the owning directory and execute
   * access to the actual entity
   * the caller does not have execute access to the owning directory and write
   * access to the actual entity
   */
  @Test(timeout = 120000)
  public void testRemoveXAttrPermissions() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name3, null, EnumSet.of(XAttrSetFlag.CREATE));

    try {
      fs.removeXAttr(path, name2);
      fs.removeXAttr(path, name2);
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("No matching attributes found", e);
    }

    /* Unknown namespace should throw an exception. */
    final String expectedExceptionString = "An XAttr name must be prefixed " +
        "with user/trusted/security/system/raw, followed by a '.'";
    try {
      fs.removeXAttr(path, "wackynamespace.foo");
      Assert.fail("expected IOException");
    } catch (RemoteException e) {
      assertEquals("Unexpected RemoteException: " + e, e.getClassName(),
          HadoopIllegalArgumentException.class.getCanonicalName());
      GenericTestUtils.assertExceptionContains(expectedExceptionString, e);
    } catch (HadoopIllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(expectedExceptionString, e);
    }

    /*
     * The 'trusted' namespace should not be accessible and should throw an
     * exception.
     */
    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] {"mygroup"});
    fs.setXAttr(path, "trusted.foo", "1234".getBytes());
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.removeXAttr(path, "trusted.foo");
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("User doesn't have permission", e);
    } finally {
      fs.removeXAttr(path, "trusted.foo");
    }

    /*
     * Test that an exception is thrown if the caller doesn't have permission to
     * get the value of the xattr.
     */

    /* Set access so that only the owner has access. */
    fs.setPermission(path, new FsPermission((short) 0700));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.removeXAttr(path, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * The caller must have "execute" (scan) access to the parent directory.
     */
    final Path childDir = new Path(path, "child" + pathCount);
    /* Set access to parent so that only the owner has access. */
    FileSystem.mkdirs(fs, childDir, FsPermission.createImmutable((short)0700));
    fs.setXAttr(childDir, name1, "1234".getBytes());
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.removeXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /* Check that read access to the owning directory is not good enough. */
    fs.setPermission(path, new FsPermission((short) 0704));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.removeXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that execute access to the owning directory and scan access to
     * the actual entity with extended attributes is not good enough.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    fs.setPermission(childDir, new FsPermission((short) 0701));
    try {
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            userFs.removeXAttr(childDir, name1);
            return null;
          }
        });
      Assert.fail("expected IOException");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Permission denied", e);
    }

    /*
     * Check that execute access to the owning directory and write access to
     * the actual entity with extended attributes is good enough.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    fs.setPermission(childDir, new FsPermission((short) 0706));
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          userFs.removeXAttr(childDir, name1);
          return null;
        }
      });
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
     * Check that execute/scan access to the parent dir is not
     * sufficient to get xattr names.
     */
    fs.setPermission(path, new FsPermission((short) 0701));
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
        try {
          final FileSystem userFs = dfsCluster.getFileSystem();
          userFs.listXAttrs(childDir);
          fail("expected AccessControlException");
        } catch (AccessControlException ace) {
          GenericTestUtils.assertExceptionContains("Permission denied", ace);
        }
        return null;
      }
      });

    /*
     * Test that xattrs in the "trusted" namespace are filtered correctly.
     */
    // Allow the user to read child path.
    fs.setPermission(childDir, new FsPermission((short) 0704));
    fs.setXAttr(childDir, "trusted.myxattr", "1234".getBytes());
    user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          List<String> xattrs = userFs.listXAttrs(childDir);
          assertTrue(xattrs.size() == 1);
          assertEquals(name1, xattrs.get(0));
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
  
  @Test(timeout = 120000)
  public void testRawXAttrs() throws Exception {
    final UserGroupInformation user = UserGroupInformation.
      createUserForTesting("user", new String[] {"mygroup"});

    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 0750));
    fs.setXAttr(rawPath, raw1, value1, EnumSet.of(XAttrSetFlag.CREATE,
        XAttrSetFlag.REPLACE));

    {
      // getXAttr
      final byte[] value = fs.getXAttr(rawPath, raw1);
      Assert.assertArrayEquals(value, value1);
    }

    {
      // getXAttrs
      final Map<String, byte[]> xattrs = fs.getXAttrs(rawPath);
      Assert.assertEquals(xattrs.size(), 1);
      Assert.assertArrayEquals(value1, xattrs.get(raw1));
      fs.removeXAttr(rawPath, raw1);
    }

    {
      // replace and re-get
      fs.setXAttr(rawPath, raw1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      fs.setXAttr(rawPath, raw1, newValue1, EnumSet.of(XAttrSetFlag.CREATE,
          XAttrSetFlag.REPLACE));

      final Map<String,byte[]> xattrs = fs.getXAttrs(rawPath);
      Assert.assertEquals(xattrs.size(), 1);
      Assert.assertArrayEquals(newValue1, xattrs.get(raw1));

      fs.removeXAttr(rawPath, raw1);
    }

    {
      // listXAttrs on rawPath ensuring raw.* xattrs are returned
      fs.setXAttr(rawPath, raw1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      fs.setXAttr(rawPath, raw2, value2, EnumSet.of(XAttrSetFlag.CREATE));

      final List<String> xattrNames = fs.listXAttrs(rawPath);
      assertTrue(xattrNames.contains(raw1));
      assertTrue(xattrNames.contains(raw2));
      assertTrue(xattrNames.size() == 2);
      fs.removeXAttr(rawPath, raw1);
      fs.removeXAttr(rawPath, raw2);
    }

    {
      // listXAttrs on non-rawPath ensuring no raw.* xattrs returned
      fs.setXAttr(rawPath, raw1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      fs.setXAttr(rawPath, raw2, value2, EnumSet.of(XAttrSetFlag.CREATE));

      final List<String> xattrNames = fs.listXAttrs(path);
      assertTrue(xattrNames.size() == 0);
      fs.removeXAttr(rawPath, raw1);
      fs.removeXAttr(rawPath, raw2);
    }

    {
      /*
       * Test non-root user operations in the "raw.*" namespace.
       */
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          // Test that non-root can not set xattrs in the "raw.*" namespace
          try {
            // non-raw path
            userFs.setXAttr(path, raw1, value1);
            fail("setXAttr should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }

          try {
            // raw path
            userFs.setXAttr(rawPath, raw1, value1);
            fail("setXAttr should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }

          // Test that non-root can not do getXAttrs in the "raw.*" namespace
          try {
            // non-raw path
            userFs.getXAttrs(rawPath);
            fail("getXAttrs should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }

          try {
            // raw path
            userFs.getXAttrs(path);
            fail("getXAttrs should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }

          // Test that non-root can not do getXAttr in the "raw.*" namespace
          try {
            // non-raw path
            userFs.getXAttr(rawPath, raw1);
            fail("getXAttr should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }

          try {
            // raw path
            userFs.getXAttr(path, raw1);
            fail("getXAttr should have thrown");
          } catch (AccessControlException e) {
            // ignore
          }
          return null;
        }
        });
    }

    {
      /*
       * Test that user who don'r have read access
       *  can not do getXAttr in the "raw.*" namespace
       */
      fs.setXAttr(rawPath, raw1, value1);
      user.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            final FileSystem userFs = dfsCluster.getFileSystem();
            try {
              // raw path
              userFs.getXAttr(rawPath, raw1);
              fail("getXAttr should have thrown");
            } catch (AccessControlException e) {
              // ignore
            }

            try {
              // non-raw path
              userFs.getXAttr(path, raw1);
              fail("getXAttr should have thrown");
            } catch (AccessControlException e) {
              // ignore
            }

            /*
            * Test that user who have parent directory execute access
            *  can also not see raw.* xattrs returned from listXAttr
            */
            try {
              // non-raw path
              userFs.listXAttrs(path);
              fail("listXAttr should have thrown AccessControlException");
            } catch (AccessControlException ace) {
              // expected
            }

            try {
              // raw path
              userFs.listXAttrs(rawPath);
              fail("listXAttr should have thrown AccessControlException");
            } catch (AccessControlException ace) {
              // expected
            }
            return null;
          }
        });
      /*
        Test user who have read access can list xattrs in "raw.*" namespace
       */
      fs.setPermission(path, new FsPermission((short) 0751));
      final Path childDir = new Path(path, "child" + pathCount);
      FileSystem.mkdirs(fs, childDir, FsPermission.createImmutable((short)
          0704));
      final Path rawChildDir =
          new Path("/.reserved/raw" + childDir.toString());
      fs.setXAttr(rawChildDir, raw1, value1);
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final FileSystem userFs = dfsCluster.getFileSystem();
          // raw path
          List<String> xattrs = userFs.listXAttrs(rawChildDir);
          assertEquals(1, xattrs.size());
          assertEquals(raw1, xattrs.get(0));
          return null;
        }
      });
      fs.removeXAttr(rawPath, raw1);
    }

    {
      /*
       * Tests that user who have read access are able to do getattr.
       */
      Path parentPath = new Path("/foo");
      fs.mkdirs(parentPath);
      fs.setOwner(parentPath, "user", "mygroup");
      // Set only execute permission for others on parent directory so that
      // any user can traverse down the directory.
      fs.setPermission(parentPath, new FsPermission("701"));
      Path childPath = new Path("/foo/bar");
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final DistributedFileSystem dfs = dfsCluster.getFileSystem();
          DFSTestUtil.createFile(dfs, childPath, 1024, (short) 1, 0xFEED);
          dfs.setPermission(childPath, new FsPermission("740"));
          return null;
        }
      });
      Path rawChildPath =
          new Path("/.reserved/raw" + childPath.toString());
      fs.setXAttr(new Path("/.reserved/raw/foo/bar"), raw1, value1);
      user.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final DistributedFileSystem dfs = dfsCluster.getFileSystem();
          // Make sure user have access to raw xattr.
          byte[] xattr = dfs.getXAttr(rawChildPath, raw1);
          assertEquals(Arrays.toString(value1), Arrays.toString(xattr));
          return null;
        }
      });

      final UserGroupInformation fakeUser = UserGroupInformation
          .createUserForTesting("fakeUser", new String[] {"fakeGroup"});
      fakeUser.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final DistributedFileSystem dfs = dfsCluster.getFileSystem();
          try {
            // Make sure user who don't have read access to file can't access
            // raw xattr.
            dfs.getXAttr(path, raw1);
            fail("should have thrown AccessControlException");
          } catch (AccessControlException ace) {
            // expected
          }
          return null;
        }
      });
      // fs.removeXAttr(rawPath, raw1);
    }
  }

  /**
   * This tests the "unreadable by superuser" xattr which denies access to a
   * file for the superuser. See HDFS-6705 for details.
   */
  @Test(timeout = 120000)
  public void testUnreadableBySuperuserXAttr() throws Exception {
    // Run tests as superuser...
    doTestUnreadableBySuperuserXAttr(fs, true);

    // ...and again as non-superuser
    final UserGroupInformation user = UserGroupInformation.
        createUserForTesting("user", new String[] { "mygroup" });
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        final FileSystem userFs = dfsCluster.getFileSystem();
        doTestUnreadableBySuperuserXAttr(userFs, false);
        return null;
      }
    });
  }

  private void doTestUnreadableBySuperuserXAttr(FileSystem userFs,
      boolean expectOpenFailure) throws Exception {

    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 0777));
    DFSTestUtil.createFile(userFs, filePath, 8192, (short) 1, 0xFEED);
    try {
      doTUBSXAInt(userFs, expectOpenFailure);
      // Deleting the file is allowed.
      userFs.delete(filePath, false);
    } finally {
      fs.delete(path, true);
    }
  }

  private void doTUBSXAInt(FileSystem userFs, boolean expectOpenFailure)
      throws Exception {

    // Test that xattr can't be set on a dir
    try {
      userFs.setXAttr(path, security1, null, EnumSet.of(XAttrSetFlag.CREATE));
    } catch (IOException e) {
      // WebHDFS throws IOException instead of RemoteException
      GenericTestUtils.assertExceptionContains("Can only set '" +
          SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' on a file", e);
    }

    // Test that xattr can actually be set. Repeatedly.
    userFs.setXAttr(filePath, security1, null,
      EnumSet.of(XAttrSetFlag.CREATE));
    verifySecurityXAttrExists(userFs);
    userFs.setXAttr(filePath, security1, null, EnumSet.of(XAttrSetFlag.CREATE,
        XAttrSetFlag.REPLACE));
    verifySecurityXAttrExists(userFs);

    // Test that the xattr can't be deleted by anyone.
    try {
      userFs.removeXAttr(filePath, security1);
      Assert.fail("Removing security xattr should fail.");
    } catch (AccessControlException e) {
      GenericTestUtils.assertExceptionContains("The xattr '" +
          SECURITY_XATTR_UNREADABLE_BY_SUPERUSER + "' can not be deleted.", e);
    }

    // Test that xattr can be read.
    verifySecurityXAttrExists(userFs);

    // Test that a value can't be set for the xattr.
    try {
      userFs.setXAttr(filePath, security1,
          value1,EnumSet.of(XAttrSetFlag.REPLACE));
      fail("Should have thrown on attempt to set value");
    } catch (AccessControlException e) {
      GenericTestUtils.assertExceptionContains("Values are not allowed", e);
    }

    // Test that unreadable by superuser xattr appears in listXAttrs results
    // (for superuser and non-superuser)
    final List<String> xattrNames = userFs.listXAttrs(filePath);
    assertTrue(xattrNames.contains(security1));
    assertTrue(xattrNames.size() == 1);

    verifyFileAccess(userFs, expectOpenFailure);

    // Rename of the file is allowed by anyone.
    Path toPath = new Path(filePath.toString() + "x");
    userFs.rename(filePath, toPath);
    userFs.rename(toPath, filePath);
  }

  private void verifySecurityXAttrExists(FileSystem userFs) throws Exception {
    try {
      final Map<String, byte[]> xattrs = userFs.getXAttrs(filePath);
      Assert.assertEquals(1, xattrs.size());
      Assert.assertNotNull(xattrs.get(security1));
      Assert.assertArrayEquals("expected empty byte[] from getXAttr",
          new byte[0], userFs.getXAttr(filePath, security1));

    } catch (AccessControlException e) {
      fail("getXAttrs failed but expected it to succeed");
    }
  }

  private void verifyFileAccess(FileSystem userFs, boolean expectOpenFailure)
      throws Exception {
    // Test that a file with the xattr can or can't be opened.
    try {
      userFs.open(filePath).read();
      assertFalse("open succeeded but expected it to fail", expectOpenFailure);
    } catch (AccessControlException e) {
      assertTrue("open failed but expected it to succeed", expectOpenFailure);
    }
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
