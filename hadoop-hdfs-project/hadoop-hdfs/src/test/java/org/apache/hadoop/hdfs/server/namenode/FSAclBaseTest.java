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

import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests NameNode interaction for all ACL modification APIs.  This test suite
 * also covers interaction of setPermission with inodes that have ACLs.
 */
public abstract class FSAclBaseTest {
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { });
  private static final UserGroupInformation SUPERGROUP_MEMBER =
    UserGroupInformation.createUserForTesting("super", new String[] {
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT });

  protected static MiniDFSCluster cluster;
  protected static Configuration conf;
  private static int pathCount = 0;
  private static Path path;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private FileSystem fs, fsAsBruce, fsAsDiana, fsAsSupergroupMember;

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    pathCount += 1;
    path = new Path("/p" + pathCount);
    initFileSystems();
  }

  @After
  public void destroyFileSystems() {
    IOUtils.cleanup(null, fs, fsAsBruce, fsAsDiana, fsAsSupergroupMember);
    fs = fsAsBruce = fsAsDiana = fsAsSupergroupMember = null;
  }

  @Test
  public void testModifyAclEntries() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesMinimal() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_WRITE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)010660);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesMinimalDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesCustomMask() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, MASK, NONE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)010600);
    assertAclFeature(true);
  }

  @Test
  public void testModifyAclEntriesStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)011750);
    assertAclFeature(true);
  }

  @Test(expected=FileNotFoundException.class)
  public void testModifyAclEntriesPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    fs.modifyAclEntries(path, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testModifyAclEntriesDefaultOnFile() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
  }

  @Test
  public void testRemoveAclEntries() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(DEFAULT, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveAclEntriesOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, USER, "bar", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "bar", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ_WRITE) }, returned);
    assertPermission((short)010760);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveAclEntriesOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, USER, "bar", READ_EXECUTE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "bar", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveAclEntriesMinimal() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0760));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_WRITE),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(ACCESS, MASK));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0760);
    assertAclFeature(false);
  }


  @Test
  public void testRemoveAclEntriesMinimalDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(ACCESS, MASK),
      aclEntry(DEFAULT, USER, "foo"),
      aclEntry(DEFAULT, MASK));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveAclEntriesStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"),
      aclEntry(DEFAULT, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)011750);
    assertAclFeature(true);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclEntriesPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo"));
    fs.removeAclEntries(path, aclSpec);
  }

  @Test
  public void testRemoveDefaultAcl() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission((short)010770);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveDefaultAclOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission((short)010770);
    assertAclFeature(true);
  }

  @Test
  public void testRemoveDefaultAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveDefaultAclMinimal() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveDefaultAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission((short)011770);
    assertAclFeature(true);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveDefaultAclPathNotFound() throws IOException {
    // Path has not been created.
    fs.removeDefaultAcl(path);
  }

  @Test
  public void testRemoveAcl() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveAclMinimalAcl() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0640);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)01750);
    assertAclFeature(false);
  }

  @Test
  public void testRemoveAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0750);
    assertAclFeature(false);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclPathNotFound() throws IOException {
    // Path has not been created.
    fs.removeAcl(path);
  }

  @Test
  public void testSetAcl() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010770);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)010640);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclMinimal() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0644));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission((short)0640);
    assertAclFeature(false);
  }

  @Test
  public void testSetAclMinimalDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010750);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclCustomMask() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, ALL),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)010670);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclStickyBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)011770);
    assertAclFeature(true);
  }

  @Test(expected=FileNotFoundException.class)
  public void testSetAclPathNotFound() throws IOException {
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
  }

  @Test(expected=AclException.class)
  public void testSetAclDefaultOnFile() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
  }

  @Test
  public void testSetPermission() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0700));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010700);
    assertAclFeature(true);
  }

  @Test
  public void testSetPermissionOnlyAccess() throws IOException {
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0600));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", READ),
      aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission((short)010600);
    assertAclFeature(true);
  }

  @Test
  public void testSetPermissionOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(ACCESS, OTHER, NONE),
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short)0700));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission((short)010700);
    assertAclFeature(true);
  }

  @Test
  public void testSetPermissionCannotSetAclBit() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setPermission(path, FsPermission.createImmutable((short)0700));
    assertPermission((short)0700);
    fs.setPermission(path,
      new FsPermissionExtension(FsPermission.
          createImmutable((short)0755), true, true));
    INode inode = cluster.getNamesystem().getFSDirectory().getNode(
      path.toUri().getPath(), false);
    assertNotNull(inode);
    FsPermission perm = inode.getFsPermission();
    assertNotNull(perm);
    assertEquals(0755, perm.toShort());
    assertEquals(0755, perm.toExtendedShort());
    assertAclFeature(false);
  }

  @Test
  public void testDefaultAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(filePath, (short)010640);
    assertAclFeature(filePath, true);
  }

  @Test
  public void testOnlyAccessAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(filePath, (short)0644);
    assertAclFeature(filePath, false);
  }

  @Test
  public void testDefaultMinimalAclNewFile() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, false);
  }

  @Test
  public void testDefaultAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(dirPath, (short)010750);
    assertAclFeature(dirPath, true);
  }

  @Test
  public void testOnlyAccessAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "foo", ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(dirPath, (short)0755);
    assertAclFeature(dirPath, false);
  }

  @Test
  public void testDefaultMinimalAclNewDir() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(dirPath, (short)010750);
    assertAclFeature(dirPath, true);
  }

  @Test
  public void testDefaultAclNewFileIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path filePath = new Path(dirPath, "file1");
    fs.create(filePath).close();
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)010750);
    assertAclFeature(dirPath, true);
    expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    s = fs.getAclStatus(filePath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(filePath, (short)010640);
    assertAclFeature(filePath, true);
  }

  @Test
  public void testDefaultAclNewDirIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path subdirPath = new Path(dirPath, "subdir1");
    fs.mkdirs(subdirPath);
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)010750);
    assertAclFeature(dirPath, true);
    s = fs.getAclStatus(subdirPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(subdirPath, (short)010750);
    assertAclFeature(subdirPath, true);
  }

  @Test
  public void testDefaultAclNewSymlinkIntermediate() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    fs.setPermission(filePath, FsPermission.createImmutable((short)0640));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    Path linkPath = new Path(dirPath, "link1");
    fs.createSymlink(filePath, linkPath, true);
    AclEntry[] expected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, NONE) };
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(dirPath, (short)010750);
    assertAclFeature(dirPath, true);
    expected = new AclEntry[] { };
    s = fs.getAclStatus(linkPath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(linkPath, (short)0640);
    assertAclFeature(linkPath, false);
    s = fs.getAclStatus(filePath);
    returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(filePath, (short)0640);
    assertAclFeature(filePath, false);
  }

  @Test
  public void testDefaultAclNewFileWithMode() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    int bufferSize = cluster.getConfiguration(0).getInt(
      CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    fs.create(filePath, new FsPermission((short)0740), false, bufferSize,
      fs.getDefaultReplication(filePath), fs.getDefaultBlockSize(path), null)
      .close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(filePath, (short)010740);
    assertAclFeature(filePath, true);
  }

  @Test
  public void testDefaultAclNewDirWithMode() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0755));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath, new FsPermission((short)0740));
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE) }, returned);
    assertPermission(dirPath, (short)010740);
    assertAclFeature(dirPath, true);
  }

  @Test
  public void testDefaultAclRenamedFile() throws Exception {
    Path dirPath = new Path(path, "dir");
    FileSystem.mkdirs(fs, dirPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(dirPath, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    fs.setPermission(filePath, FsPermission.createImmutable((short)0640));
    Path renamedFilePath = new Path(dirPath, "file1");
    fs.rename(filePath, renamedFilePath);
    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = fs.getAclStatus(renamedFilePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(renamedFilePath, (short)0640);
    assertAclFeature(renamedFilePath, false);
  }

  @Test
  public void testDefaultAclRenamedDir() throws Exception {
    Path dirPath = new Path(path, "dir");
    FileSystem.mkdirs(fs, dirPath, FsPermission.createImmutable((short)0750));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(dirPath, aclSpec);
    Path subdirPath = new Path(path, "subdir");
    FileSystem.mkdirs(fs, subdirPath, FsPermission.createImmutable((short)0750));
    Path renamedSubdirPath = new Path(dirPath, "subdir");
    fs.rename(subdirPath, renamedSubdirPath);
    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = fs.getAclStatus(renamedSubdirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(renamedSubdirPath, (short)0750);
    assertAclFeature(renamedSubdirPath, false);
  }

  @Test
  public void testSkipAclEnforcementPermsDisabled() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    fsAsBruce.modifyAclEntries(bruceFile, Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana", NONE)));
    assertFilePermissionDenied(fsAsDiana, DIANA, bruceFile);
    try {
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
      destroyFileSystems();
      restartCluster();
      initFileSystems();
      assertFilePermissionGranted(fsAsDiana, DIANA, bruceFile);
    } finally {
      conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
      restartCluster();
    }
  }

  @Test
  public void testSkipAclEnforcementSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    fsAsBruce.modifyAclEntries(bruceFile, Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana", NONE)));
    assertFilePermissionGranted(fs, DIANA, bruceFile);
    assertFilePermissionGranted(fsAsBruce, DIANA, bruceFile);
    assertFilePermissionDenied(fsAsDiana, DIANA, bruceFile);
    assertFilePermissionGranted(fsAsSupergroupMember, SUPERGROUP_MEMBER,
      bruceFile);
  }

  @Test
  public void testModifyAclEntriesMustBeOwnerOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana", ALL));
    fsAsBruce.modifyAclEntries(bruceFile, aclSpec);
    fs.modifyAclEntries(bruceFile, aclSpec);
    fsAsSupergroupMember.modifyAclEntries(bruceFile, aclSpec);
    exception.expect(AccessControlException.class);
    fsAsDiana.modifyAclEntries(bruceFile, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesMustBeOwnerOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, "diana"));
    fsAsBruce.removeAclEntries(bruceFile, aclSpec);
    fs.removeAclEntries(bruceFile, aclSpec);
    fsAsSupergroupMember.removeAclEntries(bruceFile, aclSpec);
    exception.expect(AccessControlException.class);
    fsAsDiana.removeAclEntries(bruceFile, aclSpec);
  }

  @Test
  public void testRemoveDefaultAclMustBeOwnerOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    fsAsBruce.removeDefaultAcl(bruceFile);
    fs.removeDefaultAcl(bruceFile);
    fsAsSupergroupMember.removeDefaultAcl(bruceFile);
    exception.expect(AccessControlException.class);
    fsAsDiana.removeDefaultAcl(bruceFile);
  }

  @Test
  public void testRemoveAclMustBeOwnerOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    fsAsBruce.removeAcl(bruceFile);
    fs.removeAcl(bruceFile);
    fsAsSupergroupMember.removeAcl(bruceFile);
    exception.expect(AccessControlException.class);
    fsAsDiana.removeAcl(bruceFile);
  }

  @Test
  public void testSetAclMustBeOwnerOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ));
    fsAsBruce.setAcl(bruceFile, aclSpec);
    fs.setAcl(bruceFile, aclSpec);
    fsAsSupergroupMember.setAcl(bruceFile, aclSpec);
    exception.expect(AccessControlException.class);
    fsAsDiana.setAcl(bruceFile, aclSpec);
  }

  @Test
  public void testGetAclStatusRequiresTraverseOrSuper() throws Exception {
    Path bruceDir = new Path(path, "bruce");
    Path bruceFile = new Path(bruceDir, "file");
    fs.mkdirs(bruceDir);
    fs.setOwner(bruceDir, "bruce", null);
    fsAsBruce.create(bruceFile).close();
    fsAsBruce.setAcl(bruceDir, Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", READ),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE)));
    fsAsBruce.getAclStatus(bruceFile);
    fs.getAclStatus(bruceFile);
    fsAsSupergroupMember.getAclStatus(bruceFile);
    exception.expect(AccessControlException.class);
    fsAsDiana.getAclStatus(bruceFile);
  }

  @Test
  public void testAccess() throws IOException, InterruptedException {
    Path p1 = new Path("/p1");
    fs.mkdirs(p1);
    fs.setOwner(p1, BRUCE.getShortUserName(), "groupX");
    fsAsBruce.setAcl(p1, Lists.newArrayList(
        aclEntry(ACCESS, USER, READ),
        aclEntry(ACCESS, USER, "bruce", READ),
        aclEntry(ACCESS, GROUP, NONE),
        aclEntry(ACCESS, OTHER, NONE)));
    fsAsBruce.access(p1, FsAction.READ);
    try {
      fsAsBruce.access(p1, FsAction.WRITE);
      fail("The access call should have failed.");
    } catch (AccessControlException e) {
      // expected
    }

    Path badPath = new Path("/bad/bad");
    try {
      fsAsBruce.access(badPath, FsAction.READ);
      fail("The access call should have failed");
    } catch (FileNotFoundException e) {
      // expected
    }
  }

  /**
   * Creates a FileSystem for the super-user.
   *
   * @return FileSystem for super-user
   * @throws Exception if creation fails
   */
  protected FileSystem createFileSystem() throws Exception {
    return cluster.getFileSystem();
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
    return DFSTestUtil.getFileSystemAs(user, cluster.getConfiguration(0));
  }

  /**
   * Initializes all FileSystem instances used in the tests.
   *
   * @throws Exception if initialization fails
   */
  private void initFileSystems() throws Exception {
    fs = createFileSystem();
    fsAsBruce = createFileSystem(BRUCE);
    fsAsDiana = createFileSystem(DIANA);
    fsAsSupergroupMember = createFileSystem(SUPERGROUP_MEMBER);
  }

  /**
   * Restarts the cluster without formatting, so all data is preserved.
   *
   * @throws Exception if restart fails
   */
  private void restartCluster() throws Exception {
    shutdown();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(false)
      .build();
    cluster.waitActive();
  }

  /**
   * Asserts whether or not the inode for the test path has an AclFeature.
   *
   * @param expectAclFeature boolean true if an AclFeature must be present,
   *   false if an AclFeature must not be present
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertAclFeature(boolean expectAclFeature)
      throws IOException {
    assertAclFeature(path, expectAclFeature);
  }

  /**
   * Asserts whether or not the inode for a specific path has an AclFeature.
   *
   * @param pathToCheck Path inode to check
   * @param expectAclFeature boolean true if an AclFeature must be present,
   *   false if an AclFeature must not be present
   * @throws IOException thrown if there is an I/O error
   */
  private static void assertAclFeature(Path pathToCheck,
      boolean expectAclFeature) throws IOException {
    INode inode = cluster.getNamesystem().getFSDirectory()
      .getNode(pathToCheck.toUri().getPath(), false);
    assertNotNull(inode);
    AclFeature aclFeature = inode.getAclFeature();
    if (expectAclFeature) {
      assertNotNull(aclFeature);
      // Intentionally capturing a reference to the entries, not using nested
      // calls.  This way, we get compile-time enforcement that the entries are
      // stored in an ImmutableList.
      ImmutableList<AclEntry> entries = aclFeature.getEntries();
      assertNotNull(entries);
      assertFalse(entries.isEmpty());
    } else {
      assertNull(aclFeature);
    }
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of the test path.
   *
   * @param perm short expected permission bits
   * @throws IOException thrown if there is an I/O error
   */
  private void assertPermission(short perm) throws IOException {
    assertPermission(path, perm);
  }

  /**
   * Asserts the value of the FsPermission bits on the inode of a specific path.
   *
   * @param pathToCheck Path inode to check
   * @param perm short expected permission bits
   * @throws IOException thrown if there is an I/O error
   */
  private void assertPermission(Path pathToCheck, short perm)
      throws IOException {
    AclTestHelpers.assertPermission(fs, pathToCheck, perm);
  }
}
