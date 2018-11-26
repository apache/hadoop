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

package org.apache.hadoop.fs.azurebfs;

import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assume.assumeTrue;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;

/**
 * Test acl operations.
 */
public class ITestAzureBlobFilesystemAcl extends AbstractAbfsIntegrationTest {
  private static final FsAction ALL = FsAction.ALL;
  private static final FsAction NONE = FsAction.NONE;
  private static final FsAction READ = FsAction.READ;
  private static final FsAction EXECUTE = FsAction.EXECUTE;
  private static final FsAction READ_EXECUTE = FsAction.READ_EXECUTE;
  private static final FsAction READ_WRITE = FsAction.READ_WRITE;

  private static final short RW = 0600;
  private static final short RWX = 0700;
  private static final short RWX_R = 0740;
  private static final short RWX_RW = 0760;
  private static final short RWX_RWX = 0770;
  private static final short RWX_RX = 0750;
  private static final short RWX_RX_RX = 0755;
  private static final short RW_R = 0640;
  private static final short RW_X = 0610;
  private static final short RW_RW = 0660;
  private static final short RW_RWX = 0670;
  private static final short RW_R_R = 0644;
  private static final short STICKY_RWX_RWX = 01770;

  private static final String FOO = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();
  private static final String TEST_OWNER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();

  private static Path testRoot = new Path("/test");
  private Path path;

  public ITestAzureBlobFilesystemAcl() throws Exception {
    super();
  }

  @Test
  public void testModifyAclEntries() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.mkdirs(path, FsPermission.createImmutable((short) RWX_RX));

    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);

    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE),
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);

    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testModifyAclEntriesOnlyAccess() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testModifyAclEntriesOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testModifyAclEntriesMinimal() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, READ_WRITE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission(fs, (short) RW_RW);
  }

  @Test
  public void testModifyAclEntriesMinimalDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
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
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testModifyAclEntriesCustomMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, MASK, NONE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission(fs, (short) RW);
  }

  @Test
  public void testModifyAclEntriesStickyBit() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE),
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE));
    fs.modifyAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) 01750);
  }

  @Test(expected=FileNotFoundException.class)
  public void testModifyAclEntriesPathNotFound() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE));
    fs.modifyAclEntries(path, aclSpec);
  }

  @Test (expected=Exception.class)
  public void testModifyAclEntriesDefaultOnFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.modifyAclEntries(path, aclSpec);
  }

  @Test
  public void testModifyAclEntriesWithDefaultMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);

    List<AclEntry> modifyAclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, READ_WRITE));
    fs.modifyAclEntries(path, modifyAclSpec);

    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE)}, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testModifyAclEntriesWithAccessMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);

    List<AclEntry> modifyAclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE));
    fs.modifyAclEntries(path, modifyAclSpec);

    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, GROUP, READ_EXECUTE)}, returned);
    assertPermission(fs, (short) RW_X);
  }

  @Test(expected=PathIOException.class)
  public void testModifyAclEntriesWithDuplicateEntries() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);

    List<AclEntry> modifyAclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, READ_WRITE));
    fs.modifyAclEntries(path, modifyAclSpec);
  }

  @Test
  public void testRemoveAclEntries() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO),
        aclEntry(DEFAULT, USER, FOO));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveAclEntriesOnlyAccess() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, USER, BAR, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_WRITE),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, BAR, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ_WRITE) }, returned);
    assertPermission(fs, (short) RWX_RW);
  }

  @Test
  public void testRemoveAclEntriesOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, USER, BAR, READ_EXECUTE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, BAR, READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveAclEntriesMinimal() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RWX_RW));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_WRITE),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO),
        aclEntry(ACCESS, MASK));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RWX_RW);
  }

  @Test
  public void testRemoveAclEntriesMinimalDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO),
        aclEntry(ACCESS, MASK),
        aclEntry(DEFAULT, USER, FOO),
        aclEntry(DEFAULT, MASK));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveAclEntriesStickyBit() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO),
        aclEntry(DEFAULT, USER, FOO));
    fs.removeAclEntries(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) 01750);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclEntriesPathNotFound() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO));
    fs.removeAclEntries(path, aclSpec);
  }

  @Test(expected=PathIOException.class)
  public void testRemoveAclEntriesAccessMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, MASK, EXECUTE),
        aclEntry(ACCESS, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);

    fs.removeAclEntries(path, Lists.newArrayList(aclEntry(ACCESS, MASK, NONE)));
  }

  @Test(expected=PathIOException.class)
  public void testRemoveAclEntriesDefaultMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, MASK, EXECUTE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);

    fs.removeAclEntries(path, Lists.newArrayList(aclEntry(DEFAULT, MASK, NONE)));
  }

  @Test(expected=PathIOException.class)
  public void testRemoveAclEntriesWithDuplicateEntries() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);

    List<AclEntry> removeAclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, READ_WRITE));
    fs.removeAclEntries(path, removeAclSpec);
  }

  @Test
  public void testRemoveDefaultAcl() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, (short) RWX_RWX);
  }

  @Test
  public void testRemoveDefaultAclOnlyAccess() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, (short) RWX_RWX);
  }

  @Test
  public void testRemoveDefaultAclOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveDefaultAclMinimal() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveDefaultAclStickyBit() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.removeDefaultAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, (short) STICKY_RWX_RWX);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveDefaultAclPathNotFound() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    // Path has not been created.
    fs.removeDefaultAcl(path);
  }

  @Test
  public void testRemoveAcl() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));

    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);

    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testRemoveAclMinimalAcl() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RW_R);
  }

  @Test
  public void testRemoveAclStickyBit() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) 01750);
  }

  @Test
  public void testRemoveAclOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.removeAcl(path);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test(expected=FileNotFoundException.class)
  public void testRemoveAclPathNotFound() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    // Path has not been created.
    fs.removeAcl(path);
  }

  @Test
  public void testSetAcl() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RWX);
  }

  @Test
  public void testSetAclOnlyAccess() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission(fs, (short) RW_R);
  }

  @Test
  public void testSetAclOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testSetAclMinimal() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, FOO, READ),
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
    assertPermission(fs, (short) RW_R);
  }

  @Test
  public void testSetAclMinimalDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
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
    assertPermission(fs, (short) RWX_RX);
  }

  @Test
  public void testSetAclCustomMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, MASK, ALL),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission(fs, (short) RW_RWX);
  }

  @Test
  public void testSetAclStickyBit() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 01750));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) STICKY_RWX_RWX);
  }

  @Test(expected=FileNotFoundException.class)
  public void testSetAclPathNotFound() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    // Path has not been created.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
  }

  @Test(expected=Exception.class)
  public void testSetAclDefaultOnFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
  }

  @Test
  public void testSetAclDoesNotChangeDefaultMask() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);
    // only change access acl, and default mask should not change.
    List<AclEntry> aclSpec2 = Lists.newArrayList(
        aclEntry(ACCESS, OTHER, READ_EXECUTE));
    fs.setAcl(path, aclSpec2);
    // get acl status and check result.
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, EXECUTE),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX_RX_RX);
  }

  @Test(expected=PathIOException.class)
  public void testSetAclWithDuplicateEntries() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, MASK, EXECUTE),
        aclEntry(ACCESS, MASK, EXECUTE));
    fs.setAcl(path, aclSpec);
  }

  @Test
  public void testSetPermission() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short) RWX));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX);
  }

  @Test
  public void testSetPermissionOnlyAccess() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    fs.create(path).close();
    fs.setPermission(path, FsPermission.createImmutable((short) RW_R));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, NONE));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short) RW));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, READ),
        aclEntry(ACCESS, GROUP, READ) }, returned);
    assertPermission(fs, (short) RW);
  }

  @Test
  public void testSetPermissionOnlyDefault() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, NONE),
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    fs.setPermission(path, FsPermission.createImmutable((short) RWX));
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, (short) RWX);
  }

  @Test
  public void testDefaultAclNewFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, filePath, (short) RW_R);
  }

  @Test
  @Ignore // wait umask fix to be deployed
  public void testOnlyAccessAclNewFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, filePath, (short) RW_R_R);
  }

  @Test
  public void testDefaultMinimalAclNewFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
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
    assertPermission(fs, filePath, (short) RW_R);
  }

  @Test
  public void testDefaultAclNewDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);

    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);

    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, NONE) }, returned);
    assertPermission(fs, dirPath, (short) RWX_RWX);
  }

  @Test
  public void testOnlyAccessAclNewDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FOO, ALL));
    fs.modifyAclEntries(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath);
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] { }, returned);
    assertPermission(fs, dirPath, (short) RWX_RX_RX);
  }

  @Test
  public void testDefaultMinimalAclNewDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX));
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
    assertPermission(fs, dirPath, (short) RWX_RX);
  }

  @Test
  public void testDefaultAclNewFileWithMode() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    Path filePath = new Path(path, "file1");
    int bufferSize =  4 * 1024 * 1024;
    fs.create(filePath, new FsPermission((short) RWX_R), false, bufferSize,
        fs.getDefaultReplication(filePath), fs.getDefaultBlockSize(path), null)
        .close();
    AclStatus s = fs.getAclStatus(filePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE) }, returned);
    assertPermission(fs, filePath, (short) RWX_R);
  }

  @Test
  public void testDefaultAclNewDirWithMode() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) RWX_RX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(path, aclSpec);
    Path dirPath = new Path(path, "dir1");
    fs.mkdirs(dirPath, new FsPermission((short) RWX_R));
    AclStatus s = fs.getAclStatus(dirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, FOO, ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, USER, ALL),
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(DEFAULT, GROUP, READ_EXECUTE),
        aclEntry(DEFAULT, MASK, ALL),
        aclEntry(DEFAULT, OTHER, READ_EXECUTE) }, returned);
    assertPermission(fs, dirPath, (short) RWX_R);
  }

  @Test
  public void testDefaultAclRenamedFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    Path dirPath = new Path(path, "dir");
    FileSystem.mkdirs(fs, dirPath, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(dirPath, aclSpec);
    Path filePath = new Path(path, "file1");
    fs.create(filePath).close();
    fs.setPermission(filePath, FsPermission.createImmutable((short) RW_R));
    Path renamedFilePath = new Path(dirPath, "file1");
    fs.rename(filePath, renamedFilePath);
    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = fs.getAclStatus(renamedFilePath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(fs, renamedFilePath, (short) RW_R);
  }

  @Test
  public void testDefaultAclRenamedDir() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());
    path = new Path(testRoot, UUID.randomUUID().toString());
    Path dirPath = new Path(path, "dir");
    FileSystem.mkdirs(fs, dirPath, FsPermission.createImmutable((short) RWX_RX));
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL));
    fs.setAcl(dirPath, aclSpec);
    Path subdirPath = new Path(path, "subdir");
    FileSystem.mkdirs(fs, subdirPath, FsPermission.createImmutable((short) RWX_RX));
    Path renamedSubdirPath = new Path(dirPath, "subdir");
    fs.rename(subdirPath, renamedSubdirPath);
    AclEntry[] expected = new AclEntry[] { };
    AclStatus s = fs.getAclStatus(renamedSubdirPath);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(expected, returned);
    assertPermission(fs, renamedSubdirPath, (short) RWX_RX);
  }

  @Test
  public void testEnsureAclOperationWorksForRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeTrue(fs.getIsNamespaceEnabled());

    Path rootPath = new Path("/");

    List<AclEntry> aclSpec1 = Lists.newArrayList(
        aclEntry(DEFAULT, GROUP, FOO, ALL),
        aclEntry(ACCESS, GROUP, BAR, ALL));
    fs.setAcl(rootPath, aclSpec1);
    fs.getAclStatus(rootPath);

    fs.setOwner(rootPath, TEST_OWNER, TEST_GROUP);
    fs.setPermission(rootPath, new FsPermission("777"));

    List<AclEntry> aclSpec2 = Lists.newArrayList(
        aclEntry(DEFAULT, USER, FOO, ALL),
        aclEntry(ACCESS, USER, BAR, ALL));
    fs.modifyAclEntries(rootPath, aclSpec2);
    fs.removeAclEntries(rootPath, aclSpec2);
    fs.removeDefaultAcl(rootPath);
    fs.removeAcl(rootPath);
  }

  @Test
  public void testSetOwnerForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);

    assertTrue(fs.exists(filePath));

    FileStatus oldFileStatus = fs.getFileStatus(filePath);
    fs.setOwner(filePath, TEST_OWNER, TEST_GROUP);
    FileStatus newFileStatus = fs.getFileStatus(filePath);

    assertEquals(oldFileStatus.getOwner(), newFileStatus.getOwner());
    assertEquals(oldFileStatus.getGroup(), newFileStatus.getGroup());
  }

  @Test
  public void testSetPermissionForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);

    assertTrue(fs.exists(filePath));
    FsPermission oldPermission = fs.getFileStatus(filePath).getPermission();
    // default permission for non-namespace enabled account is "777"
    FsPermission newPermission = new FsPermission("557");

    assertNotEquals(oldPermission, newPermission);

    fs.setPermission(filePath, newPermission);
    FsPermission updatedPermission = fs.getFileStatus(filePath).getPermission();
    assertEquals(oldPermission, updatedPermission);
  }

  @Test
  public void testModifyAclEntriesForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      List<AclEntry> aclSpec = Lists.newArrayList(
              aclEntry(DEFAULT, GROUP, FOO, ALL),
              aclEntry(ACCESS, GROUP, BAR, ALL));
      fs.modifyAclEntries(filePath, aclSpec);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  @Test
  public void testRemoveAclEntriesEntriesForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      List<AclEntry> aclSpec = Lists.newArrayList(
              aclEntry(DEFAULT, GROUP, FOO, ALL),
              aclEntry(ACCESS, GROUP, BAR, ALL));
      fs.removeAclEntries(filePath, aclSpec);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  @Test
  public void testRemoveDefaultAclForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      fs.removeDefaultAcl(filePath);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  @Test
  public void testRemoveAclForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      fs.removeAcl(filePath);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  @Test
  public void testSetAclForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      List<AclEntry> aclSpec = Lists.newArrayList(
              aclEntry(DEFAULT, GROUP, FOO, ALL),
              aclEntry(ACCESS, GROUP, BAR, ALL));
      fs.setAcl(filePath, aclSpec);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  @Test
  public void testGetAclStatusForNonNamespaceEnabledAccount() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Assume.assumeTrue(!fs.getIsNamespaceEnabled());
    final Path filePath = new Path(methodName.getMethodName());
    fs.create(filePath);
    try {
      AclStatus aclSpec = fs.getAclStatus(filePath);
      assertFalse("UnsupportedOperationException is expected", false);
    } catch (UnsupportedOperationException ex) {
      //no-op
    }
  }

  private void assertPermission(FileSystem fs, short perm) throws Exception {
    assertPermission(fs, path, perm);
  }

  private void assertPermission(FileSystem fs, Path pathToCheck, short perm)
      throws Exception {
    AclTestHelpers.assertPermission(fs, pathToCheck, perm);
  }
}
