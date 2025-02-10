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

package org.apache.hadoop.tools.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.tools.ECAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.apache.hadoop.util.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDistCpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestDistCpUtils.class);

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;
  private static final FsPermission fullPerm = new FsPermission((short) 777);
  private static final FsPermission almostFullPerm = new FsPermission((short) 666);
  private static final FsPermission noPerm = new FsPermission((short) 0);
  
  @BeforeAll
  public static void create() throws IOException {
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(2)
        .format(true)
        .build();
    cluster.getFileSystem().enableErasureCodingPolicy("XOR-2-1-1024k");
  }

  @AfterAll
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetRelativePathRoot() {
    Path root = new Path("/");
    Path child = new Path("/a");
    assertThat(DistCpUtils.getRelativePath(root, child)).isEqualTo("/a");
  }

  @Test
  public void testGetRelativePath() {
    Path root = new Path("/tmp/abc");
    Path child = new Path("/tmp/abc/xyz/file");
    assertThat(DistCpUtils.getRelativePath(root, child)).isEqualTo("/xyz/file");
  }

  @Test
  public void testPackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("");

    attributes.add(FileAttribute.REPLICATION);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("R");

    attributes.add(FileAttribute.BLOCKSIZE);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("RB");

    attributes.add(FileAttribute.USER);
    attributes.add(FileAttribute.CHECKSUMTYPE);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("RBUC");

    attributes.add(FileAttribute.GROUP);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("RBUGC");

    attributes.add(FileAttribute.PERMISSION);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("RBUGPC");

    attributes.add(FileAttribute.TIMES);
    assertThat(DistCpUtils.packAttributes(attributes)).isEqualTo("RBUGPCT");
  }

  @Test
  public void testUnpackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    assertEquals(attributes, DistCpUtils.unpackAttributes("RCBUGPAXTE"));

    attributes.remove(FileAttribute.REPLICATION);
    attributes.remove(FileAttribute.CHECKSUMTYPE);
    attributes.remove(FileAttribute.ACL);
    attributes.remove(FileAttribute.XATTR);
    attributes.remove(FileAttribute.ERASURECODINGPOLICY);
    assertEquals(attributes, DistCpUtils.unpackAttributes("BUGPT"));

    attributes.remove(FileAttribute.TIMES);
    assertEquals(attributes, DistCpUtils.unpackAttributes("BUGP"));

    attributes.remove(FileAttribute.BLOCKSIZE);
    assertEquals(attributes, DistCpUtils.unpackAttributes("UGP"));

    attributes.remove(FileAttribute.GROUP);
    assertEquals(attributes, DistCpUtils.unpackAttributes("UP"));

    attributes.remove(FileAttribute.USER);
    assertEquals(attributes, DistCpUtils.unpackAttributes("P"));

    attributes.remove(FileAttribute.PERMISSION);
    assertEquals(attributes, DistCpUtils.unpackAttributes(""));
  }

  @Test
  public void testPreserveDefaults() throws IOException {
    FileSystem fs = FileSystem.get(config);
    
    // preserve replication, block size, user, group, permission, 
    // checksum type and timestamps    
    EnumSet<FileAttribute> attributes = 
        DistCpUtils.unpackAttributes(
            DistCpOptionSwitch.PRESERVE_STATUS_DEFAULT.substring(1));

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);
    
    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    assertStatusEqual(fs, dst, srcStatus);
  }

  private void assertStatusEqual(final FileSystem fs,
      final Path dst,
      final CopyListingFileStatus srcStatus) throws IOException {
    FileStatus destStatus = fs.getFileStatus(dst);
    CopyListingFileStatus dstStatus = new CopyListingFileStatus(
        destStatus);

    String text = String.format("Source %s; dest %s: wrong ", srcStatus,
        destStatus);

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertEquals(srcStatus.getPermission(), dstStatus.getPermission(),
        text + "permission");
    assertEquals(srcStatus.getOwner(), dstStatus.getOwner(),
        text + "owner");
    assertEquals(srcStatus.getGroup(), dstStatus.getGroup(), text + "group");
    assertEquals(srcStatus.getAccessTime(),
        dstStatus.getAccessTime(), text + "accessTime");
    assertEquals(srcStatus.getModificationTime(),
        dstStatus.getModificationTime(), text + "modificationTime");
    assertEquals(srcStatus.getReplication(),
        dstStatus.getReplication(), text + "replication");
  }

  private void assertStatusNotEqual(final FileSystem fs,
      final Path dst,
      final CopyListingFileStatus srcStatus) throws IOException {
    FileStatus destStatus = fs.getFileStatus(dst);
    CopyListingFileStatus dstStatus = new CopyListingFileStatus(
        destStatus);

    String text = String.format("Source %s; dest %s: wrong ",
        srcStatus, destStatus);
    // FileStatus.equals only compares path field,
    // must explicitly compare all fields
    assertNotEquals(srcStatus.getPermission(), dstStatus.getPermission(),
        text + "permission");
    assertNotEquals(srcStatus.getOwner(), dstStatus.getOwner(), text + "owner");
    assertNotEquals(srcStatus.getGroup(), dstStatus.getGroup(), text + "group");
    assertNotEquals(srcStatus.getAccessTime(), dstStatus.getAccessTime(),
        text + "accessTime");
    assertNotEquals(srcStatus.getModificationTime(),
        dstStatus.getModificationTime(), text + "modificationTime");
    assertNotEquals(srcStatus.getReplication(), dstStatus.getReplication(), text + "replication");
  }


  @Test
  public void testSkipsNeedlessAttributes() throws Exception {
    FileSystem fs = FileSystem.get(config);

    // preserve replication, block size, user, group, permission,
    // checksum type and timestamps

    Path src = new Path("/tmp/testSkipsNeedlessAttributes/source");
    Path dst = new Path("/tmp/testSkipsNeedlessAttributes/dest");

    // there is no need to actually create a source file, just a file
    // status of one
    CopyListingFileStatus srcStatus = new CopyListingFileStatus(
        new FileStatus(0, false, 1, 32, 0, src));

    // if an attribute is needed, preserve will fail to find the file
    EnumSet<FileAttribute> attrs = EnumSet.of(FileAttribute.ACL,
        FileAttribute.GROUP,
        FileAttribute.PERMISSION,
        FileAttribute.TIMES,
        FileAttribute.XATTR);
    for (FileAttribute attr : attrs) {
      intercept(FileNotFoundException.class, () ->
          DistCpUtils.preserve(fs, dst, srcStatus,
              EnumSet.of(attr),
              false));
    }

    // but with the preservation flags only used
    // in file creation, this does not happen
    DistCpUtils.preserve(fs, dst, srcStatus,
        EnumSet.of(
            FileAttribute.BLOCKSIZE,
            FileAttribute.CHECKSUMTYPE),
        false);
  }

  @Test
  public void testPreserveAclsforDefaultACL() throws IOException {
    FileSystem fs = FileSystem.get(config);

    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.ACL,
        FileAttribute.PERMISSION, FileAttribute.XATTR, FileAttribute.GROUP,
        FileAttribute.USER, FileAttribute.REPLICATION, FileAttribute.XATTR,
        FileAttribute.TIMES);

    Path dest = new Path("/tmpdest");
    Path src = new Path("/testsrc");

    fs.mkdirs(src);
    fs.mkdirs(dest);

    List<AclEntry> acls = Lists.newArrayList(
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, USER, READ_WRITE), aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, READ), aclEntry(ACCESS, USER, "bar", ALL));
    final List<AclEntry> acls1 = Lists.newArrayList(aclEntry(ACCESS, USER, ALL),
        aclEntry(ACCESS, USER, "user1", ALL),
        aclEntry(ACCESS, GROUP, READ_EXECUTE),
        aclEntry(ACCESS, OTHER, EXECUTE));

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);
    fs.setAcl(src, acls);

    fs.setPermission(dest, noPerm);
    fs.setOwner(dest, "nobody", "nobody-group");
    fs.setTimes(dest, 100, 100);
    fs.setReplication(dest, (short) 2);
    fs.setAcl(dest, acls1);

    List<AclEntry> en1 = fs.getAclStatus(src).getEntries();
    List<AclEntry> dd2 = fs.getAclStatus(dest).getEntries();

    assertNotEquals(en1, dd2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(
        fs.getFileStatus(src));

    en1 = srcStatus.getAclEntries();

    DistCpUtils.preserve(fs, dest, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(
        fs.getFileStatus(dest));

    dd2 = dstStatus.getAclEntries();
    en1 = srcStatus.getAclEntries();

    // FileStatus.equals only compares path field, must explicitly compare all
    // fields
    assertStatusEqual(fs, dest, srcStatus);

    assertArrayEquals(en1.toArray(), dd2.toArray());
  }

  @Test
  public void testPreserveNothingOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertTrue(dstStatus.getAccessTime() == 100);
    assertTrue(dstStatus.getModificationTime() == 100);
    assertTrue(dstStatus.getReplication() == 0);
  }

  @Test
  public void testPreservePermissionOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.PERMISSION);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveGroupOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.GROUP);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveUserOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.USER);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
  }

  @Test
  public void testPreserveReplicationOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.REPLICATION);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    // Replication shouldn't apply to dirs so this should still be 0 == 0
    assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveTimestampOnDirectory() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.TIMES);

    Path dst = new Path("/tmp/abc");
    Path src = new Path("/tmp/src");

    createDirectory(fs, src);
    createDirectory(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
  }

  @Test
  public void testPreserveNothingOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertStatusNotEqual(fs, dst, srcStatus);
  }

  @Test
  public void testPreservePermissionOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.PERMISSION);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveGroupOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.GROUP);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveUserOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.USER);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveReplicationOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.REPLICATION);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  @Timeout(value = 60)
  public void testReplFactorNotPreservedOnErasureCodedFile() throws Exception {
    FileSystem fs = FileSystem.get(config);

    // Case 1: Verify replication attribute not preserved when the source
    // file is erasure coded and the target file is replicated.
    Path srcECDir = new Path("/tmp/srcECDir");
    Path srcECFile = new Path(srcECDir, "srcECFile");
    Path dstReplDir = new Path("/tmp/dstReplDir");
    Path dstReplFile = new Path(dstReplDir, "destReplFile");
    fs.mkdirs(srcECDir);
    fs.mkdirs(dstReplDir);
    String[] args = {"-setPolicy", "-path", "/tmp/srcECDir",
        "-policy", "XOR-2-1-1024k"};
    int res = ToolRunner.run(config, new ECAdmin(config), args);
    assertEquals(0, res, "Setting EC policy should succeed!");
    verifyReplFactorNotPreservedOnErasureCodedFile(srcECFile, true,
        dstReplFile, false);

    // Case 2: Verify replication attribute not preserved when the source
    // file is replicated and the target file is erasure coded.
    Path srcReplDir = new Path("/tmp/srcReplDir");
    Path srcReplFile = new Path(srcReplDir, "srcReplFile");
    Path dstECDir = new Path("/tmp/dstECDir");
    Path dstECFile = new Path(dstECDir, "destECFile");
    fs.mkdirs(srcReplDir);
    fs.mkdirs(dstECDir);
    args = new String[]{"-setPolicy", "-path", "/tmp/dstECDir",
        "-policy", "XOR-2-1-1024k"};
    res = ToolRunner.run(config, new ECAdmin(config), args);
    assertEquals(0, res, "Setting EC policy should succeed!");
    verifyReplFactorNotPreservedOnErasureCodedFile(srcReplFile,
        false, dstECFile, true);

    // Case 3: Verify replication attribute not altered from the default
    // INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS when both source and
    // target files are erasure coded.
    verifyReplFactorNotPreservedOnErasureCodedFile(srcECFile,
        true, dstECFile, true);
  }

  private void verifyReplFactorNotPreservedOnErasureCodedFile(Path srcFile,
      boolean isSrcEC, Path dstFile, boolean isDstEC) throws Exception {
    FileSystem fs = FileSystem.get(config);
    createFile(fs, srcFile);
    CopyListingFileStatus srcStatus = new CopyListingFileStatus(
        fs.getFileStatus(srcFile));
    if (isSrcEC) {
      assertTrue(srcStatus.isErasureCoded(), srcFile + "should be erasure coded!");
      assertEquals(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          srcStatus.getReplication());
    } else {
      assertEquals(fs.getDefaultReplication(srcFile), srcStatus.getReplication(),
          "Unexpected replication factor for " + srcFile);
    }

    createFile(fs, dstFile);
    CopyListingFileStatus dstStatus = new CopyListingFileStatus(
        fs.getFileStatus(dstFile));
    if (isDstEC) {
      assertTrue(dstStatus.isErasureCoded(), dstFile + "should be erasure coded!");
      assertEquals(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          dstStatus.getReplication(), "Unexpected replication factor for erasure coded file!");
    } else {
      assertEquals(fs.getDefaultReplication(dstFile), dstStatus.getReplication(),
          "Unexpected replication factor for " + dstFile);
    }

    // Let srcFile and dstFile differ on their FileAttribute
    fs.setPermission(srcFile, fullPerm);
    fs.setOwner(srcFile, "ec", "ec-group");
    fs.setTimes(srcFile, 0, 0);

    fs.setPermission(dstFile, noPerm);
    fs.setOwner(dstFile, "normal", "normal-group");
    fs.setTimes(dstFile, 100, 100);

    // Running preserve operations only for replication attribute
    srcStatus = new CopyListingFileStatus(fs.getFileStatus(srcFile));
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.REPLICATION);
    DistCpUtils.preserve(fs, dstFile, srcStatus, attributes, false);
    dstStatus = new CopyListingFileStatus(fs.getFileStatus(dstFile));

    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()),
        "Permission for " + srcFile + " and " + dstFile +
        " should not be same after preserve only for replication attr!");
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()),
        "File ownership should not match!");
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(
        srcStatus.getModificationTime() == dstStatus.getModificationTime());
    if (isDstEC) {
      assertEquals(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          dstStatus.getReplication(), "Unexpected replication factor for erasure coded file!");
    } else {
      assertEquals(fs.getDefaultReplication(dstFile),
          dstStatus.getReplication(), dstFile + " replication factor should be same as dst " +
          "filesystem!");
    }
    if (!isSrcEC || !isDstEC) {
      assertFalse(srcStatus.getReplication() == dstStatus.getReplication(),
          dstFile + " replication factor should not be " +
          "same as " + srcFile);
    }
  }

  @Test
  public void testPreserveTimestampOnFile() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.of(FileAttribute.TIMES);

    Path dst = new Path("/tmp/dest2");
    Path src = new Path("/tmp/src2");

    createFile(fs, src);
    createFile(fs, dst);

    fs.setPermission(src, fullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(dst, noPerm);
    fs.setOwner(dst, "nobody", "nobody-group");
    fs.setTimes(dst, 100, 100);
    fs.setReplication(dst, (short) 2);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, dst, srcStatus, attributes, false);

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test
  public void testPreserveOnFileUpwardRecursion() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    // Remove ACL because tests run with dfs.namenode.acls.enabled false
    attributes.remove(FileAttribute.ACL);
    
    Path src = new Path("/tmp/src2");
    Path f0 = new Path("/f0");
    Path f1 = new Path("/d1/f1");
    Path f2 = new Path("/d1/d2/f2");
    Path d1 = new Path("/d1/");
    Path d2 = new Path("/d1/d2/");

    createFile(fs, src);
    createFile(fs, f0);
    createFile(fs, f1);
    createFile(fs, f2);

    fs.setPermission(src, almostFullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(d1, fullPerm);
    fs.setOwner(d1, "anybody", "anybody-group");
    fs.setTimes(d1, 400, 400);
    fs.setReplication(d1, (short) 3);

    fs.setPermission(d2, fullPerm);
    fs.setOwner(d2, "anybody", "anybody-group");
    fs.setTimes(d2, 300, 300);
    fs.setReplication(d2, (short) 3);

    fs.setPermission(f0, fullPerm);
    fs.setOwner(f0, "anybody", "anybody-group");
    fs.setTimes(f0, 200, 200);
    fs.setReplication(f0, (short) 3);

    fs.setPermission(f1, fullPerm);
    fs.setOwner(f1, "anybody", "anybody-group");
    fs.setTimes(f1, 200, 200);
    fs.setReplication(f1, (short) 3);

    fs.setPermission(f2, fullPerm);
    fs.setOwner(f2, "anybody", "anybody-group");
    fs.setTimes(f2, 200, 200);
    fs.setReplication(f2, (short) 3);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, f2, srcStatus, attributes, false);

    cluster.triggerHeartbeats();

    // FileStatus.equals only compares path field, must explicitly compare all fields
    // attributes of src -> f2 ? should be yes
    assertStatusEqual(fs, f2, srcStatus);

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f0Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    assertTrue(d2Status.getAccessTime() == 300);
    assertTrue(d2Status.getModificationTime() == 300);
    assertFalse(srcStatus.getReplication() == d2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    assertTrue(d1Status.getAccessTime() == 400);
    assertTrue(d1Status.getModificationTime() == 400);
    assertFalse(srcStatus.getReplication() == d1Status.getReplication());
  }

  @Test
  public void testPreserveOnDirectoryUpwardRecursion() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    
    // Remove ACL because tests run with dfs.namenode.acls.enabled false
    attributes.remove(FileAttribute.ACL);

    Path src = new Path("/tmp/src2");
    Path f0 = new Path("/f0");
    Path f1 = new Path("/d1/f1");
    Path f2 = new Path("/d1/d2/f2");
    Path d1 = new Path("/d1/");
    Path d2 = new Path("/d1/d2/");

    createFile(fs, src);
    createFile(fs, f0);
    createFile(fs, f1);
    createFile(fs, f2);

    fs.setPermission(src, almostFullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(d1, fullPerm);
    fs.setOwner(d1, "anybody", "anybody-group");
    fs.setTimes(d1, 400, 400);
    fs.setReplication(d1, (short) 3);

    fs.setPermission(d2, fullPerm);
    fs.setOwner(d2, "anybody", "anybody-group");
    fs.setTimes(d2, 300, 300);
    fs.setReplication(d2, (short) 3);

    fs.setPermission(f0, fullPerm);
    fs.setOwner(f0, "anybody", "anybody-group");
    fs.setTimes(f0, 200, 200);
    fs.setReplication(f0, (short) 3);

    fs.setPermission(f1, fullPerm);
    fs.setOwner(f1, "anybody", "anybody-group");
    fs.setTimes(f1, 200, 200);
    fs.setReplication(f1, (short) 3);

    fs.setPermission(f2, fullPerm);
    fs.setOwner(f2, "anybody", "anybody-group");
    fs.setTimes(f2, 200, 200);
    fs.setReplication(f2, (short) 3);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, d2, srcStatus, attributes, false);

    cluster.triggerHeartbeats();

    // FileStatus.equals only compares path field, must explicitly compare all fields
    // attributes of src -> d2 ? should be yes
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    assertTrue(srcStatus.getPermission().equals(d2Status.getPermission()));
    assertTrue(srcStatus.getOwner().equals(d2Status.getOwner()));
    assertTrue(srcStatus.getGroup().equals(d2Status.getGroup()));
    assertTrue(srcStatus.getAccessTime() == d2Status.getAccessTime());
    assertTrue(srcStatus.getModificationTime() == d2Status.getModificationTime());
    assertTrue(srcStatus.getReplication() != d2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == d1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == d1Status.getModificationTime());
    assertTrue(srcStatus.getReplication() != d1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f2Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f0Status.getReplication());
  }

  @Test
  public void testPreserveOnFileDownwardRecursion() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    // Remove ACL because tests run with dfs.namenode.acls.enabled false
    attributes.remove(FileAttribute.ACL);

    Path src = new Path("/tmp/src2");
    Path f0 = new Path("/f0");
    Path f1 = new Path("/d1/f1");
    Path f2 = new Path("/d1/d2/f2");
    Path d1 = new Path("/d1/");
    Path d2 = new Path("/d1/d2/");

    createFile(fs, src);
    createFile(fs, f0);
    createFile(fs, f1);
    createFile(fs, f2);

    fs.setPermission(src, almostFullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(d1, fullPerm);
    fs.setOwner(d1, "anybody", "anybody-group");
    fs.setTimes(d1, 400, 400);
    fs.setReplication(d1, (short) 3);

    fs.setPermission(d2, fullPerm);
    fs.setOwner(d2, "anybody", "anybody-group");
    fs.setTimes(d2, 300, 300);
    fs.setReplication(d2, (short) 3);

    fs.setPermission(f0, fullPerm);
    fs.setOwner(f0, "anybody", "anybody-group");
    fs.setTimes(f0, 200, 200);
    fs.setReplication(f0, (short) 3);

    fs.setPermission(f1, fullPerm);
    fs.setOwner(f1, "anybody", "anybody-group");
    fs.setTimes(f1, 200, 200);
    fs.setReplication(f1, (short) 3);

    fs.setPermission(f2, fullPerm);
    fs.setOwner(f2, "anybody", "anybody-group");
    fs.setTimes(f2, 200, 200);
    fs.setReplication(f2, (short) 3);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, f0, srcStatus, attributes, false);

    cluster.triggerHeartbeats();

    // FileStatus.equals only compares path field, must explicitly compare all fields
    // attributes of src -> f0 ? should be yes
    assertStatusEqual(fs, f0, srcStatus);

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    assertTrue(d1Status.getAccessTime() == 400);
    assertTrue(d1Status.getModificationTime() == 400);
    assertFalse(srcStatus.getReplication() == d1Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    assertTrue(d2Status.getAccessTime() == 300);
    assertTrue(d2Status.getModificationTime() == 300);
    assertFalse(srcStatus.getReplication() == d2Status.getReplication());
  }

  @Test
  public void testPreserveOnDirectoryDownwardRecursion() throws IOException {
    FileSystem fs = FileSystem.get(config);
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    // Remove ACL because tests run with dfs.namenode.acls.enabled false
    attributes.remove(FileAttribute.ACL);

    Path src = new Path("/tmp/src2");
    Path f0 = new Path("/f0");
    Path f1 = new Path("/d1/f1");
    Path f2 = new Path("/d1/d2/f2");
    Path d1 = new Path("/d1/");
    Path d2 = new Path("/d1/d2/");
    Path root = new Path("/");

    createFile(fs, src);
    createFile(fs, f0);
    createFile(fs, f1);
    createFile(fs, f2);

    fs.setPermission(src, almostFullPerm);
    fs.setOwner(src, "somebody", "somebody-group");
    fs.setTimes(src, 0, 0);
    fs.setReplication(src, (short) 1);

    fs.setPermission(root, fullPerm);
    fs.setOwner(root, "anybody", "anybody-group");
    fs.setTimes(root, 400, 400);
    fs.setReplication(root, (short) 3);

    fs.setPermission(d1, fullPerm);
    fs.setOwner(d1, "anybody", "anybody-group");
    fs.setTimes(d1, 400, 400);
    fs.setReplication(d1, (short) 3);

    fs.setPermission(d2, fullPerm);
    fs.setOwner(d2, "anybody", "anybody-group");
    fs.setTimes(d2, 300, 300);
    fs.setReplication(d2, (short) 3);

    fs.setPermission(f0, fullPerm);
    fs.setOwner(f0, "anybody", "anybody-group");
    fs.setTimes(f0, 200, 200);
    fs.setReplication(f0, (short) 3);

    fs.setPermission(f1, fullPerm);
    fs.setOwner(f1, "anybody", "anybody-group");
    fs.setTimes(f1, 200, 200);
    fs.setReplication(f1, (short) 3);

    fs.setPermission(f2, fullPerm);
    fs.setOwner(f2, "anybody", "anybody-group");
    fs.setTimes(f2, 200, 200);
    fs.setReplication(f2, (short) 3);

    CopyListingFileStatus srcStatus = new CopyListingFileStatus(fs.getFileStatus(src));

    DistCpUtils.preserve(fs, root, srcStatus, attributes, false);

    cluster.triggerHeartbeats();

    // FileStatus.equals only compares path field, must explicitly compare all fields
    // attributes of src -> root ? should be yes
    CopyListingFileStatus rootStatus = new CopyListingFileStatus(fs.getFileStatus(root));
    assertTrue(srcStatus.getPermission().equals(rootStatus.getPermission()));
    assertTrue(srcStatus.getOwner().equals(rootStatus.getOwner()));
    assertTrue(srcStatus.getGroup().equals(rootStatus.getGroup()));
    assertTrue(srcStatus.getAccessTime() == rootStatus.getAccessTime());
    assertTrue(srcStatus.getModificationTime() == rootStatus.getModificationTime());
    assertTrue(srcStatus.getReplication() != rootStatus.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == d1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == d1Status.getModificationTime());
    assertTrue(srcStatus.getReplication() != d1Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == d2Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == d2Status.getModificationTime());
    assertTrue(srcStatus.getReplication() != d2Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f0Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    assertFalse(srcStatus.getReplication() == f2Status.getReplication());
  }

  @Test
  public void testCompareFileLengthsAndChecksums() throws Throwable {

    String base = "/tmp/verify-checksum/";
    long srcSeed = System.currentTimeMillis();
    long dstSeed = srcSeed + rand.nextLong();
    short replFactor = 2;

    FileSystem fs = FileSystem.get(config);
    Path basePath = new Path(base);
    fs.mkdirs(basePath);

    // empty lengths comparison
    Path srcWithLen0 = new Path(base + "srcLen0");
    Path dstWithLen0 = new Path(base + "dstLen0");
    fs.create(srcWithLen0).close();
    fs.create(dstWithLen0).close();
    DistCpUtils.compareFileLengthsAndChecksums(0, fs, srcWithLen0,
        null, fs, dstWithLen0, false, 0);

    // different lengths comparison
    Path srcWithLen1 = new Path(base + "srcLen1");
    Path dstWithLen2 = new Path(base + "dstLen2");
    DFSTestUtil.createFile(fs, srcWithLen1, 1, replFactor, srcSeed);
    DFSTestUtil.createFile(fs, dstWithLen2, 2, replFactor, srcSeed);

    intercept(IOException.class, DistCpConstants.LENGTH_MISMATCH_ERROR_MSG,
        () -> DistCpUtils.compareFileLengthsAndChecksums(1, fs,
                srcWithLen1, null, fs, dstWithLen2, false, 2));

    // checksums matched
    Path srcWithChecksum1 = new Path(base + "srcChecksum1");
    Path dstWithChecksum1 = new Path(base + "dstChecksum1");
    DFSTestUtil.createFile(fs, srcWithChecksum1, 1024,
        replFactor, srcSeed);
    DFSTestUtil.createFile(fs, dstWithChecksum1, 1024,
        replFactor, srcSeed);
    DistCpUtils.compareFileLengthsAndChecksums(1024, fs, srcWithChecksum1,
        null, fs, dstWithChecksum1, false, 1024);
    DistCpUtils.compareFileLengthsAndChecksums(1024, fs, srcWithChecksum1,
        fs.getFileChecksum(srcWithChecksum1), fs, dstWithChecksum1,
        false, 1024);

    // checksums mismatched
    Path dstWithChecksum2 = new Path(base + "dstChecksum2");
    DFSTestUtil.createFile(fs, dstWithChecksum2, 1024,
        replFactor, dstSeed);
    intercept(IOException.class, DistCpConstants.CHECKSUM_MISMATCH_ERROR_MSG,
        () -> DistCpUtils.compareFileLengthsAndChecksums(1024, fs,
               srcWithChecksum1, null, fs, dstWithChecksum2,
               false, 1024));

    // checksums mismatched but skipped
    DistCpUtils.compareFileLengthsAndChecksums(1024, fs, srcWithChecksum1,
        null, fs, dstWithChecksum2, true, 1024);
  }

  private static Random rand = new Random();

  public static String createTestSetup(FileSystem fs) throws IOException {
    return createTestSetup("/tmp1", fs, FsPermission.getDefault());
  }
  
  public static String createTestSetup(FileSystem fs,
                                       FsPermission perm) throws IOException {
    return createTestSetup("/tmp1", fs, perm);
  }

  public static String createTestSetup(String baseDir,
                                       FileSystem fs,
                                       FsPermission perm) throws IOException {
    String base = getBase(baseDir);
    fs.mkdirs(new Path(base + "/newTest/hello/world1"));
    fs.mkdirs(new Path(base + "/newTest/hello/world2/newworld"));
    fs.mkdirs(new Path(base + "/newTest/hello/world3/oldworld"));
    fs.setPermission(new Path(base + "/newTest"), perm);
    fs.setPermission(new Path(base + "/newTest/hello"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world1"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2/newworld"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3/oldworld"), perm);
    createFile(fs, new Path(base, "/newTest/1"));
    createFile(fs, new Path(base, "/newTest/hello/2"));
    createFile(fs, new Path(base, "/newTest/hello/world3/oldworld/3"));
    createFile(fs, new Path(base, "/newTest/hello/world2/4"));
    return base;
  }

  private static String getBase(String base) {
    String location = String.valueOf(rand.nextLong());
    return base + "/" + location;
  }

  public static String createTestSetupWithOnlyFile(FileSystem fs,
      FsPermission perm) throws IOException {
    String location = String.valueOf(rand.nextLong());
    fs.mkdirs(new Path("/tmp1/" + location));
    fs.setPermission(new Path("/tmp1/" + location), perm);
    createFile(fs, new Path("/tmp1/" + location + "/file"));
    return "/tmp1/" + location + "/file";
  }

  public static void delete(FileSystem fs, String path) {
    try {
      if (fs != null) {
        if (path != null) {
          fs.delete(new Path(path), true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception encountered ", e);
    }
  }
  
  public static void createFile(FileSystem fs, String filePath) throws IOException {
    Path path = new Path(filePath);
    createFile(fs, path);
  }

  /** Creates a new, empty file at filePath and always overwrites */
  public static void createFile(FileSystem fs, Path filePath) throws IOException {
    OutputStream out = fs.create(filePath, true);
    IOUtils.closeStream(out);
  }

  /** Creates a new, empty directory at dirPath and always overwrites */
  public static void createDirectory(FileSystem fs, Path dirPath) throws IOException {
    fs.delete(dirPath, true);
    boolean created = fs.mkdirs(dirPath);
    if (!created) {
      LOG.warn("Could not create directory " + dirPath + " this might cause test failures.");
    }
  }

  public static void verifyFoldersAreInSync(FileSystem fs, String targetBase,
      String sourceBase) throws IOException {
    Path base = new Path(targetBase);

    Stack<Path> stack = new Stack<>();
    stack.push(base);
    while (!stack.isEmpty()) {
      Path file = stack.pop();
      if (!fs.exists(file)) {
        continue;
      }
      FileStatus[] fStatus = fs.listStatus(file);
      if (fStatus == null || fStatus.length == 0) {
        continue;
      }

      for (FileStatus status : fStatus) {
        if (status.isDirectory()) {
          stack.push(status.getPath());
        }
        Path p = new Path(sourceBase + "/" +
            DistCpUtils.getRelativePath(new Path(targetBase),
                status.getPath()));
        ContractTestUtils.assertPathExists(fs,
            "path in sync with " + status.getPath(), p);
      }
    }
  }

}
