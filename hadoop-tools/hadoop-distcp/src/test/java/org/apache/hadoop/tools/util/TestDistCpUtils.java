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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.tools.ECAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDistCpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestDistCpUtils.class);

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;
  private static final FsPermission fullPerm = new FsPermission((short) 777);
  private static final FsPermission almostFullPerm = new FsPermission((short) 666);
  private static final FsPermission noPerm = new FsPermission((short) 0);
  
  @BeforeClass
  public static void create() throws IOException {
    config.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(2)
        .format(true)
        .build();
    cluster.getFileSystem().enableErasureCodingPolicy("XOR-2-1-1024k");
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetRelativePathRoot() {
    Path root = new Path("/");
    Path child = new Path("/a");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/a");
  }

  @Test
  public void testGetRelativePath() {
    Path root = new Path("/tmp/abc");
    Path child = new Path("/tmp/abc/xyz/file");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/xyz/file");
  }

  @Test
  public void testPackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "");

    attributes.add(FileAttribute.REPLICATION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "R");

    attributes.add(FileAttribute.BLOCKSIZE);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RB");

    attributes.add(FileAttribute.USER);
    attributes.add(FileAttribute.CHECKSUMTYPE);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUC");

    attributes.add(FileAttribute.GROUP);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGC");

    attributes.add(FileAttribute.PERMISSION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGPC");

    attributes.add(FileAttribute.TIMES);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGPCT");
  }

  @Test
  public void testUnpackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RCBUGPAXT"));

    attributes.remove(FileAttribute.REPLICATION);
    attributes.remove(FileAttribute.CHECKSUMTYPE);
    attributes.remove(FileAttribute.ACL);
    attributes.remove(FileAttribute.XATTR);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("BUGPT"));

    attributes.remove(FileAttribute.TIMES);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("BUGP"));

    attributes.remove(FileAttribute.BLOCKSIZE);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("UGP"));

    attributes.remove(FileAttribute.GROUP);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("UP"));

    attributes.remove(FileAttribute.USER);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("P"));

    attributes.remove(FileAttribute.PERMISSION);
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes(""));
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

    CopyListingFileStatus dstStatus = new CopyListingFileStatus(fs.getFileStatus(dst));

    // FileStatus.equals only compares path field, must explicitly compare all fields
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
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

    Assert.assertNotEquals(en1, dd2);

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
    Assert.assertEquals("getPermission", srcStatus.getPermission(),
        dstStatus.getPermission());
    Assert.assertEquals("Owner", srcStatus.getOwner(), dstStatus.getOwner());
    Assert.assertEquals("Group", srcStatus.getGroup(), dstStatus.getGroup());
    Assert.assertEquals("AccessTime", srcStatus.getAccessTime(),
        dstStatus.getAccessTime());
    Assert.assertEquals("ModificationTime", srcStatus.getModificationTime(),
        dstStatus.getModificationTime());
    Assert.assertEquals("Replication", srcStatus.getReplication(),
        dstStatus.getReplication());
    Assert.assertArrayEquals(en1.toArray(), dd2.toArray());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(dstStatus.getAccessTime() == 100);
    Assert.assertTrue(dstStatus.getModificationTime() == 100);
    Assert.assertTrue(dstStatus.getReplication() == 0);
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
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    // Replication shouldn't apply to dirs so this should still be 0 == 0
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertTrue(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == dstStatus.getReplication());
  }

  @Test (timeout = 60000)
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
    assertEquals("Setting EC policy should succeed!", 0, res);
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
    assertEquals("Setting EC policy should succeed!", 0, res);
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
      assertTrue(srcFile + "should be erasure coded!",
          srcStatus.isErasureCoded());
      assertEquals(INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          srcStatus.getReplication());
    } else {
      assertEquals("Unexpected replication factor for " + srcFile,
          fs.getDefaultReplication(srcFile), srcStatus.getReplication());
    }

    createFile(fs, dstFile);
    CopyListingFileStatus dstStatus = new CopyListingFileStatus(
        fs.getFileStatus(dstFile));
    if (isDstEC) {
      assertTrue(dstFile + "should be erasure coded!",
          dstStatus.isErasureCoded());
      assertEquals("Unexpected replication factor for erasure coded file!",
          INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          dstStatus.getReplication());
    } else {
      assertEquals("Unexpected replication factor for " + dstFile,
          fs.getDefaultReplication(dstFile), dstStatus.getReplication());
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

    assertFalse("Permission for " + srcFile + " and " + dstFile +
            " should not be same after preserve only for replication attr!",
        srcStatus.getPermission().equals(dstStatus.getPermission()));
    assertFalse("File ownership should not match!",
        srcStatus.getOwner().equals(dstStatus.getOwner()));
    assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    assertFalse(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    assertFalse(
        srcStatus.getModificationTime() == dstStatus.getModificationTime());
    if (isDstEC) {
      assertEquals("Unexpected replication factor for erasure coded file!",
          INodeFile.DEFAULT_REPL_FOR_STRIPED_BLOCKS,
          dstStatus.getReplication());
    } else {
      assertEquals(dstFile + " replication factor should be same as dst " +
              "filesystem!", fs.getDefaultReplication(dstFile),
          dstStatus.getReplication());
    }
    if (!isSrcEC || !isDstEC) {
      assertFalse(dstFile + " replication factor should not be " +
              "same as " + srcFile,
          srcStatus.getReplication() == dstStatus.getReplication());
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
    Assert.assertFalse(srcStatus.getPermission().equals(dstStatus.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(dstStatus.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(dstStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == dstStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == dstStatus.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == dstStatus.getReplication());
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
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    Assert.assertTrue(srcStatus.getPermission().equals(f2Status.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(f2Status.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(f2Status.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == f2Status.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == f2Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == f2Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    Assert.assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    Assert.assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f0Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    Assert.assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    Assert.assertTrue(d2Status.getAccessTime() == 300);
    Assert.assertTrue(d2Status.getModificationTime() == 300);
    Assert.assertFalse(srcStatus.getReplication() == d2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    Assert.assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    Assert.assertTrue(d1Status.getAccessTime() == 400);
    Assert.assertTrue(d1Status.getModificationTime() == 400);
    Assert.assertFalse(srcStatus.getReplication() == d1Status.getReplication());
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
    Assert.assertTrue(srcStatus.getPermission().equals(d2Status.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(d2Status.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(d2Status.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == d2Status.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == d2Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() != d2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    Assert.assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == d1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == d1Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() != d1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    Assert.assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f2Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    Assert.assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    Assert.assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f0Status.getReplication());
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
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    Assert.assertTrue(srcStatus.getPermission().equals(f0Status.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(f0Status.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(f0Status.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == f0Status.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == f0Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() == f0Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    Assert.assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    Assert.assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f2Status.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    Assert.assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    Assert.assertTrue(d1Status.getAccessTime() == 400);
    Assert.assertTrue(d1Status.getModificationTime() == 400);
    Assert.assertFalse(srcStatus.getReplication() == d1Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    Assert.assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    Assert.assertTrue(d2Status.getAccessTime() == 300);
    Assert.assertTrue(d2Status.getModificationTime() == 300);
    Assert.assertFalse(srcStatus.getReplication() == d2Status.getReplication());
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
    Assert.assertTrue(srcStatus.getPermission().equals(rootStatus.getPermission()));
    Assert.assertTrue(srcStatus.getOwner().equals(rootStatus.getOwner()));
    Assert.assertTrue(srcStatus.getGroup().equals(rootStatus.getGroup()));
    Assert.assertTrue(srcStatus.getAccessTime() == rootStatus.getAccessTime());
    Assert.assertTrue(srcStatus.getModificationTime() == rootStatus.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() != rootStatus.getReplication());

    // attributes of src -> d1 ? should be no
    CopyListingFileStatus d1Status = new CopyListingFileStatus(fs.getFileStatus(d1));
    Assert.assertFalse(srcStatus.getPermission().equals(d1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == d1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == d1Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() != d1Status.getReplication());

    // attributes of src -> d2 ? should be no
    CopyListingFileStatus d2Status = new CopyListingFileStatus(fs.getFileStatus(d2));
    Assert.assertFalse(srcStatus.getPermission().equals(d2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(d2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(d2Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == d2Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == d2Status.getModificationTime());
    Assert.assertTrue(srcStatus.getReplication() != d2Status.getReplication());

    // attributes of src -> f0 ? should be no
    CopyListingFileStatus f0Status = new CopyListingFileStatus(fs.getFileStatus(f0));
    Assert.assertFalse(srcStatus.getPermission().equals(f0Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f0Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f0Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f0Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f0Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f0Status.getReplication());

    // attributes of src -> f1 ? should be no
    CopyListingFileStatus f1Status = new CopyListingFileStatus(fs.getFileStatus(f1));
    Assert.assertFalse(srcStatus.getPermission().equals(f1Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f1Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f1Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f1Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f1Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f1Status.getReplication());

    // attributes of src -> f2 ? should be no
    CopyListingFileStatus f2Status = new CopyListingFileStatus(fs.getFileStatus(f2));
    Assert.assertFalse(srcStatus.getPermission().equals(f2Status.getPermission()));
    Assert.assertFalse(srcStatus.getOwner().equals(f2Status.getOwner()));
    Assert.assertFalse(srcStatus.getGroup().equals(f2Status.getGroup()));
    Assert.assertFalse(srcStatus.getAccessTime() == f2Status.getAccessTime());
    Assert.assertFalse(srcStatus.getModificationTime() == f2Status.getModificationTime());
    Assert.assertFalse(srcStatus.getReplication() == f2Status.getReplication());
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
