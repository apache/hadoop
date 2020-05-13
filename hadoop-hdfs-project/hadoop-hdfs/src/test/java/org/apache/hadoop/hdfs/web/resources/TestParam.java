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
package org.apache.hadoop.hdfs.web.resources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestParam {
  public static final Logger LOG = LoggerFactory.getLogger(TestParam.class);

  final Configuration conf = new Configuration();
 
  @Test
  public void testAccessTimeParam() {
    final AccessTimeParam p = new AccessTimeParam(AccessTimeParam.DEFAULT);
    Assert.assertEquals(-1L, p.getValue().longValue());

    new AccessTimeParam(-1L);

    try {
      new AccessTimeParam(-2L);
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testBlockSizeParam() {
    final BlockSizeParam p = new BlockSizeParam(BlockSizeParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
    Assert.assertEquals(
        conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
            DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT),
        p.getValue(conf));

    new BlockSizeParam(1L);

    try {
      new BlockSizeParam(0L);
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testBufferSizeParam() {
    final BufferSizeParam p = new BufferSizeParam(BufferSizeParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
    Assert.assertEquals(
        conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
            CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
        p.getValue(conf));

    new BufferSizeParam(1);

    try {
      new BufferSizeParam(0);
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testDelegationParam() {
    final DelegationParam p = new DelegationParam(DelegationParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
  }

  @Test
  public void testDestinationParam() {
    final DestinationParam p = new DestinationParam(DestinationParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());

    new DestinationParam("/abc");

    try {
      new DestinationParam("abc");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testGroupParam() {
    final GroupParam p = new GroupParam(GroupParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
  }

  @Test
  public void testModificationTimeParam() {
    final ModificationTimeParam p = new ModificationTimeParam(ModificationTimeParam.DEFAULT);
    Assert.assertEquals(-1L, p.getValue().longValue());

    new ModificationTimeParam(-1L);

    try {
      new ModificationTimeParam(-2L);
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testOverwriteParam() {
    final OverwriteParam p = new OverwriteParam(OverwriteParam.DEFAULT);
    Assert.assertEquals(false, p.getValue());

    new OverwriteParam("trUe");

    try {
      new OverwriteParam("abc");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testOwnerParam() {
    final OwnerParam p = new OwnerParam(OwnerParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
  }

  @Test
  public void testPermissionParam() {
    final PermissionParam p = new PermissionParam(PermissionParam.DEFAULT);
    Assert.assertEquals(new FsPermission((short)0755), p.getDirFsPermission());
    Assert.assertEquals(new FsPermission((short)0644), p.getFileFsPermission());

    new PermissionParam("0");

    try {
      new PermissionParam("-1");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    new PermissionParam("1777");

    try {
      new PermissionParam("2000");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new PermissionParam("8");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new PermissionParam("abc");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testRecursiveParam() {
    final RecursiveParam p = new RecursiveParam(RecursiveParam.DEFAULT);
    Assert.assertEquals(false, p.getValue());

    new RecursiveParam("falSe");

    try {
      new RecursiveParam("abc");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testRenewerParam() {
    final RenewerParam p = new RenewerParam(RenewerParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
  }

  @Test
  public void testReplicationParam() {
    final ReplicationParam p = new ReplicationParam(ReplicationParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
    Assert.assertEquals(
        (short)conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY,
            DFSConfigKeys.DFS_REPLICATION_DEFAULT),
        p.getValue(conf));

    new ReplicationParam((short)1);

    try {
      new ReplicationParam((short)0);
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testToSortedStringEscapesURICharacters() {
    final String sep = "&";
    Param<?, ?> ampParam = new TokenArgumentParam("token&ampersand");
    Param<?, ?> equalParam = new RenewerParam("renewer=equal");
    final String expected = "&renewer=renewer%3Dequal&token=token%26ampersand";
    final String actual = Param.toSortedString(sep, equalParam, ampParam);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void userNameEmpty() {
    UserParam userParam = new UserParam("");
    assertNull(userParam.getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void userNameInvalidStart() {
    new UserParam("1x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void userNameInvalidDollarSign() {
    new UserParam("1$x");
  }

  @Test
  public void userNameMinLength() {
    UserParam userParam = new UserParam("a");
    assertNotNull(userParam.getValue());
  }

  @Test
  public void userNameValidDollarSign() {
    UserParam userParam = new UserParam("a$");
    assertNotNull(userParam.getValue());
  }
  
  @Test
  public void testConcatSourcesParam() {
    final String[] strings = {"/", "/foo", "/bar"};
    for(int n = 0; n < strings.length; n++) {
      final String[] sub = new String[n]; 
      final Path[] paths = new Path[n];
      for(int i = 0; i < paths.length; i++) {
        paths[i] = new Path(sub[i] = strings[i]);
      }

      final String expected = StringUtils.join(",", Arrays.asList(sub));
      final ConcatSourcesParam computed = new ConcatSourcesParam(paths);
      Assert.assertEquals(expected, computed.getValue());
    }
  }

  @Test
  public void testUserNameOkAfterResettingPattern() {
    UserParam.Domain oldDomain = UserParam.getUserPatternDomain();

    String newPattern = "^[A-Za-z0-9_][A-Za-z0-9._-]*[$]?$";
    UserParam.setUserPattern(newPattern);

    UserParam userParam = new UserParam("1x");
    assertNotNull(userParam.getValue());
    userParam = new UserParam("123");
    assertNotNull(userParam.getValue());

    UserParam.setUserPatternDomain(oldDomain);
  }

  @Test
  public void testAclPermissionParam() {
    final AclPermissionParam p =
        new AclPermissionParam("user::rwx,group::r--,other::rwx,user:user1:rwx");
    List<AclEntry> setAclList =
        AclEntry.parseAclSpec("user::rwx,group::r--,other::rwx,user:user1:rwx",
            true);
    Assert.assertEquals(setAclList.toString(), p.getAclPermission(true)
        .toString());

    new AclPermissionParam("user::rw-,group::rwx,other::rw-,user:user1:rwx");
    try {
      new AclPermissionParam("user::rw--,group::rwx-,other::rw-");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    new AclPermissionParam(
        "user::rw-,group::rwx,other::rw-,user:user1:rwx,group:group1:rwx,other::rwx,mask::rwx,default:user:user1:rwx");

    try {
      new AclPermissionParam("user:r-,group:rwx,other:rw-");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new AclPermissionParam("default:::r-,default:group::rwx,other::rw-");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new AclPermissionParam("user:r-,group::rwx,other:rw-,mask:rw-,temp::rwx");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testUserGroupOkAfterAlteringAclPattern() {
    // Preserve default pattern value
    AclPermissionParam.Domain oldDomain =
        AclPermissionParam.getAclPermissionPattern();

    // Override the pattern with one that accepts '@' and numbers
    // in the first character of usernames/groupnames
    String newPattern =
        "^(default:)?(user|group|mask|other):" +
            "[[0-9A-Za-z_][@A-Za-z0-9._-]]*:([rwx-]{3})?" +
            "(,(default:)?(user|group|mask|other):" +
            "[[0-9A-Za-z_][@A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

    try {
      AclPermissionParam.setAclPermissionPattern(newPattern);

      String numericUserSpec = "user:110201:rwx";
      AclPermissionParam aclNumericUserParam =
          new AclPermissionParam(numericUserSpec);
      Assert.assertEquals(numericUserSpec, aclNumericUserParam.getValue());

      String oddGroupSpec = "group:foo@bar:rwx";
      AclPermissionParam aclGroupWithDomainParam =
          new AclPermissionParam(oddGroupSpec);
      Assert.assertEquals(oddGroupSpec, aclGroupWithDomainParam.getValue());

    } finally {
      // Revert back to the default rules for remainder of tests
      AclPermissionParam.setAclPermissionPattern(oldDomain);
    }

  }
 
  @Test
  public void testXAttrNameParam() {
    final XAttrNameParam p = new XAttrNameParam("user.a1");
    Assert.assertEquals(p.getXAttrName(), "user.a1");
  }
  
  @Test
  public void testXAttrValueParam() throws IOException {
    final XAttrValueParam p = new XAttrValueParam("0x313233");
    Assert.assertArrayEquals(p.getXAttrValue(), 
        XAttrCodec.decodeValue("0x313233"));
  }
  
  @Test
  public void testXAttrEncodingParam() {
    final XAttrEncodingParam p = new XAttrEncodingParam(XAttrCodec.BASE64);
    Assert.assertEquals(p.getEncoding(), XAttrCodec.BASE64);
    final XAttrEncodingParam p1 = new XAttrEncodingParam(p.getValueString());
    Assert.assertEquals(p1.getEncoding(), XAttrCodec.BASE64);
  }
  
  @Test
  public void testXAttrSetFlagParam() {
    EnumSet<XAttrSetFlag> flag = EnumSet.of(
        XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE);
    final XAttrSetFlagParam p = new XAttrSetFlagParam(flag);
    Assert.assertEquals(p.getFlag(), flag);
    final XAttrSetFlagParam p1 = new XAttrSetFlagParam(p.getValueString());
    Assert.assertEquals(p1.getFlag(), flag);
  }
  
  @Test
  public void testRenameOptionSetParam() {
    final RenameOptionSetParam p = new RenameOptionSetParam(
        Options.Rename.OVERWRITE, Options.Rename.NONE);
    final RenameOptionSetParam p1 = new RenameOptionSetParam(
        p.getValueString());
    Assert.assertEquals(p1.getValue(), EnumSet.of(
        Options.Rename.OVERWRITE, Options.Rename.NONE));
  }

  @Test
  public void testSnapshotNameParam() {
    final OldSnapshotNameParam s1 = new OldSnapshotNameParam("s1");
    final SnapshotNameParam s2 = new SnapshotNameParam("s2");
    Assert.assertEquals("s1", s1.getValue());
    Assert.assertEquals("s2", s2.getValue());
  }

  @Test
  public void testFsActionParam() {
    new FsActionParam("rwx");
    new FsActionParam("rw-");
    new FsActionParam("r-x");
    new FsActionParam("-wx");
    new FsActionParam("r--");
    new FsActionParam("-w-");
    new FsActionParam("--x");
    new FsActionParam("---");

    try {
      new FsActionParam("rw");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("qwx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("qrwx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("rwxx");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("xwr");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }

    try {
      new FsActionParam("r-w");
      Assert.fail();
    } catch(IllegalArgumentException e) {
      LOG.info("EXPECTED: " + e);
    }
  }

  @Test
  public void testStartAfterParam() throws Exception {
    String s = "/helloWorld";
    StartAfterParam param = new StartAfterParam(s);
    Assert.assertEquals(s, param.getValue());
  }

  @Test
  public void testStoragePolicyParam() {
    StoragePolicyParam p = new StoragePolicyParam(StoragePolicyParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
    p = new StoragePolicyParam("COLD");
    Assert.assertEquals("COLD", p.getValue());
  }

  @Test
  public void testNamespaceQuotaParam() {
    NameSpaceQuotaParam p =
        new NameSpaceQuotaParam(NameSpaceQuotaParam.DEFAULT);
    assertEquals(Long.valueOf(NameSpaceQuotaParam.DEFAULT), p.getValue());
    p = new NameSpaceQuotaParam(100L);
    assertEquals(100L, p.getValue().longValue());
  }

  @Test
  public void testStorageSpaceQuotaParam() {
    StorageSpaceQuotaParam sp = new StorageSpaceQuotaParam(
        StorageSpaceQuotaParam.DEFAULT);
    assertEquals(Long.valueOf(StorageSpaceQuotaParam.DEFAULT),
        sp.getValue());
    sp = new StorageSpaceQuotaParam(100L);
    assertEquals(100L, sp.getValue().longValue());
  }

  @Test
  public void testStorageTypeParam() {
    StorageTypeParam p = new StorageTypeParam(StorageTypeParam.DEFAULT);
    assertNull(p.getValue());
    p = new StorageTypeParam(StorageType.DISK.name());
    assertEquals(StorageType.DISK.name(), p.getValue());
  }

  @Test
  public void testECPolicyParam() {
    ECPolicyParam p = new ECPolicyParam(ECPolicyParam.DEFAULT);
    Assert.assertEquals(null, p.getValue());
    p = new ECPolicyParam("RS-6-3-1024k");
    Assert.assertEquals("RS-6-3-1024k", p.getValue());
  }

  @Test
  public void testHttpOpParams() {
    try {
      new PostOpParam("TEST");
      Assert
          .fail("Construct the PostOpParam with param value 'TEST' should be"
              + " failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "TEST is not a valid POST operation.", e);
    }
    try {
      new PutOpParam("TEST");
      Assert
          .fail("Construct the PutOpParam with param value 'TEST' should be"
              + " failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "TEST is not a valid PUT operation.", e);
    }
    try {
      new DeleteOpParam("TEST");
      Assert
          .fail("Construct the DeleteOpParam with param value 'TEST' should be"
              + " failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "TEST is not a valid DELETE operation.", e);
    }
    try {
      new GetOpParam("TEST");
      Assert
          .fail("Construct the GetOpParam with param value 'TEST' should be"
              + " failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "TEST is not a valid GET operation.", e);
    }
  }
}
