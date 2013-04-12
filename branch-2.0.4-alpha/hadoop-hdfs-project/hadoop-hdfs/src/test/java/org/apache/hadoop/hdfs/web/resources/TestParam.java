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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestParam {
  public static final Log LOG = LogFactory.getLog(TestParam.class);

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
    Assert.assertEquals(new FsPermission((short)0755), p.getFsPermission());

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
}
