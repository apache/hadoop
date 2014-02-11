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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

/**
 * Tests that the configuration flag that controls support for ACLs is off by
 * default and causes all attempted operations related to ACLs to fail.  The
 * NameNode can still load ACLs from fsimage or edits.
 */
public class TestAclConfigFlag {
  private static final Path PATH = new Path("/path");

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @After
  public void shutdown() throws Exception {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testModifyAclEntries() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.modifyAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveAclEntries() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.removeAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveDefaultAcl() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.removeAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveAcl() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.removeAcl(PATH);
  }

  @Test
  public void testSetAcl() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.setAcl(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testGetAclStatus() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.getAclStatus(PATH);
  }

  @Test
  public void testEditLog() throws Exception {
    // With ACLs enabled, set an ACL.
    initCluster(true, true);
    fs.mkdirs(PATH);
    fs.setAcl(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));

    // Restart with ACLs disabled.  Expect successful restart.
    restart(false, false);
  }

  @Test
  public void testFsImage() throws Exception {
    // With ACLs enabled, set an ACL.
    initCluster(true, true);
    fs.mkdirs(PATH);
    fs.setAcl(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));

    // Save a new checkpoint and restart with ACLs still enabled.
    restart(true, true);

    // Restart with ACLs disabled.  Expect successful restart.
    restart(false, false);
  }

  /**
   * We expect an AclException, and we want the exception text to state the
   * configuration key that controls ACL support.
   */
  private void expectException() {
    exception.expect(AclException.class);
    exception.expectMessage(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY);
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param aclsEnabled if true, ACL support is enabled
   * @throws Exception if any step fails
   */
  private void initCluster(boolean format, boolean aclsEnabled)
      throws Exception {
    Configuration conf = new Configuration();
    // not explicitly setting to false, should be false by default
    if (aclsEnabled) {
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  /**
   * Restart the cluster, optionally saving a new checkpoint.
   *
   * @param checkpoint boolean true to save a new checkpoint
   * @param aclsEnabled if true, ACL support is enabled
   * @throws Exception if restart fails
   */
  private void restart(boolean checkpoint, boolean aclsEnabled)
      throws Exception {
    NameNode nameNode = cluster.getNameNode();
    if (checkpoint) {
      NameNodeAdapter.enterSafeMode(nameNode, false);
      NameNodeAdapter.saveNamespace(nameNode);
    }
    shutdown();
    initCluster(false, aclsEnabled);
  }
}
