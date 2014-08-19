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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestFSImageWithAcl {
  private static Configuration conf;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  private void testAcl(boolean persistNamespace) throws IOException {
    Path p = new Path("/p");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.create(p).close();
    fs.mkdirs(new Path("/23"));

    AclEntry e = new AclEntry.Builder().setName("foo")
        .setPermission(READ_EXECUTE).setScope(ACCESS).setType(USER).build();
    fs.modifyAclEntries(p, Lists.newArrayList(e));

    restart(fs, persistNamespace);

    AclStatus s = cluster.getNamesystem().getAclStatus(p.toString());
    AclEntry[] returned = Lists.newArrayList(s.getEntries()).toArray(
        new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ) }, returned);

    fs.removeAcl(p);

    if (persistNamespace) {
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    }

    cluster.restartNameNode();
    cluster.waitActive();

    s = cluster.getNamesystem().getAclStatus(p.toString());
    returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] { }, returned);

    fs.modifyAclEntries(p, Lists.newArrayList(e));
    s = cluster.getNamesystem().getAclStatus(p.toString());
    returned = Lists.newArrayList(s.getEntries()).toArray(new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, "foo", READ_EXECUTE),
        aclEntry(ACCESS, GROUP, READ) }, returned);
  }

  @Test
  public void testPersistAcl() throws IOException {
    testAcl(true);
  }

  @Test
  public void testAclEditLog() throws IOException {
    testAcl(false);
  }

  private void doTestDefaultAclNewChildren(boolean persistNamespace)
      throws IOException {
    Path dirPath = new Path("/dir");
    Path filePath = new Path(dirPath, "file1");
    Path subdirPath = new Path(dirPath, "subdir1");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdirs(dirPath);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", ALL));
    fs.setAcl(dirPath, aclSpec);

    fs.create(filePath).close();
    fs.mkdirs(subdirPath);

    AclEntry[] fileExpected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE) };
    AclEntry[] subdirExpected = new AclEntry[] {
      aclEntry(ACCESS, USER, "foo", ALL),
      aclEntry(ACCESS, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, USER, ALL),
      aclEntry(DEFAULT, USER, "foo", ALL),
      aclEntry(DEFAULT, GROUP, READ_EXECUTE),
      aclEntry(DEFAULT, MASK, ALL),
      aclEntry(DEFAULT, OTHER, READ_EXECUTE) };

    AclEntry[] fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    AclEntry[] subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);

    restart(fs, persistNamespace);

    fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);

    aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "foo", READ_WRITE));
    fs.modifyAclEntries(dirPath, aclSpec);

    fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);

    restart(fs, persistNamespace);

    fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);

    fs.removeAcl(dirPath);

    fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);

    restart(fs, persistNamespace);

    fileReturned = fs.getAclStatus(filePath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(fileExpected, fileReturned);
    subdirReturned = fs.getAclStatus(subdirPath).getEntries()
      .toArray(new AclEntry[0]);
    Assert.assertArrayEquals(subdirExpected, subdirReturned);
    assertPermission(fs, subdirPath, (short)010755);
  }

  @Test
  public void testFsImageDefaultAclNewChildren() throws IOException {
    doTestDefaultAclNewChildren(true);
  }

  @Test
  public void testEditLogDefaultAclNewChildren() throws IOException {
    doTestDefaultAclNewChildren(false);
  }

  /**
   * Restart the NameNode, optionally saving a new checkpoint.
   *
   * @param fs DistributedFileSystem used for saving namespace
   * @param persistNamespace boolean true to save a new checkpoint
   * @throws IOException if restart fails
   */
  private void restart(DistributedFileSystem fs, boolean persistNamespace)
      throws IOException {
    if (persistNamespace) {
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    }

    cluster.restartNameNode();
    cluster.waitActive();
  }
}
