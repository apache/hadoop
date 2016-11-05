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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.List;

public class TestFSNamesystem {

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * Tests that the namenode edits dirs are gotten with duplicates removed
   */
  @Test
  public void testUniqueEditDirs() throws IOException {
    Configuration config = new Configuration();

    config.set(DFS_NAMENODE_EDITS_DIR_KEY, "file://edits/dir, "
        + "file://edits/dir1,file://edits/dir1"); // overlapping internally

    // getNamespaceEditsDirs removes duplicates
    Collection<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(config);
    assertEquals(2, editsDirs.size());
  }

  /**
   * Test that FSNamesystem#clear clears all leases.
   */
  @Test
  public void testFSNamespaceClearLeases() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());

    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = FSNamesystem.loadFromDisk(conf);
    LeaseManager leaseMan = fsn.getLeaseManager();
    leaseMan.addLease("client1", fsn.getFSDirectory().allocateNewInodeId());
    assertEquals(1, leaseMan.countLease());
    fsn.clear();
    leaseMan = fsn.getLeaseManager();
    assertEquals(0, leaseMan.countLease());
  }

  @Test
  /**
   * Test that isInStartupSafemode returns true only during startup safemode
   * and not also during low-resource safemode
   */
  public void testStartupSafemode() throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = Mockito.mock(FSImage.class);
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);

    fsn.leaveSafeMode(false);
    assertTrue("After leaving safemode FSNamesystem.isInStartupSafeMode still "
      + "returned true", !fsn.isInStartupSafeMode());
    assertTrue("After leaving safemode FSNamesystem.isInSafeMode still returned"
      + " true", !fsn.isInSafeMode());

    fsn.enterSafeMode(true);
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInStartupSafeMode still returned true", !fsn.isInStartupSafeMode());
    assertTrue("After entering safemode due to low resources FSNamesystem."
      + "isInSafeMode still returned false",  fsn.isInSafeMode());
  }

  @Test
  public void testReplQueuesActiveAfterStartupSafemode() throws IOException, InterruptedException{
    Configuration conf = new Configuration();

    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);

    FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
    FSNamesystem fsn = Mockito.spy(fsNamesystem);
    BlockManager bm = fsn.getBlockManager();
    Whitebox.setInternalState(bm, "namesystem", fsn);

    //Make shouldPopulaeReplQueues return true
    HAContext haContext = Mockito.mock(HAContext.class);
    HAState haState = Mockito.mock(HAState.class);
    Mockito.when(haContext.getState()).thenReturn(haState);
    Mockito.when(haState.shouldPopulateReplQueues()).thenReturn(true);
    Mockito.when(fsn.getHAContext()).thenReturn(haContext);

    //Make NameNode.getNameNodeMetrics() not return null
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);

    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues were being populated during very first "
        + "safemode", !bm.isPopulatingReplQueues());
    fsn.leaveSafeMode(false);
    assertTrue("FSNamesystem didn't leave safemode", !fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated even after leaving "
      + "safemode", bm.isPopulatingReplQueues());
    fsn.enterSafeMode(false);
    assertTrue("FSNamesystem didn't enter safemode", fsn.isInSafeMode());
    assertTrue("Replication queues weren't being populated after entering "
      + "safemode 2nd time", bm.isPopulatingReplQueues());
  }

  @Test
  public void testReset() throws Exception {
    Configuration conf = new Configuration();
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    FSImage fsImage = Mockito.mock(FSImage.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
    fsn.clear();
    assertFalse(fsn.isImageLoaded());
    final INodeDirectory root = (INodeDirectory) fsn.getFSDirectory()
            .getINode("/");
    assertTrue(root.getChildrenList(Snapshot.CURRENT_STATE_ID).isEmpty());
    fsn.imageLoadComplete();
    assertTrue(fsn.isImageLoaded());
  }

  @Test
  public void testGetEffectiveLayoutVersion() {
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(true, -60, -61, -63));
    assertEquals(-61,
        FSNamesystem.getEffectiveLayoutVersion(true, -61, -61, -63));
    assertEquals(-62,
        FSNamesystem.getEffectiveLayoutVersion(true, -62, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(true, -63, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -60, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -61, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -62, -61, -63));
    assertEquals(-63,
        FSNamesystem.getEffectiveLayoutVersion(false, -63, -61, -63));
  }

  @Test
  public void testSafemodeReplicationConf() throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = Mockito.mock(FSImage.class);
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);

    Object bmSafeMode = Whitebox.getInternalState(fsn.getBlockManager(),
        "bmSafeMode");
    int safeReplication = (int)Whitebox.getInternalState(bmSafeMode,
        "safeReplication");
    assertEquals(2, safeReplication);
  }

  @Test(timeout = 30000)
  public void testInitAuditLoggers() throws IOException {
    Configuration conf = new Configuration();
    FSImage fsImage = Mockito.mock(FSImage.class);
    FSEditLog fsEditLog = Mockito.mock(FSEditLog.class);
    Mockito.when(fsImage.getEditLog()).thenReturn(fsEditLog);
    FSNamesystem fsn;
    List<AuditLogger> auditLoggers;

    // Not to specify any audit loggers in config
    conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "");
    // Disable top logger
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, false);
    fsn = new FSNamesystem(conf, fsImage);
    auditLoggers = fsn.getAuditLoggers();
    assertTrue(auditLoggers.size() == 1);
    assertTrue(auditLoggers.get(0) instanceof FSNamesystem.DefaultAuditLogger);

    // Not to specify any audit loggers in config
    conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "");
    // Enable top logger
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    fsn = new FSNamesystem(conf, fsImage);
    auditLoggers = fsn.getAuditLoggers();
    assertTrue(auditLoggers.size() == 2);
    // the audit loggers order is not defined
    for (AuditLogger auditLogger : auditLoggers) {
      assertThat(auditLogger,
          either(instanceOf(FSNamesystem.DefaultAuditLogger.class))
              .or(instanceOf(TopAuditLogger.class)));
    }

    // Configure default audit loggers in config
    conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
    // Enable top logger
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    fsn = new FSNamesystem(conf, fsImage);
    auditLoggers = fsn.getAuditLoggers();
    assertTrue(auditLoggers.size() == 2);
    for (AuditLogger auditLogger : auditLoggers) {
      assertThat(auditLogger,
          either(instanceOf(FSNamesystem.DefaultAuditLogger.class))
              .or(instanceOf(TopAuditLogger.class)));
    }

    // Configure default and customized audit loggers in config with whitespaces
    conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY,
        " default, org.apache.hadoop.hdfs.server.namenode.TestFSNamesystem$DummyAuditLogger  ");
    // Enable top logger
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    fsn = new FSNamesystem(conf, fsImage);
    auditLoggers = fsn.getAuditLoggers();
    assertTrue(auditLoggers.size() == 3);
    for (AuditLogger auditLogger : auditLoggers) {
      assertThat(auditLogger,
          either(instanceOf(FSNamesystem.DefaultAuditLogger.class))
              .or(instanceOf(TopAuditLogger.class))
              .or(instanceOf(DummyAuditLogger.class)));
    }
  }

  static class DummyAuditLogger implements AuditLogger {
    @Override
    public void initialize(Configuration conf) {
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst, FileStatus stat) {
    }
  }
}
