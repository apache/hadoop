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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.junit.Test;

public class TestGenericJournalConf {
  private static final String DUMMY_URI = "dummy://test";

  /** 
   * Test that an exception is thrown if a journal class doesn't exist
   * in the configuration 
   */
  @Test(expected=IllegalArgumentException.class)
  public void testNotConfigured() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that an exception is thrown if a journal class doesn't
   * exist in the classloader.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testClassDoesntExist() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + ".dummy",
             "org.apache.hadoop.nonexistent");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that a implementation of JournalManager without a 
   * (Configuration,URI) constructor throws an exception
   */
  @Test
  public void testBadConstructor() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + ".dummy",
             BadConstructorJournalManager.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
             "dummy://test");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      fail("Should have failed before this point");
    } catch (IllegalArgumentException iae) {
      if (!iae.getMessage().contains("Unable to construct journal")) {
        fail("Should have failed with unable to construct exception");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that a dummy implementation of JournalManager can
   * be initialized on startup
   */
  @Test
  public void testDummyJournalManager() throws Exception {
    MiniDFSCluster cluster = null;
    Configuration conf = new Configuration();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + ".dummy",
             DummyJournalManager.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, DUMMY_URI);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, 0);
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      
      assertTrue(DummyJournalManager.shouldPromptCalled);
      assertTrue(DummyJournalManager.formatCalled);
      assertNotNull(DummyJournalManager.conf);
      assertEquals(new URI(DUMMY_URI), DummyJournalManager.uri);
      assertNotNull(DummyJournalManager.nsInfo);
      assertEquals(DummyJournalManager.nsInfo.getClusterID(),
          cluster.getNameNode().getNamesystem().getClusterId());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static class DummyJournalManager implements JournalManager {
    static Configuration conf = null;
    static URI uri = null;
    static NamespaceInfo nsInfo = null;
    static boolean formatCalled = false;
    static boolean shouldPromptCalled = false;
    
    public DummyJournalManager(Configuration conf, URI u,
        NamespaceInfo nsInfo) {
      // Set static vars so the test case can verify them.
      DummyJournalManager.conf = conf;
      DummyJournalManager.uri = u;
      DummyJournalManager.nsInfo = nsInfo; 
    }
    
    @Override
    public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
      formatCalled = true;
    }
    
    @Override
    public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
        throws IOException {
      return mock(EditLogOutputStream.class);
    }
    
    @Override
    public void finalizeLogSegment(long firstTxId, long lastTxId)
        throws IOException {
      // noop
    }

    @Override
    public void selectInputStreams(Collection<EditLogInputStream> streams,
        long fromTxnId, boolean inProgressOk, boolean onlyDurableTxns) {
    }

    @Override
    public void setOutputBufferCapacity(int size) {}

    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep)
        throws IOException {}

    @Override
    public void recoverUnfinalizedSegments() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public boolean hasSomeData() throws IOException {
      shouldPromptCalled = true;
      return false;
    }

    @Override
    public void doPreUpgrade() throws IOException {}

    @Override
    public void doUpgrade(Storage storage) throws IOException {}

    @Override
    public void doFinalize() throws IOException {}

    @Override
    public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage, int targetLayoutVersion)
        throws IOException {
      return false;
    }

    @Override
    public void doRollback() throws IOException {}

    @Override
    public void discardSegments(long startTxId) throws IOException {}

    @Override
    public long getJournalCTime() throws IOException {
      return -1;
    }
  }

  public static class BadConstructorJournalManager extends DummyJournalManager {
    public BadConstructorJournalManager() {
      super(null, null, null);
    }
  }
}
