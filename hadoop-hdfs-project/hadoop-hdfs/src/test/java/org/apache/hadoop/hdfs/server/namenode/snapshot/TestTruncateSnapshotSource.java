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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ordinary HA replay must retain a pending truncate source without snapshots. */
@Timeout(120)
public class TestTruncateSnapshotSource {
  @TempDir
  private java.nio.file.Path directory;

  @Test
  public void testStandbyReplayRetainsSourceAfterLastSnapshotDelete()
      throws Exception {
    checkSourceRetention(true);
  }

  @Test
  public void testActiveRetainsSourceAfterLastSnapshotDelete() throws Exception {
    checkSourceRetention(false);
  }

  @Test
  public void testCloseReplayKeepsSourceUntilLastSnapshotDelete() throws Exception {
    checkSourceRetention(true, true);
  }

  private void checkSourceRetention(boolean replay) throws Exception {
    checkSourceRetention(replay, false);
  }

  private void checkSourceRetention(boolean replay, boolean keepSnapshot)
      throws Exception {
    assertTrue(INodeFile.class.desiredAssertionStatus(),
        "NameNode runtime assertions must be enabled");
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong("dfs.namenode.fs-limits.min-block-size", 512);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf,
        directory.resolve("dfs").toFile())
        .baseDir(directory.resolve("journals").toString());
    builder.getDfsBuilder().numDataNodes(3);
    MiniQJMHACluster qjm = builder.build();
    try {
      MiniDFSCluster cluster = qjm.getDfsCluster();
      cluster.waitActive();
      cluster.transitionToActive(0);
      cluster.getNamesystem(1).getEditLogTailer().stop();
      Path parent = new Path("/snapshots");
      Path file = new Path(parent, "file");
      try (DistributedFileSystem active = (DistributedFileSystem)
          FileSystem.newInstance(cluster.getURI(0), conf)) {
        active.mkdirs(parent);
        DFSTestUtil.createFile(active, file, 1536, (short) 3, 12345L);
        byte[] original = DFSTestUtil.readFileBuffer(active, file);
        active.allowSnapshot(parent);
        active.createSnapshot(parent, "only");
        ExtendedBlock source = active.getClient().getLocatedBlocks(file.toString(), 0)
            .get(1).getBlock();
        for (DataNode dn : cluster.getDataNodes()) {
          DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
        }
        assertFalse(active.truncate(file, original.length - 1));
        cluster.getNameNode(0).getRpcServer().rollEditLog();
        cluster.getNamesystem(1).getEditLogTailer().doTailEdits();
        int node = replay ? 1 : 0;
        cluster.getNamesystem(node).readLock(RwLockMode.GLOBAL);
        try {
          INodeFile inode = cluster.getNamesystem(node).getFSDirectory()
              .getINode(file.toString()).asFile();
          assertEquals(replay ? BlockUCState.UNDER_CONSTRUCTION
              : BlockUCState.UNDER_RECOVERY,
              inode.getLastBlock().getBlockUCState());
          assertEquals(source.getBlockId(), inode.getLastBlock()
              .getUnderConstructionFeature().getTruncateBlock().getBlockId());
        } finally {
          cluster.getNamesystem(node).readUnlock(RwLockMode.GLOBAL, "testTruncateState");
        }
        if (!keepSnapshot) {
          active.deleteSnapshot(parent, "only");
        }
        cluster.getNameNode(0).getRpcServer().rollEditLog();
        cluster.getNamesystem(1).getEditLogTailer().doTailEdits();
        assertEquals(keepSnapshot,
            active.exists(new Path(parent, ".snapshot/only/file")));
        assertNotNull(cluster.getNamesystem(node).getBlockManager()
            .getStoredBlock(source.getLocalBlock()),
            "A pending copy source must survive deletion of its last snapshot"
                + " on node " + node);
        if (replay) {
          cluster.transitionToStandby(0);
          cluster.getNamesystem(0).getEditLogTailer().stop();
          cluster.transitionToActive(1);
          for (DataNode dn : cluster.getDataNodes()) {
            DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
          }
          try (DistributedFileSystem promoted = (DistributedFileSystem)
              FileSystem.newInstance(cluster.getURI(1), conf)) {
            GenericTestUtils.waitFor(() -> {
              try {
                return promoted.recoverLease(file);
              } catch (IOException failure) {
                throw new RuntimeException(failure);
              }
            }, 100, 20000);
            assertTrue(promoted.isFileClosed(file));
            assertArrayEquals(Arrays.copyOf(original, original.length - 1),
                DFSTestUtil.readFileBuffer(promoted, file));
            cluster.getNameNode(1).getRpcServer().rollEditLog();
            cluster.getNamesystem(0).getEditLogTailer().doTailEdits();
            for (int index = 0; index < 2; index++) {
              if (keepSnapshot) {
                assertNotNull(cluster.getNamesystem(index).getBlockManager()
                    .getStoredBlock(source.getLocalBlock()));
              } else {
                assertNull(cluster.getNamesystem(index).getBlockManager()
                    .getStoredBlock(source.getLocalBlock()),
                    "Replaying CLOSE must release an unreferenced source"
                        + " on node " + index);
              }
            }
            if (keepSnapshot) {
              promoted.deleteSnapshot(parent, "only");
              cluster.getNameNode(1).getRpcServer().rollEditLog();
              cluster.getNamesystem(0).getEditLogTailer().doTailEdits();
              for (int index = 0; index < 2; index++) {
                assertNull(cluster.getNamesystem(index).getBlockManager()
                    .getStoredBlock(source.getLocalBlock()));
              }
            }
          }
        }
      } finally {
        for (DataNode dn : cluster.getDataNodes()) {
          DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
        }
      }
    } finally {
      qjm.shutdown();
    }
  }
}
