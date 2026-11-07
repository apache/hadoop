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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapterMockitoUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

/** Rollback images must be saved at a segment boundary, including RPC tailing. */
@Timeout(120)
public class TestAsyncPrepareRollbackBoundary {
  @TempDir
  private java.nio.file.Path directory;

  @Test
  public void testAsyncPrepareUsesFinalizedSegmentBoundary() throws Exception {
    checkRollback(Long.MAX_VALUE);
  }

  @Test
  public void testAsyncPrepareBoundaryWithSingleTxnBatches() throws Exception {
    checkRollback(1);
  }

  private void checkRollback(long batch) throws Exception {
    try (Fixture f = new Fixture(directory, true, batch)) {
      Path before = new Path("/before-prepare");
      Path after = new Path("/after-prepare");
      Path newWrite = new Path("/after-rollback");
      try (DistributedFileSystem writer = client(f.dfs.getURI(0), f.conf)) {
        assertTrue(writer.mkdirs(before));
        writer.rollingUpgrade(RollingUpgradeAction.PREPARE);
        awaitRollbackImage(writer);
        f.assertAlignedImage(0);
        f.assertAlignedImage(1);
        assertTrue(writer.mkdirs(after));
      }
      f.dfs.shutdownNameNode(1);
      // Isolate this bug from the independent stale JN RPC-cache rollback bug.
      // Both NNs used RPC tailing during PREPARE; rollback uses finalized streams.
      f.dfs.getConfiguration(0).setBoolean(
          DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, false);
      f.dfs.restartNameNode(0, true, "-rollingUpgrade", "rollback");
      f.dfs.transitionToActive(0);
      try (DistributedFileSystem rolled = client(f.dfs.getURI(0), f.conf)) {
        assertTrue(rolled.exists(before));
        assertFalse(rolled.exists(after));
        assertTrue(rolled.mkdirs(newWrite));
      }
      f.dfs.restartNameNode(0);
      f.dfs.transitionToActive(0);
      try (DistributedFileSystem restarted = client(f.dfs.getURI(0), f.conf)) {
        assertTrue(restarted.exists(before));
        assertFalse(restarted.exists(after));
        assertTrue(restarted.exists(newWrite));
      }
    }
  }

  @Test
  public void testFailoverWhileRollbackImageIsPending() throws Exception {
    checkPendingCheckpoint(false);
  }

  @Test
  public void testRestartWhileRollbackImageIsPending() throws Exception {
    checkPendingCheckpoint(true);
  }

  private void checkPendingCheckpoint(boolean restart) throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Fixture f = new Fixture(directory, true, Long.MAX_VALUE);
    try {
      FSImage image = NameNodeAdapterMockitoUtil.spyOnFsImage(f.dfs.getNameNode(1));
      doAnswer(call -> {
        Canceler canceler = call.getArgument(2);
        entered.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!canceler.isCancelled() && !release.await(100, TimeUnit.MILLISECONDS)) {
          if (System.nanoTime() >= deadline) {
            release.countDown();
            throw new IOException("Timed out waiting for checkpoint cancellation");
          }
        }
        // Let the real saver observe cancellation and throw its own exception.
        return call.callRealMethod();
      }).when(image).saveNamespace(any(FSNamesystem.class),
          eq(NameNodeFile.IMAGE_ROLLBACK), any(Canceler.class));
      Path later = new Path("/after-pinned-boundary");
      try (DistributedFileSystem writer = client(f.dfs.getURI(0), f.conf)) {
        writer.rollingUpgrade(RollingUpgradeAction.PREPARE);
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        assertFalse(image.hasRollbackFSImage());
        assertTrue(writer.mkdirs(later));
        if (restart) {
          f.dfs.restartNameNode(1);
          awaitRollbackImage(writer);
          f.assertAlignedImage(0);
          f.assertAlignedImage(1);
        }
        f.dfs.transitionToStandby(0);
        f.dfs.transitionToActive(1);
      }
      try (DistributedFileSystem promoted = client(f.dfs.getURI(1), f.conf)) {
        assertTrue(promoted.exists(later), "Failover catch-up must pass the image boundary");
        assertTrue(promoted.mkdirs(new Path("/new-active-write")));
      }
    } finally {
      release.countDown();
      f.close();
    }
  }

  @Test
  public void testObserverDoesNotWaitForRollbackImage() throws Exception {
    checkNonCheckpointing(true);
  }

  @Test
  public void testDisabledCheckpointerDoesNotWaitForRollbackImage() throws Exception {
    checkNonCheckpointing(false);
  }

  private void checkNonCheckpointing(boolean observer) throws Exception {
    try (Fixture f = new Fixture(directory, observer, Long.MAX_VALUE)) {
      if (observer) {
        f.dfs.transitionToObserver(1);
      }
      Path later = new Path("/after-prepare-without-checkpointer");
      try (DistributedFileSystem writer = client(f.dfs.getURI(0), f.conf)) {
        writer.rollingUpgrade(RollingUpgradeAction.PREPARE);
        assertTrue(writer.mkdirs(later));
      }
      GenericTestUtils.waitFor(() -> hasInode(f.dfs.getNamesystem(1), later), 100, 10000);
      assertFalse(f.dfs.getNamesystem(1).getFSImage().hasRollbackFSImage());
      if (observer) {
        assertEquals(HAServiceState.OBSERVER, f.dfs.getNamesystem(1).getState());
      } else {
        f.dfs.transitionToStandby(0);
        f.dfs.transitionToActive(1);
        try (DistributedFileSystem promoted = client(f.dfs.getURI(1), f.conf)) {
          assertTrue(promoted.exists(later));
        }
      }
    }
  }

  private static boolean hasInode(FSNamesystem ns, Path path) {
    ns.readLock(RwLockMode.GLOBAL);
    try {
      return ns.getFSDirectory().getINode(path.toString()) != null;
    } catch (IOException failure) {
      throw new RuntimeException(failure);
    } finally {
      ns.readUnlock(RwLockMode.GLOBAL, "testPrepareTailing");
    }
  }

  private static void awaitRollbackImage(DistributedFileSystem fs) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        RollingUpgradeInfo info = fs.rollingUpgrade(RollingUpgradeAction.QUERY);
        return info != null && info.createdRollbackImages();
      } catch (IOException failure) {
        throw new RuntimeException(failure);
      }
    }, 100, 15000);
  }

  private static DistributedFileSystem client(URI uri, Configuration conf) throws Exception {
    return UserGroupInformation.createRemoteUser(
        UserGroupInformation.getCurrentUser().getUserName()).doAs(
            (PrivilegedExceptionAction<DistributedFileSystem>)
                () -> (DistributedFileSystem) FileSystem.newInstance(uri, conf));
  }

  private static final class Fixture implements AutoCloseable {
    private final Configuration conf = new HdfsConfiguration();
    private final MiniQJMHACluster cluster;
    private final MiniDFSCluster dfs;

    private Fixture(java.nio.file.Path dir, boolean checkpoints, long batch) throws Exception {
      conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      conf.setBoolean(DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, checkpoints);
      conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
      conf.setLong(EditLogTailer.DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY, batch);
      cluster = new MiniQJMHACluster.Builder(conf, dir.resolve("dfs").toFile())
          .baseDir(dir.resolve("journals").toString()).build();
      dfs = cluster.getDfsCluster();
      dfs.waitActive();
      dfs.transitionToActive(0);
    }

    private void assertAlignedImage(int nn) {
      File current = dfs.getNameNode(nn).getFSImage().getStorage()
          .getStorageDir(0).getCurrentDir();
      long txid = -1;
      for (File image : current.listFiles()) {
        if (image.getName().matches("fsimage_rollback_[0-9]+")) {
          txid = Math.max(txid, Long.parseLong(image.getName().substring(17)));
        }
      }
      assertTrue(txid >= 0, "Expected a rollback image");
      for (int i = 0; i < 3; i++) {
        boolean aligned = false;
        File journal = cluster.getJournalCluster().getCurrentDir(i, MiniQJMHACluster.NAMESERVICE);
        for (File segment : journal.listFiles()) {
          String name = segment.getName();
          if (name.matches("edits_[0-9]+-[0-9]+")) {
            aligned |= Long.parseLong(name.substring(name.indexOf('-') + 1)) == txid;
          }
        }
        assertTrue(aligned, "Rollback image must end at a finalized segment on JN " + i);
      }
    }

    @Override
    public void close() throws IOException {
      cluster.shutdown();
    }
  }
}
