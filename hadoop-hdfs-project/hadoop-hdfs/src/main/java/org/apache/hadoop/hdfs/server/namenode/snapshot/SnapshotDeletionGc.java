/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager.DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS_DEFAULT;

public class SnapshotDeletionGc {
  public static final Logger LOG = LoggerFactory.getLogger(
      SnapshotDeletionGc.class);

  private final FSNamesystem namesystem;
  private final long deletionOrderedGcPeriodMs;
  private final AtomicReference<Timer> timer = new AtomicReference<>();

  public SnapshotDeletionGc(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;

    this.deletionOrderedGcPeriodMs = conf.getLong(
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS,
        DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS_DEFAULT);
    LOG.info("{} = {}", DFS_NAMENODE_SNAPSHOT_DELETION_ORDERED_GC_PERIOD_MS,
        deletionOrderedGcPeriodMs);
  }

  public void schedule() {
    if (timer.get() != null) {
      return;
    }
    final Timer t = new Timer(getClass().getSimpleName(), true);
    if (timer.compareAndSet(null, t)) {
      LOG.info("Schedule at fixed rate {}",
          StringUtils.formatTime(deletionOrderedGcPeriodMs));
      t.scheduleAtFixedRate(new GcTask(),
          deletionOrderedGcPeriodMs, deletionOrderedGcPeriodMs);
    }
  }

  public void cancel() {
    final Timer t = timer.getAndSet(null);
    if (t != null) {
      LOG.info("cancel");
      t.cancel();
    }
  }

  private void gcDeletedSnapshot(String name) {
    final Snapshot.Root deleted;
    namesystem.readLock();
    try {
      deleted = namesystem.getSnapshotManager().chooseDeletedSnapshot();
    } catch (Throwable e) {
      LOG.error("Failed to chooseDeletedSnapshot", e);
      throw e;
    } finally {
      namesystem.readUnlock("gcDeletedSnapshot");
    }
    if (deleted == null) {
      LOG.trace("{}: no snapshots are marked as deleted.", name);
      return;
    }

    final String snapshotRoot = deleted.getRootFullPathName();
    final String snapshotName = deleted.getLocalName();
    LOG.info("{}: delete snapshot {} from {}",
        name, snapshotName, snapshotRoot);

    try {
      namesystem.gcDeletedSnapshot(snapshotRoot, snapshotName);
    } catch (Throwable e) {
      LOG.error("Failed to gcDeletedSnapshot " + deleted.getFullPathName(), e);
    }
  }

  private class GcTask extends TimerTask {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void run() {
      final int id = count.incrementAndGet();
      gcDeletedSnapshot(getClass().getSimpleName() + " #" + id);
    }
  }
}
