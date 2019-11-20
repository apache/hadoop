/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_BACKUP_INODEFILTER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_BACKUP_INODEFILTER_KEY;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.partition;
import static org.apache.hadoop.util.ReflectionUtils.newInstance;

public class PhasedSyncMountSnapshotUpdateFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(PhasedSyncMountSnapshotUpdateFactory.class);
  private final SyncServiceFileFilter syncServiceFileFilter;

  private PhasedPlanFactory phasedPlanFactory;

  public PhasedSyncMountSnapshotUpdateFactory(Namesystem namesystem, BlockManager
      blockManager, Configuration conf) {
    Class<? extends SyncServiceFileFilter> backupPolicyINodeFilterClass =
        conf.getClass(DFS_PROVIDED_BACKUP_INODEFILTER_KEY,
            DFS_PROVIDED_BACKUP_INODEFILTER_DEFAULT,
            SyncServiceFileFilter.class);
    this.syncServiceFileFilter = newInstance(backupPolicyINodeFilterClass,
        conf);
    FilePlanner filePlanner = new FilePlanner(namesystem, blockManager);
    DirectoryPlanner directoryPlanner = new DirectoryPlanner(filePlanner,
        namesystem.getFSDirectory(), syncServiceFileFilter);
    this.phasedPlanFactory = new PhasedPlanFactory(filePlanner,
        directoryPlanner, conf);
  }

  public PhasedPlan createPlanFromDiffReport(SyncMount syncMount,
      SnapshotDiffReport diffReport, Optional<Integer> sourceSnapshotId,
      int targetSnapshotId) {

    LOG.info("Creating phased plan for SyncMount {} and targetSnapshotId {}",
        syncMount, targetSnapshotId);

    PartitionedDiffReport partitionedDiffReport = partition(diffReport,
        this.syncServiceFileFilter);

    PhasedPlan phasedPlan = phasedPlanFactory.createFromPartitionedDiffReport(
        partitionedDiffReport, syncMount, diffReport.getFromSnapshot(),
        sourceSnapshotId, targetSnapshotId);

    return phasedPlan;

  }

}
