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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Fast and accurate class to tell how much space HDFS is using. This class gets
 * hdfs used space from FsDatasetImpl#volumeMap#ReplicaInfos that uses an in
 * memory way.
 *
 * Getting hdfs used space by ReplicaCachingGetSpaceUsed impl only includes
 * block and meta files, but DU impl is blockpool dir based statistics that will
 * include additional files, e.g. tmp dir, scanner.cursor file. Getting space
 * used by DU impl will be greater than by ReplicaCachingGetSpaceUsed impl, but
 * the latter is more accurate.
 *
 * Setting fs.getspaceused.classname to
 * org.apache.hadoop.hdfs.server.datanode.fsdataset
 * impl.ReplicaCachingGetSpaceUsed in your core-site.xml if we want to enable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaCachingGetSpaceUsed extends FSCachingGetSpaceUsed {
  static final Logger LOG =
      LoggerFactory.getLogger(ReplicaCachingGetSpaceUsed.class);

  private static final long DEEP_COPY_REPLICA_THRESHOLD_MS = 50;
  private static final long REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_MS = 1000;
  private final FsVolumeImpl volume;
  private final String bpid;

  public ReplicaCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
    setShouldFirstRefresh(false);
    volume = builder.getVolume();
    bpid = builder.getBpid();
  }

  @Override
  protected void refresh() {
    long start = Time.monotonicNow();
    long dfsUsed = 0;
    long count = 0;

    FsDatasetSpi fsDataset = volume.getDataset();
    try {
      Collection<ReplicaInfo> replicaInfos =
          (Collection<ReplicaInfo>) fsDataset.deepCopyReplica(bpid);
      long cost = Time.monotonicNow() - start;
      if (cost > DEEP_COPY_REPLICA_THRESHOLD_MS) {
        LOG.debug(
            "Copy replica infos, blockPoolId: {}, replicas size: {}, "
                + "duration: {}ms",
            bpid, replicaInfos.size(), Time.monotonicNow() - start);
      }

      if (CollectionUtils.isNotEmpty(replicaInfos)) {
        for (ReplicaInfo replicaInfo : replicaInfos) {
          if (Objects.equals(replicaInfo.getVolume().getStorageID(),
              volume.getStorageID())) {
            dfsUsed += replicaInfo.getBytesOnDisk();
            dfsUsed += replicaInfo.getMetadataLength();
            count++;
          }
        }
      }

      this.used.set(dfsUsed);
      cost = Time.monotonicNow() - start;
      if (cost > REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_MS) {
        LOG.debug(
            "Refresh dfs used, bpid: {}, replicas size: {}, dfsUsed: {} "
                + "on volume: {}, duration: {}ms",
            bpid, count, used, volume.getStorageID(),
            Time.monotonicNow() - start);
      }
    } catch (Exception e) {
      LOG.error("ReplicaCachingGetSpaceUsed refresh error", e);
    }
  }
}
