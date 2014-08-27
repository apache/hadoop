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


import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

class LazyWriteReplicaTracker {

  enum State {
    IN_MEMORY,
    LAZY_PERSIST_IN_PROGRESS,
    LAZY_PERSIST_COMPLETE,
  }

  static class ReplicaState implements Comparable<ReplicaState> {

    final String bpid;
    final long blockId;
    State state;

    /**
     * transient storage volume that holds the original replica.
     */
    final FsVolumeImpl transientVolume;

    /**
     * Persistent volume that holds or will hold the saved replica.
     */
    FsVolumeImpl lazyPersistVolume;
    File savedBlockFile;

    ReplicaState(final String bpid, final long blockId, FsVolumeImpl transientVolume) {
      this.bpid = bpid;
      this.blockId = blockId;
      this.transientVolume = transientVolume;
      state = State.IN_MEMORY;
      lazyPersistVolume = null;
      savedBlockFile = null;
    }

    @Override
    public int hashCode() {
      return bpid.hashCode() ^ (int) blockId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      ReplicaState otherState = (ReplicaState) other;
      return (otherState.bpid.equals(bpid) && otherState.blockId == blockId);
    }

    @Override
    public int compareTo(ReplicaState other) {
      if (blockId == other.blockId) {
        return 0;
      } else if (blockId < other.blockId) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  final FsDatasetImpl fsDataset;

  /**
   * Map of blockpool ID to map of blockID to ReplicaInfo.
   */
  final Map<String, Map<Long, ReplicaState>> replicaMaps;

  /**
   * A map of blockId to persist complete time for transient blocks. This allows
   * us to evict LRU blocks from transient storage. Protected by 'this'
   * Object lock.
   */
  final Map<ReplicaState, Long> persistTimeMap;

  LazyWriteReplicaTracker(final FsDatasetImpl fsDataset) {
    this.fsDataset = fsDataset;
    replicaMaps = new HashMap<String, Map<Long, ReplicaState>>();
    persistTimeMap = new HashMap<ReplicaState, Long>();
  }

  TreeMultimap<Long, ReplicaState> getLruMap() {
    // TODO: This can be made more efficient.
    TreeMultimap<Long, ReplicaState> reversedMap = TreeMultimap.create();
    for (Map.Entry<ReplicaState, Long> entry : persistTimeMap.entrySet()) {
      reversedMap.put(entry.getValue(), entry.getKey());
    }
    return reversedMap;
  }

  synchronized void addReplica(String bpid, long blockId,
                               final FsVolumeImpl transientVolume) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);
    if (map == null) {
      map = new HashMap<Long, ReplicaState>();
      replicaMaps.put(bpid, map);
    }
    map.put(blockId, new ReplicaState(bpid, blockId, transientVolume));
  }

  synchronized void recordStartLazyPersist(
      final String bpid, final long blockId, FsVolumeImpl checkpointVolume) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);
    ReplicaState replicaState = map.get(blockId);
    replicaState.state = State.LAZY_PERSIST_IN_PROGRESS;
    replicaState.lazyPersistVolume = checkpointVolume;
  }

  synchronized void recordEndLazyPersist(
      final String bpid, final long blockId, File savedBlockFile) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);
    ReplicaState replicaState = map.get(blockId);

    if (replicaState == null) {
      throw new IllegalStateException("Unknown replica bpid=" +
          bpid + "; blockId=" + blockId);
    }
    replicaState.state = State.LAZY_PERSIST_COMPLETE;
    replicaState.savedBlockFile = savedBlockFile;
    persistTimeMap.put(replicaState, System.currentTimeMillis() / 1000);
  }

  synchronized void discardReplica(
      final String bpid, final long blockId, boolean force) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);
    ReplicaState replicaState = map.get(blockId);

    if (replicaState == null) {
      if (force) {
        return;
      }
      throw new IllegalStateException("Unknown replica bpid=" +
          bpid + "; blockId=" + blockId);
    }

    if (replicaState.state != State.LAZY_PERSIST_COMPLETE && !force) {
      throw new IllegalStateException("Discarding replica without " +
          "saving it to disk bpid=" + bpid + "; blockId=" + blockId);

    }

    map.remove(blockId);
    persistTimeMap.remove(replicaState);
  }
}
