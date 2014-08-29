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


import com.google.common.collect.TreeMultimap;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.io.File;
import java.util.*;

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
    final FsVolumeSpi transientVolume;

    /**
     * Persistent volume that holds or will hold the saved replica.
     */
    FsVolumeImpl lazyPersistVolume;
    File savedBlockFile;

    ReplicaState(final String bpid, final long blockId, FsVolumeSpi transientVolume) {
      this.bpid = bpid;
      this.blockId = blockId;
      this.transientVolume = transientVolume;
      state = State.IN_MEMORY;
      lazyPersistVolume = null;
      savedBlockFile = null;
    }

    @Override
    public String toString() {
      return "[Bpid=" + bpid + ";blockId=" + blockId + "]";
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
   * Queue of replicas that need to be written to disk.
   */
  final Queue<ReplicaState> replicasNotPersisted;

  /**
   * A map of blockId to persist complete time for transient blocks. This allows
   * us to evict LRU blocks from transient storage. Protected by 'this'
   * Object lock.
   */
  final Map<ReplicaState, Long> replicasPersisted;

  LazyWriteReplicaTracker(final FsDatasetImpl fsDataset) {
    this.fsDataset = fsDataset;
    replicaMaps = new HashMap<String, Map<Long, ReplicaState>>();
    replicasNotPersisted = new LinkedList<ReplicaState>();
    replicasPersisted = new HashMap<ReplicaState, Long>();
  }

  TreeMultimap<Long, ReplicaState> getLruMap() {
    // TODO: This can be made more efficient.
    TreeMultimap<Long, ReplicaState> reversedMap = TreeMultimap.create();
    for (Map.Entry<ReplicaState, Long> entry : replicasPersisted.entrySet()) {
      reversedMap.put(entry.getValue(), entry.getKey());
    }
    return reversedMap;
  }

  synchronized void addReplica(String bpid, long blockId,
                               final FsVolumeSpi transientVolume) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);
    if (map == null) {
      map = new HashMap<Long, ReplicaState>();
      replicaMaps.put(bpid, map);
    }
    ReplicaState replicaState = new ReplicaState(bpid, blockId, transientVolume);
    map.put(blockId, replicaState);
    replicasNotPersisted.add(replicaState);
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

    if (replicasNotPersisted.peek() == replicaState) {
      // Common case.
      replicasNotPersisted.remove();
    } else {
      // Should never occur in practice as lazy writer always persists
      // the replica at the head of the queue before moving to the next
      // one.
      replicasNotPersisted.remove(replicaState);
    }
    replicasPersisted.put(replicaState, System.currentTimeMillis() / 1000);
  }

  synchronized ReplicaState dequeueNextReplicaToPersist() {
    while (replicasNotPersisted.size() != 0) {
      ReplicaState replicaState = replicasNotPersisted.remove();
      Map<Long, ReplicaState> replicaMap = replicaMaps.get(replicaState.bpid);

      if (replicaMap != null && replicaMap.get(replicaState.blockId) != null) {
        return replicaState;
      }

      // The replica no longer exists, look for the next one.
    }
    return null;
  }

  synchronized void reenqueueReplica(final ReplicaState replicaState) {
    replicasNotPersisted.add(replicaState);
  }

  synchronized int numReplicasNotPersisted() {
    return replicasNotPersisted.size();
  }

  synchronized void discardReplica(
      final String bpid, final long blockId, boolean force) {
    Map<Long, ReplicaState> map = replicaMaps.get(bpid);

    if (map == null) {
      return;
    }

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
    replicasPersisted.remove(replicaState);

    // Leave the replica in replicasNotPersisted if its present.
    // dequeueNextReplicaToPersist will GC it eventually.
  }
}
