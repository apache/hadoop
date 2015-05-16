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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Time;

import java.io.File;
import java.util.*;

/**
 * An implementation of RamDiskReplicaTracker that uses an LRU
 * eviction scheme.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RamDiskReplicaLruTracker extends RamDiskReplicaTracker {

  private class RamDiskReplicaLru extends RamDiskReplica {
    long lastUsedTime;

    private RamDiskReplicaLru(String bpid, long blockId,
                              FsVolumeImpl ramDiskVolume,
                              long lockedBytesReserved) {
      super(bpid, blockId, ramDiskVolume, lockedBytesReserved);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }
  }

  /**
   * Map of blockpool ID to <map of blockID to ReplicaInfo>.
   */
  Map<String, Map<Long, RamDiskReplicaLru>> replicaMaps;

  /**
   * Queue of replicas that need to be written to disk.
   * Stale entries are GC'd by dequeueNextReplicaToPersist.
   */
  Queue<RamDiskReplicaLru> replicasNotPersisted;

  /**
   * Map of persisted replicas ordered by their last use times.
   */
  TreeMultimap<Long, RamDiskReplicaLru> replicasPersisted;

  RamDiskReplicaLruTracker() {
    replicaMaps = new HashMap<>();
    replicasNotPersisted = new LinkedList<>();
    replicasPersisted = TreeMultimap.create();
  }

  @Override
  synchronized void addReplica(final String bpid, final long blockId,
                               final FsVolumeImpl transientVolume,
                               long lockedBytesReserved) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    if (map == null) {
      map = new HashMap<>();
      replicaMaps.put(bpid, map);
    }
    RamDiskReplicaLru ramDiskReplicaLru =
        new RamDiskReplicaLru(bpid, blockId, transientVolume,
                              lockedBytesReserved);
    map.put(blockId, ramDiskReplicaLru);
    replicasNotPersisted.add(ramDiskReplicaLru);
  }

  @Override
  synchronized void touch(final String bpid,
                          final long blockId) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      return;
    }

    ramDiskReplicaLru.numReads.getAndIncrement();

    // Reinsert the replica with its new timestamp.
    if (replicasPersisted.remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru)) {
      ramDiskReplicaLru.lastUsedTime = Time.monotonicNow();
      replicasPersisted.put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
    }
  }

  @Override
  synchronized void recordStartLazyPersist(
      final String bpid, final long blockId, FsVolumeImpl checkpointVolume) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);
    ramDiskReplicaLru.setLazyPersistVolume(checkpointVolume);
  }

  @Override
  synchronized void recordEndLazyPersist(
      final String bpid, final long blockId, final File[] savedFiles) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      throw new IllegalStateException("Unknown replica bpid=" +
          bpid + "; blockId=" + blockId);
    }
    ramDiskReplicaLru.recordSavedBlockFiles(savedFiles);

    if (replicasNotPersisted.peek() == ramDiskReplicaLru) {
      // Common case.
      replicasNotPersisted.remove();
    } else {
      // Caller error? Fallback to O(n) removal.
      replicasNotPersisted.remove(ramDiskReplicaLru);
    }

    ramDiskReplicaLru.lastUsedTime = Time.monotonicNow();
    replicasPersisted.put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
    ramDiskReplicaLru.isPersisted = true;
  }

  @Override
  synchronized RamDiskReplicaLru dequeueNextReplicaToPersist() {
    while (replicasNotPersisted.size() != 0) {
      RamDiskReplicaLru ramDiskReplicaLru = replicasNotPersisted.remove();
      Map<Long, RamDiskReplicaLru> replicaMap =
          replicaMaps.get(ramDiskReplicaLru.getBlockPoolId());

      if (replicaMap != null && replicaMap.get(ramDiskReplicaLru.getBlockId()) != null) {
        return ramDiskReplicaLru;
      }

      // The replica no longer exists, look for the next one.
    }
    return null;
  }

  @Override
  synchronized void reenqueueReplicaNotPersisted(final RamDiskReplica ramDiskReplicaLru) {
    replicasNotPersisted.add((RamDiskReplicaLru) ramDiskReplicaLru);
  }

  @Override
  synchronized int numReplicasNotPersisted() {
    return replicasNotPersisted.size();
  }

  @Override
  synchronized RamDiskReplicaLru getNextCandidateForEviction() {
    final Iterator<RamDiskReplicaLru> it = replicasPersisted.values().iterator();
    while (it.hasNext()) {
      final RamDiskReplicaLru ramDiskReplicaLru = it.next();
      it.remove();

      Map<Long, RamDiskReplicaLru> replicaMap =
          replicaMaps.get(ramDiskReplicaLru.getBlockPoolId());

      if (replicaMap != null && replicaMap.get(ramDiskReplicaLru.getBlockId()) != null) {
        return ramDiskReplicaLru;
      }

      // The replica no longer exists, look for the next one.
    }
    return null;
  }

  /**
   * Discard any state we are tracking for the given replica. This could mean
   * the block is either deleted from the block space or the replica is no longer
   * on transient storage.
   *
   * @param deleteSavedCopies true if we should delete the saved copies on
   *                          persistent storage. This should be set by the
   *                          caller when the block is no longer needed.
   */
  @Override
  synchronized void discardReplica(
      final String bpid, final long blockId,
      boolean deleteSavedCopies) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);

    if (map == null) {
      return;
    }

    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      return;
    }

    if (deleteSavedCopies) {
      ramDiskReplicaLru.deleteSavedFiles();
    }

    map.remove(blockId);
    replicasPersisted.remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);

    // replicasNotPersisted will be lazily GC'ed.
  }

  @Override
  synchronized RamDiskReplica getReplica(
    final String bpid, final long blockId) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);

    if (map == null) {
      return null;
    }

    return map.get(blockId);
  }
}
