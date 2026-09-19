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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.slf4j.Logger;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Maps a datnode to the set of excess redundancy details.
 *
 * This class is thread safe.
 */
class ExcessRedundancyMap {
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private final Map<String, LightWeightHashSet<Block>> map = new HashMap<>();
  private final AtomicLong size = new AtomicLong(0L);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * @return the number of redundancies in this map.
   */
  long size() {
    return size.get();
  }

  /**
   * @return the number of redundancies corresponding to the given datanode.
   */
  @VisibleForTesting
  int getSize4Testing(String dnUuid) {
    lock.readLock().lock();
    try {
      final LightWeightHashSet<Block> set = map.get(dnUuid);
      return set == null? 0: set.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  void clear() {
    lock.writeLock().lock();
    try {
      map.clear();
      size.set(0L);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @return does this map contains a redundancy corresponding to the given
   *         datanode and the given block?
   */
  boolean contains(DatanodeDescriptor dn, BlockInfo blk) {
    lock.readLock().lock();
    try {
      final LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
      return set != null && set.contains(blk);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Add the redundancy of the given block stored in the given datanode to the
   * map.
   *
   * @return true if the block is added.
   */
  boolean add(DatanodeDescriptor dn, BlockInfo blk) {
    lock.writeLock().lock();
    try {
      LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
      if (set == null) {
        set = new LightWeightHashSet<>();
        map.put(dn.getDatanodeUuid(), set);
      }
      final boolean added = set.add(new ExcessBlockInfo(blk));
      if (added) {
        size.incrementAndGet();
        blockLog.debug("BLOCK* ExcessRedundancyMap.add({}, {})", dn, blk);
      }
      return added;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove the redundancy corresponding to the given datanode and the given
   * block.
   *
   * @return true if the block is removed.
   */
  boolean remove(DatanodeDescriptor dn, BlockInfo blk) {
    lock.writeLock().lock();
    try {
      final LightWeightHashSet<Block> set = map.get(dn.getDatanodeUuid());
      if (set == null) {
        return false;
      }
      final boolean removed = set.remove(blk);
      if (removed) {
        size.decrementAndGet();
        blockLog.debug("BLOCK* ExcessRedundancyMap.remove({}, {})", dn, blk);

        if (set.isEmpty()) {
          map.remove(dn.getDatanodeUuid());
        }
      }
      return removed;
    } finally {
      lock.writeLock().unlock();
    }
  }

  Map<String, LightWeightHashSet<Block>> getExcessRedundancyMap() {
    lock.readLock().lock();
    try {
      return map;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * An object that contains information about a block that is being excess redundancy.
   * It records the timestamp when added excess redundancy map of this block.
   */
  static class ExcessBlockInfo extends Block {
    private long timeStamp;
    private final BlockInfo blockInfo;

    ExcessBlockInfo(BlockInfo blockInfo) {
      super(blockInfo.getBlockId(), blockInfo.getNumBytes(), blockInfo.getGenerationStamp());
      this.timeStamp = monotonicNow();
      this.blockInfo = blockInfo;
    }

    public BlockInfo getBlockInfo() {
      return blockInfo;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = monotonicNow();
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }
}
