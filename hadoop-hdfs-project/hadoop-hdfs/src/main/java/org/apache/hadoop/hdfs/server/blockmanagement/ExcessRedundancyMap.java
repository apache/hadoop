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

import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Maps a datnode to the set of excess redundancy details.
 *
 * This class is thread safe.
 */
class ExcessRedundancyMap {
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private final Map<String, LightWeightHashSet<BlockInfo>> map =new HashMap<>();
  private final AtomicLong size = new AtomicLong(0L);

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
  synchronized int getSize4Testing(String dnUuid) {
    final LightWeightHashSet<BlockInfo> set = map.get(dnUuid);
    return set == null? 0: set.size();
  }

  synchronized void clear() {
    map.clear();
    size.set(0L);
  }

  /**
   * @return does this map contains a redundancy corresponding to the given
   *         datanode and the given block?
   */
  synchronized boolean contains(DatanodeDescriptor dn, BlockInfo blk) {
    final LightWeightHashSet<BlockInfo> set = map.get(dn.getDatanodeUuid());
    return set != null && set.contains(blk);
  }

  /**
   * Add the redundancy of the given block stored in the given datanode to the
   * map.
   *
   * @return true if the block is added.
   */
  synchronized boolean add(DatanodeDescriptor dn, BlockInfo blk) {
    LightWeightHashSet<BlockInfo> set = map.get(dn.getDatanodeUuid());
    if (set == null) {
      set = new LightWeightHashSet<>();
      map.put(dn.getDatanodeUuid(), set);
    }
    final boolean added = set.add(blk);
    if (added) {
      size.incrementAndGet();
      blockLog.debug("BLOCK* ExcessRedundancyMap.add({}, {})", dn, blk);
    }
    return added;
  }

  /**
   * Remove the redundancy corresponding to the given datanode and the given
   * block.
   *
   * @return true if the block is removed.
   */
  synchronized boolean remove(DatanodeDescriptor dn, BlockInfo blk) {
    final LightWeightHashSet<BlockInfo> set = map.get(dn.getDatanodeUuid());
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
  }
}
