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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * Helper class used by the CacheReplicationManager and CacheReplicationMonitor
 * to select datanodes where blocks should be cached or uncached.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class CacheReplicationPolicy {

  /**
   * @return List of datanodes with sufficient capacity to cache the block
   */
  private static List<DatanodeDescriptor> selectSufficientCapacity(Block block,
      List<DatanodeDescriptor> targets) {
    List<DatanodeDescriptor> sufficient =
        new ArrayList<DatanodeDescriptor>(targets.size());
    for (DatanodeDescriptor dn: targets) {
      long remaining = dn.getCacheRemaining();
      if (remaining >= block.getNumBytes()) {
        sufficient.add(dn);
      }
    }
    return sufficient;
  }

  /**
   * Returns a random datanode from targets, weighted by the amount of free
   * cache capacity on the datanode. Prunes unsuitable datanodes from the
   * targets list.
   * 
   * @param block Block to be cached
   * @param targets List of potential cache targets
   * @return a random DN, or null if no datanodes are available or have enough
   *         cache capacity.
   */
  private static DatanodeDescriptor randomDatanodeByRemainingCache(Block block,
      List<DatanodeDescriptor> targets) {
    // Hold a lottery biased by the amount of free space to decide
    // who gets the block
    Collections.shuffle(targets);
    TreeMap<Long, DatanodeDescriptor> lottery =
        new TreeMap<Long, DatanodeDescriptor>();
    long totalCacheAvailable = 0;
    for (DatanodeDescriptor dn: targets) {
      long remaining = dn.getCacheRemaining();
      totalCacheAvailable += remaining;
      lottery.put(totalCacheAvailable, dn);
    }
    // Pick our lottery winner
    RandomData r = new RandomDataImpl();
    long winningTicket = r.nextLong(0, totalCacheAvailable - 1);
    Entry<Long, DatanodeDescriptor> winner = lottery.higherEntry(winningTicket);
    return winner.getValue();
  }

  /**
   * Chooses numTargets new cache replicas for a block from a list of targets.
   * Will return fewer targets than requested if not enough nodes are available.
   * 
   * @return List of target datanodes
   */
  static List<DatanodeDescriptor> chooseTargetsToCache(Block block,
      List<DatanodeDescriptor> targets, int numTargets) {
    List<DatanodeDescriptor> sufficient =
        selectSufficientCapacity(block, targets);
    List<DatanodeDescriptor> chosen =
        new ArrayList<DatanodeDescriptor>(numTargets);
    for (int i = 0; i < numTargets && !sufficient.isEmpty(); i++) {
      chosen.add(randomDatanodeByRemainingCache(block, sufficient));
    }
    return chosen;
  }

  /**
   * Given a list cache replicas where a block is cached, choose replicas to
   * uncache to drop the cache replication factor down to replication.
   * 
   * @param nodes list of datanodes where the block is currently cached
   * @param replication desired replication factor
   * @return List of datanodes to uncache
   */
  public static List<DatanodeDescriptor> chooseTargetsToUncache(
      List<DatanodeDescriptor> nodes, short replication) {
    final int effectiveReplication = nodes.size();
    List<DatanodeDescriptor> targets =
        new ArrayList<DatanodeDescriptor>(effectiveReplication);
    Collections.shuffle(nodes);
    final int additionalTargetsNeeded = effectiveReplication - replication;
    int chosen = 0;
    while (chosen < additionalTargetsNeeded && !nodes.isEmpty()) {
      targets.add(nodes.get(chosen));
      chosen++;
    }
    return targets;
  }

}
