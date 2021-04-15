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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * <p>
 * This class has the following functionality:
 *
 * <p>
 * ResourceUsageWithPartialShuffleMultiNodeLookupPolicy
 * holds sorted nodes list based on the
 * resource usage of nodes at given time.
 * Also inorder to prevent hot accessing node with multi-thread scheduling,
 * we add the partial shuffle with SHUFFLE_INTERVAL default is 10.
 * Se details YARN-10738.
 * </p>
 */
public class ResourceUsageWithPartialShuffleMultiNodeLookupPolicy
    <N extends SchedulerNode> implements MultiNodeLookupPolicy<N> {

  private Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  private Comparator<N> comparator;
  // Shuffle interval(the shuffle size of every shuffle).
  private static final int SHUFFLE_INTERVAL = 10;

  public ResourceUsageWithPartialShuffleMultiNodeLookupPolicy() {
    this.comparator = new Comparator<N>() {
      @Override
      public int compare(N o1, N o2) {
        int allocatedDiff = o1.getAllocatedResource()
            .compareTo(o2.getAllocatedResource());
        if (allocatedDiff == 0) {
          return o1.getNodeID().compareTo(o2.getNodeID());
        }
        return allocatedDiff;
      }
    };
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    Iterator<N> beforePartialShuffle =
        getNodesPerPartition(partition).iterator();
    int counter = 0;
    List<N> list = new ArrayList<N>();
    while(beforePartialShuffle.hasNext()) {
      list.add(beforePartialShuffle.next());
      // Every shuffle interval(the shuffle size of every shuffle),
      // we should shuffle to prevent
      // hot accessing node when multi scheduling.
      // It's very important for big clusters.
      if (counter > 0 && counter % SHUFFLE_INTERVAL == 0) {
        Collections.
            shuffle(list.subList(counter -10, counter));
      }
      ++counter;
    }
    return list.iterator();
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    Set<N> nodeList = new ConcurrentSkipListSet<N>(comparator);
    nodeList.addAll(nodes);
    nodesPerPartition.put(partition, Collections.unmodifiableSet(nodeList));
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }

  public Map<String, Set<N>> getNodesPerPartition() {
    return nodesPerPartition;
  }

  public Comparator<N> getComparator() {
    return comparator;
  }
}
