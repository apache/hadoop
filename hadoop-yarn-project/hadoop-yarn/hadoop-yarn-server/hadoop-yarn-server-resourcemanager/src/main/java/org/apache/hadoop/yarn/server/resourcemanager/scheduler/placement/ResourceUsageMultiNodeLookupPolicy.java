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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * <p>
 * This class has the following functionality:
 *
 * <p>
 * ResourceUsageMultiNodeLookupPolicy holds sorted nodes list based on the
 * resource usage of nodes at given time.
 * </p>
 */
public class ResourceUsageMultiNodeLookupPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N> {

  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  protected Comparator<N> comparator;

  public ResourceUsageMultiNodeLookupPolicy() {
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
    return getNodesPerPartition(partition).iterator();
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
}
