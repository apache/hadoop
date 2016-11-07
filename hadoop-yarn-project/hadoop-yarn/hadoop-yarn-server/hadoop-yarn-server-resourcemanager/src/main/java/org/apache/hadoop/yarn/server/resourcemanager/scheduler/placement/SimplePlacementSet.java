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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A simple PlacementSet which keeps an unordered map
 */
public class SimplePlacementSet<N extends SchedulerNode>
    implements PlacementSet<N> {

  private Map<NodeId, N> map;
  private String partition;

  public SimplePlacementSet(N node) {
    if (null != node) {
      // Only one node in the initial PlacementSet
      this.map = ImmutableMap.of(node.getNodeID(), node);
      this.partition = node.getPartition();
    } else {
      this.map = Collections.emptyMap();
      this.partition = NodeLabel.DEFAULT_NODE_LABEL_PARTITION;
    }
  }

  public SimplePlacementSet(Map<NodeId, N> map, String partition) {
    this.map = map;
    this.partition = partition;
  }

  @Override
  public Map<NodeId, N> getAllNodes() {
    return map;
  }

  @Override
  public long getVersion() {
    return 0L;
  }

  @Override
  public String getPartition() {
    return partition;
  }
}
