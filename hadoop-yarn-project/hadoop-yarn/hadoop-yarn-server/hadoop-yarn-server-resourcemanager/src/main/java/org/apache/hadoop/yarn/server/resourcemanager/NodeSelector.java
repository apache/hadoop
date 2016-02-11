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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.Collection;
import java.util.List;

/**
 * Simple Node selector interface contractually obligating the implementor to
 * provide the caller with an ordered list of nodes. It also provides
 * convenience methods to specify criterion
 */
public interface NodeSelector {

  /**
   * SelectionHint allows callers to provide additional suggestions to be
   * used for selection
   */
  class SelectionHint {

    private final NodeId[] nodeIds;

    // minimum number of nodes to include from the Hint in the returned list
    private final int minToInclude;

    public SelectionHint(Collection<NodeId> nodeIds,
        int minNodesToInclude) {
      this.nodeIds = nodeIds.toArray(new NodeId[0]);
      this.minToInclude = minNodesToInclude;
    }

    public NodeId[] getNodeIds() {
      return nodeIds;
    }

    public int getMinToInclude() {
      return minToInclude;
    }

  }

  /**
   * Select an ordered list of Nodes based on the Implementation
   * @return Ordered list of Nodes
   */
  List<NodeId> selectNodes();

  /**
   * Select an ordered list of Nodes based on the Implementation. Also
   * allows callers to specify some hints in terms of specific node or
   * list of nodes (as well as a how many from the list is needed)
   * @return Ordered list of Nodes
   */
  List<NodeId> selectNodes(Collection<SelectionHint> hints);

}
