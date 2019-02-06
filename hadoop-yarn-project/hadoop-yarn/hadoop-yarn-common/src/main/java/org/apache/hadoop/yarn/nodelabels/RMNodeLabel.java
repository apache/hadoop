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

package org.apache.hadoop.yarn.nodelabels;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Partition representation in RM.
 */
public class RMNodeLabel extends AbstractLabel
    implements Comparable<RMNodeLabel> {
  private boolean exclusive;
  private NodeLabel nodeLabel;
  private Set<NodeId> nodeIds;

  public RMNodeLabel(NodeLabel nodeLabel) {
    this(nodeLabel.getName(), Resource.newInstance(0, 0), 0,
        nodeLabel.isExclusive());
  }

  public RMNodeLabel(String labelName) {
    this(labelName, Resource.newInstance(0, 0), 0,
        NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }
  
  protected RMNodeLabel(String labelName, Resource res, int activeNMs,
      boolean exclusive) {
    super(labelName, res, activeNMs);
    this.exclusive = exclusive;
    this.nodeLabel = NodeLabel.newInstance(labelName, exclusive);
    nodeIds = new HashSet<NodeId>();
  }

  public void setIsExclusive(boolean exclusive) {
    this.exclusive = exclusive;
  }
  
  public boolean getIsExclusive() {
    return this.exclusive;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RMNodeLabel) {
      RMNodeLabel other = (RMNodeLabel) obj;
      return Resources.equals(getResource(), other.getResource())
          && StringUtils.equals(getLabelName(), other.getLabelName())
          && (other.getNumActiveNMs() == getNumActiveNMs());
    }
    return false;
  }


  public RMNodeLabel getCopy() {
    return new RMNodeLabel(getLabelName(), getResource(), getNumActiveNMs(),
        exclusive);
  }
  
  @Override
  public int hashCode() {
    final int prime = 502357;
    return (int) ((((long) getLabelName().hashCode() << 8)
        + (getResource().hashCode() << 4) + getNumActiveNMs()) % prime);
  }


  @Override
  public int compareTo(RMNodeLabel o) {
    // We should always put empty label entry first after sorting
    if (getLabelName().isEmpty() != o.getLabelName().isEmpty()) {
      if (getLabelName().isEmpty()) {
        return -1;
      }
      return 1;
    }
    
    return getLabelName().compareTo(o.getLabelName());
  }

  public NodeLabel getNodeLabel() {
    return this.nodeLabel;
  }

  public void addNodeId(NodeId node) {
    nodeIds.add(node);
  }

  public void removeNodeId(NodeId node) {
    nodeIds.remove(node);
  }

  public Set<NodeId> getAssociatedNodeIds() {
    return new HashSet<NodeId>(nodeIds);
  }
}