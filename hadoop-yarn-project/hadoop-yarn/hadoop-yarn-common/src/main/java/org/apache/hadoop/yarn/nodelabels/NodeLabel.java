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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

public class NodeLabel implements Comparable<NodeLabel> {
  private Resource resource;
  private int numActiveNMs;
  private String labelName;
  private Set<NodeId> nodeIds;

  public NodeLabel(String labelName) {
    this(labelName, Resource.newInstance(0, 0), 0);
  }
  
  protected NodeLabel(String labelName, Resource res, int activeNMs) {
    this.labelName = labelName;
    this.resource = res;
    this.numActiveNMs = activeNMs;
    this.nodeIds = new HashSet<NodeId>();
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

  public void addNode(Resource nodeRes) {
    Resources.addTo(resource, nodeRes);
    numActiveNMs++;
  }
  
  public void removeNode(Resource nodeRes) {
    Resources.subtractFrom(resource, nodeRes);
    numActiveNMs--;
  }

  public Resource getResource() {
    return this.resource;
  }

  public int getNumActiveNMs() {
    return numActiveNMs;
  }
  
  public String getLabelName() {
    return labelName;
  }
  
  public NodeLabel getCopy() {
    return new NodeLabel(labelName, resource, numActiveNMs);
  }
  
  @Override
  public int compareTo(NodeLabel o) {
    // We should always put empty label entry first after sorting
    if (labelName.isEmpty() != o.getLabelName().isEmpty()) {
      if (labelName.isEmpty()) {
        return -1;
      }
      return 1;
    }
    
    return labelName.compareTo(o.getLabelName());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NodeLabel) {
      NodeLabel other = (NodeLabel) obj;
      return Resources.equals(resource, other.getResource())
          && StringUtils.equals(labelName, other.getLabelName())
          && (other.getNumActiveNMs() == numActiveNMs); 
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    final int prime = 502357;
    return (int) ((((long) labelName.hashCode() << 8)
        + (resource.hashCode() << 4) + numActiveNMs) % prime);
  }
}