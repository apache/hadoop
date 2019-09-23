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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Reference of NodeAttribute in RM.
 */
public class RMNodeAttribute extends AbstractLabel {

  private NodeAttribute attribute;
  // TODO need to revisit whether we need to make this concurrent implementation
  private Map<String, AttributeValue> nodes = new HashMap<>();

  public RMNodeAttribute(NodeAttribute attribute) {
    this(attribute.getAttributeKey().getAttributeName(),
        Resource.newInstance(0, 0), 0, attribute);
  }

  public RMNodeAttribute(String labelName, Resource res, int activeNMs,
      NodeAttribute attribute) {
    super(labelName, res, activeNMs);
    this.attribute = attribute;
  }

  public NodeAttribute getAttribute() {
    return attribute;
  }

  public void setAttribute(NodeAttribute attribute) {
    this.attribute = attribute;
  }

  public NodeAttributeType getAttributeType() {
    return attribute.getAttributeType();
  }

  public void addNode(String node, AttributeValue attributeValue) {
    nodes.put(node, attributeValue);
  }

  public void removeNode(String node) {
    nodes.remove(node);
  }

  public Map<String, AttributeValue> getAssociatedNodeIds() {
    return new HashMap<String,  AttributeValue>(nodes);
  }

  @Override
  public int hashCode() {
    return attribute.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RMNodeAttribute other = (RMNodeAttribute) obj;
    if (attribute == null) {
      if (other.attribute != null) {
        return false;
      }
    } else if (!attribute.equals(other.attribute)) {
      return false;
    }
    return true;
  }
}