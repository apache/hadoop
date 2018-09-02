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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;

/**
 * Event capturing details to store the Node Attributes in the backend store.
 */
public class NodeAttributesStoreEvent
    extends AbstractEvent<NodeAttributesStoreEventType> {
  private Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMapping;
  private AttributeMappingOperationType operation;

  public NodeAttributesStoreEvent(
      Map<String, Map<NodeAttribute, AttributeValue>> nodeAttributeMappingList,
      AttributeMappingOperationType operation) {
    super(NodeAttributesStoreEventType.STORE_ATTRIBUTES);
    this.nodeAttributeMapping = nodeAttributeMappingList;
    this.operation = operation;
  }

  public Map<String,
      Map<NodeAttribute, AttributeValue>> getNodeAttributeMappingList() {
    return nodeAttributeMapping;
  }

  public AttributeMappingOperationType getOperation() {
    return operation;
  }
}