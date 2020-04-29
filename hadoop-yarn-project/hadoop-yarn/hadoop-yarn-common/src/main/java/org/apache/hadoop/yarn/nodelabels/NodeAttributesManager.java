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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;

/**
 * This class captures all interactions for Attributes with RM.
 */
public abstract class NodeAttributesManager extends AbstractService {
  public NodeAttributesManager(String name) {
    super(name);
  }

  /**
   * To completely replace the mappings for a given node with the new Set of
   * Attributes which are under a given prefix. If the mapping contains an
   * attribute whose type does not match a previously existing Attribute
   * under the same prefix (name space) then exception is thrown.
   * Key would be name of the node and value would be set of Attributes to
   * be mapped. If the prefix is null, then all node attributes will be
   * replaced regardless of what prefix they have.
   *
   * @param prefix node attribute prefix
   * @param nodeAttributeMapping host name to a set of node attributes mapping
   * @throws IOException if failed to replace attributes
   */
  public abstract void replaceNodeAttributes(String prefix,
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException;

  /**
   * It adds or updates the attribute mapping for a given node with out
   * impacting other existing attribute mapping. Key would be name of the node
   * and value would be set of Attributes to be mapped.
   *
   * @param nodeAttributeMapping
   * @throws IOException
   */
  public abstract void addNodeAttributes(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException;

  /**
   * It removes the specified attribute mapping for a given node with out
   * impacting other existing attribute mapping. Key would be name of the node
   * and value would be set of Attributes to be removed.
   *
   * @param nodeAttributeMapping
   * @throws IOException
   */
  public abstract void removeNodeAttributes(
      Map<String, Set<NodeAttribute>> nodeAttributeMapping) throws IOException;

  /**
   * Returns a set of node attributes whose prefix is one of the given
   * prefixes; if the prefix set is null or empty, all attributes are returned;
   * if prefix set is given but no mapping could be found, an empty set
   * is returned.
   *
   * @param prefix set of prefix string's for which the attributes needs to
   *          returned
   * @return Set of node Attributes
   */
  public abstract Set<NodeAttribute> getClusterNodeAttributes(
      Set<String> prefix);

  /**
   * Return a map of Nodes to attribute value for the given NodeAttributeKeys.
   * If the attributeKeys set is null or empty, then mapping for all attributes
   * are returned.
   *
   * @return a Map of attributeKeys to a map of hostnames to its attribute
   *         values.
   */
  public abstract Map<NodeAttributeKey,
      Map<String, AttributeValue>> getAttributesToNodes(
      Set<NodeAttributeKey> attributes);

  /**
   * NodeAttribute to AttributeValue Map.
   *
   * @return Map of NodeAttribute to AttributeValue.
   */
  public abstract Map<NodeAttribute, AttributeValue> getAttributesForNode(
      String hostName);

  /**
   * Get All node to Attributes list based on filter.
   *
   * @return List of NodeToAttributes matching filter. If empty
   * or null is passed as argument will return all.
   */
  public abstract List<NodeToAttributes> getNodeToAttributes(
      Set<String> prefix);

  /**
   * Get all node to Attributes mapping.
   *
   * @return Map of String to Set of nodesToAttributes matching
   * filter. If empty or null is passed as argument will return all.
   */
  public abstract Map<String, Set<NodeAttribute>> getNodesToAttributes(
      Set<String> hostNames);

  // futuristic
  // public set<NodeId> getNodesMatchingExpression(String nodeLabelExp);

  /**
   * Refresh node attributes on a given node during RM recovery.
   * @param nodeId Node Id
   */
  public abstract void refreshNodeAttributesToScheduler(NodeId nodeId);
}
