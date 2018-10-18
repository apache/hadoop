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
package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * node to attribute value mapping for all or given set of Node AttributeKey's.
 * </p>
 *
 * @see ApplicationClientProtocol#getAttributesToNodes
 *      (GetAttributesToNodesRequest)
 */
@Public
@Evolving
public abstract class GetAttributesToNodesResponse {
  public static GetAttributesToNodesResponse newInstance(
      Map<NodeAttributeKey, List<NodeToAttributeValue>> map) {
    GetAttributesToNodesResponse response =
        Records.newRecord(GetAttributesToNodesResponse.class);
    response.setAttributeToNodes(map);
    return response;
  }

  @Public
  @Evolving
  public abstract void setAttributeToNodes(
      Map<NodeAttributeKey, List<NodeToAttributeValue>> map);

  /**
   * Get mapping of NodeAttributeKey to its associated mapping of list of
   * NodeToAttributeValue associated with attribute.
   *
   * @return Map of node attributes to list of NodeToAttributeValue.
   */
  @Public
  @Evolving
  public abstract Map<NodeAttributeKey,
      List<NodeToAttributeValue>> getAttributesToNodes();
}
