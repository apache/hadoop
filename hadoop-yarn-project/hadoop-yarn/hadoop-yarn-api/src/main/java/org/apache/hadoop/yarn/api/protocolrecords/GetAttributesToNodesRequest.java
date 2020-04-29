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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The request from clients to get node to attribute value mapping for all or
 * given set of Node AttributeKey's in the cluster from the
 * <code>ResourceManager</code>.
 * </p>
 *
 * @see ApplicationClientProtocol#getAttributesToNodes
 *      (GetAttributesToNodesRequest)
 */
@Public
@Evolving
public abstract class GetAttributesToNodesRequest {

  public static GetAttributesToNodesRequest newInstance() {
    return Records.newRecord(GetAttributesToNodesRequest.class);
  }

  public static GetAttributesToNodesRequest newInstance(
      Set<NodeAttributeKey> attributes) {
    GetAttributesToNodesRequest request =
        Records.newRecord(GetAttributesToNodesRequest.class);
    request.setNodeAttributes(attributes);
    return request;
  }

  /**
   * Set node attributeKeys for which the mapping of hostname to attribute value
   * is required.
   *
   * @param attributes Set of NodeAttributeKey provided.
   */
  @Public
  @Unstable
  public abstract void setNodeAttributes(Set<NodeAttributeKey> attributes);

  /**
   * Get node attributeKeys for which mapping of hostname to attribute value is
   * required.
   *
   * @return Set of NodeAttributeKey
   */
  @Public
  @Unstable
  public abstract Set<NodeAttributeKey> getNodeAttributes();
}
