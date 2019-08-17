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
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * a node attributes in cluster.
 * </p>
 *
 * @see ApplicationClientProtocol#getClusterNodeAttributes
 * (GetClusterNodeAttributesRequest)
 */
@Public
@Evolving
public abstract class GetClusterNodeAttributesResponse {

  /**
   * Create instance of GetClusterNodeAttributesResponse.
   *
   * @param attributes
   * @return GetClusterNodeAttributesResponse.
   */
  public static GetClusterNodeAttributesResponse newInstance(
      Set<NodeAttributeInfo> attributes) {
    GetClusterNodeAttributesResponse response =
        Records.newRecord(GetClusterNodeAttributesResponse.class);
    response.setNodeAttributes(attributes);
    return response;
  }

  /**
   * Set node attributes to the response.
   *
   * @param attributes Map of Node attributeKey to Type.
   */
  @Public
  @Unstable
  public abstract void setNodeAttributes(Set<NodeAttributeInfo> attributes);

  /**
   * Get node attributes from the response.
   *
   * @return Node attributes.
   */
  @Public
  @Unstable
  public abstract Set<NodeAttributeInfo> getNodeAttributes();
}
