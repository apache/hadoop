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

import static org.apache.hadoop.classification.InterfaceAudience.*;
import static org.apache.hadoop.classification.InterfaceStability.*;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

/**
 * <p>
 * The request from clients to get attribtues to nodes mapping
 * in the cluster from the <code>ResourceManager</code>.
 * </p>
 *
 * @see ApplicationClientProtocol#getAttributesToNodes
 * (GetAttributesToNodesRequest)
 */
@Public
@Evolving
public abstract class GetAttributesToNodesRequest {

  public static GetAttributesToNodesRequest newInstance() {
    return Records.newRecord(GetAttributesToNodesRequest.class);
  }

  public static GetAttributesToNodesRequest newInstance(
      Set<NodeAttribute> attributes) {
    GetAttributesToNodesRequest request =
        Records.newRecord(GetAttributesToNodesRequest.class);
    request.setNodeAttributes(attributes);
    return request;
  }

  /**
   * Set node attributes for which the mapping is required.
   *
   * @param attributes Set<NodeAttribute> provided.
   */
  @Public
  @Unstable
  public abstract void setNodeAttributes(Set<NodeAttribute> attributes);

  /**
   * Get node attributes for which mapping mapping is required.
   *
   * @return Set<NodeAttribute>
   */
  @Public
  @Unstable
  public abstract Set<NodeAttribute> getNodeAttributes();
}
