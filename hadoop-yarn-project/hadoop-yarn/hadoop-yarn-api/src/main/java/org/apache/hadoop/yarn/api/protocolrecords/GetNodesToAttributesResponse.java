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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * nodes to attributes mapping.
 * </p>
 *
 * @see ApplicationClientProtocol#getNodesToAttributes
 * (GetNodesToAttributesRequest)
 */
@Public
@Evolving
public abstract class GetNodesToAttributesResponse {

  public static GetNodesToAttributesResponse newInstance(
      Map<String, Set<NodeAttribute>> map) {
    GetNodesToAttributesResponse response =
        Records.newRecord(GetNodesToAttributesResponse.class);
    response.setNodeToAttributes(map);
    return response;
  }

  @Public
  @Evolving
  public abstract void setNodeToAttributes(Map<String, Set<NodeAttribute>> map);

  /**
   * Get hostnames to NodeAttributes mapping.
   *
   * @return Map of host to attributes.
   */
  @Public
  @Evolving
  public abstract Map<String, Set<NodeAttribute>> getNodeToAttributes();
}
