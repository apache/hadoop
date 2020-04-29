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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

/**
 * <p>
 * The request from clients to get nodes to attributes mapping
 * in the cluster from the <code>ResourceManager</code>.
 * </p>
 *
 * @see ApplicationClientProtocol#getNodesToAttributes
 * (GetNodesToAttributesRequest)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class GetNodesToAttributesRequest {

  public static GetNodesToAttributesRequest newInstance(Set<String> hostNames) {
    GetNodesToAttributesRequest request =
        Records.newRecord(GetNodesToAttributesRequest.class);
    request.setHostNames(hostNames);
    return request;
  }

  /**
   * Set hostnames for which mapping is required.
   *
   * @param hostnames
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public abstract void setHostNames(Set<String> hostnames);

  /**
   * Get hostnames for which mapping is required.
   *
   * @return Set of hostnames.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public abstract Set<String> getHostNames();
}
