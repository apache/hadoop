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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.util.Records;

@Private
@Unstable
public abstract class RefreshNodesRequest {
  @Private
  @Stable
  public static RefreshNodesRequest newInstance() {
    RefreshNodesRequest request = Records.newRecord(RefreshNodesRequest.class);
    return request;
  }

  @Private
  @Unstable
  public static RefreshNodesRequest newInstance(
      DecommissionType decommissionType) {
    RefreshNodesRequest request = Records.newRecord(RefreshNodesRequest.class);
    request.setDecommissionType(decommissionType);
    return request;
  }

  @Private
  @Unstable
  public static RefreshNodesRequest newInstance(
      DecommissionType decommissionType, Integer timeout) {
    RefreshNodesRequest request = Records.newRecord(RefreshNodesRequest.class);
    request.setDecommissionType(decommissionType);
    request.setDecommissionTimeout(timeout);
    return request;
  }

  /**
   * Set the DecommissionType
   * 
   * @param decommissionType
   */
  public abstract void setDecommissionType(DecommissionType decommissionType);

  /**
   * Get the DecommissionType
   * 
   * @return decommissionType
   */
  public abstract DecommissionType getDecommissionType();

  /**
   * Set the DecommissionTimeout.
   *
   * @param timeout graceful decommission timeout in seconds
   */
  public abstract void setDecommissionTimeout(Integer timeout);

  /**
   * Get the DecommissionTimeout.
   *
   * @return decommissionTimeout
   */
  public abstract Integer getDecommissionTimeout();
}