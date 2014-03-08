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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>ResourceManager</code> to the client for 
 * a request to get a new {@link ApplicationId} for submitting applications.</p>
 * 
 * <p>Clients can submit an application with the returned
 * {@link ApplicationId}.</p>
 *
 * @see ApplicationClientProtocol#getNewApplication(GetNewApplicationRequest)
 */
@Public
@Stable
public abstract class GetNewApplicationResponse {

  @Private
  @Unstable
  public static GetNewApplicationResponse newInstance(
      ApplicationId applicationId, Resource minCapability,
      Resource maxCapability) {
    GetNewApplicationResponse response =
        Records.newRecord(GetNewApplicationResponse.class);
    response.setApplicationId(applicationId);
    response.setMaximumResourceCapability(maxCapability);
    return response;
  }

  /**
   * Get the <em>new</em> <code>ApplicationId</code> allocated by the 
   * <code>ResourceManager</code>.
   * @return <em>new</em> <code>ApplicationId</code> allocated by the 
   *          <code>ResourceManager</code>
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();

  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * Get the maximum capability for any {@link Resource} allocated by the 
   * <code>ResourceManager</code> in the cluster.
   * @return maximum capability of allocated resources in the cluster
   */
  @Public
  @Stable
  public abstract Resource getMaximumResourceCapability();
  
  @Private
  @Unstable
  public abstract void setMaximumResourceCapability(Resource capability); 
}
