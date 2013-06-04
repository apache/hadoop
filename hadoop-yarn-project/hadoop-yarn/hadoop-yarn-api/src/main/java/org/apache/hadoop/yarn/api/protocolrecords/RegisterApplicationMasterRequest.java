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
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the <code>ApplicationMaster</code> to 
 * <code>ResourceManager</code> on registration.</p>
 * 
 * <p>The registration includes details such as:
 *   <ul>
 *     <li>
 *         {@link ApplicationAttemptId} being managed by the 
 *         <code>ApplicationMaster</code>
 *     </li>
 *     <li>Hostname on which the AM is running.</li>
 *     <li>RPC Port</li>
 *     <li>Tracking URL</li>
 *   </ul>
 * </p>
 * 
 * @see AMRMProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)
 */
@Public
@Stable
public abstract class RegisterApplicationMasterRequest {

  public static RegisterApplicationMasterRequest newInstance(
      ApplicationAttemptId applicationAttemptId, String host, int port,
      String trackingUrl) {
    RegisterApplicationMasterRequest request =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    request.setApplicationAttemptId(applicationAttemptId);
    request.setHost(host);
    request.setRpcPort(port);
    request.setTrackingUrl(trackingUrl);
    return request;
  }

  /**
   * Get the <code>ApplicationAttemptId</code> being managed by the 
   * <code>ApplicationMaster</code>.
   * @return <code>ApplicationAttemptId</code> being managed by the 
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract ApplicationAttemptId getApplicationAttemptId();
  
  /**
   * Set the <code>ApplicationAttemptId</code> being managed by the 
   * <code>ApplicationMaster</code>.
   * @param applicationAttemptId <code>ApplicationAttemptId</code> being managed  
   *                             by the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code> is 
   * running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code> is running
   */
  @Public
  @Stable
  public abstract String getHost();
  
  /**
   * Set the <em>host</em> on which the <code>ApplicationMaster</code> is 
   * running.
   * @param host <em>host</em> on which the <code>ApplicationMaster</code> 
   *             is running
   */
  @Private
  @Unstable
  public abstract void setHost(String host);

  /**
   * Get the <em>RPC port</em> on which the <code>ApplicationMaster</code> 
   * is responding. 
   * @return the <em>RPC port<em> on which the <code>ApplicationMaster</code> is 
   *         responding
   */
  @Public
  @Stable
  public abstract int getRpcPort();
  
  /**
   * Set the <em>RPC port<em> on which the <code>ApplicationMaster</code> is 
   * responding.
   * @param port <em>RPC port<em> on which the <code>ApplicationMaster</code> is 
   *             responding
   */
  @Public
  @Stable
  public abstract void setRpcPort(int port);

  /**
   * Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
   * @return <em>tracking URL</em> for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract String getTrackingUrl();
  
  /**
   * Set the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
   * @param trackingUrl <em>tracking URL</em> for the 
   *                    <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setTrackingUrl(String trackingUrl);
}
