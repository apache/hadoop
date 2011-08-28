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

/**
 * <p>The request sent by the <code>ApplicationMaster</code> to 
 * <code>ResourceManager</code> on registration via the 
 * {@link AMRMProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)} 
 * api.</p>
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
 */
@Public
@Stable
public interface RegisterApplicationMasterRequest {

  /**
   * Get the {@link ApplicationAttemptId} being managed by the 
   * <code>ApplicationMaster</code>.
   * @return <code>ApplicationAttemptId</code> being managed by the 
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  ApplicationAttemptId getApplicationAttemptId();
  
  @Private
  @Unstable
  void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get the host on which the <code>ApplicationMaster</code> is running.
   * @return host on which the <code>ApplicationMaster</code> is running
   */
  @Public
  @Stable
  String getHost();
  
  @Private
  @Unstable
  void setHost(String host);

  /**
   * Get the RPC port on which the <code>ApplicationMaster</code> is responding. 
   * @return the RPC port on which the <code>ApplicationMaster</code> is 
   *         responding
   */
  @Public
  @Stable
  int getRpcPort();
  
  @Private
  @Unstable
  void setRpcPort(int port);

  /**
   * Get the tracking URL for the <code>ApplicationMaster</code>.
   * @return the tracking URL for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  String getTrackingUrl();
  
  @Private
  @Unstable
  void setTrackingUrl(String string);
}
