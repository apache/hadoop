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
 * <p>The finalization request sent by the <code>ApplicationMaster</code> to 
 * inform the <code>ResourceManager</code> about its completion via the
 * {@link AMRMProtocol#finishApplicationMaster(FinishApplicationMasterRequest)}
 * api.</p>
 * 
 * <p>The final request includes details such:
 *   <ul>
 *     <li>
 *         {@link ApplicationAttemptId} being managed by the 
 *         <code>ApplicationMaster</code>
 *     </li>
 *     <li>Final state of the <code>ApplicationMaster</code></li>
 *     <li>
 *       Diagnostic information in case of failure of the
 *       <code>ApplicationMaster</code>
 *     </li>
 *     <li>Tracking URL</li>
 *   </ul>
 * </p>
 *
 */
public interface FinishApplicationMasterRequest {

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
  void setAppAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get final state of the <code>ApplicationMaster</code>.
   * @return final state of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  String getFinalState();
  
  @Private
  @Unstable
  void setFinalState(String string);

  /**
   * Get diagnostic information if the application failed.
   * @return diagnostic information if the application failed
   */
  @Public
  @Stable
  String getDiagnostics();
  
  @Private
  @Unstable
  void setDiagnostics(String string);

  /**
   * Get the tracking URL for the <code>ApplicationMaster</code>.
   * @return the tracking URL for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  String getTrackingUrl();
  
  @Private
  @Unstable
  void setTrackingUrl(String historyUrl);

}
