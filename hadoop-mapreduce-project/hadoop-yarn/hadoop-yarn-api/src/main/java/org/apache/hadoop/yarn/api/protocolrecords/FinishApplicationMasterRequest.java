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
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

/**
 * <p>The finalization request sent by the <code>ApplicationMaster</code> to
 * inform the <code>ResourceManager</code> about its completion.</p>
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
 * @see AMRMProtocol#finishApplicationMaster(FinishApplicationMasterRequest)
 */
public interface FinishApplicationMasterRequest {

  /**
   * Get the <code>ApplicationAttemptId</code> being managed by the
   * <code>ApplicationMaster</code>.
   * @return <code>ApplicationAttemptId</code> being managed by the
   *         <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * Set the <code>ApplicationAttemptId</code> being managed by the
   * <code>ApplicationMaster</code>.
   * @param applicationAttemptId <code>ApplicationAttemptId</code> being managed
   *                             by the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  void setAppAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get <em>final state</em> of the <code>ApplicationMaster</code>.
   * @return <em>final state</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * Set the <em>finish state</em> of the <code>ApplicationMaster</code>
   * @param finishState <em>finish state</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  void setFinishApplicationStatus(FinalApplicationStatus finishState);

  /**
   * Get <em>diagnostic information</em> on application failure.
   * @return <em>diagnostic information</em> on application failure
   */
  @Public
  @Stable
  String getDiagnostics();

  /**
   * Set <em>diagnostic information</em> on application failure.
   * @param diagnostics <em>diagnostic information</em> on application failure
   */
  @Public
  @Stable
  void setDiagnostics(String diagnostics);

  /**
   * Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
   * @return <em>tracking URL</em>for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  String getTrackingUrl();

  /**
   * Set the <em>tracking URL</em>for the <code>ApplicationMaster</code>
   * @param url <em>tracking URL</em>for the
   *                   <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  void setTrackingUrl(String url);

}
