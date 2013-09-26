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
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The finalization request sent by the <code>ApplicationMaster</code> to
 * inform the <code>ResourceManager</code> about its completion.</p>
 *
 * <p>The final request includes details such:
 *   <ul>
 *     <li>Final state of the <code>ApplicationMaster</code></li>
 *     <li>
 *       Diagnostic information in case of failure of the
 *       <code>ApplicationMaster</code>
 *     </li>
 *     <li>Tracking URL</li>
 *   </ul>
 * </p>
 *
 * @see ApplicationMasterProtocol#finishApplicationMaster(FinishApplicationMasterRequest)
 */
@Public
@Stable
public abstract class FinishApplicationMasterRequest {

  @Public
  @Stable
  public static FinishApplicationMasterRequest newInstance(
      FinalApplicationStatus finalAppStatus, String diagnostics, String url) {
    FinishApplicationMasterRequest request =
        Records.newRecord(FinishApplicationMasterRequest.class);
    request.setFinalApplicationStatus(finalAppStatus);
    request.setDiagnostics(diagnostics);
    request.setTrackingUrl(url);
    return request;
  }

  /**
   * Get <em>final state</em> of the <code>ApplicationMaster</code>.
   * @return <em>final state</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract FinalApplicationStatus getFinalApplicationStatus();

  /**
   * Set the <em>final state</em> of the <code>ApplicationMaster</code>
   * @param finalState <em>final state</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setFinalApplicationStatus(FinalApplicationStatus finalState);

  /**
   * Get <em>diagnostic information</em> on application failure.
   * @return <em>diagnostic information</em> on application failure
   */
  @Public
  @Stable
  public abstract String getDiagnostics();

  /**
   * Set <em>diagnostic information</em> on application failure.
   * @param diagnostics <em>diagnostic information</em> on application failure
   */
  @Public
  @Stable
  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
   * This url if contains scheme then that will be used by resource manager
   * web application proxy otherwise it will default to http.
   * @return <em>tracking URL</em>for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract String getTrackingUrl();

  /**
   * Set the <em>final tracking URL</em>for the <code>ApplicationMaster</code>.
   * This is the web-URL to which ResourceManager or web-application proxy will
   * redirect client/users once the application is finished and the
   * <code>ApplicationMaster</code> is gone.
   * <p>
   * If the passed url has a scheme then that will be used by the
   * ResourceManager and web-application proxy, otherwise the scheme will
   * default to http.
   * </p>
   * <p>
   * Empty, null, "N/A" strings are all valid besides a real URL. In case an url
   * isn't explicitly passed, it defaults to "N/A" on the ResourceManager.
   * <p>
   *
   * @param url
   *          <em>tracking URL</em>for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setTrackingUrl(String url);

}
