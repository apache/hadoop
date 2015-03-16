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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * {@code ApplicationAttemptReport} is a report of an application attempt.
 * <p>
 * It includes details such as:
 * <ul>
 *   <li>{@link ApplicationAttemptId} of the application.</li>
 *   <li>Host on which the <code>ApplicationMaster</code> of this attempt is
 *   running.</li>
 *   <li>RPC port of the <code>ApplicationMaster</code> of this attempt.</li>
 *   <li>Tracking URL.</li>
 *   <li>Diagnostic information in case of errors.</li>
 *   <li>{@link YarnApplicationAttemptState} of the application attempt.</li>
 *   <li>{@link ContainerId} of the master Container.</li>
 * </ul>
 */
@Public
@Unstable
public abstract class ApplicationAttemptReport {

  @Private
  @Unstable
  public static ApplicationAttemptReport newInstance(
      ApplicationAttemptId applicationAttemptId, String host, int rpcPort,
      String url, String oUrl, String diagnostics,
      YarnApplicationAttemptState state, ContainerId amContainerId) {
    ApplicationAttemptReport report =
        Records.newRecord(ApplicationAttemptReport.class);
    report.setApplicationAttemptId(applicationAttemptId);
    report.setHost(host);
    report.setRpcPort(rpcPort);
    report.setTrackingUrl(url);
    report.setOriginalTrackingUrl(oUrl);
    report.setDiagnostics(diagnostics);
    report.setYarnApplicationAttemptState(state);
    report.setAMContainerId(amContainerId);
    return report;
  }

  /**
   * Get the <em>YarnApplicationAttemptState</em> of the application attempt.
   * 
   * @return <em>YarnApplicationAttemptState</em> of the application attempt
   */
  @Public
  @Unstable
  public abstract YarnApplicationAttemptState getYarnApplicationAttemptState();

  @Private
  @Unstable
  public abstract void setYarnApplicationAttemptState(
      YarnApplicationAttemptState yarnApplicationAttemptState);

  /**
   * Get the <em>RPC port</em> of this attempt <code>ApplicationMaster</code>.
   * 
   * @return <em>RPC port</em> of this attempt <code>ApplicationMaster</code>
   */
  @Public
  @Unstable
  public abstract int getRpcPort();

  @Private
  @Unstable
  public abstract void setRpcPort(int rpcPort);

  /**
   * Get the <em>host</em> on which this attempt of
   * <code>ApplicationMaster</code> is running.
   * 
   * @return <em>host</em> on which this attempt of
   *         <code>ApplicationMaster</code> is running
   */
  @Public
  @Unstable
  public abstract String getHost();

  @Private
  @Unstable
  public abstract void setHost(String host);

  /**
   * Get the <em>diagnositic information</em> of the application attempt in case
   * of errors.
   * 
   * @return <em>diagnositic information</em> of the application attempt in case
   *         of errors
   */
  @Public
  @Unstable
  public abstract String getDiagnostics();

  @Private
  @Unstable
  public abstract void setDiagnostics(String diagnostics);

  /**
   * Get the <em>tracking url</em> for the application attempt.
   * 
   * @return <em>tracking url</em> for the application attempt
   */
  @Public
  @Unstable
  public abstract String getTrackingUrl();

  @Private
  @Unstable
  public abstract void setTrackingUrl(String url);

  /**
   * Get the <em>original tracking url</em> for the application attempt.
   * 
   * @return <em>original tracking url</em> for the application attempt
   */
  @Public
  @Unstable
  public abstract String getOriginalTrackingUrl();

  @Private
  @Unstable
  public abstract void setOriginalTrackingUrl(String oUrl);

  /**
   * Get the <code>ApplicationAttemptId</code> of this attempt of the
   * application
   * 
   * @return <code>ApplicationAttemptId</code> of the attempt
   */
  @Public
  @Unstable
  public abstract ApplicationAttemptId getApplicationAttemptId();

  @Private
  @Unstable
  public abstract void setApplicationAttemptId(
      ApplicationAttemptId applicationAttemptId);

  /**
   * Get the <code>ContainerId</code> of AMContainer for this attempt
   * 
   * @return <code>ContainerId</code> of the attempt
   */
  @Public
  @Unstable
  public abstract ContainerId getAMContainerId();

  @Private
  @Unstable
  public abstract void setAMContainerId(ContainerId amContainerId);
}
