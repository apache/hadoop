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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * an application attempt report.
 * </p>
 * 
 * <p>
 * The response includes an {@link ApplicationAttemptReport} which has the
 * details about the particular application attempt
 * </p>
 * 
 * @see ApplicationAttemptReport
 * @see ApplicationHistoryProtocol#getApplicationAttemptReport(GetApplicationAttemptReportRequest)
 */
@Public
@Unstable
public abstract class GetApplicationAttemptReportResponse {

  @Public
  @Unstable
  public static GetApplicationAttemptReportResponse newInstance(
      ApplicationAttemptReport ApplicationAttemptReport) {
    GetApplicationAttemptReportResponse response =
        Records.newRecord(GetApplicationAttemptReportResponse.class);
    response.setApplicationAttemptReport(ApplicationAttemptReport);
    return response;
  }

  /**
   * Get the <code>ApplicationAttemptReport</code> for the application attempt.
   * 
   * @return <code>ApplicationAttemptReport</code> for the application attempt
   */
  @Public
  @Unstable
  public abstract ApplicationAttemptReport getApplicationAttemptReport();

  /**
   * Get the <code>ApplicationAttemptReport</code> for the application attempt.
   * 
   * @param applicationAttemptReport
   *          <code>ApplicationAttemptReport</code> for the application attempt
   */
  @Public
  @Unstable
  public abstract void setApplicationAttemptReport(
      ApplicationAttemptReport applicationAttemptReport);
}
