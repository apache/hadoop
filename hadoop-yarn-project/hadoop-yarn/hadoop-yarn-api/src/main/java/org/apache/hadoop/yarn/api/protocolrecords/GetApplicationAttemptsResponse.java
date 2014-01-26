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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The response sent by the <code>ResourceManager</code> to a client requesting
 * a list of {@link ApplicationAttemptReport} for application attempts.
 * </p>
 * 
 * <p>
 * The <code>ApplicationAttemptReport</code> for each application includes the
 * details of an application attempt.
 * </p>
 * 
 * @see ApplicationAttemptReport
 * @see ApplicationHistoryProtocol#getApplicationAttempts(GetApplicationAttemptsRequest)
 */
@Public
@Unstable
public abstract class GetApplicationAttemptsResponse {

  @Public
  @Unstable
  public static GetApplicationAttemptsResponse newInstance(
      List<ApplicationAttemptReport> applicationAttempts) {
    GetApplicationAttemptsResponse response =
        Records.newRecord(GetApplicationAttemptsResponse.class);
    response.setApplicationAttemptList(applicationAttempts);
    return response;
  }

  /**
   * Get a list of <code>ApplicationReport</code> of an application.
   * 
   * @return a list of <code>ApplicationReport</code> of an application
   */
  @Public
  @Unstable
  public abstract List<ApplicationAttemptReport> getApplicationAttemptList();

  /**
   * Get a list of <code>ApplicationReport</code> of an application.
   * 
   * @param applicationAttempts
   *          a list of <code>ApplicationReport</code> of an application
   */
  @Public
  @Unstable
  public abstract void setApplicationAttemptList(
      List<ApplicationAttemptReport> applicationAttempts);
}
