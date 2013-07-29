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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ApplicationHistoryReader {

  /**
   * This method returns Application {@link ApplicationHistoryData} for the specified
   * {@link ApplicationId}.
   * 
   * @return {@link ApplicationHistoryData} for the ApplicationId.
   */
   ApplicationHistoryData getApplication(ApplicationId appId);

  /**
   * This method returns all Application {@link ApplicationHistoryData}s
   * 
   * @return map {@link ApplicationId, @link ApplicationHistoryData}s.
   */
  Map<ApplicationId, ApplicationHistoryData> getAllApplications();

  /**
   * Application can have multiple application attempts
   * {@link ApplicationAttemptHistoryData}. This method returns the all
   * {@link ApplicationAttemptHistoryData}s for the Application.
   * 
   * @return all {@link ApplicationAttemptHistoryData}s for the Application.
   */
  Map<ApplicationAttemptId, ApplicationAttemptHistoryData> getApplicationAttempts(
      ApplicationId appId);

  /**
   * This method returns {@link ApplicationAttemptHistoryData} for specified
   * {@link ApplicationId}.
   * 
   * @param {@link ApplicationAttemptId}
   * @return {@link ApplicationAttemptHistoryData} for ApplicationAttemptId
   */
  ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId);

  /**
   * This method returns {@link Container} for specified {@link ContainerId}.
   * 
   * @param {@link ContainerId}
   * @return {@link Container} for ContainerId
   */
  ContainerHistoryData getAMContainer(ContainerId containerId);

  /**
   * This method returns {@link ContainerHistoryData} for specified
   * {@link ApplicationAttemptId}.
   * 
   * @param {@link ApplicationAttemptId}
   * @return {@link ContainerHistoryData} for ApplicationAttemptId
   */
  ContainerHistoryData getContainer(ApplicationAttemptId appAttemptId);
}
