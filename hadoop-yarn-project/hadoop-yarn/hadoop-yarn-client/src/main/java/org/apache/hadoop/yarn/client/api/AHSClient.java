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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.api.impl.AHSClientImpl;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AHSClient extends AbstractService {

  /**
   * Create a new instance of AHSClient.
   */
  @Public
  public static AHSClient createAHSClient() {
    AHSClient client = new AHSClientImpl();
    return client;
  }

  @Private
  public AHSClient(String name) {
    super(name);
  }

  /**
   * Get a report of the given Application.
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * <p>
   * If the user does not have <code>VIEW_APP</code> access then the following
   * fields in the report will be set to stubbed values:
   * <ul>
   *   <li>host - set to "N/A"</li>
   *   <li>RPC port - set to -1</li>
   *   <li>client token - set to "N/A"</li>
   *   <li>diagnostics - set to "N/A"</li>
   *   <li>tracking URL - set to "N/A"</li>
   *   <li>original tracking URL - set to "N/A"</li>
   *   <li>resource usage report - all values are -1</li>
   * </ul>
   * 
   * @param appId
   *          {@link ApplicationId} of the application that needs a report
   * @return application report
   * @throws YarnException
   * @throws IOException
   */
  public abstract ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException;

  /**
   * <p>
   * Get a report (ApplicationReport) of all Applications in the cluster.
   * </p>
   * 
   * <p>
   * If the user does not have <code>VIEW_APP</code> access for an application
   * then the corresponding report will be filtered as described in
   * {@link #getApplicationReport(ApplicationId)}.
   * </p>
   * 
   * @return a list of reports for all applications
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationReport> getApplications()
      throws YarnException, IOException;

  /**
   * <p>
   * Get a report of the given ApplicationAttempt.
   * </p>
   * 
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * </p>
   * 
   * @param applicationAttemptId
   *          {@link ApplicationAttemptId} of the application attempt that needs
   *          a report
   * @return application attempt report
   * @throws YarnException
   * @throws ApplicationAttemptNotFoundException if application attempt
   *         not found
   * @throws IOException
   */
  public abstract ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException;

  /**
   * <p>
   * Get a report of all (ApplicationAttempts) of Application in the cluster.
   * </p>
   * 
   * @param applicationId
   * @return a list of reports for all application attempts for specified
   *         application
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId applicationId) throws YarnException, IOException;

  /**
   * <p>
   * Get a report of the given Container.
   * </p>
   * 
   * <p>
   * In secure mode, <code>YARN</code> verifies access to the application, queue
   * etc. before accepting the request.
   * </p>
   * 
   * @param containerId
   *          {@link ContainerId} of the container that needs a report
   * @return container report
   * @throws YarnException
   * @throws ContainerNotFoundException if container not found
   * @throws IOException
   */
  public abstract ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException;

  /**
   * <p>
   * Get a report of all (Containers) of ApplicationAttempt in the cluster.
   * </p>
   * 
   * @param applicationAttemptId
   * @return a list of reports of all containers for specified application
   *         attempt
   * @throws YarnException
   * @throws IOException
   */
  public abstract List<ContainerReport> getContainers(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException;
}
