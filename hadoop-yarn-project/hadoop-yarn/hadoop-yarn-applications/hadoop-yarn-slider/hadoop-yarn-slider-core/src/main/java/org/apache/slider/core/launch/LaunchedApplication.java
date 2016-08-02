/*
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

package org.apache.slider.core.launch;

import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.tools.Duration;

import java.io.IOException;

/**
 * Launched App with logic around it.
 */
public class LaunchedApplication {

  protected final ApplicationId applicationId;
  protected final SliderYarnClientImpl yarnClient;

  public LaunchedApplication(ApplicationId applicationId,
                             SliderYarnClientImpl yarnClient) {
    assert applicationId != null;
    assert yarnClient != null;
    this.applicationId = applicationId;
    this.yarnClient = yarnClient;
  }

  public LaunchedApplication(SliderYarnClientImpl yarnClient,
                             ApplicationReport report) {
    this.yarnClient = yarnClient;
    this.applicationId = report.getApplicationId();
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  /**
   * Monitor the submitted application for reaching the requested state.
   * Will also report if the app reaches a later state (failed, killed, etc)
   * Kill application if duration!= null & time expires. 
   * @param duration how long to wait -must be more than 0
   * @param desiredState desired state.
   * @return the application report -null on a timeout
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationReport monitorAppToState(YarnApplicationState desiredState, Duration duration)
    throws YarnException, IOException {
    return yarnClient.monitorAppToState(applicationId, desiredState, duration);
  }

  /**
   * Kill the submitted application by sending a call to the ASM
   * @throws YarnException
   * @throws IOException
   */
  public boolean forceKill(String reason)
    throws YarnException, IOException {
    if (applicationId != null) {
      yarnClient.killRunningApplication(applicationId, reason);
      return true;
    }
    return false;
  }

  /**
   * Kill the application
   * @return the response
   * @throws YarnException YARN problems
   * @throws IOException IO problems
   */
  public KillApplicationResponse kill(String reason) throws
                                                     YarnException,
                                                     IOException {
    return yarnClient.killRunningApplication(applicationId, reason);
  }

  /**
   * Get the application report of this application
   * @return an application report
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationReport getApplicationReport()
    throws YarnException, IOException {
    return yarnClient.getApplicationReport(applicationId);
  }
}
