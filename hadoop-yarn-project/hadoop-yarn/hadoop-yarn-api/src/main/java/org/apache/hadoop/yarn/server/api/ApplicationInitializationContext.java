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

package org.apache.hadoop.yarn.server.api;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/**
 * Initialization context for {@link AuxiliaryService} when starting an
 * application.
 */
@Public
@Evolving
public class ApplicationInitializationContext {

  private final String user;
  private final ApplicationId applicationId;
  private ByteBuffer appDataForService;

  @Private
  @Unstable
  public ApplicationInitializationContext(String user, ApplicationId applicationId,
      ByteBuffer appDataForService) {
    this.user = user;
    this.applicationId = applicationId;
    this.appDataForService = appDataForService;
  }

  /**
   * Get the user-name of the application-submitter
   * 
   * @return user-name
   */
  public String getUser() {
    return this.user;
  }

  /**
   * Get {@link ApplicationId} of the application
   * 
   * @return applications ID
   */
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  /**
   * Get the data sent to the NodeManager via
   * {@link ContainerManagementProtocol#startContainers(StartContainersRequest)}
   * as part of {@link ContainerLaunchContext#getServiceData()}
   * 
   * @return the servicesData for this application.
   */
  public ByteBuffer getApplicationDataForService() {
    return this.appDataForService;
  }
}