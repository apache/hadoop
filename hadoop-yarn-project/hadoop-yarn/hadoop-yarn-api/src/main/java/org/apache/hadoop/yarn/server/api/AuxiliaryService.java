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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * A generic service that will be started by the NodeManager. This is a service
 * that administrators have to configure on each node by setting
 * {@link YarnConfiguration#NM_AUX_SERVICES}.
 * 
 */
@Public
@Evolving
public abstract class AuxiliaryService extends AbstractService {

  private Path recoveryPath = null;

  protected AuxiliaryService(String name) {
    super(name);
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   *
   * @return state storage path or null if recovery is not enabled
   */
  protected Path getRecoveryPath() {
    return recoveryPath;
  }

  /**
   * A new application is started on this NodeManager. This is a signal to
   * this {@link AuxiliaryService} about the application initialization.
   * 
   * @param initAppContext context for the application's initialization
   */
  public abstract void initializeApplication(
      ApplicationInitializationContext initAppContext);

  /**
   * An application is finishing on this NodeManager. This is a signal to this
   * {@link AuxiliaryService} about the same.
   * 
   * @param stopAppContext context for the application termination
   */
  public abstract void stopApplication(
      ApplicationTerminationContext stopAppContext);

  /**
   * Retrieve meta-data for this {@link AuxiliaryService}. Applications using
   * this {@link AuxiliaryService} SHOULD know the format of the meta-data -
   * ideally each service should provide a method to parse out the information
   * to the applications. One example of meta-data is contact information so
   * that applications can access the service remotely. This will only be called
   * after the service's {@link #start()} method has finished. the result may be
   * cached.
   * 
   * <p>
   * The information is passed along to applications via
   * {@link StartContainersResponse#getAllServicesMetaData()} that is returned by
   * {@link ContainerManagementProtocol#startContainers(StartContainersRequest)}
   * </p>
   * 
   * @return meta-data for this service that should be made available to
   *         applications.
   */
  public abstract ByteBuffer getMetaData();

  /**
   * A new container is started on this NodeManager. This is a signal to
   * this {@link AuxiliaryService} about the container initialization.
   * This method is called when the NodeManager receives the container launch
   * command from the ApplicationMaster and before the container process is 
   * launched.
   *
   * @param initContainerContext context for the container's initialization
   */
  public void initializeContainer(ContainerInitializationContext
      initContainerContext) {
  }

  /**
   * A container is finishing on this NodeManager. This is a signal to this
   * {@link AuxiliaryService} about the same.
   *
   * @param stopContainerContext context for the container termination
   */
  public void stopContainer(ContainerTerminationContext stopContainerContext) {
  }

  /**
   * Set the path for this auxiliary service to use for storing state
   * that will be used during recovery.
   *
   * @param recoveryPath where recoverable state should be stored
   */
  public void setRecoveryPath(Path recoveryPath) {
    this.recoveryPath = recoveryPath;
  }
}
