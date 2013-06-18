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
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
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

  protected AuxiliaryService(String name) {
    super(name);
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
   * {@link StartContainerResponse#getAllServicesMetaData()} that is returned by
   * {@link ContainerManagementProtocol#startContainer(StartContainerRequest)}
   * </p>
   * 
   * @return meta-data for this service that should be made available to
   *         applications.
   */
  public abstract ByteBuffer getMetaData();
}