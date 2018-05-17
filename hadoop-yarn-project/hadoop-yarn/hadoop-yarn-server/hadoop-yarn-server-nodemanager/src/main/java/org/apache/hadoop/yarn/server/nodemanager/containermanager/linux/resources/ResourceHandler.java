/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.List;

/**
 * Handler interface for resource subsystems' isolation and enforcement. e.g cpu, memory, network, disks etc
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ResourceHandler {

  /**
   * Bootstrap resource susbsystem.
   *
   * @return (possibly empty) list of operations that require elevated
   * privileges
   */
  List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException;

  /**
   * Prepare a resource environment for container launch
   *
   * @param container Container being launched
   * @return (possibly empty) list of operations that require elevated
   * privileges e.g a) create a custom cgroup b) add pid for container to tasks
   * file for a cgroup.
   * @throws ResourceHandlerException
   */
  List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException;

  /**
   * Require state for container that was already launched
   *
   * @param containerId id of the container being reacquired.
   * @return (possibly empty) list of operations that require elevated
   * privileges
   * @throws ResourceHandlerException
   */

  List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException;

  /**
   * Update state for container that was already launched
   *
   * @param container the container being updated.
   * @return (possibly empty) list of operations that require elevated
   * privileges
   * @throws ResourceHandlerException
   */

  List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException;

  /**
   * Perform any tasks necessary after container completion.
   * @param containerId of the container that was completed.
   * @return (possibly empty) list of operations that require elevated
   * privileges
   * @throws ResourceHandlerException
   */
  List<PrivilegedOperation> postComplete(ContainerId containerId) throws
      ResourceHandlerException;

  /**
   * Teardown environment for resource subsystem if requested. This method
   * needs to be used with care since it could impact running containers.
   *
   * @return (possibly empty) list of operations that require elevated
   * privileges
   */
  List<PrivilegedOperation> teardown() throws ResourceHandlerException;
}