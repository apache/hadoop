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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

/**
 * Interface to make different resource plugins (e.g. GPU) can update docker run
 * command without adding logic to Docker runtime.
 */
public interface DockerCommandPlugin {
  /**
   * Update docker run command
   * @param dockerRunCommand docker run command
   * @param container NM container
   * @throws ContainerExecutionException if any issue occurs
   */
  void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
      Container container) throws ContainerExecutionException;

  /**
   * Create volume when needed.
   * @param container container
   * @return {@link DockerVolumeCommand} to create volume
   * @throws ContainerExecutionException when any issue happens
   */
  DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException;

  /**
   * Cleanup volumes created for one docker container
   * @param container container
   * @return {@link DockerVolumeCommand} to remove volume
   * @throws ContainerExecutionException when any issue happens
   */
  DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException;

  // Add support to other docker command when required.
}
