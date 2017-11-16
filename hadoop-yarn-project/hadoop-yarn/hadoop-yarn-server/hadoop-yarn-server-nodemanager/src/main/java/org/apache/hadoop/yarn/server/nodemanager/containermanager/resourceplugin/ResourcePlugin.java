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

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;

/**
 * {@link ResourcePlugin} is an interface for node manager to easier support
 * discovery/manage/isolation for new resource types.
 *
 * <p>
 * It has two major part: {@link ResourcePlugin#createResourceHandler(Context,
 * CGroupsHandler, PrivilegedOperationExecutor)} and
 * {@link ResourcePlugin#getNodeResourceHandlerInstance()}, see javadocs below
 * for more details.
 * </p>
 */
public interface ResourcePlugin {
  /**
   * Initialize the plugin, this will be invoked during NM startup.
   * @param context NM Context
   * @throws YarnException when any issue occurs
   */
  void initialize(Context context) throws YarnException;

  /**
   * Plugin needs to return {@link ResourceHandler} when any special isolation
   * required for the resource type. This will be added to
   * {@link ResourceHandlerChain} during NodeManager startup. When no special
   * isolation need, return null.
   *
   * @param nmContext NodeManager context.
   * @param cGroupsHandler CGroupsHandler
   * @param privilegedOperationExecutor Privileged Operation Executor.
   * @return ResourceHandler
   */
  ResourceHandler createResourceHandler(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor);

  /**
   * Plugin needs to return {@link NodeResourceUpdaterPlugin} when any discovery
   * mechanism required for the resource type. For example, if we want to set
   * resource-value during NM registration or send update during NM-RM heartbeat
   * We can implement a {@link NodeResourceUpdaterPlugin} and update fields of
   * {@link org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest}
   * or {@link org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest}
   *
   * This will be invoked during every node status update or node registration,
   * please avoid creating new instance every time.
   *
   * @return NodeResourceUpdaterPlugin, could be null when no discovery needed.
   */
  NodeResourceUpdaterPlugin getNodeResourceHandlerInstance();

  /**
   * Do cleanup of the plugin, this will be invoked when
   * {@link org.apache.hadoop.yarn.server.nodemanager.NodeManager} stops
   * @throws YarnException if any issue occurs
   */
  void cleanup() throws YarnException;

  /**
   * Plugin need to get {@link DockerCommandPlugin}. This will be invoked by
   * {@link DockerLinuxContainerRuntime} when execute docker commands such as
   * run/stop/pull, etc.
   *
   * @return DockerCommandPlugin instance. return null if plugin doesn't
   *         have requirement to update docker command.
   */
  DockerCommandPlugin getDockerCommandPluginInstance();
}
