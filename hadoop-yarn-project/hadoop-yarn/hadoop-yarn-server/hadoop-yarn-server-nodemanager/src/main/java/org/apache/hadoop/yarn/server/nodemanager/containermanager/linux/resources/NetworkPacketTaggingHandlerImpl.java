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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

/**
 * The network packet tagging handler implementation.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NetworkPacketTaggingHandlerImpl
    implements ResourceHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(NetworkPacketTaggingHandlerImpl.class);

  private final CGroupsHandler cGroupsHandler;

  private Configuration conf;
  private NetworkTagMappingManager tagMappingManager;

  public NetworkPacketTaggingHandlerImpl(
      PrivilegedOperationExecutor privilegedOperationExecutor,
      CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  /**
   * Bootstrapping network-tagging-handler - mounts net_cls
   * controller.
   * @param configuration yarn configuration in use
   * @return (potentially empty) list of privileged operations to execute.
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    conf = configuration;

    cGroupsHandler
        .initializeCGroupController(CGroupsHandler.CGroupController.NET_CLS);

    this.tagMappingManager = createNetworkTagMappingManager(conf);
    this.tagMappingManager.initialize(conf);
    return null;
  }

  /**
   * Pre-start hook for network-tagging-handler. A cgroup is created
   * and a net_cls classid is generated and written to a cgroup file.
   *
   * @param container Container being launched
   * @return privileged operations for some cgroups operations.
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    String classIdStr = this.tagMappingManager.getNetworkTagHexID(
        container);

    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController
            .NET_CLS, containerIdStr);
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.NET_CLS,
        containerIdStr, CGroupsHandler.CGROUP_PARAM_CLASSID, classIdStr);

    //Now create a privileged operation in order to update the tasks file with
    //the pid of the running container process (root of process tree). This can
    //only be done at the time of launching the container, in a privileged
    //executable.
    String tasksFile = cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr);
    String opArg = new StringBuffer(PrivilegedOperation.CGROUP_ARG_PREFIX)
        .append(tasksFile).toString();
    List<PrivilegedOperation> ops = new ArrayList<>();

    ops.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP, opArg));

    return ops;
  }

  /**
   * Reacquires state for a container - reads the classid from the cgroup
   * being used for the container being reacquired.
   * @param containerId if of the container being reacquired.
   * @return (potentially empty) list of privileged operations
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  /**
   * Cleanup operation once container is completed - deletes cgroup.
   *
   * @param containerId of the container that was completed.
   * @return a list of PrivilegedOperations.
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    LOG.info("postComplete for container: " + containerId.toString());
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.NET_CLS,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    LOG.debug("teardown(): Nothing to do");

    return null;
  }

  @Private
  @VisibleForTesting
  public NetworkTagMappingManager createNetworkTagMappingManager(
      Configuration conf) {
    return NetworkTagMappingManagerFactory.getManager(conf);
  }

  @Override
  public String toString() {
    return NetworkPacketTaggingHandlerImpl.class.getName();
  }
}
