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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TrafficControlBandwidthHandlerImpl
    implements OutboundBandwidthResourceHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(TrafficControlBandwidthHandlerImpl.class);
  //In the absence of 'scheduling' support, we'll 'infer' the guaranteed
  //outbound bandwidth for each container based on this number. This will
  //likely go away once we add support on the RM for this resource type.
  private static final int MAX_CONTAINER_COUNT = 50;

  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final CGroupsHandler cGroupsHandler;
  private final TrafficController trafficController;
  private final ConcurrentHashMap<ContainerId, Integer> containerIdClassIdMap;

  private Configuration conf;
  private String device;
  private boolean strictMode;
  private int containerBandwidthMbit;
  private int rootBandwidthMbit;
  private int yarnBandwidthMbit;

  public TrafficControlBandwidthHandlerImpl(PrivilegedOperationExecutor
      privilegedOperationExecutor, CGroupsHandler cGroupsHandler,
      TrafficController trafficController) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.cGroupsHandler = cGroupsHandler;
    this.trafficController = trafficController;
    this.containerIdClassIdMap = new ConcurrentHashMap<>();
  }

  /**
   * Bootstrapping 'outbound-bandwidth' resource handler - mounts net_cls
   * controller and bootstraps a traffic control bandwidth shaping hierarchy
   * @param configuration yarn configuration in use
   * @return (potentially empty) list of privileged operations to execute.
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    conf = configuration;
    //We'll do this inline for the time being - since this is a one time
    //operation. At some point, LCE code can be refactored to batch mount
    //operations across multiple controllers - cpu, net_cls, blkio etc
    cGroupsHandler
        .initializeCGroupController(CGroupsHandler.CGroupController.NET_CLS);
    device = conf.get(YarnConfiguration.NM_NETWORK_RESOURCE_INTERFACE,
        YarnConfiguration.DEFAULT_NM_NETWORK_RESOURCE_INTERFACE);
    strictMode = configuration.getBoolean(YarnConfiguration
        .NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, YarnConfiguration
        .DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    rootBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT, YarnConfiguration
        .DEFAULT_NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_MBIT);
    yarnBandwidthMbit = conf.getInt(YarnConfiguration
        .NM_NETWORK_RESOURCE_OUTBOUND_BANDWIDTH_YARN_MBIT, rootBandwidthMbit);
    containerBandwidthMbit = (int) Math.ceil((double) yarnBandwidthMbit /
        MAX_CONTAINER_COUNT);

    StringBuffer logLine = new StringBuffer("strict mode is set to :")
        .append(strictMode).append(System.lineSeparator());

    if (strictMode) {
      logLine.append("container bandwidth will be capped to soft limit.")
          .append(System.lineSeparator());
    } else {
      logLine.append(
          "containers will be allowed to use spare YARN bandwidth.")
          .append(System.lineSeparator());
    }

    logLine
        .append("containerBandwidthMbit soft limit (in mbit/sec) is set to : ")
        .append(containerBandwidthMbit);

    LOG.info(logLine.toString());
    trafficController.bootstrap(device, rootBandwidthMbit, yarnBandwidthMbit);

    return null;
  }

  /**
   * Pre-start hook for 'outbound-bandwidth' resource. A cgroup is created
   * and a net_cls classid is generated and written to a cgroup file. A
   * traffic control shaping rule is created in order to limit outbound
   * bandwidth utilization.
   * @param container Container being launched
   * @return privileged operations for some cgroups/tc operations.
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    int classId = trafficController.getNextClassId();
    String classIdStr = trafficController.getStringForNetClsClassId(classId);

    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController
            .NET_CLS,
        containerIdStr);
    cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.NET_CLS,
        containerIdStr, CGroupsHandler.CGROUP_PARAM_CLASSID, classIdStr);
    containerIdClassIdMap.put(container.getContainerId(), classId);

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

    //Create a privileged operation to create a tc rule for this container
    //We'll return this to the calling (Linux) Container Executor
    //implementation for batching optimizations so that we don't fork/exec
    //additional times during container launch.
    TrafficController.BatchBuilder builder = trafficController.new
        BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE);

    builder.addContainerClass(classId, containerBandwidthMbit, strictMode);
    ops.add(builder.commitBatchToTempFile());

    return ops;
  }

  /**
   * Reacquires state for a container - reads the classid from the cgroup
   * being used for the container being reacquired
   * @param containerId if of the container being reacquired.
   * @return (potentially empty) list of privileged operations
   * @throws ResourceHandlerException
   */

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    String containerIdStr = containerId.toString();

    LOG.debug("Attempting to reacquire classId for container: {}",
        containerIdStr);

    String classIdStrFromFile = cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.NET_CLS, containerIdStr,
        CGroupsHandler.CGROUP_PARAM_CLASSID);
    int classId = trafficController
        .getClassIdFromFileContents(classIdStrFromFile);

    LOG.info("Reacquired containerId -> classId mapping: " + containerIdStr
        + " -> " + classId);
    containerIdClassIdMap.put(containerId, classId);

    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  /**
   * Returns total bytes sent per container to be used for metrics tracking
   * purposes.
   * @return a map of containerId to bytes sent
   * @throws ResourceHandlerException
   */
  public Map<ContainerId, Integer> getBytesSentPerContainer()
      throws ResourceHandlerException {
    Map<Integer, Integer> classIdStats = trafficController.readStats();
    Map<ContainerId, Integer> containerIdStats = new HashMap<>();

    for (Map.Entry<ContainerId, Integer> entry : containerIdClassIdMap
        .entrySet()) {
      ContainerId containerId = entry.getKey();
      Integer classId = entry.getValue();
      Integer bytesSent = classIdStats.get(classId);

      if (bytesSent == null) {
        LOG.warn("No bytes sent metric found for container: " + containerId +
            " with classId: " + classId);
        continue;
      }
      containerIdStats.put(containerId, bytesSent);
    }

    return containerIdStats;
  }

  /**
   * Cleanup operations once container is completed - deletes cgroup and
   * removes traffic shaping rule(s).
   * @param containerId of the container that was completed.
   * @return null
   * @throws ResourceHandlerException
   */
  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    LOG.info("postComplete for container: " + containerId.toString());
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.NET_CLS,
        containerId.toString());

    Integer classId = containerIdClassIdMap.get(containerId);

    if (classId != null) {
      PrivilegedOperation op = trafficController.new
          BatchBuilder(PrivilegedOperation.OperationType.TC_MODIFY_STATE)
          .deleteContainerClass(classId).commitBatchToTempFile();

      try {
        privilegedOperationExecutor.executePrivilegedOperation(op, false);
        trafficController.releaseClassId(classId);
      } catch (PrivilegedOperationException e) {
        LOG.warn("Failed to delete tc rule for classId: " + classId);
        throw new ResourceHandlerException(
            "Failed to delete tc rule for classId:" + classId);
      }
    } else {
      LOG.warn("Not cleaning up tc rules. classId unknown for container: " +
          containerId.toString());
    }

    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    LOG.debug("teardown(): Nothing to do");

    return null;
  }

  @Override
  public String toString() {
    return TrafficControlBandwidthHandlerImpl.class.getName();
  }
}
