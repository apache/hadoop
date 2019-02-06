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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.gpu;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GpuResourceHandlerImpl implements ResourceHandler {
  final static Log LOG = LogFactory
      .getLog(GpuResourceHandlerImpl.class);

  // This will be used by container-executor to add necessary clis
  public static final String EXCLUDED_GPUS_CLI_OPTION = "--excluded_gpus";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";

  private GpuResourceAllocator gpuAllocator;
  private CGroupsHandler cGroupsHandler;
  private PrivilegedOperationExecutor privilegedOperationExecutor;

  public GpuResourceHandlerImpl(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    gpuAllocator = new GpuResourceAllocator(nmContext);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    List<Integer> minorNumbersOfUsableGpus;
    try {
      minorNumbersOfUsableGpus = GpuDiscoverer.getInstance()
          .getMinorNumbersOfGpusUsableByYarn();
    } catch (YarnException e) {
      LOG.error("Exception when trying to get usable GPU device", e);
      throw new ResourceHandlerException(e);
    }

    for (int minorNumber : minorNumbersOfUsableGpus) {
      gpuAllocator.addGpu(minorNumber);
    }

    // And initialize cgroups
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);

    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();

    // Assign Gpus to container if requested some.
    GpuResourceAllocator.GpuAllocation allocation = gpuAllocator.assignGpus(
        container);

    // Create device cgroups for the container
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    try {
      // Execute c-e to setup GPU isolation before launch the container
      PrivilegedOperation privilegedOperation = new PrivilegedOperation(
          PrivilegedOperation.OperationType.GPU, Arrays
          .asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
      if (!allocation.getDeniedGPUs().isEmpty()) {
        privilegedOperation.appendArgs(Arrays.asList(EXCLUDED_GPUS_CLI_OPTION,
            StringUtils.join(",", allocation.getDeniedGPUs())));
      }

      privilegedOperationExecutor.executePrivilegedOperation(
          privilegedOperation, true);
    } catch (PrivilegedOperationException e) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      LOG.warn("Could not update cgroup for container", e);
      throw new ResourceHandlerException(e);
    }

    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
            + cGroupsHandler.getPathForCGroupTasks(
            CGroupsHandler.CGroupController.DEVICES, containerIdStr)));

    return ret;
  }

  @VisibleForTesting
  public GpuResourceAllocator getGpuAllocator() {
    return gpuAllocator;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    gpuAllocator.recoverAssignedGpus(containerId);
    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> postComplete(
      ContainerId containerId) throws ResourceHandlerException {
    gpuAllocator.cleanupAssignGpus(containerId);
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
