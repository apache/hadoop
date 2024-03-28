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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation for using CGroups to control the number of the process in container.
 *
 * The process number controller is used to allow a cgroup hierarchy to stop any
 * new tasks from being fork()'d or clone()'d after a certain limit is reached.
 * @see <a href="https://www.kernel.org/doc/Documentation/cgroup-v1/pids.txt">PIDS</a>
 */

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsPidsResourceHandlerImpl implements PidsResourceHandler {

  static final Log LOG = LogFactory.getLog(CGroupsPidsResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  private static final CGroupsHandler.CGroupController PIDS = CGroupsHandler.CGroupController.PIDS;
  private int processMaxCount;

  CGroupsPidsResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration conf)
      throws ResourceHandlerException {
    this.cGroupsHandler.initializeCGroupController(PIDS);
    processMaxCount =
        conf.getInt(YarnConfiguration.NM_CONTAINER_PROCESS_MAX_LIMIT_NUM,
            YarnConfiguration.DEFAULT_NM_CONTAINER_PROCESS_NUM_MAX_LIMIT);
    if (processMaxCount < 0){
      throw new ResourceHandlerException(
          "Illegal value '" + processMaxCount + "' "
              + YarnConfiguration.
                NM_CONTAINER_PROCESS_MAX_LIMIT_NUM
              + ". Value must be positive number.");
    }
    LOG.info("Maximum number of processes  is " + processMaxCount);

    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {

    String cgroupId = container.getContainerId().toString();
    cGroupsHandler.createCGroup(PIDS, cgroupId);
    try {
      cGroupsHandler.updateCGroupParam(PIDS, cgroupId,
          CGroupsHandler.CGROUP_PIDS_MAX, String.valueOf(processMaxCount));
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(PIDS, cgroupId);
      LOG.error("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
         .getPathForCGroupTasks(PIDS, cgroupId)));
    return ret;
  }

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

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    cGroupsHandler.deleteCGroup(PIDS, containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  public int getProcessMaxCount() {
    return processMaxCount;
  }
}