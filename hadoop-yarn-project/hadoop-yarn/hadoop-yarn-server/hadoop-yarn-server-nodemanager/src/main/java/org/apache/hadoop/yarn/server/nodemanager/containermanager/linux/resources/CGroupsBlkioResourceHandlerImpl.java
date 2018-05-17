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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Handler class to handle the blkio controller. Currently it splits resources
 * evenly across all containers. Once we have scheduling sorted out, we can
 * modify the function to represent the disk resources allocated.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CGroupsBlkioResourceHandlerImpl implements DiskResourceHandler {

  static final Logger LOG =
       LoggerFactory.getLogger(CGroupsBlkioResourceHandlerImpl.class);

  private CGroupsHandler cGroupsHandler;
  // Arbitrarily choose a weight - all that matters is that all containers
  // get the same weight assigned to them. Once we have scheduling support
  // this number will be determined dynamically for each container.
  @VisibleForTesting
  static final String DEFAULT_WEIGHT = "500";
  private static final String PARTITIONS_FILE = "/proc/partitions";

  CGroupsBlkioResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    this.cGroupsHandler = cGroupsHandler;
    // check for linux so that we don't print messages for tests running on
    // other platforms
    if(Shell.LINUX) {
      checkDiskScheduler();
    }
  }


  private void checkDiskScheduler() {
    String data;

    // read /proc/partitions and check to make sure that sd* and hd*
    // are using the CFQ scheduler. If they aren't print a warning
    try {
      byte[] contents = Files.readAllBytes(Paths.get(PARTITIONS_FILE));
      data = new String(contents, "UTF-8").trim();
    } catch (IOException e) {
      String msg = "Couldn't read " + PARTITIONS_FILE +
          "; can't determine disk scheduler type";
      LOG.warn(msg, e);
      return;
    }
    String[] lines = data.split(System.lineSeparator());
    if (lines.length > 0) {
      for (String line : lines) {
        String[] columns = line.split("\\s+");
        if (columns.length > 4) {
          String partition = columns[4];
          // check some known partitions to make sure  the disk scheduler
          // is cfq - not meant to be comprehensive, more a sanity check
          if (partition.startsWith("sd") || partition.startsWith("hd")
              || partition.startsWith("vd") || partition.startsWith("xvd")) {
            String schedulerPath =
                "/sys/block/" + partition + "/queue/scheduler";
            File schedulerFile = new File(schedulerPath);
            if (schedulerFile.exists()) {
              try {
                byte[] contents = Files.readAllBytes(Paths.get(schedulerPath));
                String schedulerString = new String(contents, "UTF-8").trim();
                if (!schedulerString.contains("[cfq]")) {
                  LOG.warn("Device " + partition + " does not use the CFQ"
                      + " scheduler; disk isolation using "
                      + "CGroups will not work on this partition.");
                }
              } catch (IOException ie) {
                LOG.warn(
                    "Unable to determine disk scheduler type for partition "
                      + partition, ie);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    // if bootstrap is called on this class, disk is already enabled
    // so no need to check again
    this.cGroupsHandler
      .initializeCGroupController(CGroupsHandler.CGroupController.BLKIO);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {

    String cgroupId = container.getContainerId().toString();
    cGroupsHandler
      .createCGroup(CGroupsHandler.CGroupController.BLKIO, cgroupId);
    try {
      cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.BLKIO,
          cgroupId, CGroupsHandler.CGROUP_PARAM_BLKIO_WEIGHT, DEFAULT_WEIGHT);
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
          cgroupId);
      LOG.warn("Could not update cgroup for container", re);
      throw re;
    }
    List<PrivilegedOperation> ret = new ArrayList<>();
    ret.add(new PrivilegedOperation(
      PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
      PrivilegedOperation.CGROUP_ARG_PREFIX
          + cGroupsHandler.getPathForCGroupTasks(
            CGroupsHandler.CGroupController.BLKIO, cgroupId)));
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
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.BLKIO,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
