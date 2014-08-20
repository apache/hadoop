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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * This is a ContainerLaunch which has been recovered after an NM restart (for
 * rolling upgrades)
 */
public class RecoveredContainerLaunch extends ContainerLaunch {

  private static final Log LOG = LogFactory.getLog(
    RecoveredContainerLaunch.class);

  public RecoveredContainerLaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager)
  {
    super(context, configuration, dispatcher, exec, app, container, dirsHandler,
      containerManager);
    this.shouldLaunchContainer.set(true);
  }

  /**
   * Wait on the process specified in pid file and return its exit code
   */
  @SuppressWarnings("unchecked")
  @Override
  public Integer call() {
    int retCode = ExitCode.LOST.getExitCode();
    ContainerId containerId = container.getContainerId();
    String appIdStr = ConverterUtils.toString(
        containerId.getApplicationAttemptId().getApplicationId());
    String containerIdStr = ConverterUtils.toString(containerId);

    dispatcher.getEventHandler().handle(new ContainerEvent(containerId,
        ContainerEventType.CONTAINER_LAUNCHED));

    try {
      File pidFile = locatePidFile(appIdStr, containerIdStr);
      if (pidFile != null) {
        String pidPathStr = pidFile.getPath();
        pidFilePath = new Path(pidPathStr);
        exec.activateContainer(containerId, pidFilePath);
        retCode = exec.reacquireContainer(container.getUser(), containerId);
      } else {
        LOG.warn("Unable to locate pid file for container " + containerIdStr);
      }
    } catch (IOException e) {
        LOG.error("Unable to recover container " + containerIdStr, e);
    } finally {
      this.completed.set(true);
      exec.deactivateContainer(containerId);
      try {
        getContext().getNMStateStore().storeContainerCompleted(containerId,
            retCode);
      } catch (IOException e) {
        LOG.error("Unable to set exit code for container " + containerId);
      }
    }

    if (retCode != 0) {
      LOG.warn("Recovered container exited with a non-zero exit code "
          + retCode);
      this.dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId,
          ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, retCode,
          "Container exited with a non-zero exit code " + retCode));
      return retCode;
    }

    LOG.info("Recovered container " + containerId + " succeeded");
    dispatcher.getEventHandler().handle(
        new ContainerEvent(containerId,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    return 0;
  }

  private File locatePidFile(String appIdStr, String containerIdStr) {
    String pidSubpath= getPidFileSubpath(appIdStr, containerIdStr);
    for (String dir : getContext().getLocalDirsHandler().getLocalDirs()) {
      File pidFile = new File(dir, pidSubpath);
      if (pidFile.exists()) {
        return pidFile;
      }
    }
    return null;
  }
}
