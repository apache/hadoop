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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Relaunch container.
 */
public class ContainerRelaunch extends ContainerLaunch {

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerRelaunch.class);

  public ContainerRelaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    super(context, configuration, dispatcher, exec, app, container, dirsHandler,
        containerManager);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Integer call() {
    if (!validateContainerState()) {
      return 0;
    }

    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    int ret = -1;
    Path containerLogDir;
    try {
      Path containerWorkDir = getContainerWorkDir();
      cleanupPreviousContainerFiles(containerWorkDir);

      containerLogDir = getContainerLogDir();

      Map<Path, List<String>> localResources = getLocalizedResources();

      String appIdStr = app.getAppId().toString();
      Path nmPrivateContainerScriptPath =
          getNmPrivateContainerScriptPath(appIdStr, containerIdStr);
      Path nmPrivateTokensPath =
          getNmPrivateTokensPath(appIdStr, containerIdStr);
      pidFilePath = getPidFilePath(appIdStr, containerIdStr);

      LOG.info("Relaunch container with "
          + "workDir = " + containerWorkDir.toString()
          + ", logDir = " + containerLogDir.toString()
          + ", nmPrivateContainerScriptPath = "
          + nmPrivateContainerScriptPath.toString()
          + ", nmPrivateTokensPath = " + nmPrivateTokensPath.toString()
          + ", pidFilePath = " + pidFilePath.toString());

      List<String> localDirs = dirsHandler.getLocalDirs();
      List<String> logDirs = dirsHandler.getLogDirs();
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);
      List<String> containerLogDirs = getContainerLogDirs(logDirs);
      List<String> filecacheDirs = getNMFilecacheDirs(localDirs);
      List<String> userLocalDirs = getUserLocalDirs(localDirs);
      List<String> userFilecacheDirs = getUserFilecacheDirs(localDirs);
      List<String> applicationLocalDirs = getApplicationLocalDirs(localDirs,
          appIdStr);

      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }

      ret = launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setLocalizedResources(localResources)
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setUser(container.getUser())
          .setAppId(appIdStr)
          .setContainerWorkDir(containerWorkDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .setFilecacheDirs(filecacheDirs)
          .setUserLocalDirs(userLocalDirs)
          .setContainerLocalDirs(containerLocalDirs)
          .setContainerLogDirs(containerLogDirs)
          .setUserFilecacheDirs(userFilecacheDirs)
          .setApplicationLocalDirs(applicationLocalDirs)
          .build());
    } catch (ConfigurationException e) {
      LOG.error("Failed to launch container due to configuration error.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      // Mark the node as unhealthy
      getContext().getNodeStatusUpdater().reportException(e);
      return ret;
    } catch (Throwable e) {
      LOG.warn("Failed to relaunch container.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerId, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      return ret;
    } finally {
      setContainerCompletedStatus(ret);
    }

    handleContainerExitCode(ret, containerLogDir);

    return ret;
  }

  private Path getContainerWorkDir() throws IOException {
    String containerWorkDir = container.getWorkDir();
    if (containerWorkDir == null
        || !dirsHandler.isGoodLocalDir(containerWorkDir)) {
      throw new IOException(
          "Could not find a good work dir " + containerWorkDir
          + " for container " + container);
    }

    return new Path(containerWorkDir);
  }

  private Path getContainerLogDir() throws IOException {
    String containerLogDir = container.getLogDir();
    if (containerLogDir == null || !dirsHandler.isGoodLogDir(containerLogDir)) {
      throw new IOException("Could not find a good log dir " + containerLogDir
          + " for container " + container);
    }

    return new Path(containerLogDir);
  }

  private Path getNmPrivateContainerScriptPath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + CONTAINER_SCRIPT);
  }

  private Path getNmPrivateTokensPath(String appIdStr,
       String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
            + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
            containerIdStr));
  }

  private Path getPidFilePath(String appIdStr,
      String containerIdStr) throws IOException {
    return dirsHandler.getLocalPathForRead(
        getPidFileSubpath(appIdStr, containerIdStr));
  }

  /**
   * Clean up container's previous files for container relaunch.
   */
  private void cleanupPreviousContainerFiles(Path containerWorkDir) {
    // delete ContainerScriptPath
    deleteAsUser(new Path(containerWorkDir, CONTAINER_SCRIPT));
    // delete TokensPath
    deleteAsUser(new Path(containerWorkDir, FINAL_CONTAINER_TOKENS_FILE));
  }

  private void deleteAsUser(Path path) {
    try {
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(container.getUser())
          .setSubDir(path)
          .build());
    } catch (Exception e) {
      LOG.warn("Failed to delete " + path, e);
    }
  }
}
