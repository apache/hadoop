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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The launcher for the containers. This service should be started only after
 * the {@link ResourceLocalizationService} is started as it depends on creation
 * of system directories on the local file-system.
 * 
 */
public class ContainersLauncher extends AbstractService
    implements EventHandler<ContainersLauncherEvent> {

  private static final Log LOG = LogFactory.getLog(ContainersLauncher.class);

  private final Context context;
  private final ContainerExecutor exec;
  private final Dispatcher dispatcher;
  private final ContainerManagerImpl containerManager;

  private LocalDirsHandlerService dirsHandler;
  @VisibleForTesting
  public ExecutorService containerLauncher =
    Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("ContainersLauncher #%d")
          .build());
  @VisibleForTesting
  public final Map<ContainerId, ContainerLaunch> running =
    Collections.synchronizedMap(new HashMap<ContainerId, ContainerLaunch>());

  public ContainersLauncher(Context context, Dispatcher dispatcher,
      ContainerExecutor exec, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    super("containers-launcher");
    this.exec = exec;
    this.context = context;
    this.dispatcher = dispatcher;
    this.dirsHandler = dirsHandler;
    this.containerManager = containerManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    try {
      //TODO Is this required?
      FileContext.getLocalFSFileContext(conf);
    } catch (UnsupportedFileSystemException e) {
      throw new YarnRuntimeException("Failed to start ContainersLauncher", e);
    }
    super.serviceInit(conf);
  }

  @Override
  protected  void serviceStop() throws Exception {
    containerLauncher.shutdownNow();
    super.serviceStop();
  }

  @Override
  public void handle(ContainersLauncherEvent event) {
    // TODO: ContainersLauncher launches containers one by one!!
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerId();
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        Application app =
          context.getApplications().get(
              containerId.getApplicationAttemptId().getApplicationId());

        ContainerLaunch launch =
            new ContainerLaunch(context, getConfig(), dispatcher, exec, app,
              event.getContainer(), dirsHandler, containerManager);
        containerLauncher.submit(launch);
        running.put(containerId, launch);
        break;
      case RECOVER_CONTAINER:
        app = context.getApplications().get(
            containerId.getApplicationAttemptId().getApplicationId());
        launch = new RecoveredContainerLaunch(context, getConfig(), dispatcher,
            exec, app, event.getContainer(), dirsHandler, containerManager);
        containerLauncher.submit(launch);
        running.put(containerId, launch);
        break;
      case CLEANUP_CONTAINER:
        ContainerLaunch launcher = running.remove(containerId);
        if (launcher == null) {
          // Container not launched. So nothing needs to be done.
          return;
        }

        // Cleanup a container whether it is running/killed/completed, so that
        // no sub-processes are alive.
        try {
          launcher.cleanupContainer();
        } catch (IOException e) {
          LOG.warn("Got exception while cleaning container " + containerId
              + ". Ignoring.");
        }
        break;
    }
  }

}
