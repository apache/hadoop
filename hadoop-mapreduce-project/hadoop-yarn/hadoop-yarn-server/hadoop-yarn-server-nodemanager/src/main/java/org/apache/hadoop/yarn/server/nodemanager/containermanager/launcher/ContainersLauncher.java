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
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.service.AbstractService;

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
  private final ExecutorService containerLauncher =
    Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("ContainersLauncher #%d")
          .build());
  private final Map<ContainerId,RunningContainer> running =
    Collections.synchronizedMap(new HashMap<ContainerId,RunningContainer>());

  private static final class RunningContainer {
    public RunningContainer(Future<Integer> submit,
        ContainerLaunch launcher) {
      this.runningcontainer = submit;
      this.launcher = launcher;
    }

    Future<Integer> runningcontainer;
    ContainerLaunch launcher;
  }


  public ContainersLauncher(Context context, Dispatcher dispatcher,
      ContainerExecutor exec) {
    super("containers-launcher");
    this.exec = exec;
    this.context = context;
    this.dispatcher = dispatcher;
  }

  @Override
  public void init(Configuration conf) {
    try {
      //TODO Is this required?
      FileContext.getLocalFSFileContext(conf);
    } catch (UnsupportedFileSystemException e) {
      throw new YarnException("Failed to start ContainersLauncher", e);
    }
    super.init(conf);
  }

  @Override
  public void stop() {
    containerLauncher.shutdownNow();
    super.stop();
  }

  @Override
  public void handle(ContainersLauncherEvent event) {
    // TODO: ContainersLauncher launches containers one by one!!
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerID();
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        Application app =
          context.getApplications().get(
              containerId.getApplicationAttemptId().getApplicationId());
      ContainerLaunch launch =
          new ContainerLaunch(getConfig(), dispatcher, exec, app,
              event.getContainer());
        running.put(containerId,
            new RunningContainer(containerLauncher.submit(launch), 
                launch));
        break;
      case CLEANUP_CONTAINER:
        RunningContainer rContainerDatum = running.remove(containerId);
        Future<Integer> rContainer = rContainerDatum.runningcontainer;
        if (rContainer != null 
            && !rContainer.isDone()) {
          // Cancel the future so that it won't be launched 
          // if it isn't already.
          rContainer.cancel(false);
        }

        // Cleanup a container whether it is running/killed/completed, so that
        // no sub-processes are alive.
        try {
          rContainerDatum.launcher.cleanupContainer();
        } catch (IOException e) {
          LOG.warn("Got exception while cleaning container " + containerId
              + ". Ignoring.");
        }
        break;
    }
  }

}
