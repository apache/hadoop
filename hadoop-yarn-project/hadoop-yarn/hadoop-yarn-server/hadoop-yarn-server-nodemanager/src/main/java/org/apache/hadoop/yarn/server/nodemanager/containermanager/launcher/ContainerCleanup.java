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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.EXIT_CODE_FILE_SUFFIX;

/**
 * Cleanup the container.
 * Cancels the launch if launch has not started yet or signals
 * the executor to not execute the process if not already done so.
 * Also, sends a SIGTERM followed by a SIGKILL to the process if
 * the process id is available.
 */
public class ContainerCleanup implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerCleanup.class);

  private final Context context;
  private final Configuration conf;
  private final Dispatcher dispatcher;
  private final ContainerExecutor exec;
  private final Container container;
  private final ContainerLaunch launch;
  private final long sleepDelayBeforeSigKill;


  public ContainerCleanup(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec,
      Container container,
      ContainerLaunch containerLaunch) {

    this.context = Preconditions.checkNotNull(context, "context");
    this.conf = Preconditions.checkNotNull(configuration, "config");
    this.dispatcher = Preconditions.checkNotNull(dispatcher, "dispatcher");
    this.exec = Preconditions.checkNotNull(exec, "exec");
    this.container = Preconditions.checkNotNull(container, "container");
    this.launch = Preconditions.checkNotNull(containerLaunch, "launch");
    this.sleepDelayBeforeSigKill = conf.getLong(
        YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
        YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS);
  }

  @Override
  public void run() {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Cleaning up container " + containerIdStr);

    try {
      context.getNMStateStore().storeContainerKilled(containerId);
    } catch (IOException e) {
      LOG.error("Unable to mark container " + containerId
          + " killed in store", e);
    }

    // launch flag will be set to true if process already launched
    boolean alreadyLaunched = !launch.markLaunched();
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " No cleanup needed to be done");
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marking container " + containerIdStr + " as inactive");
    }
    // this should ensure that if the container process has not launched
    // by this time, it will never be launched
    exec.deactivateContainer(containerId);
    Path pidFilePath = launch.getPidFilePath();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting pid for container {} to kill"
              + " from pid file {}", containerIdStr, pidFilePath != null ?
          pidFilePath : "null");
    }

    // however the container process may have already started
    try {

      // get process id from pid file if available
      // else if shell is still active, get it from the shell
      String processId = launch.getContainerPid();

      // kill process
      String user = container.getUser();
      if (processId != null) {
        signalProcess(processId, user, containerIdStr);
      } else {
        // Normally this means that the process was notified about
        // deactivateContainer above and did not start.
        // Since we already set the state to RUNNING or REINITIALIZING
        // we have to send a killed event to continue.
        if (!launch.isLaunchCompleted()) {
          LOG.warn("Container clean up before pid file created "
              + containerIdStr);
          dispatcher.getEventHandler().handle(
              new ContainerExitEvent(container.getContainerId(),
                  ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
                  Shell.WINDOWS ?
                      ContainerExecutor.ExitCode.FORCE_KILLED.getExitCode() :
                      ContainerExecutor.ExitCode.TERMINATED.getExitCode(),
                  "Container terminated before pid file created."));
          // There is a possibility that the launch grabbed the file name before
          // the deactivateContainer above but it was slow enough to avoid
          // getContainerPid.
          // Increasing YarnConfiguration.NM_PROCESS_KILL_WAIT_MS
          // reduces the likelihood of this race condition and process leak.
        }
        // The Docker container may not have fully started, reap the container.
        if (DockerLinuxContainerRuntime.isDockerContainerRequested(conf,
            container.getLaunchContext().getEnvironment())) {
          reapDockerContainerNoPid(user);
        }
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to cleanup container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.warn(message);
      dispatcher.getEventHandler().handle(
          new ContainerDiagnosticsUpdateEvent(containerId, message));
    } finally {
      // cleanup pid file if present
      if (pidFilePath != null) {
        try {
          FileContext lfs = FileContext.getLocalFSFileContext();
          lfs.delete(pidFilePath, false);
          lfs.delete(pidFilePath.suffix(EXIT_CODE_FILE_SUFFIX), false);
        } catch (IOException ioe) {
          LOG.warn("{} exception trying to delete pid file {}. Ignoring.",
              containerId, pidFilePath, ioe);
        }
      }
    }

    try {
      // Reap the container
      launch.reapContainer();
    } catch (IOException ioe) {
      LOG.warn("{} exception trying to reap container. Ignoring.", containerId,
          ioe);
    }
  }

  private void signalProcess(String processId, String user,
      String containerIdStr) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending signal to pid " + processId + " as user " + user
          + " for container " + containerIdStr);
    }
    final ContainerExecutor.Signal signal =
        sleepDelayBeforeSigKill > 0 ? ContainerExecutor.Signal.TERM :
            ContainerExecutor.Signal.KILL;

    boolean result = sendSignal(user, processId, signal);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sent signal " + signal + " to pid " + processId + " as user "
          + user + " for container " + containerIdStr + ", result="
          + (result ? "success" : "failed"));
    }
    if (sleepDelayBeforeSigKill > 0) {
      new ContainerExecutor.DelayedProcessKiller(container, user, processId,
          sleepDelayBeforeSigKill, ContainerExecutor.Signal.KILL, exec).start();
    }
  }

  private boolean sendSignal(String user, String processId,
      ContainerExecutor.Signal signal)
      throws IOException {
    return exec.signalContainer(
        new ContainerSignalContext.Builder().setContainer(container)
            .setUser(user).setPid(processId).setSignal(signal).build());
  }

  private void reapDockerContainerNoPid(String user) throws IOException {
    String containerIdStr =
        container.getContainerTokenIdentifier().getContainerID().toString();
    LOG.info("Unable to obtain pid, but docker container request detected. "
        + "Attempting to reap container " + containerIdStr);
    boolean result = exec.reapContainer(
        new ContainerReapContext.Builder()
            .setContainer(container)
            .setUser(container.getUser())
            .build());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sent signal to docker container " + containerIdStr
          + " as user " + user + ", result=" + (result ? "success" : "failed"));
    }
  }
}
