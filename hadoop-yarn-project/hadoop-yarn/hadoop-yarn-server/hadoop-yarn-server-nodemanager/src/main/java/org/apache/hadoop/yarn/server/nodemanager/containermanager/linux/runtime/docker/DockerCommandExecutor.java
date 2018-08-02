/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for executing common docker operations.
 */
public final class DockerCommandExecutor {
  private static final Logger LOG =
       LoggerFactory.getLogger(DockerCommandExecutor.class);

  /**
   * Potential states that the docker status can return.
   */
  public enum DockerContainerStatus {
    CREATED("created"),
    RUNNING("running"),
    STOPPED("stopped"),
    RESTARTING("restarting"),
    REMOVING("removing"),
    DEAD("dead"),
    EXITED("exited"),
    NONEXISTENT("nonexistent"),
    UNKNOWN("unknown");

    private final String name;

    DockerContainerStatus(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private DockerCommandExecutor() {
  }

  /**
   * Execute a docker command and return the output.
   *
   * @param dockerCommand               the docker command to run.
   * @param containerId                 the id of the container.
   * @param env                         environment for the container.
   * @param privilegedOperationExecutor the privileged operations executor.
   * @param disableFailureLogging       disable logging for known rc failures.
   * @return the output of the operation.
   * @throws ContainerExecutionException if the operation fails.
   */
  public static String executeDockerCommand(DockerCommand dockerCommand,
      String containerId, Map<String, String> env,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      boolean disableFailureLogging, Context nmContext)
      throws ContainerExecutionException {
    PrivilegedOperation dockerOp = dockerCommand.preparePrivilegedOperation(
        dockerCommand, containerId, env, nmContext);

    if (disableFailureLogging) {
      dockerOp.disableFailureLogging();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running docker command: " + dockerCommand);
    }
    try {
      String result = privilegedOperationExecutor
          .executePrivilegedOperation(null, dockerOp, null,
              env, true, false);
      if (result != null && !result.isEmpty()) {
        result = result.trim();
      }
      return result;
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException("Docker operation failed",
          e.getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  /**
   * Get the status of the docker container. This runs a docker inspect to
   * get the status. If the container no longer exists, docker inspect throws
   * an exception and the nonexistent status is returned.
   *
   * @param containerId                 the id of the container.
   * @param privilegedOperationExecutor the privileged operations executor.
   * @return a {@link DockerContainerStatus} representing the current status.
   */
  public static DockerContainerStatus getContainerStatus(String containerId,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      Context nmContext) {
    try {
      DockerContainerStatus dockerContainerStatus;
      String currentContainerStatus =
          executeStatusCommand(containerId,
          privilegedOperationExecutor, nmContext);
      if (currentContainerStatus == null) {
        dockerContainerStatus = DockerContainerStatus.UNKNOWN;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.CREATED.getName())) {
        dockerContainerStatus = DockerContainerStatus.CREATED;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.RUNNING.getName())) {
        dockerContainerStatus = DockerContainerStatus.RUNNING;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.STOPPED.getName())) {
        dockerContainerStatus = DockerContainerStatus.STOPPED;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.RESTARTING.getName())) {
        dockerContainerStatus = DockerContainerStatus.RESTARTING;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.REMOVING.getName())) {
        dockerContainerStatus = DockerContainerStatus.REMOVING;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.DEAD.getName())) {
        dockerContainerStatus = DockerContainerStatus.DEAD;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.EXITED.getName())) {
        dockerContainerStatus = DockerContainerStatus.EXITED;
      } else if (currentContainerStatus
          .equals(DockerContainerStatus.NONEXISTENT.getName())) {
        dockerContainerStatus = DockerContainerStatus.NONEXISTENT;
      } else {
        dockerContainerStatus = DockerContainerStatus.UNKNOWN;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: " + dockerContainerStatus.getName()
            + " ContainerId: " + containerId);
      }
      return dockerContainerStatus;
    } catch (ContainerExecutionException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: "
            + DockerContainerStatus.NONEXISTENT.getName()
            + " ContainerId: " + containerId);
      }
      return DockerContainerStatus.NONEXISTENT;
    }
  }

  /**
   * Execute the docker inspect command to retrieve the docker container's
   * status.
   *
   * @param containerId                 the id of the container.
   * @param privilegedOperationExecutor the privileged operations executor.
   * @return the current container status.
   * @throws ContainerExecutionException if the docker operation fails to run.
   */
  private static String executeStatusCommand(String containerId,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      Context nmContext)
      throws ContainerExecutionException {
    DockerInspectCommand dockerInspectCommand =
        new DockerInspectCommand(containerId).getContainerStatus();
    try {
      return DockerCommandExecutor.executeDockerCommand(dockerInspectCommand,
          containerId, null, privilegedOperationExecutor, true, nmContext);
    } catch (ContainerExecutionException e) {
      throw new ContainerExecutionException(e);
    }
  }

  /**
   * Is the container in a stoppable state?
   *
   * @param containerStatus   the container's {@link DockerContainerStatus}.
   * @return                  is the container in a stoppable state.
   */
  public static boolean isStoppable(DockerContainerStatus containerStatus) {
    if (containerStatus.equals(DockerContainerStatus.RUNNING)
        || containerStatus.equals(DockerContainerStatus.RESTARTING)) {
      return true;
    }
    return false;
  }

  /**
   * Is the container in a killable state?
   *
   * @param containerStatus   the container's {@link DockerContainerStatus}.
   * @return                  is the container in a killable state.
   */
  public static boolean isKillable(DockerContainerStatus containerStatus) {
    return isStoppable(containerStatus);
  }

  /**
   * Is the container in a removable state?
   *
   * @param containerStatus   the container's {@link DockerContainerStatus}.
   * @return                  is the container in a removable state.
   */
  public static boolean isRemovable(DockerContainerStatus containerStatus) {
    return !containerStatus.equals(DockerContainerStatus.NONEXISTENT)
        && !containerStatus.equals(DockerContainerStatus.UNKNOWN)
        && !containerStatus.equals(DockerContainerStatus.REMOVING)
        && !containerStatus.equals(DockerContainerStatus.RUNNING);
  }

  /**
   * Is the container in a startable state?
   *
   * @param containerStatus   the container's {@link DockerContainerStatus}.
   * @return                  is the container in a startable state.
   */
  public static boolean isStartable(DockerContainerStatus containerStatus) {
    if (containerStatus.equals(DockerContainerStatus.EXITED)
        || containerStatus.equals(DockerContainerStatus.STOPPED)) {
      return true;
    }
    return false;
  }
}