/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.util.List;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory
      .getLog(DefaultLinuxContainerRuntime.class);
  private Configuration conf;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;

  public DefaultLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.conf = conf;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    //nothing to do here at the moment.
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation launchOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LAUNCH_CONTAINER);

    //All of these arguments are expected to be available in the runtime context
    launchOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.
            RunAsUserCommand.LAUNCH_CONTAINER.getValue()),
        ctx.getExecutionAttribute(APPID),
        ctx.getExecutionAttribute(CONTAINER_ID_STR),
        ctx.getExecutionAttribute(CONTAINER_WORK_DIR).toString(),
        ctx.getExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH).toUri()
            .getPath(),
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
        ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOCAL_DIRS)),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOG_DIRS)),
        ctx.getExecutionAttribute(RESOURCES_OPTIONS));

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }

    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> prefixCommands = (List<String>) ctx.getExecutionAttribute(
        CONTAINER_LAUNCH_PREFIX_COMMANDS);

    try {
      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
            launchOp, null, container.getLaunchContext().getEnvironment(),
            false, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);

      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(PrivilegedOperation.RunAsUserCommand
            .SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    //Some failures here are acceptable. Let the calling executor decide.
    signalOp.disableFailureLogging();

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, container.getLaunchContext().getEnvironment(),
          false, true);
    } catch (PrivilegedOperationException e) {
      //Don't log the failure here. Some kinds of signaling failures are
      // acceptable. Let the calling executor decide what to do.
      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }
}
