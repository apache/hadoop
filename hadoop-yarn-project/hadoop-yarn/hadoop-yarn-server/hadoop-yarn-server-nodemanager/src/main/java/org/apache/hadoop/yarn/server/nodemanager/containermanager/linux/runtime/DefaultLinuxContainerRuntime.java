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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * This class is a {@link ContainerRuntime} implementation that uses the
 * native {@code container-executor} binary via a
 * {@link PrivilegedOperationExecutor} instance to launch processes using the
 * standard process model.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DefaultLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultLinuxContainerRuntime.class);
  private static final String TMP_FILE_PREFIX = "yarn.";
  private static final String TMP_FILE_SUFFIX = ".cmd";
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private Configuration conf;

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   */
  public DefaultLinuxContainerRuntime(PrivilegedOperationExecutor
      privilegedOperationExecutor) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);
    if (type == null) {
      type = conf.get(YarnConfiguration.LINUX_CONTAINER_RUNTIME_TYPE);
    }
    return type == null || type.isEmpty() || type.equals("default");
  }

  @Override
  public void initialize(Configuration conf, Context nmContext)
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
        ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath());
    Path keystorePath = ctx.getExecutionAttribute(NM_PRIVATE_KEYSTORE_PATH);
    Path truststorePath = ctx.getExecutionAttribute(NM_PRIVATE_TRUSTSTORE_PATH);
    if (keystorePath != null && truststorePath != null) {
      launchOp.appendArgs("--https",
          keystorePath.toUri().getPath(),
          truststorePath.toUri().getPath());
    } else {
      launchOp.appendArgs("--http");
    }
    launchOp.appendArgs(ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOCAL_DIRS)),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            ctx.getExecutionAttribute(LOG_DIRS)),
        ctx.getExecutionAttribute(RESOURCES_OPTIONS));

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }

    // Some failures here are acceptable. Let the calling executor decide.
    launchOp.disableFailureLogging();

    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> prefixCommands = (List<String>) ctx.getExecutionAttribute(
        CONTAINER_LAUNCH_PREFIX_COMMANDS);

    try {
      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
            launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException("Launch container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void relaunchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    launchContainer(ctx);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
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
          signalOp, null, null, false, false);
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

  @Override
  public String[] getIpAndHost(Container container) {
    return ContainerExecutor.getLocalIpAndHost(container);
  }

  @Override
  public String getExposedPorts(Container container) {
    return null;
  }

  @Override
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    IOStreamPair output;
    try {
      PrivilegedOperation privOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.EXEC_CONTAINER);
      String commandFile = writeCommandToTempFile(ctx);
      privOp.appendArgs(commandFile);
      privOp.disableFailureLogging();
      output =
          privilegedOperationExecutor.executePrivilegedInteractiveOperation(
              null, privOp);
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException(
          "Execute container interactive shell failed", e.getExitCode(),
          e.getOutput(), e.getErrorOutput());
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException executing command: ", ie);
      throw new ContainerExecutionException(ie.getMessage());
    }
    return output;
  }

  private String writeCommandToTempFile(ContainerExecContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    File cmdDir = null;
    String appId = container.getContainerId().getApplicationAttemptId()
        .getApplicationId().toString();
    String containerId = container.getContainerId().toString();
    String filePrefix = containerId.toString();
    try {
      String cmdDirPath = ctx.getLocalDirsHandlerService().getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR +
          appId + Path.SEPARATOR + filePrefix + Path.SEPARATOR).toString();
      cmdDir = new File(cmdDirPath);
      if (!cmdDir.mkdirs() && !cmdDir.exists()) {
        throw new IOException("Cannot create container private directory "
            + cmdDir);
      }
      File commandFile = File.createTempFile(TMP_FILE_PREFIX + filePrefix,
          TMP_FILE_SUFFIX, cmdDir);
      try (
          Writer writer = new OutputStreamWriter(
              new FileOutputStream(commandFile.toString()), "UTF-8");
          PrintWriter printWriter = new PrintWriter(writer);
      ) {
        Map<String, List<String>> cmd = new HashMap<String, List<String>>();
        // command = exec
        List<String> exec = new ArrayList<String>();
        exec.add("exec");
        cmd.put("command", exec);
        // user = foobar
        List<String> user = new ArrayList<String>();
        user.add(container.getUser());
        cmd.put("user", user);
        // launch-command = bash,-i
        List<String> commands = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        sb.append("/bin/");
        sb.append(ctx.getShell());
        commands.add(sb.toString());
        commands.add("-ir");
        cmd.put("launch-command", commands);
        // workdir = ../nm-local-dir/usercache/appcache/appid/containerid
        List<String> workdir = new ArrayList<String>();
        workdir.add(container.getWorkDir());
        cmd.put("workdir", workdir);
        // generate cmd file
        printWriter.println("[command-execution]");
        for (Map.Entry<String, List<String>> entry :
            cmd.entrySet()) {
          if (entry.getKey().contains("=")) {
            throw new ContainerExecutionException(
                "'=' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          if (entry.getValue().contains("\n")) {
            throw new ContainerExecutionException(
                "'\\n' found in entry for docker command file, key = " + entry
                    .getKey() + "; value = " + entry.getValue());
          }
          printWriter.println("  " + entry.getKey() + "=" + StringUtils
              .join(",", entry.getValue()));
        }
        return commandFile.toString();
      }
    } catch (IOException e) {
      LOG.warn("Unable to write command to " + cmdDir);
      throw new ContainerExecutionException(e);
    }
  }
}
