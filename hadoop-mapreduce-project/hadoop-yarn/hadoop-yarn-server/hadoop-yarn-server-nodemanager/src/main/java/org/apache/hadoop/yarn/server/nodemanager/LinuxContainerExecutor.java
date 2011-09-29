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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(LinuxContainerExecutor.class);

  private String containerExecutorExe;
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    containerExecutorExe = getContainerExecutorExecutablePath(conf);
  }

  /**
   * List of commands that the setuid script will execute.
   */
  enum Commands {
    INITIALIZE_JOB(0),
    LAUNCH_CONTAINER(1),
    SIGNAL_CONTAINER(2),
    DELETE_AS_USER(3),
    DELETE_LOG_AS_USER(4);

    private int value;
    Commands(int value) {
      this.value = value;
    }
    int getValue() {
      return value;
    }
  }

  /**
   * Result codes returned from the C container-executor.
   * These must match the values in container-executor.h.
   */
  enum ResultCode {
    OK(0),
    INVALID_USER_NAME(2),
    INVALID_TASK_PID(9),
    INVALID_TASKCONTROLLER_PERMISSIONS(22),
    INVALID_CONFIG_FILE(24);

    private final int value;
    ResultCode(int value) {
      this.value = value;
    }
    int getValue() {
      return value;
    }
  }

  protected String getContainerExecutorExecutablePath(Configuration conf) {
    File hadoopBin = new File(System.getenv("YARN_HOME"), "bin");
    String defaultPath =
      new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
      ? defaultPath
      : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, defaultPath);
  }

  @Override
  public void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      List<Path> localDirs) throws IOException, InterruptedException {
    List<String> command = new ArrayList<String>(
      Arrays.asList(containerExecutorExe, 
                    user, 
                    Integer.toString(Commands.INITIALIZE_JOB.getValue()),
                    appId,
                    nmPrivateContainerTokensPath.toUri().getPath().toString()));
    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
    command.add(jvm.toString());
    command.add("-classpath");
    command.add(System.getProperty("java.class.path"));
    command.add(ContainerLocalizer.class.getName());
    command.add(user);
    command.add(appId);
    command.add(locId);
    command.add(nmAddr.getHostName());
    command.add(Integer.toString(nmAddr.getPort()));
    for (Path p : localDirs) {
      command.add(p.toUri().getPath().toString());
    }
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    // TODO: DEBUG
    LOG.info("initApplication: " + Arrays.toString(commandArray));
    if (LOG.isDebugEnabled()) {
      LOG.debug("initApplication: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode);
      logOutput(shExec.getOutput());
      throw new IOException("App initialization failed (" + exitCode + ")", e);
    }
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateCotainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir) throws IOException {

    ContainerId containerId = container.getContainerID();
    String containerIdStr = ConverterUtils.toString(containerId);
    List<String> command = new ArrayList<String>(
      Arrays.asList(containerExecutorExe, 
                    user, 
                    Integer.toString(Commands.LAUNCH_CONTAINER.getValue()),
                    appId,
                    containerIdStr,
                    containerWorkDir.toString(),
                    nmPrivateCotainerScriptPath.toUri().getPath().toString(),
                    nmPrivateTokensPath.toUri().getPath().toString()));
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    launchCommandObjs.put(containerId, shExec);
    // DEBUG
    LOG.info("launchContainer: " + Arrays.toString(commandArray));
    String output = shExec.getOutput();
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(output);
      }
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // container-executor's output
      if (exitCode != 143 && exitCode != 137) {
        LOG.warn("Exception from container-launch : ", e);
        logOutput(output);
        String diagnostics = "Exception from container-launch: \n"
            + StringUtils.stringifyException(e) + "\n" + output;
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      launchCommandObjs.remove(containerId);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Output from LinuxContainerExecutor's launchContainer follows:");
      logOutput(output);
    }
    return 0;
  }

  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {

    String[] command =
        new String[] { containerExecutorExe,
                   user,
                   Integer.toString(Commands.SIGNAL_CONTAINER.getValue()),
                   pid,
                   Integer.toString(signal.getValue()) };
    ShellCommandExecutor shExec = new ShellCommandExecutor(command);
    if (LOG.isDebugEnabled()) {
      LOG.debug("signalContainer: " + Arrays.toString(command));
    }
    try {
      shExec.execute();
    } catch (ExitCodeException e) {
      int ret_code = shExec.getExitCode();
      if (ret_code == ResultCode.INVALID_TASK_PID.getValue()) {
        return false;
      }
      logOutput(shExec.getOutput());
      throw new IOException("Problem signalling container " + pid + " with " +
                            signal + "; exit = " + ret_code);
    }
    return true;
  }

  @Override
  public void deleteAsUser(String user, Path dir, Path... baseDirs) {
    List<String> command = new ArrayList<String>(
        Arrays.asList(containerExecutorExe,
                    user,
                    Integer.toString(Commands.DELETE_AS_USER.getValue()),
                    dir == null ? "" : dir.toUri().getPath()));
    if (baseDirs == null || baseDirs.length == 0) {
      LOG.info("Deleting absolute path : " + dir);
    } else {
      for (Path baseDir : baseDirs) {
        Path del = dir == null ? baseDir : new Path(baseDir, dir);
        LOG.info("Deleting path : " + del);
        command.add(baseDir.toUri().getPath());
      }
    }
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    LOG.info(" -- DEBUG -- deleteAsUser: " + Arrays.toString(commandArray));
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteAsUser: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
      if (LOG.isDebugEnabled()) {
        logOutput(shExec.getOutput());
      }
    } catch (IOException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode);
      if (exitCode != 0) {
        LOG.error("DeleteAsUser for " + dir.toUri().getPath()
            + " returned with non-zero exit code" + exitCode);
        LOG.error("Output from LinuxContainerExecutor's deleteAsUser follows:");
        logOutput(shExec.getOutput());
      }
    }
  }
}
