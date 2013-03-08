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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(LinuxContainerExecutor.class);

  private String containerExecutorExe;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    containerExecutorExe = getContainerExecutorExecutablePath(conf);
    
    resourcesHandler = ReflectionUtils.newInstance(
            conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
              DefaultLCEResourcesHandler.class, LCEResourcesHandler.class), conf);
    resourcesHandler.setConf(conf);

    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY) != null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = conf
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 
          YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }
  }

  /**
   * List of commands that the setuid script will execute.
   */
  enum Commands {
    INITIALIZE_CONTAINER(0),
    LAUNCH_CONTAINER(1),
    SIGNAL_CONTAINER(2),
    DELETE_AS_USER(3);

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
    UNABLE_TO_EXECUTE_CONTAINER_SCRIPT(7),
    INVALID_CONTAINER_PID(9),
    INVALID_CONTAINER_EXEC_PERMISSIONS(22),
    INVALID_CONFIG_FILE(24),
    WRITE_CGROUP_FAILED(27);

    private final int value;
    ResultCode(int value) {
      this.value = value;
    }
    int getValue() {
      return value;
    }
  }

  protected String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
      new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
      ? defaultPath
      : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, defaultPath);
  }

  protected void addSchedPriorityCommand(List<String> command) {
    if (containerSchedPriorityIsSet) {
      command.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment)));
    }
  }

  @Override 
  public void init() throws IOException {        
    // Send command to executor which will just start up, 
    // verify configuration/permissions and exit
    List<String> command = new ArrayList<String>(
        Arrays.asList(containerExecutorExe,
            "--checksetup"));
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkLinuxExecutorSetup: " + Arrays.toString(commandArray));
    }
    try {
      shExec.execute();
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode);
      logOutput(shExec.getOutput());
      throw new IOException("Linux container executor not configured properly"
          + " (error=" + exitCode + ")", e);
    }
   
    resourcesHandler.init(this);
  }
  
  @Override
  public void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      List<String> localDirs, List<String> logDirs)
      throws IOException, InterruptedException {

    List<String> command = new ArrayList<String>();
    addSchedPriorityCommand(command);
    command.addAll(Arrays.asList(containerExecutorExe,
                   user, 
                   Integer.toString(Commands.INITIALIZE_CONTAINER.getValue()),
                   appId,
                   nmPrivateContainerTokensPath.toUri().getPath().toString(),
                   StringUtils.join(",", localDirs),
                   StringUtils.join(",", logDirs)));

    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");
    command.add(jvm.toString());
    command.add("-classpath");
    command.add(System.getProperty("java.class.path"));
    String javaLibPath = System.getProperty("java.library.path");
    if (javaLibPath != null) {
      command.add("-Djava.library.path=" + javaLibPath);
    }
    command.add(ContainerLocalizer.class.getName());
    command.add(user);
    command.add(appId);
    command.add(locId);
    command.add(nmAddr.getHostName());
    command.add(Integer.toString(nmAddr.getPort()));
    for (String dir : localDirs) {
      command.add(dir);
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
      throw new IOException("App initialization failed (" + exitCode + 
          ") with output: " + shExec.getOutput(), e);
    }
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateCotainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {

    ContainerId containerId = container.getContainerID();
    String containerIdStr = ConverterUtils.toString(containerId);
    
    resourcesHandler.preExecute(containerId,
            container.getLaunchContext().getResource());
    String resourcesOptions = resourcesHandler.getResourcesOption(
            containerId);

    ShellCommandExecutor shExec = null;

    try {
      Path pidFilePath = getPidFilePath(containerId);
      if (pidFilePath != null) {
        List<String> command = new ArrayList<String>();
        addSchedPriorityCommand(command);
        command.addAll(Arrays.asList(
            containerExecutorExe, user, Integer
                .toString(Commands.LAUNCH_CONTAINER.getValue()), appId,
            containerIdStr, containerWorkDir.toString(),
            nmPrivateCotainerScriptPath.toUri().getPath().toString(),
            nmPrivateTokensPath.toUri().getPath().toString(),
            pidFilePath.toString(),
            StringUtils.join(",", localDirs),
            StringUtils.join(",", logDirs),
            resourcesOptions));
        String[] commandArray = command.toArray(new String[command.size()]);
        shExec = new ShellCommandExecutor(commandArray, null, // NM's cwd
            container.getLaunchContext().getEnvironment()); // sanitized env
        // DEBUG
        LOG.info("launchContainer: " + Arrays.toString(commandArray));
        shExec.execute();
        if (LOG.isDebugEnabled()) {
          logOutput(shExec.getOutput());
        }
      } else {
        LOG.info("Container was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (ExitCodeException e) {

      if (null == shExec) {
        return -1;
      }

      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // container-executor's output
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch : ", e);
        logOutput(shExec.getOutput());
        String diagnostics = "Exception from container-launch: \n"
            + StringUtils.stringifyException(e) + "\n" + shExec.getOutput();
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      resourcesHandler.postExecute(containerId);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Output from LinuxContainerExecutor's launchContainer follows:");
      logOutput(shExec.getOutput());
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
      if (ret_code == ResultCode.INVALID_CONTAINER_PID.getValue()) {
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
  
  public void mountCgroups(List<String> cgroupKVs, String hierarchy)
         throws IOException {
    List<String> command = new ArrayList<String>(
            Arrays.asList(containerExecutorExe, "--mount-cgroups", hierarchy));
    command.addAll(cgroupKVs);
    
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);

    if (LOG.isDebugEnabled()) {
        LOG.debug("mountCgroups: " + Arrays.toString(commandArray));
    }

    try {
        shExec.execute();
    } catch (IOException e) {
        int ret_code = shExec.getExitCode();
        logOutput(shExec.getOutput());
        throw new IOException("Problem mounting cgroups " + cgroupKVs + 
                  "; exit code = " + ret_code, e);
    }
  }  
}
