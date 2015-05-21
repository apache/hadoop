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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(LinuxContainerExecutor.class);

  private String nonsecureLocalUser;
  private Pattern nonsecureLocalUserPattern;
  private String containerExecutorExe;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;

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
    nonsecureLocalUser = conf.get(
        YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY,
        YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);
    nonsecureLocalUserPattern = Pattern.compile(
        conf.get(YarnConfiguration.NM_NONSECURE_MODE_USER_PATTERN_KEY,
            YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_USER_PATTERN));        
    containerLimitUsers = conf.getBoolean(
      YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS,
      YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LIMIT_USERS);
    if (!containerLimitUsers) {
      LOG.warn(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS +
          ": impersonation without authentication enabled");
    }
  }

  void verifyUsernamePattern(String user) {
    if (!UserGroupInformation.isSecurityEnabled() &&
        !nonsecureLocalUserPattern.matcher(user).matches()) {
        throw new IllegalArgumentException("Invalid user name '" + user + "'," +
            " it must match '" + nonsecureLocalUserPattern.pattern() + "'");
      }
  }

  String getRunAsUser(String user) {
    if (UserGroupInformation.isSecurityEnabled() ||
       !containerLimitUsers) {
      return user;
    } else {
      return nonsecureLocalUser;
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
      LOG.warn("Exit code from container executor initialization is : "
          + exitCode, e);
      logOutput(shExec.getOutput());
      throw new IOException("Linux container executor not configured properly"
          + " (error=" + exitCode + ")", e);
    }

    try {
      Configuration conf = super.getConf();

      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf);
      if (resourceHandlerChain != null) {
        resourceHandlerChain.bootstrap(conf);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Failed to bootstrap configured resource subsystems! ", e);
      throw new IOException("Failed to bootstrap configured resource subsystems!");
    }

    resourcesHandler.init(this);
  }
  
  @Override
  public void startLocalizer(LocalizerStartContext ctx)
      throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();
    
    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);
    List<String> command = new ArrayList<String>();
    addSchedPriorityCommand(command);
    command.addAll(Arrays.asList(containerExecutorExe,
                   runAsUser,
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
    command.addAll(ContainerLocalizer.getJavaOpts(getConf()));
    buildMainArgs(command, user, appId, locId, nmAddr, localDirs);
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
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
      LOG.warn("Exit code from container " + locId + " startLocalizer is : "
          + exitCode, e);
      logOutput(shExec.getOutput());
      throw new IOException("Application " + appId + " initialization failed" +
      		" (exitCode=" + exitCode + ") with output: " + shExec.getOutput(), e);
    }
  }

  @VisibleForTesting
  public void buildMainArgs(List<String> command, String user, String appId,
      String locId, InetSocketAddress nmAddr, List<String> localDirs) {
    ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr,
      localDirs);
  }

  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    ContainerId containerId = container.getContainerId();
    String containerIdStr = ConverterUtils.toString(containerId);
    
    resourcesHandler.preExecute(containerId,
            container.getResource());
    String resourcesOptions = resourcesHandler.getResourcesOption(
            containerId);
    String tcCommandFile = null;

    try {
      if (resourceHandlerChain != null) {
        List<PrivilegedOperation> ops = resourceHandlerChain
            .preStart(container);

        if (ops != null) {
          List<PrivilegedOperation> resourceOps = new ArrayList<>();

          resourceOps.add(new PrivilegedOperation
              (PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
                  resourcesOptions));

          for (PrivilegedOperation op : ops) {
            switch (op.getOperationType()) {
              case ADD_PID_TO_CGROUP:
                resourceOps.add(op);
                break;
              case TC_MODIFY_STATE:
                tcCommandFile = op.getArguments().get(0);
                break;
              default:
                LOG.warn("PrivilegedOperation type unsupported in launch: "
                    + op.getOperationType());
            }
          }

          if (resourceOps.size() > 1) {
            //squash resource operations
            try {
              PrivilegedOperation operation = PrivilegedOperationExecutor
                  .squashCGroupOperations(resourceOps);
              resourcesOptions = operation.getArguments().get(0);
            } catch (PrivilegedOperationException e) {
              LOG.error("Failed to squash cgroup operations!", e);
              throw new ResourceHandlerException("Failed to squash cgroup operations!");
            }
          }
        }
      }
    } catch (ResourceHandlerException e) {
      LOG.error("ResourceHandlerChain.preStart() failed!", e);
      throw new IOException("ResourceHandlerChain.preStart() failed!");
    }

    ShellCommandExecutor shExec = null;

    try {
      Path pidFilePath = getPidFilePath(containerId);
      if (pidFilePath != null) {
        List<String> command = new ArrayList<String>();
        addSchedPriorityCommand(command);
        command.addAll(Arrays.asList(
            containerExecutorExe, runAsUser, user, Integer
                .toString(Commands.LAUNCH_CONTAINER.getValue()), appId,
            containerIdStr, containerWorkDir.toString(),
            nmPrivateContainerScriptPath.toUri().getPath().toString(),
            nmPrivateTokensPath.toUri().getPath().toString(),
            pidFilePath.toString(),
            StringUtils.join(",", localDirs),
            StringUtils.join(",", logDirs),
            resourcesOptions));

        if (tcCommandFile != null) {
            command.add(tcCommandFile);
        }

        String[] commandArray = command.toArray(new String[command.size()]);
        shExec = new ShellCommandExecutor(commandArray, null, // NM's cwd
            container.getLaunchContext().getEnvironment()); // sanitized env
        if (LOG.isDebugEnabled()) {
          LOG.debug("launchContainer: " + Arrays.toString(commandArray));
        }
        shExec.execute();
        if (LOG.isDebugEnabled()) {
          logOutput(shExec.getOutput());
        }
      } else {
        LOG.info("Container was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // container-executor's output
      if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: "
            + containerId + " and exit code: " + exitCode , e);

        StringBuilder builder = new StringBuilder();
        builder.append("Exception from container-launch.\n");
        builder.append("Container id: " + containerId + "\n");
        builder.append("Exit code: " + exitCode + "\n");
        if (!Optional.fromNullable(e.getMessage()).or("").isEmpty()) {
          builder.append("Exception message: " + e.getMessage() + "\n");
        }
        builder.append("Stack trace: "
            + StringUtils.stringifyException(e) + "\n");
        if (!shExec.getOutput().isEmpty()) {
          builder.append("Shell output: " + shExec.getOutput() + "\n");
        }
        String diagnostics = builder.toString();
        logOutput(diagnostics);
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
      } else {
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            "Container killed on request. Exit code is " + exitCode));
      }
      return exitCode;
    } finally {
      resourcesHandler.postExecute(containerId);

      try {
        if (resourceHandlerChain != null) {
          resourceHandlerChain.postComplete(containerId);
        }
      } catch (ResourceHandlerException e) {
        LOG.warn("ResourceHandlerChain.postComplete failed for " +
            "containerId: " + containerId + ". Exception: " + e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Output from LinuxContainerExecutor's launchContainer follows:");
      logOutput(shExec.getOutput());
    }
    return 0;
  }

  @Override
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    ContainerId containerId = ctx.getContainerId();

    try {
      //Resource handler chain needs to reacquire container state
      //as well
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.reacquireContainer(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.reacquireContainer failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }

      return super.reacquireContainer(ctx);
    } finally {
      resourcesHandler.postExecute(containerId);
      if (resourceHandlerChain != null) {
        try {
          resourceHandlerChain.postComplete(containerId);
        } catch (ResourceHandlerException e) {
          LOG.warn("ResourceHandlerChain.postComplete failed for " +
              "containerId: " + containerId + " Exception: " + e);
        }
      }
    }
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    String[] command =
        new String[] { containerExecutorExe,
                   runAsUser,
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
      LOG.warn("Error in signalling container " + pid + " with " + signal
          + "; exit = " + ret_code, e);
      logOutput(shExec.getOutput());
      throw new IOException("Problem signalling container " + pid + " with "
          + signal + "; output: " + shExec.getOutput() + " and exitCode: "
          + ret_code, e);
    }
    return true;
  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx) {
    String user = ctx.getUser();
    Path dir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    String dirString = dir == null ? "" : dir.toUri().getPath();

    List<String> command = new ArrayList<String>(
        Arrays.asList(containerExecutorExe,
                    runAsUser,
                    user,
                    Integer.toString(Commands.DELETE_AS_USER.getValue()),
                    dirString));
    List<String> pathsToDelete = new ArrayList<String>();
    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + dir);
      pathsToDelete.add(dirString);
    } else {
      for (Path baseDir : baseDirs) {
        Path del = dir == null ? baseDir : new Path(baseDir, dir);
        LOG.info("Deleting path : " + del);
        pathsToDelete.add(del.toString());
        command.add(baseDir.toUri().getPath());
      }
    }
    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
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
      LOG.error("DeleteAsUser for " + StringUtils.join(" ", pathsToDelete)
          + " returned with exit code: " + exitCode, e);
      LOG.error("Output from LinuxContainerExecutor's deleteAsUser follows:");
      logOutput(shExec.getOutput());
    }
  }
  
  @Override
  public boolean isContainerProcessAlive(ContainerLivenessContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();

    // Send a test signal to the process as the user to see if it's alive
    return signalContainer(new ContainerSignalContext.Builder()
        .setUser(user)
        .setPid(pid)
        .setSignal(Signal.NULL)
        .build());
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
        LOG.warn("Exception in LinuxContainerExecutor mountCgroups ", e);
        logOutput(shExec.getOutput());
        throw new IOException("Problem mounting cgroups " + cgroupKVs + 
          "; exit code = " + ret_code + " and output: " + shExec.getOutput(), e);
    }
  }  
}
