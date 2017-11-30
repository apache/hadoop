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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DefaultLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DelegatingLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.CgroupsLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * <p>This class provides {@link Container} execution using a native
 * {@code container-executor} binary. By using a helper written it native code,
 * this class is able to do several things that the
 * {@link DefaultContainerExecutor} cannot, such as execution of applications
 * as the applications' owners, provide localization that takes advantage of
 * mapping the application owner to a UID on the execution host, resource
 * management through Linux CGROUPS, and Docker support.</p>
 *
 * <p>If {@code hadoop.security.authetication} is set to {@code simple},
 * then the
 * {@code yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users}
 * property will determine whether the {@code LinuxContainerExecutor} runs
 * processes as the application owner or as the default user, as set in the
 * {@code yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user}
 * property.</p>
 *
 * <p>The {@code LinuxContainerExecutor} will manage applications through an
 * appropriate {@link LinuxContainerRuntime} instance. This class uses a
 * {@link DelegatingLinuxContainerRuntime} instance, which will delegate calls
 * to either a {@link DefaultLinuxContainerRuntime} instance or a
 * {@link DockerLinuxContainerRuntime} instance, depending on the job's
 * configuration.</p>
 *
 * @see LinuxContainerRuntime
 * @see DelegatingLinuxContainerRuntime
 * @see DefaultLinuxContainerRuntime
 * @see DockerLinuxContainerRuntime
 * @see DockerLinuxContainerRuntime#isDockerContainerRequested
 */
public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Logger LOG =
       LoggerFactory.getLogger(LinuxContainerExecutor.class);

  private String nonsecureLocalUser;
  private Pattern nonsecureLocalUserPattern;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;
  private ResourceHandler resourceHandlerChain;
  private LinuxContainerRuntime linuxContainerRuntime;

  /**
   * The container exit code.
   */
  public enum ExitCode {
    SUCCESS(0),
    INVALID_ARGUMENT_NUMBER(1),
    INVALID_COMMAND_PROVIDED(3),
    INVALID_NM_ROOT_DIRS(5),
    SETUID_OPER_FAILED(6),
    UNABLE_TO_EXECUTE_CONTAINER_SCRIPT(7),
    UNABLE_TO_SIGNAL_CONTAINER(8),
    INVALID_CONTAINER_PID(9),
    OUT_OF_MEMORY(18),
    INITIALIZE_USER_FAILED(20),
    PATH_TO_DELETE_IS_NULL(21),
    INVALID_CONTAINER_EXEC_PERMISSIONS(22),
    INVALID_CONFIG_FILE(24),
    SETSID_OPER_FAILED(25),
    WRITE_PIDFILE_FAILED(26),
    WRITE_CGROUP_FAILED(27),
    TRAFFIC_CONTROL_EXECUTION_FAILED(28),
    DOCKER_RUN_FAILED(29),
    ERROR_OPENING_DOCKER_FILE(30),
    ERROR_READING_DOCKER_FILE(31),
    FEATURE_DISABLED(32),
    COULD_NOT_CREATE_SCRIPT_COPY(33),
    COULD_NOT_CREATE_CREDENTIALS_FILE(34),
    COULD_NOT_CREATE_WORK_DIRECTORIES(35),
    COULD_NOT_CREATE_APP_LOG_DIRECTORIES(36),
    COULD_NOT_CREATE_TMP_DIRECTORIES(37),
    ERROR_CREATE_CONTAINER_DIRECTORIES_ARGUMENTS(38);

    private final int code;

    ExitCode(int exitCode) {
      this.code = exitCode;
    }

    /**
     * Get the exit code as an int.
     * @return the exit code as an int
     */
    public int getExitCode() {
      return code;
    }

    @Override
    public String toString() {
      return String.valueOf(code);
    }
  }

  /**
   * Default constructor to allow for creation through reflection.
   */
  public LinuxContainerExecutor() {
  }

  /**
   * Create a LinuxContainerExecutor with a provided
   * {@link LinuxContainerRuntime}.  Used primarily for testing.
   *
   * @param linuxContainerRuntime the runtime to use
   */
  public LinuxContainerExecutor(LinuxContainerRuntime linuxContainerRuntime) {
    this.linuxContainerRuntime = linuxContainerRuntime;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    resourcesHandler = getResourcesHandler(conf);

    containerSchedPriorityIsSet = false;
    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY)
        != null) {
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

  private LCEResourcesHandler getResourcesHandler(Configuration conf) {
    LCEResourcesHandler handler = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.NM_LINUX_CONTAINER_RESOURCES_HANDLER,
            DefaultLCEResourcesHandler.class, LCEResourcesHandler.class), conf);

    // Stop using CgroupsLCEResourcesHandler
    // use the resource handler chain instead
    // ResourceHandlerModule will create the cgroup cpu module if
    // CgroupsLCEResourcesHandler is set
    if (handler instanceof CgroupsLCEResourcesHandler) {
      handler =
          ReflectionUtils.newInstance(DefaultLCEResourcesHandler.class, conf);
    }
    handler.setConf(conf);
    return handler;
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
   * Get the path to the {@code container-executor} binary. The path will
   * be absolute.
   *
   * @param conf the {@link Configuration}
   * @return the path to the {@code container-executor} binary
   */
  protected String getContainerExecutorExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "container-executor").getAbsolutePath();
    return null == conf
        ? defaultPath
        : conf.get(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        defaultPath);
  }

  /**
   * Add a niceness level to the process that will be executed.  Adds
   * {@code -n <nice>} to the given command. The niceness level will be
   * taken from the
   * {@code yarn.nodemanager.container-executer.os.sched.prioity} property.
   *
   * @param command the command to which to add the niceness setting.
   */
  protected void addSchedPriorityCommand(List<String> command) {
    if (containerSchedPriorityIsSet) {
      command.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment)));
    }
  }

  protected PrivilegedOperationExecutor getPrivilegedOperationExecutor() {
    return PrivilegedOperationExecutor.getInstance(getConf());
  }

  @Override
  public void init(Context nmContext) throws IOException {
    Configuration conf = super.getConf();

    // Send command to executor which will just start up,
    // verify configuration/permissions and exit
    try {
      PrivilegedOperation checkSetupOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.CHECK_SETUP);
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(checkSetupOp,
          false);
    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container executor initialization is : "
          + exitCode, e);

      throw new IOException("Linux container executor not configured properly"
          + " (error=" + exitCode + ")", e);
    }

    try {
      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf, nmContext);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resource handler chain enabled = " + (resourceHandlerChain
            != null));
      }
      if (resourceHandlerChain != null) {
        LOG.debug("Bootstrapping resource handler chain");
        resourceHandlerChain.bootstrap(conf);
      }
    } catch (ResourceHandlerException e) {
      LOG.error("Failed to bootstrap configured resource subsystems! ", e);
      throw new IOException(
          "Failed to bootstrap configured resource subsystems!");
    }

    try {
      if (linuxContainerRuntime == null) {
        LinuxContainerRuntime runtime = new DelegatingLinuxContainerRuntime();

        runtime.initialize(conf, nmContext);
        this.linuxContainerRuntime = runtime;
      }
    } catch (ContainerExecutionException e) {
      LOG.error("Failed to initialize linux container runtime(s)!", e);
      throw new IOException("Failed to initialize linux container runtime(s)!");
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
    PrivilegedOperation initializeContainerOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.INITIALIZE_CONTAINER);
    List<String> prefixCommands = new ArrayList<>();

    addSchedPriorityCommand(prefixCommands);
    initializeContainerOp.appendArgs(
        runAsUser,
        user,
        Integer.toString(
            PrivilegedOperation.RunAsUserCommand.INITIALIZE_CONTAINER
                .getValue()),
        appId,
        nmPrivateContainerTokensPath.toUri().getPath().toString(),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            localDirs),
        StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
            logDirs));

    File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");
    initializeContainerOp.appendArgs(jvm.toString());
    initializeContainerOp.appendArgs("-classpath");
    initializeContainerOp.appendArgs(System.getProperty("java.class.path"));
    String javaLibPath = System.getProperty("java.library.path");
    if (javaLibPath != null) {
      initializeContainerOp.appendArgs("-Djava.library.path=" + javaLibPath);
    }

    initializeContainerOp.appendArgs(ContainerLocalizer.getJavaOpts(getConf()));

    List<String> localizerArgs = new ArrayList<>();

    buildMainArgs(localizerArgs, user, appId, locId, nmAddr, localDirs);

    Path containerLogDir = getContainerLogDir(dirsHandler, appId, locId);
    localizerArgs = replaceWithContainerLogDir(localizerArgs, containerLogDir);

    initializeContainerOp.appendArgs(localizerArgs);

    try {
      Configuration conf = super.getConf();
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(prefixCommands,
          initializeContainerOp, null, null, false, true);

    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container " + locId + " startLocalizer is : "
          + exitCode, e);

      throw new IOException("Application " + appId + " initialization failed" +
          " (exitCode=" + exitCode + ") with output: " + e.getOutput(), e);
    }
  }

  private List<String> replaceWithContainerLogDir(List<String> commands,
      Path containerLogDir) {
    List<String> newCmds = new ArrayList<>(commands.size());

    for (String item : commands) {
      newCmds.add(item.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,
          containerLogDir.toString()));
    }

    return newCmds;
  }

  private Path getContainerLogDir(LocalDirsHandlerService dirsHandler,
      String appId, String containerId) throws IOException {
    String relativeContainerLogDir = ContainerLaunch
        .getRelativeContainerLogDir(appId, containerId);

    return dirsHandler.getLogPathForWrite(relativeContainerLogDir,
        false);
  }

  /**
   * Set up the {@link ContainerLocalizer}.
   *
   * @param command the current ShellCommandExecutor command line
   * @param user localization user
   * @param appId localized app id
   * @param locId localizer id
   * @param nmAddr nodemanager address
   * @param localDirs list of local dirs
   * @see ContainerLocalizer#buildMainArgs
   */
  @VisibleForTesting
  public void buildMainArgs(List<String> command, String user, String appId,
      String locId, InetSocketAddress nmAddr, List<String> localDirs) {
    ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr,
        localDirs, super.getConf());
  }

  @Override
  public void prepareContainer(ContainerPrepareContext ctx) throws IOException {

    ContainerRuntimeContext.Builder builder =
        new ContainerRuntimeContext.Builder(ctx.getContainer());

    builder.setExecutionAttribute(LOCALIZED_RESOURCES,
            ctx.getLocalizedResources())
        .setExecutionAttribute(USER, ctx.getUser())
        .setExecutionAttribute(CONTAINER_LOCAL_DIRS,
            ctx.getContainerLocalDirs())
        .setExecutionAttribute(CONTAINER_RUN_CMDS, ctx.getCommands())
        .setExecutionAttribute(CONTAINER_ID_STR,
            ctx.getContainer().getContainerId().toString());

    try {
      linuxContainerRuntime.prepareContainer(builder.build());
    } catch (ContainerExecutionException e) {
      throw new IOException("Unable to prepare container: ", e);
    }
  }

  @Override
  protected void updateEnvForWhitelistVars(Map<String, String> env) {
    if (linuxContainerRuntime.useWhitelistEnv(env)) {
      super.updateEnvForWhitelistVars(env);
    }
  }

  @Override
  public int launchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();

    verifyUsernamePattern(user);

    ContainerId containerId = container.getContainerId();

    resourcesHandler.preExecute(containerId,
            container.getResource());
    String resourcesOptions = resourcesHandler.getResourcesOption(containerId);
    String tcCommandFile = null;

    try {
      if (resourceHandlerChain != null) {
        List<PrivilegedOperation> ops = resourceHandlerChain
            .preStart(container);

        if (ops != null) {
          List<PrivilegedOperation> resourceOps = new ArrayList<>();

          resourceOps.add(new PrivilegedOperation(
              PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
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
              throw new ResourceHandlerException(
                  "Failed to squash cgroup operations!");
            }
          }
        }
      }
    } catch (ResourceHandlerException e) {
      LOG.error("ResourceHandlerChain.preStart() failed!", e);
      throw new IOException("ResourceHandlerChain.preStart() failed!", e);
    }

    try {
      Path pidFilePath = getPidFilePath(containerId);
      if (pidFilePath != null) {

        ContainerRuntimeContext runtimeContext = buildContainerRuntimeContext(
            ctx, pidFilePath, resourcesOptions, tcCommandFile);

        linuxContainerRuntime.launchContainer(runtimeContext);
      } else {
        LOG.info(
            "Container was marked as inactive. Returning terminated error");
        return ContainerExecutor.ExitCode.TERMINATED.getExitCode();
      }
    } catch (ContainerExecutionException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
      // terminated/killed forcefully. In all other cases, log the
      // output
      if (exitCode != ContainerExecutor.ExitCode.FORCE_KILLED.getExitCode()
          && exitCode != ContainerExecutor.ExitCode.TERMINATED.getExitCode()) {
        LOG.warn("Exception from container-launch with container ID: "
            + containerId + " and exit code: " + exitCode, e);

        StringBuilder builder = new StringBuilder();
        builder.append("Exception from container-launch.\n");
        builder.append("Container id: " + containerId + "\n");
        builder.append("Exit code: " + exitCode + "\n");
        if (!Optional.fromNullable(e.getErrorOutput()).or("").isEmpty()) {
          builder.append("Exception message: " + e.getErrorOutput() + "\n");
        }
        //Skip stack trace
        String output = e.getOutput();
        if (output != null && !e.getOutput().isEmpty()) {
          builder.append("Shell output: " + output + "\n");
        }
        String diagnostics = builder.toString();
        logOutput(diagnostics);
        container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
            diagnostics));
        if (exitCode ==
                ExitCode.INVALID_CONTAINER_EXEC_PERMISSIONS.getExitCode() ||
            exitCode ==
                ExitCode.INVALID_CONFIG_FILE.getExitCode() ||
            exitCode ==
                ExitCode.COULD_NOT_CREATE_SCRIPT_COPY.getExitCode() ||
            exitCode ==
                ExitCode.COULD_NOT_CREATE_CREDENTIALS_FILE.getExitCode() ||
            exitCode ==
                ExitCode.COULD_NOT_CREATE_WORK_DIRECTORIES.getExitCode() ||
            exitCode ==
                ExitCode.COULD_NOT_CREATE_APP_LOG_DIRECTORIES.getExitCode() ||
            exitCode ==
                ExitCode.COULD_NOT_CREATE_TMP_DIRECTORIES.getExitCode()) {
          throw new ConfigurationException(
              "Linux Container Executor reached unrecoverable exception", e);
        }
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

    return 0;
  }

  private ContainerRuntimeContext buildContainerRuntimeContext(
      ContainerStartContext ctx, Path pidFilePath,
      String resourcesOptions, String tcCommandFile) {

    List<String> prefixCommands = new ArrayList<>();
    addSchedPriorityCommand(prefixCommands);

    Container container = ctx.getContainer();

    ContainerRuntimeContext.Builder builder = new ContainerRuntimeContext
            .Builder(container);
    if (prefixCommands.size() > 0) {
      builder.setExecutionAttribute(CONTAINER_LAUNCH_PREFIX_COMMANDS,
              prefixCommands);
    }

    builder.setExecutionAttribute(LOCALIZED_RESOURCES,
        ctx.getLocalizedResources())
      .setExecutionAttribute(RUN_AS_USER, getRunAsUser(ctx.getUser()))
      .setExecutionAttribute(USER, ctx.getUser())
      .setExecutionAttribute(APPID, ctx.getAppId())
      .setExecutionAttribute(CONTAINER_ID_STR,
        container.getContainerId().toString())
      .setExecutionAttribute(CONTAINER_WORK_DIR, ctx.getContainerWorkDir())
      .setExecutionAttribute(NM_PRIVATE_CONTAINER_SCRIPT_PATH,
        ctx.getNmPrivateContainerScriptPath())
      .setExecutionAttribute(NM_PRIVATE_TOKENS_PATH,
        ctx.getNmPrivateTokensPath())
      .setExecutionAttribute(PID_FILE_PATH, pidFilePath)
      .setExecutionAttribute(LOCAL_DIRS, ctx.getLocalDirs())
      .setExecutionAttribute(LOG_DIRS, ctx.getLogDirs())
      .setExecutionAttribute(FILECACHE_DIRS, ctx.getFilecacheDirs())
      .setExecutionAttribute(USER_LOCAL_DIRS, ctx.getUserLocalDirs())
      .setExecutionAttribute(CONTAINER_LOCAL_DIRS, ctx.getContainerLocalDirs())
      .setExecutionAttribute(CONTAINER_LOG_DIRS, ctx.getContainerLogDirs())
      .setExecutionAttribute(RESOURCES_OPTIONS, resourcesOptions);

    if (tcCommandFile != null) {
      builder.setExecutionAttribute(TC_COMMAND_FILE, tcCommandFile);
    }

    return builder.build();
  }

  @Override
  public String[] getIpAndHost(Container container)
      throws ContainerExecutionException {
    return linuxContainerRuntime.getIpAndHost(container);
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
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    verifyUsernamePattern(user);
    String runAsUser = getRunAsUser(user);

    ContainerRuntimeContext runtimeContext = new ContainerRuntimeContext
        .Builder(container)
        .setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(PID, pid)
        .setExecutionAttribute(SIGNAL, signal)
        .build();

    try {
      linuxContainerRuntime.signalContainer(runtimeContext);
    } catch (ContainerExecutionException e) {
      int retCode = e.getExitCode();
      if (retCode == PrivilegedOperation.ResultCode.INVALID_CONTAINER_PID
          .getValue()) {
        return false;
      }
      LOG.warn("Error in signalling container " + pid + " with " + signal
          + "; exit = " + retCode, e);
      logOutput(e.getOutput());
      throw new IOException("Problem signalling container " + pid + " with "
          + signal + "; output: " + e.getOutput() + " and exitCode: "
          + retCode, e);
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

    PrivilegedOperation deleteAsUserOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.DELETE_AS_USER, (String) null);

    deleteAsUserOp.appendArgs(
        runAsUser,
        user,
        Integer.toString(PrivilegedOperation.
            RunAsUserCommand.DELETE_AS_USER.getValue()),
        dirString);

    List<String> pathsToDelete = new ArrayList<String>();
    if (baseDirs == null || baseDirs.size() == 0) {
      LOG.info("Deleting absolute path : " + dir);
      pathsToDelete.add(dirString);
    } else {
      for (Path baseDir : baseDirs) {
        Path del = dir == null ? baseDir : new Path(baseDir, dir);
        LOG.info("Deleting path : " + del);
        pathsToDelete.add(del.toString());
        deleteAsUserOp.appendArgs(baseDir.toUri().getPath());
      }
    }

    try {
      Configuration conf = super.getConf();
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(deleteAsUserOp,
          false);
    }   catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.error("DeleteAsUser for " + StringUtils.join(" ", pathsToDelete)
          + " returned with exit code: " + exitCode, e);
    }
  }

  @Override
  protected File[] readDirAsUser(String user, Path dir) {
    List<File> files = new ArrayList<>();
    PrivilegedOperation listAsUserOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.LIST_AS_USER, (String)null);
    String runAsUser = getRunAsUser(user);
    String dirString = "";

    if (dir != null) {
      dirString = dir.toUri().getPath();
    }

    listAsUserOp.appendArgs(runAsUser, user,
        Integer.toString(
            PrivilegedOperation.RunAsUserCommand.LIST_AS_USER.getValue()),
        dirString);

    try {
      PrivilegedOperationExecutor privOpExecutor =
          getPrivilegedOperationExecutor();

      String results =
          privOpExecutor.executePrivilegedOperation(listAsUserOp, true);

      for (String file: results.split("\n")) {
        // The container-executor always dumps its log output to stdout, which
        // includes 3 lines that start with "main : "
        if (!file.startsWith("main :")) {
          files.add(new File(new File(dirString), file));
        }
      }
    } catch (PrivilegedOperationException e) {
      LOG.error("ListAsUser for " + dir + " returned with exit code: "
          + e.getExitCode(), e);
    }

    return files.toArray(new File[files.size()]);
  }

  @Override
  public void symLink(String target, String symlink) {

  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Container container = ctx.getContainer();

    // Send a test signal to the process as the user to see if it's alive
    return signalContainer(new ContainerSignalContext.Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .setSignal(Signal.NULL)
        .build());
  }

  /**
   * Mount a CGROUPS controller at the requested mount point and create
   * a hierarchy for the NodeManager to manage.
   *
   * @param cgroupKVs a key-value pair of the form
   * {@code controller=mount-path}
   * @param hierarchy the top directory of the hierarchy for the NodeManager
   * @throws IOException if there is a problem mounting the CGROUPS
   */
  public void mountCgroups(List<String> cgroupKVs, String hierarchy)
      throws IOException {
    try {
      PrivilegedOperation mountCGroupsOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.MOUNT_CGROUPS, hierarchy);
      Configuration conf = super.getConf();

      mountCGroupsOp.appendArgs(cgroupKVs);
      PrivilegedOperationExecutor privilegedOperationExecutor =
          getPrivilegedOperationExecutor();

      privilegedOperationExecutor.executePrivilegedOperation(mountCGroupsOp,
          false);
    } catch (PrivilegedOperationException e) {
      int exitCode = e.getExitCode();
      LOG.warn("Exception in LinuxContainerExecutor mountCgroups ", e);

      throw new IOException("Problem mounting cgroups " + cgroupKVs +
          "; exit code = " + exitCode + " and output: " + e.getOutput(),
          e);
    }
  }

  @VisibleForTesting
  public ResourceHandler getResourceHandler() {
    return resourceHandlerChain;
  }
}
