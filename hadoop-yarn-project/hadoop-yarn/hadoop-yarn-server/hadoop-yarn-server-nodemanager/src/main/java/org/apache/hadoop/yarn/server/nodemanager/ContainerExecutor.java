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
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is abstraction of the mechanism used to launch a container on the
 * underlying OS.  All executor implementations must extend ContainerExecutor.
 */
public abstract class ContainerExecutor implements Configurable {
  private static final Log LOG = LogFactory.getLog(ContainerExecutor.class);
  protected static final String WILDCARD = "*";

  /**
   * The permissions to use when creating the launch script.
   */
  public static final FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
      FsPermission.createImmutable((short)0700);

  /**
   * The relative path to which debug information will be written.
   *
   * @see ContainerLaunch.ShellScriptBuilder#listDebugInformation
   */
  public static final String DIRECTORY_CONTENTS = "directory.info";

  private Configuration conf;
  private final ConcurrentMap<ContainerId, Path> pidFiles =
      new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReadLock readLock = lock.readLock();
  private final WriteLock writeLock = lock.writeLock();

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Run the executor initialization steps.
   * Verify that the necessary configs and permissions are in place.
   *
   * @throws IOException if initialization fails
   */
  public abstract void init() throws IOException;

  /**
   * This function localizes the JAR file on-demand.
   * On Windows the ContainerLaunch creates a temporary special JAR manifest of
   * other JARs to workaround the CLASSPATH length. In a secure cluster this
   * JAR must be localized so that the container has access to it.
   * The default implementation returns the classpath passed to it, which
   * is expected to have been created in the node manager's <i>fprivate</i>
   * folder, which will not work with secure Windows clusters.
   *
   * @param jarPath the path to the JAR to localize
   * @param target the directory where the JAR file should be localized
   * @param owner the name of the user who should own the localized file
   * @return the path to the localized JAR file
   * @throws IOException if localization fails
   */
  public Path localizeClasspathJar(Path jarPath, Path target, String owner)
      throws IOException {
    return jarPath;
  }


  /**
   * Prepare the environment for containers in this application to execute.
   * <pre>
   * For $x in local.dirs
   *   create $x/$user/$appId
   * Copy $nmLocal/appTokens {@literal ->} $N/$user/$appId
   * For $rsrc in private resources
   *   Copy $rsrc {@literal ->} $N/$user/filecache/[idef]
   * For $rsrc in job resources
   *   Copy $rsrc {@literal ->} $N/$user/$appId/filecache/idef
   * </pre>
   *
   * @param ctx LocalizerStartContext that encapsulates necessary information
   *            for starting a localizer.
   * @throws IOException for most application init failures
   * @throws InterruptedException if application init thread is halted by NM
   */
  public abstract void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException;


  /**
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param ctx Encapsulates information necessary for launching containers.
   * @return the return status of the launch
   * @throws IOException if the container launch fails
   */
  public abstract int launchContainer(ContainerStartContext ctx) throws
      IOException;

  /**
   * Signal container with the specified signal.
   *
   * @param ctx Encapsulates information necessary for signaling containers.
   * @return returns true if the operation succeeded
   * @throws IOException if signaling the container fails
   */
  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  /**
   * Delete specified directories as a given user.
   *
   * @param ctx Encapsulates information necessary for deletion.
   * @throws IOException if delete fails
   * @throws InterruptedException if interrupted while waiting for the deletion
   * operation to complete
   */
  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException;

  /**
   * Create a symlink file which points to the target.
   * @param target The target for symlink
   * @param symlink the symlink file
   * @throws IOException Error when creating symlinks
   */
  public abstract void symLink(String target, String symlink)
      throws IOException;

  /**
   * Check if a container is alive.
   * @param ctx Encapsulates information necessary for container liveness check.
   * @return true if container is still alive
   * @throws IOException if there is a failure while checking the container
   * status
   */
  public abstract boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException;

  /**
   * Recover an already existing container. This is a blocking call and returns
   * only when the container exits.  Note that the container must have been
   * activated prior to this call.
   *
   * @param ctx encapsulates information necessary to reacquire container
   * @return The exit code of the pre-existing container
   * @throws IOException if there is a failure while reacquiring the container
   * @throws InterruptedException if interrupted while waiting to reacquire
   * the container
   */
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {
    Container container = ctx.getContainer();
    String user = ctx.getUser();
    ContainerId containerId = ctx.getContainerId();
    Path pidPath = getPidFilePath(containerId);

    if (pidPath == null) {
      LOG.warn(containerId + " is not active, returning terminated error");

      return ExitCode.TERMINATED.getExitCode();
    }

    String pid = ProcessIdFileReader.getProcessId(pidPath);

    if (pid == null) {
      throw new IOException("Unable to determine pid for " + containerId);
    }

    LOG.info("Reacquiring " + containerId + " with pid " + pid);

    ContainerLivenessContext livenessContext = new ContainerLivenessContext
        .Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .build();

    while (isContainerAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    // wait for exit code file to appear
    final int sleepMsec = 100;
    int msecLeft = 2000;
    String exitCodeFile = ContainerLaunch.getExitCodeFile(pidPath.toString());
    File file = new File(exitCodeFile);

    while (!file.exists() && msecLeft >= 0) {
      if (!isContainerActive(containerId)) {
        LOG.info(containerId + " was deactivated");

        return ExitCode.TERMINATED.getExitCode();
      }

      Thread.sleep(sleepMsec);

      msecLeft -= sleepMsec;
    }

    if (msecLeft < 0) {
      throw new IOException("Timeout while waiting for exit code from "
          + containerId);
    }

    try {
      return Integer.parseInt(FileUtils.readFileToString(file).trim());
    } catch (NumberFormatException e) {
      throw new IOException("Error parsing exit code from pid " + pid, e);
    }
  }

  /**
   * This method writes out the launch environment of a container to the
   * default container launch script. For the default container script path see
   * {@link ContainerLaunch#CONTAINER_SCRIPT}.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values
   * @param resources the resources which have been localized for this
   * container. Symlinks will be created to these localized resources
   * @param command the command that will be run
   * @param logDir the log dir to which to copy debugging information
   * @param user the username of the job owner
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir,
      String user) throws IOException {
    this.writeLaunchEnv(out, environment, resources, command, logDir, user,
        ContainerLaunch.CONTAINER_SCRIPT);
  }

  /**
   * This method writes out the launch environment of a container to a specified
   * path.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values
   * @param resources the resources which have been localized for this
   * container. Symlinks will be created to these localized resources
   * @param command the command that will be run
   * @param logDir the log dir to which to copy debugging information
   * @param user the username of the job owner
   * @param outFilename the path to which to write the launch environment
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  @VisibleForTesting
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir,
      String user, String outFilename) throws IOException {
    ContainerLaunch.ShellScriptBuilder sb =
        ContainerLaunch.ShellScriptBuilder.create();
    Set<String> whitelist = new HashSet<>();

    whitelist.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    whitelist.add(ApplicationConstants.Environment.JAVA_HOME.name());

    if (environment != null) {
      for (Map.Entry<String, String> env : environment.entrySet()) {
        if (!whitelist.contains(env.getKey())) {
          sb.env(env.getKey(), env.getValue());
        } else {
          sb.whitelistedEnv(env.getKey(), env.getValue());
        }
      }
    }

    if (resources != null) {
      for (Map.Entry<Path, List<String>> resourceEntry :
          resources.entrySet()) {
        for (String linkName : resourceEntry.getValue()) {
          if (new Path(linkName).getName().equals(WILDCARD)) {
            // If this is a wildcarded path, link to everything in the
            // directory from the working directory
            for (File wildLink : readDirAsUser(user, resourceEntry.getKey())) {
              sb.symlink(new Path(wildLink.toString()),
                  new Path(wildLink.getName()));
            }
          } else {
            sb.symlink(resourceEntry.getKey(), new Path(linkName));
          }
        }
      }
    }

    // dump debugging information if configured
    if (getConf() != null &&
        getConf().getBoolean(YarnConfiguration.NM_LOG_CONTAINER_DEBUG_INFO,
        YarnConfiguration.DEFAULT_NM_LOG_CONTAINER_DEBUG_INFO)) {
      sb.copyDebugInformation(new Path(outFilename),
          new Path(logDir, outFilename));
      sb.listDebugInformation(new Path(logDir, DIRECTORY_CONTENTS));
    }

    sb.command(command);

    PrintStream pout = null;

    try {
      pout = new PrintStream(out, false, "UTF-8");
      sb.write(pout);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Return the files in the target directory. If retrieving the list of files
   * requires specific access rights, that access will happen as the
   * specified user. The list will not include entries for "." or "..".
   *
   * @param user the user as whom to access the target directory
   * @param dir the target directory
   * @return a list of files in the target directory
   */
  protected File[] readDirAsUser(String user, Path dir) {
    return new File(dir.toString()).listFiles();
  }

  /**
   * The container exit code.
   */
  public enum ExitCode {
    SUCCESS(0),
    FORCE_KILLED(137),
    TERMINATED(143),
    LOST(154);

    private final int code;

    private ExitCode(int exitCode) {
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
   * The constants for the signals.
   */
  public enum Signal {
    NULL(0, "NULL"),
    QUIT(3, "SIGQUIT"),
    KILL(9, "SIGKILL"),
    TERM(15, "SIGTERM");

    private final int value;
    private final String str;

    private Signal(int value, String str) {
      this.str = str;
      this.value = value;
    }

    /**
     * Get the signal number.
     * @return the signal number
     */
    public int getValue() {
      return value;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  /**
   * Log each line of the output string as INFO level log messages.
   *
   * @param output the output string to log
   */
  protected void logOutput(String output) {
    String shExecOutput = output;

    if (shExecOutput != null) {
      for (String str : shExecOutput.split("\n")) {
        LOG.info(str);
      }
    }
  }

  /**
   * Get the pidFile of the container.
   *
   * @param containerId the container ID
   * @return the path of the pid-file for the given containerId.
   */
  protected Path getPidFilePath(ContainerId containerId) {
    try {
      readLock.lock();
      return (this.pidFiles.get(containerId));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * On Windows, the {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group. On
   * non-Windows hosts, the {code}groupId{code} parameter is ignored.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID
   * @param userName the job owner's username
   * @param pidFile the path to the container's PID file
   * @param config the configuration
   * @return the command line to execute
   */
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration config) {
    return getRunCommand(command, groupId, userName, pidFile, config, null);
  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * On Windows, the {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group. On
   * non-Windows hosts, the {code}groupId{code} parameter is ignored.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID for Windows. On other operating systems
   * it is ignored.
   * @param userName the job owner's username for Windows. On other operating
   * systems it is ignored.
   * @param pidFile the path to the container's PID file on Windows. On other
   * operating systems it is ignored.
   * @param config the configuration
   * @param resource on Windows this parameter controls memory and CPU limits.
   * If null, no limits are set. On other operating systems it is ignored.
   * @return the command line to execute
   */
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration config, Resource resource) {
    if (Shell.WINDOWS) {
      return getRunCommandForWindows(command, groupId, userName, pidFile,
          config, resource);
    } else {
      return getRunCommandForOther(command, config);
    }

  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * The {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID
   * @param userName the job owner's username
   * @param pidFile the path to the container's PID file
   * @param config the configuration
   * @param resource this parameter controls memory and CPU limits.
   * If null, no limits are set.
   * @return the command line to execute
   */
  protected String[] getRunCommandForWindows(String command, String groupId,
      String userName, Path pidFile, Configuration config, Resource resource) {
    int cpuRate = -1;
    int memory = -1;

    if (resource != null) {
      if (config.getBoolean(
          YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED,
          YarnConfiguration.
            DEFAULT_NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED)) {
        memory = (int) resource.getMemorySize();
      }

      if (config.getBoolean(
          YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED,
          YarnConfiguration.DEFAULT_NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED)) {
        int containerVCores = resource.getVirtualCores();
        int nodeVCores = NodeManagerHardwareUtils.getVCores(config);
        int nodeCpuPercentage =
            NodeManagerHardwareUtils.getNodeCpuPercentage(config);

        float containerCpuPercentage =
            (float)(nodeCpuPercentage * containerVCores) / nodeVCores;

        // CPU should be set to a percentage * 100, e.g. 20% cpu rate limit
        // should be set as 20 * 100.
        cpuRate = Math.min(10000, (int)(containerCpuPercentage * 100));
      }
    }

    return new String[] {
        Shell.getWinUtilsPath(),
        "task",
        "create",
        "-m",
        String.valueOf(memory),
        "-c",
        String.valueOf(cpuRate),
        groupId,
        "cmd /c " + command
    };
  }

  /**
   * Return a command line to execute the given command in the OS shell.
   *
   * @param command the command to execute
   * @param config the configuration
   * @return the command line to execute
   */
  protected String[] getRunCommandForOther(String command,
      Configuration config) {
    List<String> retCommand = new ArrayList<>();
    boolean containerSchedPriorityIsSet = false;
    int containerSchedPriorityAdjustment =
        YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY;

    if (config.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY) !=
        null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = config
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY,
          YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }

    if (containerSchedPriorityIsSet) {
      retCommand.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment)));
    }

    retCommand.addAll(Arrays.asList("bash", command));

    return retCommand.toArray(new String[retCommand.size()]);
  }

  /**
   * Return whether the container is still active.
   *
   * @param containerId the target container's ID
   * @return true if the container is active
   */
  protected boolean isContainerActive(ContainerId containerId) {
    try {
      readLock.lock();

      return (this.pidFiles.containsKey(containerId));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Mark the container as active.
   *
   * @param containerId the container ID
   * @param pidFilePath the path where the executor should write the PID
   * of the launched process
   */
  public void activateContainer(ContainerId containerId, Path pidFilePath) {
    try {
      writeLock.lock();
      this.pidFiles.put(containerId, pidFilePath);
    } finally {
      writeLock.unlock();
    }
  }

  // LinuxContainerExecutor overrides this method and behaves differently.
  public String[] getIpAndHost(Container container) {
    return getLocalIpAndHost(container);
  }

  // ipAndHost[0] contains ip.
  // ipAndHost[1] contains hostname.
  public static String[] getLocalIpAndHost(Container container) {
    String[] ipAndHost = new String[2];
    try {
      InetAddress address = InetAddress.getLocalHost();
      ipAndHost[0] = address.getHostAddress();
      ipAndHost[1] = address.getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to get Local hostname and ip for " + container
          .getContainerId(), e);
    }
    return ipAndHost;
  }

  /**
   * Mark the container as inactive. For inactive containers this
   * method has no effect.
   *
   * @param containerId the container ID
   */
  public void deactivateContainer(ContainerId containerId) {
    try {
      writeLock.lock();
      this.pidFiles.remove(containerId);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get the process-identifier for the container.
   *
   * @param containerID the container ID
   * @return the process ID of the container if it has already launched,
   * or null otherwise
   */
  public String getProcessId(ContainerId containerID) {
    String pid = null;
    Path pidFile = pidFiles.get(containerID);

    // If PID is null, this container hasn't launched yet.
    if (pidFile != null) {
      try {
        pid = ProcessIdFileReader.getProcessId(pidFile);
      } catch (IOException e) {
        LOG.error("Got exception reading pid from pid-file " + pidFile, e);
      }
    }

    return pid;
  }

  /**
   * This class will signal a target container after a specified delay.
   * @see #signalContainer
   */
  public static class DelayedProcessKiller extends Thread {
    private final Container container;
    private final String user;
    private final String pid;
    private final long delay;
    private final Signal signal;
    private final ContainerExecutor containerExecutor;

    /**
     * Basic constructor.
     *
     * @param container the container to signal
     * @param user the user as whow to send the signal
     * @param pid the PID of the container process
     * @param delayMS the period of time to wait in millis before signaling
     * the container
     * @param signal the signal to send
     * @param containerExecutor the executor to use to send the signal
     */
    public DelayedProcessKiller(Container container, String user, String pid,
        long delayMS, Signal signal, ContainerExecutor containerExecutor) {
      this.container = container;
      this.user = user;
      this.pid = pid;
      this.delay = delayMS;
      this.signal = signal;
      this.containerExecutor = containerExecutor;
      setName("Task killer for " + pid);
      setDaemon(false);
    }

    @Override
    public void run() {
      try {
        Thread.sleep(delay);
        containerExecutor.signalContainer(new ContainerSignalContext.Builder()
            .setContainer(container)
            .setUser(user)
            .setPid(pid)
            .setSignal(signal)
            .build());
      } catch (InterruptedException e) {
        interrupt();
      } catch (IOException e) {
        String message = "Exception when user " + user + " killing task " + pid
            + " in DelayedProcessKiller: " + StringUtils.stringifyException(e);
        LOG.warn(message);
        container.handle(new ContainerDiagnosticsUpdateEvent(
            container.getContainerId(), message));
      }
    }
  }
}
