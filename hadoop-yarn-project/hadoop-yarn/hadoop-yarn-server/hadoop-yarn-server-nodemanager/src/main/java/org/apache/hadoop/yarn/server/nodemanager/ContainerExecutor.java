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

public abstract class ContainerExecutor implements Configurable {

  private static final Log LOG = LogFactory.getLog(ContainerExecutor.class);
  final public static FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
    FsPermission.createImmutable((short) 0700);

  public static final String DIRECTORY_CONTENTS = "directory.info";

  private Configuration conf;

  private ConcurrentMap<ContainerId, Path> pidFiles =
      new ConcurrentHashMap<ContainerId, Path>();

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
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
   * Verify that the necessary configs, permissions are in place.
   * @throws IOException
   */
  public abstract void init() throws IOException;

  /**
   * On Windows the ContainerLaunch creates a temporary special jar manifest of 
   * other jars to workaround the CLASSPATH length. In a  secure cluster this 
   * jar must be localized so that the container has access to it. 
   * This function localizes on-demand the jar.
   * 
   * @param classPathJar
   * @param owner
   * @throws IOException
   */
  public Path localizeClasspathJar(Path classPathJar, Path pwd, String owner) 
      throws IOException {
    // Non-secure executor simply use the classpath created 
    // in the NM fprivate folder
    return classPathJar;
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
   * @param ctx LocalizerStartContext that encapsulates necessary information
   *            for starting a localizer.
   * @throws IOException For most application init failures
   * @throws InterruptedException If application init thread is halted by NM
   */
  public abstract void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException;


  /**
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param ctx Encapsulates information necessary for launching containers.
   * @return the return status of the launch
   * @throws IOException
   */
  public abstract int launchContainer(ContainerStartContext ctx) throws
      IOException;

  /**
   * Signal container with the specified signal.
   * @param ctx Encapsulates information necessary for signaling containers.
   * @return returns true if the operation succeeded
   * @throws IOException
   */
  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  /**
   * Delete specified directories as a given user.
   * @param ctx Encapsulates information necessary for deletion.
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException;

  /**
   * Check if a container is alive.
   * @param ctx Encapsulates information necessary for container liveness check.
   * @return true if container is still alive
   * @throws IOException
   */
  public abstract boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException;

  /**
   * Recover an already existing container. This is a blocking call and returns
   * only when the container exits.  Note that the container must have been
   * activated prior to this call.
   * @param ctx encapsulates information necessary to reacquire container
   * @return The exit code of the pre-existing container
   * @throws IOException
   * @throws InterruptedException 
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

    String pid = null;
    pid = ProcessIdFileReader.getProcessId(pidPath);
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
    while(isContainerAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    // wait for exit code file to appear
    String exitCodeFile = ContainerLaunch.getExitCodeFile(pidPath.toString());
    File file = new File(exitCodeFile);
    final int sleepMsec = 100;
    int msecLeft = 2000;
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
   * This method writes out the launch environment of a container. This can be
   * overridden by extending ContainerExecutors to provide different behaviors
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment The environment variables and their values
   * @param resources The resources which have been localized for this container
   * Symlinks will be created to these localized resources
   * @param command The command that will be run.
   * @param logDir The log dir to copy debugging information to
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir)
      throws IOException {
    this.writeLaunchEnv(out, environment, resources, command, logDir,
        ContainerLaunch.CONTAINER_SCRIPT);
  }

  @VisibleForTesting
  public void writeLaunchEnv(OutputStream out,
      Map<String, String> environment, Map<Path, List<String>> resources,
      List<String> command, Path logDir, String outFilename)
      throws IOException {
    ContainerLaunch.ShellScriptBuilder sb =
      ContainerLaunch.ShellScriptBuilder.create();
    Set<String> whitelist = new HashSet<String>();
    whitelist.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    whitelist.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    whitelist.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    whitelist.add(ApplicationConstants.Environment.JAVA_HOME.name());
    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        if (!whitelist.contains(env.getKey())) {
          sb.env(env.getKey().toString(), env.getValue().toString());
        } else {
          sb.whitelistedEnv(env.getKey().toString(), env.getValue().toString());
        }
      }
    }
    if (resources != null) {
      for (Map.Entry<Path,List<String>> entry : resources.entrySet()) {
        for (String linkName : entry.getValue()) {
          sb.symlink(entry.getKey(), new Path(linkName));
        }
      }
    }

    // dump debugging information if configured
    if (getConf() != null && getConf().getBoolean(
        YarnConfiguration.NM_LOG_CONTAINER_DEBUG_INFO,
        YarnConfiguration.DEFAULT_NM_LOG_CONTAINER_DEBUG_INFO)) {
      sb.copyDebugInformation(new Path(outFilename), new Path(logDir, outFilename));
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

  public enum ExitCode {
    SUCCESS(0),
    FORCE_KILLED(137),
    TERMINATED(143),
    LOST(154);
    private final int code;

    private ExitCode(int exitCode) {
      this.code = exitCode;
    }

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
    NULL(0, "NULL"), QUIT(3, "SIGQUIT"), 
    KILL(9, "SIGKILL"), TERM(15, "SIGTERM");
    private final int value;
    private final String str;
    private Signal(int value, String str) {
      this.str = str;
      this.value = value;
    }
    public int getValue() {
      return value;
    }
    @Override
    public String toString() {
      return str;
    }
  }

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
   * @param containerId
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

  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration conf) {
    return getRunCommand(command, groupId, userName, pidFile, conf, null);
  }
  
  /** 
   *  Return a command to execute the given command in OS shell.
   *  On Windows, the passed in groupId can be used to launch
   *  and associate the given groupId in a process group. On
   *  non-Windows, groupId is ignored. 
   */
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration conf, Resource resource) {
    boolean containerSchedPriorityIsSet = false;
    int containerSchedPriorityAdjustment = 
        YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY;

    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY) != 
        null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = conf 
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 
          YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }
  
    if (Shell.WINDOWS) {
      int cpuRate = -1;
      int memory = -1;
      if (resource != null) {
        if (conf
            .getBoolean(
                YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED,
                YarnConfiguration.DEFAULT_NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED)) {
          memory = (int) resource.getMemorySize();
        }

        if (conf.getBoolean(
            YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED,
            YarnConfiguration.DEFAULT_NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED)) {
          int containerVCores = resource.getVirtualCores();
          int nodeVCores = NodeManagerHardwareUtils.getVCores(conf);
          int nodeCpuPercentage =
              NodeManagerHardwareUtils.getNodeCpuPercentage(conf);

          float containerCpuPercentage =
              (float) (nodeCpuPercentage * containerVCores) / nodeVCores;

          // CPU should be set to a percentage * 100, e.g. 20% cpu rate limit
          // should be set as 20 * 100.
          cpuRate = Math.min(10000, (int) (containerCpuPercentage * 100));
        }
      }
      return new String[] { Shell.getWinUtilsPath(), "task", "create", "-m",
          String.valueOf(memory), "-c", String.valueOf(cpuRate), groupId,
          "cmd /c " + command };
    } else {
      List<String> retCommand = new ArrayList<String>();
      if (containerSchedPriorityIsSet) {
        retCommand.addAll(Arrays.asList("nice", "-n",
            Integer.toString(containerSchedPriorityAdjustment)));
      }
      retCommand.addAll(Arrays.asList("bash", command));
      return retCommand.toArray(new String[retCommand.size()]);
    }

  }

  /**
   * Is the container still active?
   * @param containerId
   * @return true if the container is active else false.
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
   * Mark the container as active
   * 
   * @param containerId
   *          the ContainerId
   * @param pidFilePath
   *          Path where the executor should write the pid of the launched
   *          process
   */
  public void activateContainer(ContainerId containerId, Path pidFilePath) {
    try {
      writeLock.lock();
      this.pidFiles.put(containerId, pidFilePath);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Mark the container as inactive.
   * Done iff the container is still active. Else treat it as
   * a no-op
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
   * Get the process-identifier for the container
   * 
   * @param containerID
   * @return the processid of the container if it has already launched,
   *         otherwise return null
   */
  public String getProcessId(ContainerId containerID) {
    String pid = null;
    Path pidFile = pidFiles.get(containerID);
    if (pidFile == null) {
      // This container isn't even launched yet.
      return pid;
    }
    try {
      pid = ProcessIdFileReader.getProcessId(pidFile);
    } catch (IOException e) {
      LOG.error("Got exception reading pid from pid-file " + pidFile, e);
    }
    return pid;
  }

  public static class DelayedProcessKiller extends Thread {
    private Container container;
    private final String user;
    private final String pid;
    private final long delay;
    private final Signal signal;
    private final ContainerExecutor containerExecutor;

    public DelayedProcessKiller(Container container, String user, String pid,
        long delay, Signal signal, ContainerExecutor containerExecutor) {
      this.container = container;
      this.user = user;
      this.pid = pid;
      this.delay = delay;
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
        return;
      } catch (IOException e) {
        String message = "Exception when user " + user + " killing task " + pid
            + " in DelayedProcessKiller: " + StringUtils.stringifyException(e);
        LOG.warn(message);
        container.handle(new ContainerDiagnosticsUpdateEvent(container
          .getContainerId(), message));
      }
    }
  }
}
