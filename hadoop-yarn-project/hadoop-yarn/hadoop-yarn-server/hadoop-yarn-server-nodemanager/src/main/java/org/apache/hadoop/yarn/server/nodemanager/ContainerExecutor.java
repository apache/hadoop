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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;

public abstract class ContainerExecutor implements Configurable {

  private static final Log LOG = LogFactory.getLog(ContainerExecutor.class);
  final public static FsPermission TASK_LAUNCH_SCRIPT_PERMISSION =
    FsPermission.createImmutable((short) 0700);

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
   * Prepare the environment for containers in this application to execute.
   * For $x in local.dirs
   *   create $x/$user/$appId
   * Copy $nmLocal/appTokens -> $N/$user/$appId
   * For $rsrc in private resources
   *   Copy $rsrc -> $N/$user/filecache/[idef]
   * For $rsrc in job resources
   *   Copy $rsrc -> $N/$user/$appId/filecache/idef
   * @param user user name of application owner
   * @param appId id of the application
   * @param nmPrivateContainerTokens path to localized credentials, rsrc by NM
   * @param nmAddr RPC address to contact NM
   * @param localDirs nm-local-dirs
   * @param logDirs nm-log-dirs
   * @throws IOException For most application init failures
   * @throws InterruptedException If application init thread is halted by NM
   */
  public abstract void startLocalizer(Path nmPrivateContainerTokens,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      List<String> localDirs, List<String> logDirs)
    throws IOException, InterruptedException;


  /**
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param container the container to be launched
   * @param nmPrivateContainerScriptPath the path for launch script
   * @param nmPrivateTokensPath the path for tokens for the container
   * @param user the user of the container
   * @param appId the appId of the container
   * @param containerWorkDir the work dir for the container
   * @param localDirs nm-local-dirs to be used for this container
   * @param logDirs nm-log-dirs to be used for this container
   * @return the return status of the launch
   * @throws IOException
   */
  public abstract int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir, List<String> localDirs,
      List<String> logDirs) throws IOException;

  public abstract boolean signalContainer(String user, String pid,
      Signal signal)
      throws IOException;

  public abstract void deleteAsUser(String user, Path subDir, Path... basedirs)
      throws IOException, InterruptedException;

  public enum ExitCode {
    FORCE_KILLED(137),
    TERMINATED(143);
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


  /** 
   * Return a command to execute the given command in OS shell.
   */
  protected static String[] getRunCommand(String command, Configuration conf) {
    List<String> retCommand = new ArrayList<String>();
    if (conf.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY) !=
        null) {
      int containerSchedPriorityAdjustment = conf
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY, 0);
      retCommand.addAll(Arrays.asList("nice", "-n",
          Integer.toString(containerSchedPriorityAdjustment)));
    }
    retCommand.addAll(Arrays.asList("bash", "-c", command));
    return retCommand.toArray(new String[retCommand.size()]);
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

  public static final boolean isSetsidAvailable = isSetsidSupported();
  private static boolean isSetsidSupported() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
    }
    return setsidSupported;
  }

  public static class DelayedProcessKiller extends Thread {
    private final String user;
    private final String pid;
    private final long delay;
    private final Signal signal;
    private final ContainerExecutor containerExecutor;

    public DelayedProcessKiller(String user, String pid, long delay,
        Signal signal,
        ContainerExecutor containerExecutor) {
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
        containerExecutor.signalContainer(user, pid, signal);
      } catch (InterruptedException e) {
        return;
      } catch (IOException e) {
        LOG.warn("Exception when killing task " + pid, e);
      }
    }
  }

}
