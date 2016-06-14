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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.DelayedProcessKiller;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class ContainerLaunch implements Callable<Integer> {

  private static final Log LOG = LogFactory.getLog(ContainerLaunch.class);

  public static final String CONTAINER_SCRIPT =
    Shell.appendScriptExtension("launch_container");
  public static final String FINAL_CONTAINER_TOKENS_FILE = "container_tokens";

  private static final String PID_FILE_NAME_FMT = "%s.pid";
  private static final String EXIT_CODE_FILE_SUFFIX = ".exitcode";

  protected final Dispatcher dispatcher;
  protected final ContainerExecutor exec;
  protected final Application app;
  protected final Container container;
  private final Configuration conf;
  private final Context context;
  private final ContainerManagerImpl containerManager;
  
  protected AtomicBoolean shouldLaunchContainer = new AtomicBoolean(false);
  protected AtomicBoolean completed = new AtomicBoolean(false);

  private long sleepDelayBeforeSigKill = 250;
  private long maxKillWaitTime = 2000;

  protected Path pidFilePath = null;

  protected final LocalDirsHandlerService dirsHandler;

  public ContainerLaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {
    this.context = context;
    this.conf = configuration;
    this.app = app;
    this.exec = exec;
    this.container = container;
    this.dispatcher = dispatcher;
    this.dirsHandler = dirsHandler;
    this.containerManager = containerManager;
    this.sleepDelayBeforeSigKill =
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS);
    this.maxKillWaitTime =
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS);
  }

  @VisibleForTesting
  public static String expandEnvironment(String var,
      Path containerLogDir) {
    var = var.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,
      containerLogDir.toString());
    var =  var.replace(ApplicationConstants.CLASS_PATH_SEPARATOR,
      File.pathSeparator);

    // replace parameter expansion marker. e.g. {{VAR}} on Windows is replaced
    // as %VAR% and on Linux replaced as "$VAR"
    if (Shell.WINDOWS) {
      var = var.replaceAll("(\\{\\{)|(\\}\\})", "%");
    } else {
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_LEFT, "$");
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_RIGHT, "");
    }
    return var;
  }

  @Override
  @SuppressWarnings("unchecked") // dispatcher not typed
  public Integer call() {
    if (!validateContainerState()) {
      return 0;
    }

    final ContainerLaunchContext launchContext = container.getLaunchContext();
    ContainerId containerID = container.getContainerId();
    String containerIdStr = containerID.toString();
    final List<String> command = launchContext.getCommands();
    int ret = -1;

    Path containerLogDir;
    try {
      Map<Path, List<String>> localResources = getLocalizedResources();

      final String user = container.getUser();
      // /////////////////////////// Variable expansion
      // Before the container script gets written out.
      List<String> newCmds = new ArrayList<String>(command.size());
      String appIdStr = app.getAppId().toString();
      String relativeContainerLogDir = ContainerLaunch
          .getRelativeContainerLogDir(appIdStr, containerIdStr);
      containerLogDir =
          dirsHandler.getLogPathForWrite(relativeContainerLogDir, false);
      recordContainerLogDir(containerID, containerLogDir.toString());
      for (String str : command) {
        // TODO: Should we instead work via symlinks without this grammar?
        newCmds.add(expandEnvironment(str, containerLogDir));
      }
      launchContext.setCommands(newCmds);

      Map<String, String> environment = launchContext.getEnvironment();
      // Make a copy of env to iterate & do variable expansion
      for (Entry<String, String> entry : environment.entrySet()) {
        String value = entry.getValue();
        value = expandEnvironment(value, containerLogDir);
        entry.setValue(value);
      }
      // /////////////////////////// End of variable expansion

      FileContext lfs = FileContext.getLocalFSFileContext();

      Path nmPrivateContainerScriptPath =
          dirsHandler.getLocalPathForWrite(
              getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
                  + CONTAINER_SCRIPT);
      Path nmPrivateTokensPath =
          dirsHandler.getLocalPathForWrite(
              getContainerPrivateDir(appIdStr, containerIdStr)
                  + Path.SEPARATOR
                  + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                      containerIdStr));
      Path nmPrivateClasspathJarDir = 
          dirsHandler.getLocalPathForWrite(
              getContainerPrivateDir(appIdStr, containerIdStr));
      DataOutputStream containerScriptOutStream = null;
      DataOutputStream tokensOutStream = null;

      // Select the working directory for the container
      Path containerWorkDir =
          dirsHandler.getLocalPathForWrite(ContainerLocalizer.USERCACHE
              + Path.SEPARATOR + user + Path.SEPARATOR
              + ContainerLocalizer.APPCACHE + Path.SEPARATOR + appIdStr
              + Path.SEPARATOR + containerIdStr,
              LocalDirAllocator.SIZE_UNKNOWN, false);
      recordContainerWorkDir(containerID, containerWorkDir.toString());

      String pidFileSubpath = getPidFileSubpath(appIdStr, containerIdStr);

      // pid file should be in nm private dir so that it is not 
      // accessible by users
      pidFilePath = dirsHandler.getLocalPathForWrite(pidFileSubpath);
      List<String> localDirs = dirsHandler.getLocalDirs();
      List<String> logDirs = dirsHandler.getLogDirs();
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);
      List<String> containerLogDirs = getContainerLogDirs(logDirs);

      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }

      try {
        // /////////// Write out the container-script in the nmPrivate space.
        List<Path> appDirs = new ArrayList<Path>(localDirs.size());
        for (String localDir : localDirs) {
          Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
          Path userdir = new Path(usersdir, user);
          Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
          appDirs.add(new Path(appsdir, appIdStr));
        }
        containerScriptOutStream =
          lfs.create(nmPrivateContainerScriptPath,
              EnumSet.of(CREATE, OVERWRITE));

        // Set the token location too.
        environment.put(
            ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME, 
            new Path(containerWorkDir, 
                FINAL_CONTAINER_TOKENS_FILE).toUri().getPath());
        // Sanitize the container's environment
        sanitizeEnv(environment, containerWorkDir, appDirs, containerLogDirs,
          localResources, nmPrivateClasspathJarDir);

        // Write out the environment
        exec.writeLaunchEnv(containerScriptOutStream, environment,
          localResources, launchContext.getCommands(),
            new Path(containerLogDirs.get(0)));

        // /////////// End of writing out container-script

        // /////////// Write out the container-tokens in the nmPrivate space.
        tokensOutStream =
            lfs.create(nmPrivateTokensPath, EnumSet.of(CREATE, OVERWRITE));
        Credentials creds = container.getCredentials();
        creds.writeTokenStorageToStream(tokensOutStream);
        // /////////// End of writing out container-tokens
      } finally {
        IOUtils.cleanup(LOG, containerScriptOutStream, tokensOutStream);
      }

      ret = launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setLocalizedResources(localResources)
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setUser(user)
          .setAppId(appIdStr)
          .setContainerWorkDir(containerWorkDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .setContainerLocalDirs(containerLocalDirs)
          .setContainerLogDirs(containerLogDirs)
          .build());
    } catch (Throwable e) {
      LOG.warn("Failed to launch container.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      return ret;
    } finally {
      setContainerCompletedStatus(ret);
    }

    handleContainerExitCode(ret, containerLogDir);

    return ret;
  }

  @SuppressWarnings("unchecked")
  protected boolean validateContainerState() {
    // CONTAINER_KILLED_ON_REQUEST should not be missed if the container
    // is already at KILLING
    if (container.getContainerState() == ContainerState.KILLING) {
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(container.getContainerId(),
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
              Shell.WINDOWS ? ExitCode.FORCE_KILLED.getExitCode() :
                  ExitCode.TERMINATED.getExitCode(),
              "Container terminated before launch."));
      return false;
    }

    return true;
  }

  protected List<String> getContainerLogDirs(List<String> logDirs) {
    List<String> containerLogDirs = new ArrayList<>(logDirs.size());
    String appIdStr = app.getAppId().toString();
    String containerIdStr = container.getContainerId().toString();
    String relativeContainerLogDir = ContainerLaunch
        .getRelativeContainerLogDir(appIdStr, containerIdStr);

    for(String logDir : logDirs) {
      containerLogDirs.add(logDir + Path.SEPARATOR + relativeContainerLogDir);
    }

    return containerLogDirs;
  }

  protected List<String> getContainerLocalDirs(List<String> localDirs) {
    List<String> containerLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    String appIdStr = app.getAppId().toString();
    String relativeContainerLocalDir = ContainerLocalizer.USERCACHE
        + Path.SEPARATOR + user + Path.SEPARATOR + ContainerLocalizer.APPCACHE
        + Path.SEPARATOR + appIdStr + Path.SEPARATOR;

    for (String localDir : localDirs) {
      containerLocalDirs.add(localDir + Path.SEPARATOR
          + relativeContainerLocalDir);
    }

    return containerLocalDirs;
  }

  protected Map<Path, List<String>> getLocalizedResources()
      throws YarnException {
    Map<Path, List<String>> localResources = container.getLocalizedResources();
    if (localResources == null) {
      throw RPCUtil.getRemoteException(
          "Unable to get local resources when Container " + container
              + " is at " + container.getContainerState());
    }
    return localResources;
  }

  @SuppressWarnings("unchecked")
  protected int launchContainer(ContainerStartContext ctx) throws IOException {
    ContainerId containerId = container.getContainerId();

    // LaunchContainer is a blocking call. We are here almost means the
    // container is launched, so send out the event.
    dispatcher.getEventHandler().handle(new ContainerEvent(
        containerId,
        ContainerEventType.CONTAINER_LAUNCHED));
    context.getNMStateStore().storeContainerLaunched(containerId);

    // Check if the container is signalled to be killed.
    if (!shouldLaunchContainer.compareAndSet(false, true)) {
      LOG.info("Container " + containerId + " not launched as "
          + "cleanup already called");
      return ExitCode.TERMINATED.getExitCode();
    } else {
      exec.activateContainer(containerId, pidFilePath);
      return exec.launchContainer(ctx);
    }
  }

  protected void setContainerCompletedStatus(int exitCode) {
    ContainerId containerId = container.getContainerId();
    completed.set(true);
    exec.deactivateContainer(containerId);
    try {
      if (!container.shouldRetry(exitCode)) {
        context.getNMStateStore().storeContainerCompleted(containerId,
            exitCode);
      }
    } catch (IOException e) {
      LOG.error("Unable to set exit code for container " + containerId);
    }
  }

  @SuppressWarnings("unchecked")
  protected void handleContainerExitCode(int exitCode, Path containerLogDir) {
    ContainerId containerId = container.getContainerId();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Container " + containerId + " completed with exit code "
          + exitCode);
    }

    StringBuilder diagnosticInfo =
        new StringBuilder("Container exited with a non-zero exit code ");
    diagnosticInfo.append(exitCode);
    diagnosticInfo.append(". ");
    if (exitCode == ExitCode.FORCE_KILLED.getExitCode()
        || exitCode == ExitCode.TERMINATED.getExitCode()) {
      // If the process was killed, Send container_cleanedup_after_kill and
      // just break out of this method.
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(containerId,
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST, exitCode,
              diagnosticInfo.toString()));
    } else if (exitCode != 0) {
      handleContainerExitWithFailure(containerId, exitCode, containerLogDir,
          diagnosticInfo);
    } else {
      LOG.info("Container " + containerId + " succeeded ");
      dispatcher.getEventHandler().handle(
          new ContainerEvent(containerId,
              ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    }
  }

  /**
   * Tries to tail and fetch TAIL_SIZE_IN_BYTES of data from the error log.
   * ErrorLog filename is not fixed and depends upon app, hence file name
   * pattern is used.
   * @param containerID
   * @param ret
   * @param containerLogDir
   * @param diagnosticInfo
   */
  @SuppressWarnings("unchecked")
  protected void handleContainerExitWithFailure(ContainerId containerID,
      int ret, Path containerLogDir, StringBuilder diagnosticInfo) {
    LOG.warn(diagnosticInfo);

    String errorFileNamePattern =
        conf.get(YarnConfiguration.NM_CONTAINER_STDERR_PATTERN,
            YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_PATTERN);
    FSDataInputStream errorFileIS = null;
    try {
      FileSystem fileSystem = FileSystem.getLocal(conf).getRaw();
      FileStatus[] errorFileStatuses = fileSystem
          .globStatus(new Path(containerLogDir, errorFileNamePattern));
      if (errorFileStatuses != null && errorFileStatuses.length != 0) {
        long tailSizeInBytes =
            conf.getLong(YarnConfiguration.NM_CONTAINER_STDERR_BYTES,
                YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_BYTES);
        Path errorFile = errorFileStatuses[0].getPath();
        long fileSize = errorFileStatuses[0].getLen();

        // if more than one file matches the stderr pattern, take the latest
        // modified file, and also append the file names in the diagnosticInfo
        if (errorFileStatuses.length > 1) {
          String[] errorFileNames = new String[errorFileStatuses.length];
          long latestModifiedTime = errorFileStatuses[0].getModificationTime();
          errorFileNames[0] = errorFileStatuses[0].getPath().getName();
          for (int i = 1; i < errorFileStatuses.length; i++) {
            errorFileNames[i] = errorFileStatuses[i].getPath().getName();
            if (errorFileStatuses[i]
                .getModificationTime() > latestModifiedTime) {
              latestModifiedTime = errorFileStatuses[i].getModificationTime();
              errorFile = errorFileStatuses[i].getPath();
              fileSize = errorFileStatuses[i].getLen();
            }
          }
          diagnosticInfo.append("Error files: ")
              .append(StringUtils.join(", ", errorFileNames)).append(".\n");
        }

        long startPosition =
            (fileSize < tailSizeInBytes) ? 0 : fileSize - tailSizeInBytes;
        int bufferSize =
            (int) ((fileSize < tailSizeInBytes) ? fileSize : tailSizeInBytes);
        byte[] tailBuffer = new byte[bufferSize];
        errorFileIS = fileSystem.open(errorFile);
        errorFileIS.readFully(startPosition, tailBuffer);

        diagnosticInfo.append("Last ").append(tailSizeInBytes)
            .append(" bytes of ").append(errorFile.getName()).append(" :\n")
            .append(new String(tailBuffer, StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      LOG.error("Failed to get tail of the container's error log file", e);
    } finally {
      IOUtils.cleanup(LOG, errorFileIS);
    }

    this.dispatcher.getEventHandler()
        .handle(new ContainerExitEvent(containerID,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
            diagnosticInfo.toString()));
  }

  protected String getPidFileSubpath(String appIdStr, String containerIdStr) {
    return getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
        + String.format(ContainerLaunch.PID_FILE_NAME_FMT, containerIdStr);
  }
  
  /**
   * Cleanup the container.
   * Cancels the launch if launch has not started yet or signals
   * the executor to not execute the process if not already done so.
   * Also, sends a SIGTERM followed by a SIGKILL to the process if
   * the process id is available.
   * @throws IOException
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  public void cleanupContainer() throws IOException {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Cleaning up container " + containerIdStr);

    try {
      context.getNMStateStore().storeContainerKilled(containerId);
    } catch (IOException e) {
      LOG.error("Unable to mark container " + containerId
          + " killed in store", e);
    }

    // launch flag will be set to true if process already launched
    boolean alreadyLaunched = !shouldLaunchContainer.compareAndSet(false, true);
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " No cleanup needed to be done");
      return;
    }

    LOG.debug("Marking container " + containerIdStr + " as inactive");
    // this should ensure that if the container process has not launched 
    // by this time, it will never be launched
    exec.deactivateContainer(containerId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting pid for container " + containerIdStr + " to kill"
          + " from pid file " 
          + (pidFilePath != null ? pidFilePath.toString() : "null"));
    }
    
    // however the container process may have already started
    try {

      // get process id from pid file if available
      // else if shell is still active, get it from the shell
      String processId = null;
      if (pidFilePath != null) {
        processId = getContainerPid(pidFilePath);
      }

      // kill process
      if (processId != null) {
        String user = container.getUser();
        LOG.debug("Sending signal to pid " + processId
            + " as user " + user
            + " for container " + containerIdStr);

        final Signal signal = sleepDelayBeforeSigKill > 0
          ? Signal.TERM
          : Signal.KILL;

        boolean result = exec.signalContainer(
            new ContainerSignalContext.Builder()
                .setContainer(container)
                .setUser(user)
                .setPid(processId)
                .setSignal(signal)
                .build());

        LOG.debug("Sent signal " + signal + " to pid " + processId
          + " as user " + user
          + " for container " + containerIdStr
          + ", result=" + (result? "success" : "failed"));

        if (sleepDelayBeforeSigKill > 0) {
          new DelayedProcessKiller(container, user,
              processId, sleepDelayBeforeSigKill, Signal.KILL, exec).start();
        }
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to cleanup container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.warn(message);
      dispatcher.getEventHandler().handle(
        new ContainerDiagnosticsUpdateEvent(containerId, message));
    } finally {
      // cleanup pid file if present
      if (pidFilePath != null) {
        FileContext lfs = FileContext.getLocalFSFileContext();
        lfs.delete(pidFilePath, false);
        lfs.delete(pidFilePath.suffix(EXIT_CODE_FILE_SUFFIX), false);
      }
    }
  }

  /**
   * Send a signal to the container.
   *
   *
   * @throws IOException
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  public void signalContainer(SignalContainerCommand command)
      throws IOException {
    ContainerId containerId =
        container.getContainerTokenIdentifier().getContainerID();
    String containerIdStr = containerId.toString();
    String user = container.getUser();
    Signal signal = translateCommandToSignal(command);
    if (signal.equals(Signal.NULL)) {
      LOG.info("ignore signal command " + command);
      return;
    }

    LOG.info("Sending signal " + command + " to container " + containerIdStr);

    boolean alreadyLaunched = !shouldLaunchContainer.compareAndSet(false, true);
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " Not sending the signal");
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting pid for container " + containerIdStr
          + " to send signal to from pid file "
          + (pidFilePath != null ? pidFilePath.toString() : "null"));
    }

    try {
      // get process id from pid file if available
      // else if shell is still active, get it from the shell
      String processId = null;
      if (pidFilePath != null) {
        processId = getContainerPid(pidFilePath);
      }

      if (processId != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending signal to pid " + processId
              + " as user " + user
              + " for container " + containerIdStr);
        }

        boolean result = exec.signalContainer(
            new ContainerSignalContext.Builder()
                .setContainer(container)
                .setUser(user)
                .setPid(processId)
                .setSignal(signal)
                .build());

        String diagnostics = "Sent signal " + command
            + " (" + signal + ") to pid " + processId
            + " as user " + user
            + " for container " + containerIdStr
            + ", result=" + (result ? "success" : "failed");
        LOG.info(diagnostics);

        dispatcher.getEventHandler().handle(
            new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
      }
    } catch (Exception e) {
      String message =
          "Exception when sending signal to container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.warn(message);
    }
  }

  @VisibleForTesting
  public static Signal translateCommandToSignal(
      SignalContainerCommand command) {
    Signal signal = Signal.NULL;
    switch (command) {
      case OUTPUT_THREAD_DUMP:
        // TODO for windows support.
        signal = Shell.WINDOWS ? Signal.NULL: Signal.QUIT;
        break;
      case GRACEFUL_SHUTDOWN:
        signal = Signal.TERM;
        break;
      case FORCEFUL_SHUTDOWN:
        signal = Signal.KILL;
        break;
    }
    return signal;
  }

  /**
   * Loop through for a time-bounded interval waiting to
   * read the process id from a file generated by a running process.
   * @param pidFilePath File from which to read the process id
   * @return Process ID
   * @throws Exception
   */
  private String getContainerPid(Path pidFilePath) throws Exception {
    String containerIdStr = 
        container.getContainerId().toString();
    String processId = null;
    LOG.debug("Accessing pid for container " + containerIdStr
        + " from pid file " + pidFilePath);
    int sleepCounter = 0;
    final int sleepInterval = 100;

    // loop waiting for pid file to show up 
    // until our timer expires in which case we admit defeat
    while (true) {
      processId = ProcessIdFileReader.getProcessId(pidFilePath);
      if (processId != null) {
        LOG.debug("Got pid " + processId + " for container "
            + containerIdStr);
        break;
      }
      else if ((sleepCounter*sleepInterval) > maxKillWaitTime) {
        LOG.info("Could not get pid for " + containerIdStr
        		+ ". Waited for " + maxKillWaitTime + " ms.");
        break;
      }
      else {
        ++sleepCounter;
        Thread.sleep(sleepInterval);
      }
    }
    return processId;
  }

  public static String getRelativeContainerLogDir(String appIdStr,
      String containerIdStr) {
    return appIdStr + Path.SEPARATOR + containerIdStr;
  }

  protected String getContainerPrivateDir(String appIdStr,
      String containerIdStr) {
    return getAppPrivateDir(appIdStr) + Path.SEPARATOR + containerIdStr
        + Path.SEPARATOR;
  }

  private String getAppPrivateDir(String appIdStr) {
    return ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR
        + appIdStr;
  }

  Context getContext() {
    return context;
  }

  public static abstract class ShellScriptBuilder {
    public static ShellScriptBuilder create() {
      return Shell.WINDOWS ? new WindowsShellScriptBuilder() :
        new UnixShellScriptBuilder();
    }

    private static final String LINE_SEPARATOR =
        System.getProperty("line.separator");
    private final StringBuilder sb = new StringBuilder();

    public abstract void command(List<String> command) throws IOException;

    public abstract void whitelistedEnv(String key, String value) throws IOException;

    public abstract void env(String key, String value) throws IOException;

    public final void symlink(Path src, Path dst) throws IOException {
      if (!src.isAbsolute()) {
        throw new IOException("Source must be absolute");
      }
      if (dst.isAbsolute()) {
        throw new IOException("Destination must be relative");
      }
      if (dst.toUri().getPath().indexOf('/') != -1) {
        mkdir(dst.getParent());
      }
      link(src, dst);
    }

    /**
     * Method to copy files that are useful for debugging container failures.
     * This method will be called by ContainerExecutor when setting up the
     * container launch script. The method should take care to make sure files
     * are read-able by the yarn user if the files are to undergo
     * log-aggregation.
     * @param src path to the source file
     * @param dst path to the destination file - should be absolute
     * @throws IOException
     */
    public abstract void copyDebugInformation(Path src, Path dst)
        throws IOException;

    /**
     * Method to dump debug information to the a target file. This method will
     * be called by ContainerExecutor when setting up the container launch
     * script.
     * @param output the file to which debug information is to be written
     * @throws IOException
     */
    public abstract void listDebugInformation(Path output) throws IOException;

    @Override
    public String toString() {
      return sb.toString();
    }

    public final void write(PrintStream out) throws IOException {
      out.append(sb);
    }

    protected final void line(String... command) {
      for (String s : command) {
        sb.append(s);
      }
      sb.append(LINE_SEPARATOR);
    }

    protected abstract void link(Path src, Path dst) throws IOException;

    protected abstract void mkdir(Path path) throws IOException;
  }

  private static final class UnixShellScriptBuilder extends ShellScriptBuilder {

    private void errorCheck() {
      line("hadoop_shell_errorcode=$?");
      line("if [ $hadoop_shell_errorcode -ne 0 ]");
      line("then");
      line("  exit $hadoop_shell_errorcode");
      line("fi");
    }

    public UnixShellScriptBuilder(){
      line("#!/bin/bash");
      line();
    }

    @Override
    public void command(List<String> command) {
      line("exec /bin/bash -c \"", StringUtils.join(" ", command), "\"");
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) {
      line("export ", key, "=${", key, ":-", "\"", value, "\"}");
    }

    @Override
    public void env(String key, String value) {
      line("export ", key, "=\"", value, "\"");
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      line("ln -sf \"", src.toUri().getPath(), "\" \"", dst.toString(), "\"");
      errorCheck();
    }

    @Override
    protected void mkdir(Path path) {
      line("mkdir -p ", path.toString());
      errorCheck();
    }

    @Override
    public void copyDebugInformation(Path src, Path dest) throws IOException {
      line("# Creating copy of launch script");
      line("cp \"", src.toUri().getPath(), "\" \"", dest.toUri().getPath(),
          "\"");
      // set permissions to 640 because we need to be able to run
      // log aggregation in secure mode as well
      if(dest.isAbsolute()) {
        line("chmod 640 \"", dest.toUri().getPath(), "\"");
      }
    }

    @Override
    public void listDebugInformation(Path output) throws  IOException {
      line("# Determining directory contents");
      line("echo \"ls -l:\" 1>\"", output.toString(), "\"");
      line("ls -l 1>>\"", output.toString(), "\"");

      // don't run error check because if there are loops
      // find will exit with an error causing container launch to fail
      // find will follow symlinks outside the work dir if such sylimks exist
      // (like public/app local resources)
      line("echo \"find -L . -maxdepth 5 -ls:\" 1>>\"", output.toString(),
          "\"");
      line("find -L . -maxdepth 5 -ls 1>>\"", output.toString(), "\"");
      line("echo \"broken symlinks(find -L . -maxdepth 5 -type l -ls):\" 1>>\"",
          output.toString(), "\"");
      line("find -L . -maxdepth 5 -type l -ls 1>>\"", output.toString(), "\"");
    }
  }

  private static final class WindowsShellScriptBuilder
      extends ShellScriptBuilder {

    private void errorCheck() {
      line("@if %errorlevel% neq 0 exit /b %errorlevel%");
    }

    private void lineWithLenCheck(String... commands) throws IOException {
      Shell.checkWindowsCommandLineLength(commands);
      line(commands);
    }

    public WindowsShellScriptBuilder() {
      line("@setlocal");
      line();
    }

    @Override
    public void command(List<String> command) throws IOException {
      lineWithLenCheck("@call ", StringUtils.join(" ", command));
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    public void env(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      File srcFile = new File(src.toUri().getPath());
      String srcFileStr = srcFile.getPath();
      String dstFileStr = new File(dst.toString()).getPath();
      lineWithLenCheck(String.format("@%s symlink \"%s\" \"%s\"",
          Shell.getWinUtilsPath(), dstFileStr, srcFileStr));
      errorCheck();
    }

    @Override
    protected void mkdir(Path path) throws IOException {
      lineWithLenCheck(String.format("@if not exist \"%s\" mkdir \"%s\"",
          path.toString(), path.toString()));
      errorCheck();
    }

    @Override
    public void copyDebugInformation(Path src, Path dest)
        throws IOException {
      // no need to worry about permissions - in secure mode
      // WindowsSecureContainerExecutor will set permissions
      // to allow NM to read the file
      line("rem Creating copy of launch script");
      lineWithLenCheck(String.format("copy \"%s\" \"%s\"", src.toString(),
          dest.toString()));
    }

    @Override
    public void listDebugInformation(Path output) throws IOException {
      line("rem Determining directory contents");
      lineWithLenCheck(
          String.format("@echo \"dir:\" > \"%s\"", output.toString()));
      lineWithLenCheck(String.format("dir >> \"%s\"", output.toString()));
    }
  }

  private static void putEnvIfNotNull(
      Map<String, String> environment, String variable, String value) {
    if (value != null) {
      environment.put(variable, value);
    }
  }
  
  private static void putEnvIfAbsent(
      Map<String, String> environment, String variable) {
    if (environment.get(variable) == null) {
      putEnvIfNotNull(environment, variable, System.getenv(variable));
    }
  }
  
  public void sanitizeEnv(Map<String, String> environment, Path pwd,
      List<Path> appDirs, List<String> containerLogDirs,
      Map<Path, List<String>> resources,
      Path nmPrivateClasspathJarDir) throws IOException {
    /**
     * Non-modifiable environment variables
     */

    environment.put(Environment.CONTAINER_ID.name(), container
        .getContainerId().toString());

    environment.put(Environment.NM_PORT.name(),
      String.valueOf(this.context.getNodeId().getPort()));

    environment.put(Environment.NM_HOST.name(), this.context.getNodeId()
      .getHost());

    environment.put(Environment.NM_HTTP_PORT.name(),
      String.valueOf(this.context.getHttpPort()));

    environment.put(Environment.LOCAL_DIRS.name(),
        StringUtils.join(",", appDirs));

    environment.put(Environment.LOG_DIRS.name(),
      StringUtils.join(",", containerLogDirs));

    environment.put(Environment.USER.name(), container.getUser());
    
    environment.put(Environment.LOGNAME.name(), container.getUser());

    environment.put(Environment.HOME.name(),
        conf.get(
            YarnConfiguration.NM_USER_HOME_DIR, 
            YarnConfiguration.DEFAULT_NM_USER_HOME_DIR
            )
        );
    
    environment.put(Environment.PWD.name(), pwd.toString());
    
    putEnvIfNotNull(environment, 
        Environment.HADOOP_CONF_DIR.name(), 
        System.getenv(Environment.HADOOP_CONF_DIR.name())
        );

    if (!Shell.WINDOWS) {
      environment.put("JVM_PID", "$$");
    }

    /**
     * Modifiable environment variables
     */
    
    // allow containers to override these variables
    String[] whitelist = conf.get(YarnConfiguration.NM_ENV_WHITELIST, YarnConfiguration.DEFAULT_NM_ENV_WHITELIST).split(",");
    
    for(String whitelistEnvVariable : whitelist) {
      putEnvIfAbsent(environment, whitelistEnvVariable.trim());
    }

    // variables here will be forced in, even if the container has specified them.
    Apps.setEnvFromInputString(environment, conf.get(
      YarnConfiguration.NM_ADMIN_USER_ENV,
      YarnConfiguration.DEFAULT_NM_ADMIN_USER_ENV), File.pathSeparator);

    // TODO: Remove Windows check and use this approach on all platforms after
    // additional testing.  See YARN-358.
    if (Shell.WINDOWS) {
      
      String inputClassPath = environment.get(Environment.CLASSPATH.name());

      if (inputClassPath != null && !inputClassPath.isEmpty()) {

        //On non-windows, localized resources
        //from distcache are available via the classpath as they were placed
        //there but on windows they are not available when the classpath
        //jar is created and so they "are lost" and have to be explicitly
        //added to the classpath instead.  This also means that their position
        //is lost relative to other non-distcache classpath entries which will
        //break things like mapreduce.job.user.classpath.first.  An environment
        //variable can be set to indicate that distcache entries should come
        //first

        boolean preferLocalizedJars = Boolean.parseBoolean(
          environment.get(Environment.CLASSPATH_PREPEND_DISTCACHE.name())
          );

        boolean needsSeparator = false;
        StringBuilder newClassPath = new StringBuilder();
        if (!preferLocalizedJars) {
          newClassPath.append(inputClassPath);
          needsSeparator = true;
        }

        // Localized resources do not exist at the desired paths yet, because the
        // container launch script has not run to create symlinks yet.  This
        // means that FileUtil.createJarWithClassPath can't automatically expand
        // wildcards to separate classpath entries for each file in the manifest.
        // To resolve this, append classpath entries explicitly for each
        // resource.
        for (Map.Entry<Path,List<String>> entry : resources.entrySet()) {
          boolean targetIsDirectory = new File(entry.getKey().toUri().getPath())
            .isDirectory();

          for (String linkName : entry.getValue()) {
            // Append resource.
            if (needsSeparator) {
              newClassPath.append(File.pathSeparator);
            } else {
              needsSeparator = true;
            }
            newClassPath.append(pwd.toString())
              .append(Path.SEPARATOR).append(linkName);

            // FileUtil.createJarWithClassPath must use File.toURI to convert
            // each file to a URI to write into the manifest's classpath.  For
            // directories, the classpath must have a trailing '/', but
            // File.toURI only appends the trailing '/' if it is a directory that
            // already exists.  To resolve this, add the classpath entries with
            // explicit trailing '/' here for any localized resource that targets
            // a directory.  Then, FileUtil.createJarWithClassPath will guarantee
            // that the resulting entry in the manifest's classpath will have a
            // trailing '/', and thus refer to a directory instead of a file.
            if (targetIsDirectory) {
              newClassPath.append(Path.SEPARATOR);
            }
          }
        }
        if (preferLocalizedJars) {
          if (needsSeparator) {
            newClassPath.append(File.pathSeparator);
          }
          newClassPath.append(inputClassPath);
        }

        // When the container launches, it takes the parent process's environment
        // and then adds/overwrites with the entries from the container launch
        // context.  Do the same thing here for correct substitution of
        // environment variables in the classpath jar manifest.
        Map<String, String> mergedEnv = new HashMap<String, String>(
          System.getenv());
        mergedEnv.putAll(environment);
        
        // this is hacky and temporary - it's to preserve the windows secure
        // behavior but enable non-secure windows to properly build the class
        // path for access to job.jar/lib/xyz and friends (see YARN-2803)
        Path jarDir;
        if (exec instanceof WindowsSecureContainerExecutor) {
          jarDir = nmPrivateClasspathJarDir;
        } else {
          jarDir = pwd; 
        }
        String[] jarCp = FileUtil.createJarWithClassPath(
          newClassPath.toString(), jarDir, pwd, mergedEnv);
        // In a secure cluster the classpath jar must be localized to grant access
        Path localizedClassPathJar = exec.localizeClasspathJar(
            new Path(jarCp[0]), pwd, container.getUser());
        String replacementClassPath = localizedClassPathJar.toString() + jarCp[1];
        environment.put(Environment.CLASSPATH.name(), replacementClassPath);
      }
    }
    // put AuxiliaryService data to environment
    for (Map.Entry<String, ByteBuffer> meta : containerManager
        .getAuxServiceMetaData().entrySet()) {
      AuxiliaryServiceHelper.setServiceDataIntoEnv(
          meta.getKey(), meta.getValue(), environment);
    }
  }

  public static String getExitCodeFile(String pidFile) {
    return pidFile + EXIT_CODE_FILE_SUFFIX;
  }

  private void recordContainerLogDir(ContainerId containerId,
      String logDir) throws IOException{
    if (container.isRetryContextSet()) {
      container.setLogDir(logDir);
      context.getNMStateStore().storeContainerLogDir(containerId, logDir);
    }
  }

  private void recordContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException{
    if (container.isRetryContextSet()) {
      container.setWorkDir(workDir);
      context.getNMStateStore().storeContainerWorkDir(containerId, workDir);
    }
  }
}
