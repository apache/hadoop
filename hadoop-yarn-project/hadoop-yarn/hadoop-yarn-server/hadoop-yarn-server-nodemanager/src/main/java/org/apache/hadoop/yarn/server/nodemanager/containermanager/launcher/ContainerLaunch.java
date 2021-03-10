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
import static org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.TOKEN_FILE_NAME_FMT;

import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.server.security.AMSecretKeys;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLaunch implements Callable<Integer> {

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerLaunch.class);

  private static final String CONTAINER_PRE_LAUNCH_PREFIX = "prelaunch";
  public static final String CONTAINER_PRE_LAUNCH_STDOUT = CONTAINER_PRE_LAUNCH_PREFIX + ".out";
  public static final String CONTAINER_PRE_LAUNCH_STDERR = CONTAINER_PRE_LAUNCH_PREFIX + ".err";

  public static final String CONTAINER_SCRIPT =
    Shell.appendScriptExtension("launch_container");

  public static final String FINAL_CONTAINER_TOKENS_FILE = "container_tokens";
  public static final String SYSFS_DIR = "sysfs";

  public static final String KEYSTORE_FILE = "yarn_provided.keystore";
  public static final String TRUSTSTORE_FILE = "yarn_provided.truststore";

  private static final String PID_FILE_NAME_FMT = "%s.pid";
  static final String EXIT_CODE_FILE_SUFFIX = ".exitcode";

  protected final Dispatcher dispatcher;
  protected final ContainerExecutor exec;
  protected final Application app;
  protected final Container container;
  private final Configuration conf;
  private final Context context;
  private final ContainerManagerImpl containerManager;

  protected AtomicBoolean containerAlreadyLaunched = new AtomicBoolean(false);
  protected AtomicBoolean shouldPauseContainer = new AtomicBoolean(false);

  protected AtomicBoolean completed = new AtomicBoolean(false);

  private volatile boolean killedBeforeStart = false;
  private long maxKillWaitTime = 2000;

  protected Path pidFilePath = null;

  protected final LocalDirsHandlerService dirsHandler;

  private final Lock launchLock = new ReentrantLock();

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
    this.maxKillWaitTime =
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS);
  }

  @VisibleForTesting
  public static String expandEnvironment(String var,
      Path containerLogDir) {
    var = var.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,
      containerLogDir.toString());
    var = var.replace(ApplicationConstants.CLASS_PATH_SEPARATOR,
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

  private void expandAllEnvironmentVars(
      Map<String, String> environment, Path containerLogDir) {
    for (Entry<String, String> entry : environment.entrySet()) {
      String value = entry.getValue();
      value = expandEnvironment(value, containerLogDir);
      entry.setValue(value);
    }
  }

  private void addKeystoreVars(Map<String, String> environment,
      Path containerWorkDir) {
    environment.put(ApplicationConstants.KEYSTORE_FILE_LOCATION_ENV_NAME,
        new Path(containerWorkDir,
            ContainerLaunch.KEYSTORE_FILE).toUri().getPath());
    environment.put(ApplicationConstants.KEYSTORE_PASSWORD_ENV_NAME,
        new String(container.getCredentials().getSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE_PASSWORD),
            StandardCharsets.UTF_8));
  }

  private void addTruststoreVars(Map<String, String> environment,
                               Path containerWorkDir) {
    environment.put(
        ApplicationConstants.TRUSTSTORE_FILE_LOCATION_ENV_NAME,
        new Path(containerWorkDir,
            ContainerLaunch.TRUSTSTORE_FILE).toUri().getPath());
    environment.put(ApplicationConstants.TRUSTSTORE_PASSWORD_ENV_NAME,
        new String(container.getCredentials().getSecretKey(
            AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE_PASSWORD),
            StandardCharsets.UTF_8));
  }

  @Override
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

      // The actual expansion of environment variables happens after calling
      // sanitizeEnv.  This allows variables specified in NM_ADMIN_USER_ENV
      // to reference user or container-defined variables.
      Map<String, String> environment = launchContext.getEnvironment();
      // /////////////////////////// End of variable expansion

      // Use this to track variables that are added to the environment by nm.
      LinkedHashSet<String> nmEnvVars = new LinkedHashSet<String>();

      FileContext lfs = FileContext.getLocalFSFileContext();

      Path nmPrivateContainerScriptPath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + CONTAINER_SCRIPT);
      Path nmPrivateTokensPath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + String.format(TOKEN_FILE_NAME_FMT, containerIdStr));
      Path nmPrivateKeystorePath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + KEYSTORE_FILE);
      Path nmPrivateTruststorePath = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
              + TRUSTSTORE_FILE);
      Path nmPrivateClasspathJarDir = dirsHandler.getLocalPathForWrite(
          getContainerPrivateDir(appIdStr, containerIdStr));

      // Select the working directory for the container
      Path containerWorkDir = deriveContainerWorkDir();
      recordContainerWorkDir(containerID, containerWorkDir.toString());

      // Select a root dir for all csi volumes for the container
      Path csiVolumesRoot = deriveCsiVolumesRootDir();
      recordContainerCsiVolumesRootDir(containerID, csiVolumesRoot.toString());

      String pidFileSubpath = getPidFileSubpath(appIdStr, containerIdStr);
      // pid file should be in nm private dir so that it is not
      // accessible by users
      pidFilePath = dirsHandler.getLocalPathForWrite(pidFileSubpath);
      List<String> localDirs = dirsHandler.getLocalDirs();
      List<String> localDirsForRead = dirsHandler.getLocalDirsForRead();
      List<String> logDirs = dirsHandler.getLogDirs();
      List<String> filecacheDirs = getNMFilecacheDirs(localDirsForRead);
      List<String> userLocalDirs = getUserLocalDirs(localDirs);
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);
      List<String> containerLogDirs = getContainerLogDirs(logDirs);
      List<String> userFilecacheDirs = getUserFilecacheDirs(localDirsForRead);
      List<String> applicationLocalDirs = getApplicationLocalDirs(localDirs,
          appIdStr);

      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }
      List<Path> appDirs = new ArrayList<Path>(localDirs.size());
      for (String localDir : localDirs) {
        Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
        Path userdir = new Path(usersdir, user);
        Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
        appDirs.add(new Path(appsdir, appIdStr));
      }

      byte[] keystore = container.getCredentials().getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_KEYSTORE);
      if (keystore != null) {
        try (DataOutputStream keystoreOutStream =
                 lfs.create(nmPrivateKeystorePath,
                     EnumSet.of(CREATE, OVERWRITE))) {
          keystoreOutStream.write(keystore);
        }
      } else {
        nmPrivateKeystorePath = null;
      }
      byte[] truststore = container.getCredentials().getSecretKey(
          AMSecretKeys.YARN_APPLICATION_AM_TRUSTSTORE);
      if (truststore != null) {
        try (DataOutputStream truststoreOutStream =
                 lfs.create(nmPrivateTruststorePath,
                     EnumSet.of(CREATE, OVERWRITE))) {
          truststoreOutStream.write(truststore);
        }
      } else {
        nmPrivateTruststorePath = null;
      }

      // Set the token location too.
      addToEnvMap(environment, nmEnvVars,
          ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME,
          new Path(containerWorkDir,
              FINAL_CONTAINER_TOKENS_FILE).toUri().getPath());

      // /////////// Write out the container-script in the nmPrivate space.
      try (DataOutputStream containerScriptOutStream =
               lfs.create(nmPrivateContainerScriptPath,
                   EnumSet.of(CREATE, OVERWRITE))) {
        // Sanitize the container's environment
        sanitizeEnv(environment, containerWorkDir, appDirs, userLocalDirs,
            containerLogDirs, localResources, nmPrivateClasspathJarDir,
            nmEnvVars);

        expandAllEnvironmentVars(environment, containerLogDir);

        // Add these if needed after expanding so we don't expand key values.
        if (keystore != null) {
          addKeystoreVars(environment, containerWorkDir);
        }
        if (truststore != null) {
          addTruststoreVars(environment, containerWorkDir);
        }

        prepareContainer(localResources, containerLocalDirs);

        // Write out the environment
        exec.writeLaunchEnv(containerScriptOutStream, environment,
            localResources, launchContext.getCommands(),
            containerLogDir, user, nmEnvVars);
      }
      // /////////// End of writing out container-script

      // /////////// Write out the container-tokens in the nmPrivate space.
      try (DataOutputStream tokensOutStream =
               lfs.create(nmPrivateTokensPath, EnumSet.of(CREATE, OVERWRITE))) {
        Credentials creds = container.getCredentials();
        creds.writeTokenStorageToStream(tokensOutStream);
      }
      // /////////// End of writing out container-tokens

      ret = launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setLocalizedResources(localResources)
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setNmPrivateKeystorePath(nmPrivateKeystorePath)
          .setNmPrivateTruststorePath(nmPrivateTruststorePath)
          .setUser(user)
          .setAppId(appIdStr)
          .setContainerWorkDir(containerWorkDir)
          .setContainerCsiVolumesRootDir(csiVolumesRoot)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .setFilecacheDirs(filecacheDirs)
          .setUserLocalDirs(userLocalDirs)
          .setContainerLocalDirs(containerLocalDirs)
          .setContainerLogDirs(containerLogDirs)
          .setUserFilecacheDirs(userFilecacheDirs)
          .setApplicationLocalDirs(applicationLocalDirs).build());
    } catch (ConfigurationException e) {
      LOG.error("Failed to launch container due to configuration error.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      // Mark the node as unhealthy
      context.getNodeStatusUpdater().reportException(e);
      return ret;
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

  /**
   * Volumes mount point root:
   *   ${YARN_LOCAL_DIR}/usercache/${user}/filecache/csiVolumes/app/container
   * CSI volumes may creates the mount point with different permission bits.
   * If we create the volume mount under container work dir, it may
   * mess up the existing permission structure, which is restricted by
   * linux container executor. So we put all volume mounts under a same
   * root dir so it is easier cleanup.
   **/
  private Path deriveCsiVolumesRootDir() throws IOException {
    final String containerVolumePath =
        ContainerLocalizer.USERCACHE + Path.SEPARATOR
            + container.getUser() + Path.SEPARATOR
            + ContainerLocalizer.FILECACHE + Path.SEPARATOR
            + ContainerLocalizer.CSI_VOLIUME_MOUNTS_ROOT + Path.SEPARATOR
            + app.getAppId().toString() + Path.SEPARATOR
            + container.getContainerId().toString();
    return dirsHandler.getLocalPathForWrite(containerVolumePath,
        LocalDirAllocator.SIZE_UNKNOWN, false);
  }

  private Path deriveContainerWorkDir() throws IOException {

    final String containerWorkDirPath =
        ContainerLocalizer.USERCACHE +
        Path.SEPARATOR +
        container.getUser() +
        Path.SEPARATOR +
        ContainerLocalizer.APPCACHE +
        Path.SEPARATOR +
        app.getAppId().toString() +
        Path.SEPARATOR +
        container.getContainerId().toString();

    final Path containerWorkDir =
        dirsHandler.getLocalPathForWrite(
          containerWorkDirPath,
          LocalDirAllocator.SIZE_UNKNOWN, false);

    return containerWorkDir;
  }

  private void prepareContainer(Map<Path, List<String>> localResources,
      List<String> containerLocalDirs) throws IOException {

    exec.prepareContainer(new ContainerPrepareContext.Builder()
        .setContainer(container)
        .setLocalizedResources(localResources)
        .setUser(container.getUser())
        .setContainerLocalDirs(containerLocalDirs)
        .setCommands(container.getLaunchContext().getCommands())
        .build());
  }

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

    for (String logDir : logDirs) {
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

  protected List<String> getUserLocalDirs(List<String> localDirs) {
    List<String> userLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();

    for (String localDir : localDirs) {
      String userLocalDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR;

      userLocalDirs.add(userLocalDir);
    }

    return userLocalDirs;
  }

  protected List<String> getNMFilecacheDirs(List<String> localDirs) {
    List<String> filecacheDirs = new ArrayList<>(localDirs.size());

    for (String localDir : localDirs) {
      String filecacheDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.FILECACHE;

      filecacheDirs.add(filecacheDir);
    }

    return filecacheDirs;
  }

  protected List<String> getUserFilecacheDirs(List<String> localDirs) {
    List<String> userFilecacheDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    for (String localDir : localDirs) {
      String userFilecacheDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR + ContainerLocalizer.FILECACHE;
      userFilecacheDirs.add(userFilecacheDir);
    }
    return userFilecacheDirs;
  }

  protected List<String> getApplicationLocalDirs(List<String> localDirs,
      String appIdStr) {
    List<String> applicationLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    for (String localDir : localDirs) {
      String appLocalDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR + ContainerLocalizer.APPCACHE
          + Path.SEPARATOR + appIdStr;
      applicationLocalDirs.add(appLocalDir);
    }
    return applicationLocalDirs;
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

  protected int launchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    int launchPrep = prepareForLaunch(ctx);
    if (launchPrep == 0) {
      launchLock.lock();
      try {
        return exec.launchContainer(ctx);
      } finally {
        launchLock.unlock();
      }
    }
    return launchPrep;
  }

  protected int relaunchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    int launchPrep = prepareForLaunch(ctx);
    if (launchPrep == 0) {
      launchLock.lock();
      try {
        return exec.relaunchContainer(ctx);
      } finally {
        launchLock.unlock();
      }
    }
    return launchPrep;
  }

  void reapContainer() throws IOException {
    launchLock.lock();
    try {
      // Reap the container
      boolean result = exec.reapContainer(
          new ContainerReapContext.Builder()
              .setContainer(container)
              .setUser(container.getUser())
              .build());
      if (!result) {
        throw new IOException("Reap container failed for container " +
            container.getContainerId());
      }
      cleanupContainerFiles(getContainerWorkDir());
    } finally {
      launchLock.unlock();
    }
  }

  protected int prepareForLaunch(ContainerStartContext ctx) throws IOException {
    ContainerId containerId = container.getContainerId();
    if (container.isMarkedForKilling()) {
      LOG.info("Container " + containerId + " not launched as it has already "
          + "been marked for Killing");
      this.killedBeforeStart = true;
      return ExitCode.TERMINATED.getExitCode();
    }
    // LaunchContainer is a blocking call. We are here almost means the
    // container is launched, so send out the event.
    dispatcher.getEventHandler().handle(new ContainerEvent(
        containerId,
        ContainerEventType.CONTAINER_LAUNCHED));
    context.getNMStateStore().storeContainerLaunched(containerId);

    // Check if the container is signalled to be killed.
    if (!containerAlreadyLaunched.compareAndSet(false, true)) {
      LOG.info("Container " + containerId + " not launched as "
          + "cleanup already called");
      return ExitCode.TERMINATED.getExitCode();
    } else {
      exec.activateContainer(containerId, pidFilePath);
    }
    return ExitCode.SUCCESS.getExitCode();
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

  protected void handleContainerExitCode(int exitCode, Path containerLogDir) {
    ContainerId containerId = container.getContainerId();
    LOG.debug("Container {} completed with exit code {}", containerId,
        exitCode);

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
   *
   * @param containerID
   * @param ret
   * @param containerLogDir
   * @param diagnosticInfo
   */
  protected void handleContainerExitWithFailure(ContainerId containerID,
      int ret, Path containerLogDir, StringBuilder diagnosticInfo) {
    LOG.warn("Container launch failed : " + diagnosticInfo.toString());

    FileSystem fileSystem = null;
    long tailSizeInBytes =
        conf.getLong(YarnConfiguration.NM_CONTAINER_STDERR_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_BYTES);

    // Append container prelaunch stderr to diagnostics
    try {
      fileSystem = FileSystem.getLocal(conf).getRaw();
      FileStatus preLaunchErrorFileStatus = fileSystem
          .getFileStatus(new Path(containerLogDir, ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR));

      Path errorFile = preLaunchErrorFileStatus.getPath();
      long fileSize = preLaunchErrorFileStatus.getLen();

      diagnosticInfo.append("Error file: ")
          .append(ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR).append(".\n");
      ;

      byte[] tailBuffer = tailFile(errorFile, fileSize, tailSizeInBytes);
      diagnosticInfo.append("Last ").append(tailSizeInBytes)
          .append(" bytes of ").append(errorFile.getName()).append(" :\n")
          .append(new String(tailBuffer, StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Failed to get tail of the container's prelaunch error log file", e);
    }

    // Append container stderr to diagnostics
    String errorFileNamePattern =
        conf.get(YarnConfiguration.NM_CONTAINER_STDERR_PATTERN,
            YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_PATTERN);

    try {
      if (fileSystem == null) {
        fileSystem = FileSystem.getLocal(conf).getRaw();
      }
      FileStatus[] errorFileStatuses = fileSystem
          .globStatus(new Path(containerLogDir, errorFileNamePattern));
      if (errorFileStatuses != null && errorFileStatuses.length != 0) {
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

        byte[] tailBuffer = tailFile(errorFile, fileSize, tailSizeInBytes);
        String tailBufferMsg = new String(tailBuffer, StandardCharsets.UTF_8);
        diagnosticInfo.append("Last ").append(tailSizeInBytes)
            .append(" bytes of ").append(errorFile.getName()).append(" :\n")
            .append(tailBufferMsg).append("\n")
            .append(analysesErrorMsgOfContainerExitWithFailure(tailBufferMsg));

      }
    } catch (IOException e) {
      LOG.error("Failed to get tail of the container's error log file", e);
    }
    this.dispatcher.getEventHandler()
        .handle(new ContainerExitEvent(containerID,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
            diagnosticInfo.toString()));
  }

  private byte[] tailFile(Path filePath, long fileSize, long tailSizeInBytes) throws IOException {
    FSDataInputStream errorFileIS = null;
    FileSystem fileSystem = FileSystem.getLocal(conf).getRaw();
    try {
      long startPosition =
          (fileSize < tailSizeInBytes) ? 0 : fileSize - tailSizeInBytes;
      int bufferSize =
          (int) ((fileSize < tailSizeInBytes) ? fileSize : tailSizeInBytes);
      byte[] tailBuffer = new byte[bufferSize];
      errorFileIS = fileSystem.open(filePath);
      errorFileIS.readFully(startPosition, tailBuffer);
      return tailBuffer;
    } finally {
      IOUtils.cleanupWithLogger(LOG, errorFileIS);
    }
  }

  private String analysesErrorMsgOfContainerExitWithFailure(String errorMsg) {
    StringBuilder analysis = new StringBuilder();
    if (errorMsg.indexOf("Error: Could not find or load main class"
        + " org.apache.hadoop.mapreduce") != -1) {
      analysis.append(
          "Please check whether your <HADOOP_HOME>/etc/hadoop/mapred-site.xml "
          + "contains the below configuration:\n");
      analysis.append("<property>\n")
          .append("  <name>yarn.app.mapreduce.am.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n<property>\n")
          .append("  <name>mapreduce.map.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n<property>\n")
          .append("  <name>mapreduce.reduce.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n");
    }
    return analysis.toString();
  }

  protected String getPidFileSubpath(String appIdStr, String containerIdStr) {
    return getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
        + String.format(ContainerLaunch.PID_FILE_NAME_FMT, containerIdStr);
  }

  /**
   * Send a signal to the container.
   *
   *
   * @throws IOException
   */
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

    boolean alreadyLaunched =
        !containerAlreadyLaunched.compareAndSet(false, true);
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " Not sending the signal");
      return;
    }

    LOG.debug("Getting pid for container {} to send signal to from pid"
        + " file {}", containerIdStr,
        (pidFilePath != null ? pidFilePath.toString() : "null"));

    try {
      // get process id from pid file if available
      // else if shell is still active, get it from the shell
      String processId = getContainerPid();
      if (processId != null) {
        LOG.debug("Sending signal to pid {} as user {} for container {}",
            processId, user, containerIdStr);

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
   * Pause the container.
   * Cancels the launch if the container isn't launched yet. Otherwise asks the
   * executor to pause the container.
   * @throws IOException in case of errors.
   */
  public void pauseContainer() throws IOException {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Pausing the container " + containerIdStr);

    // The pause event is only handled if the container is in the running state
    // (the container state machine), so we don't check for
    // shouldLaunchContainer over here

    if (!shouldPauseContainer.compareAndSet(false, true)) {
      LOG.info("Container " + containerId + " not paused as "
          + "resume already called");
      return;
    }

    try {
      // Pause the container
      exec.pauseContainer(container);

      // PauseContainer is a blocking call. We are here almost means the
      // container is paused, so send out the event.
      dispatcher.getEventHandler().handle(new ContainerEvent(
          containerId,
          ContainerEventType.CONTAINER_PAUSED));

      try {
        this.context.getNMStateStore().storeContainerPaused(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been paused.", e);
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to pause container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.info(message);
      container.handle(new ContainerKillEvent(container.getContainerId(),
          ContainerExitStatus.PREEMPTED, "Container preempted as there was "
          + " an exception in pausing it."));
    }
  }

  /**
   * Resume the container.
   * Cancels the launch if the container isn't launched yet. Otherwise asks the
   * executor to pause the container.
   * @throws IOException in case of error.
   */
  public void resumeContainer() throws IOException {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Resuming the container " + containerIdStr);

    // The resume event is only handled if the container is in a paused state
    // so we don't check for the launched flag here.

    // paused flag will be set to true if process already paused
    boolean alreadyPaused = !shouldPauseContainer.compareAndSet(false, true);
    if (!alreadyPaused) {
      LOG.info("Container " + containerIdStr + " not paused."
          + " No resume necessary");
      return;
    }

    // If the container has already started
    try {
      exec.resumeContainer(container);
      // ResumeContainer is a blocking call. We are here almost means the
      // container is resumed, so send out the event.
      dispatcher.getEventHandler().handle(new ContainerEvent(
          containerId,
          ContainerEventType.CONTAINER_RESUMED));

      try {
        this.context.getNMStateStore().removeContainerPaused(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been resumed.", e);
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to resume container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.info(message);
      container.handle(new ContainerKillEvent(container.getContainerId(),
          ContainerExitStatus.PREEMPTED, "Container preempted as there was "
          + " an exception in pausing it."));
    }
  }

  /**
   * Loop through for a time-bounded interval waiting to
   * read the process id from a file generated by a running process.
   * @return Process ID; null when pidFilePath is null
   * @throws Exception
   */
  String getContainerPid() throws Exception {
    if (pidFilePath == null) {
      return null;
    }
    String containerIdStr = 
        container.getContainerId().toString();
    String processId;
    LOG.debug("Accessing pid for container {} from pid file {}",
        containerIdStr, pidFilePath);
    int sleepCounter = 0;
    final int sleepInterval = 100;

    // loop waiting for pid file to show up 
    // until our timer expires in which case we admit defeat
    while (true) {
      processId = ProcessIdFileReader.getProcessId(pidFilePath);
      if (processId != null) {
        LOG.debug("Got pid {} for container {}", processId, containerIdStr);
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
      return create(Shell.osType);
    }

    @VisibleForTesting
    public static ShellScriptBuilder create(Shell.OSType osType) {
      return (osType == Shell.OSType.OS_TYPE_WIN) ?
          new WindowsShellScriptBuilder() :
          new UnixShellScriptBuilder();
    }

    private static final String LINE_SEPARATOR =
        System.getProperty("line.separator");
    private final StringBuilder sb = new StringBuilder();

    public abstract void command(List<String> command) throws IOException;

    protected static final String ENV_PRELAUNCH_STDOUT = "PRELAUNCH_OUT";
    protected static final String ENV_PRELAUNCH_STDERR = "PRELAUNCH_ERR";

    private boolean redirectStdOut = false;
    private boolean redirectStdErr = false;

    /**
     * Set stdout for the shell script
     * @param stdoutDir stdout must be an absolute path
     * @param stdOutFile stdout file name
     * @throws IOException thrown when stdout path is not absolute
     */
    public final void stdout(Path stdoutDir, String stdOutFile) throws IOException {
      if (!stdoutDir.isAbsolute()) {
        throw new IOException("Stdout path must be absolute");
      }
      redirectStdOut = true;
      setStdOut(new Path(stdoutDir, stdOutFile));
    }

    /**
     * Set stderr for the shell script
     * @param stderrDir stderr must be an absolute path
     * @param stdErrFile stderr file name
     * @throws IOException thrown when stderr path is not absolute
     */
    public final void stderr(Path stderrDir, String stdErrFile) throws IOException {
      if (!stderrDir.isAbsolute()) {
        throw new IOException("Stdout path must be absolute");
      }
      redirectStdErr = true;
      setStdErr(new Path(stderrDir, stdErrFile));
    }

    protected abstract void setStdOut(Path stdout) throws IOException;

    protected abstract void setStdErr(Path stdout) throws IOException;

    public abstract void env(String key, String value) throws IOException;

    public abstract void whitelistedEnv(String key, String value)
        throws IOException;

    public abstract void echo(String echoStr) throws IOException;

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
     * Method to dump debug information to a target file. This method will
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

    protected final void buildCommand(String... command) {
      for (String s : command) {
        sb.append(s);
      }
    }

    protected final void linebreak(String... command) {
      sb.append(LINE_SEPARATOR);
    }

    protected final void line(String... command) {
      buildCommand(command);
      linebreak();
    }

    public void setExitOnFailure() {
      // Dummy implementation
    }

    protected abstract void link(Path src, Path dst) throws IOException;

    protected abstract void mkdir(Path path) throws IOException;

    boolean doRedirectStdOut() {
      return redirectStdOut;
    }

    boolean doRedirectStdErr() {
      return redirectStdErr;
    }

    /**
     * Parse an environment value and returns all environment keys it uses.
     * @param envVal an environment variable's value
     * @return all environment variable names used in <code>envVal</code>.
     */
    public Set<String> getEnvDependencies(final String envVal) {
      return Collections.emptySet();
    }

    /**
     * Returns a dependency ordered version of <code>envs</code>. Does not alter
     * input <code>envs</code> map.
     * @param envs environment map
     * @return a dependency ordered version of <code>envs</code>
     */
    public final Map<String, String> orderEnvByDependencies(
        Map<String, String> envs) {
      if (envs == null || envs.size() < 2) {
        return envs;
      }
      final Map<String, String> ordered = new LinkedHashMap<String, String>();
      class Env {
        private boolean resolved = false;
        private final Collection<Env> deps = new ArrayList<>();
        private final String name;
        private final String value;
        Env(String name, String value) {
          this.name = name;
          this.value = value;
        }
        void resolve() {
          resolved = true;
          for (Env dep : deps) {
            if (!dep.resolved) {
              dep.resolve();
            }
          }
          ordered.put(name, value);
        }
      }
      final Map<String, Env> singletons = new HashMap<>();
      for (Map.Entry<String, String> e : envs.entrySet()) {
        Env env = singletons.get(e.getKey());
        if (env == null) {
          env = new Env(e.getKey(), e.getValue());
          singletons.put(env.name, env);
        }
        for (String depStr : getEnvDependencies(env.value)) {
          if (!envs.containsKey(depStr)) {
            continue;
          }
          Env depEnv = singletons.get(depStr);
          if (depEnv == null) {
            depEnv = new Env(depStr, envs.get(depStr));
            singletons.put(depStr, depEnv);
          }
          env.deps.add(depEnv);
        }
      }
      for (Env env : singletons.values()) {
        if (!env.resolved) {
          env.resolve();
        }
      }
      return ordered;
    }
  }

  private static final class UnixShellScriptBuilder extends ShellScriptBuilder {
    @SuppressWarnings("unused")
    private void errorCheck() {
      line("hadoop_shell_errorcode=$?");
      line("if [[ \"$hadoop_shell_errorcode\" -ne 0 ]]");
      line("then");
      line("  exit $hadoop_shell_errorcode");
      line("fi");
    }

    public UnixShellScriptBuilder() {
      line("#!/bin/bash");
      line();
    }

    @Override
    public void command(List<String> command) {
      line("exec /bin/bash -c \"", StringUtils.join(" ", command), "\"");
    }

    @Override
    public void setStdOut(final Path stdout) throws IOException {
      line("export ", ENV_PRELAUNCH_STDOUT, "=\"", stdout.toString(), "\"");
      // tee is needed for DefaultContainerExecutor error propagation to stdout
      // Close stdout of subprocess to prevent it from writing to the stdout file
      line("exec >\"${" + ENV_PRELAUNCH_STDOUT + "}\"");
    }

    @Override
    public void setStdErr(final Path stderr) throws IOException {
      line("export ", ENV_PRELAUNCH_STDERR, "=\"", stderr.toString(), "\"");
      // tee is needed for DefaultContainerExecutor error propagation to stderr
      // Close stdout of subprocess to prevent it from writing to the stdout file
      line("exec 2>\"${" + ENV_PRELAUNCH_STDERR + "}\"");
    }

    @Override
    public void env(String key, String value) throws IOException {
      line("export ", key, "=\"", value, "\"");
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      line("export ", key, "=${", key, ":-", "\"", value, "\"}");
    }

    @Override
    public void echo(final String echoStr) throws IOException {
      line("echo \"" + echoStr + "\"");
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      line("ln -sf -- \"", src.toUri().getPath(), "\" \"", dst.toString(), "\"");
    }

    @Override
    protected void mkdir(Path path) throws IOException {
      line("mkdir -p ", path.toString());
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

    @Override
    public void setExitOnFailure() {
      line("set -o pipefail -e");
    }

    /**
     * Parse <code>envVal</code> using bash-like syntax to extract env variables
     * it depends on.
     */
    @Override
    public Set<String> getEnvDependencies(final String envVal) {
      if (envVal == null || envVal.isEmpty()) {
        return Collections.emptySet();
      }
      final Set<String> deps = new HashSet<>();
      // env/whitelistedEnv dump values inside double quotes
      boolean inDoubleQuotes = true;
      char c;
      int i = 0;
      final int len = envVal.length();
      while (i < len) {
        c = envVal.charAt(i);
        if (c == '"') {
          inDoubleQuotes = !inDoubleQuotes;
        } else if (c == '\'' && !inDoubleQuotes) {
          i++;
          // eat until closing simple quote
          while (i < len) {
            c = envVal.charAt(i);
            if (c == '\\') {
              i++;
            }
            if (c == '\'') {
              break;
            }
            i++;
          }
        } else if (c == '\\') {
          i++;
        } else if (c == '$') {
          i++;
          if (i >= len) {
            break;
          }
          c = envVal.charAt(i);
          if (c == '{') { // for ${... bash like syntax
            i++;
            if (i >= len) {
              break;
            }
            c = envVal.charAt(i);
            if (c == '#') { // for ${#... bash array syntax
              i++;
              if (i >= len) {
                break;
              }
            }
          }
          final int start = i;
          while (i < len) {
            c = envVal.charAt(i);
            if (c != '$' && (
                (i == start && Character.isJavaIdentifierStart(c)) ||
                    (i > start && Character.isJavaIdentifierPart(c)))) {
              i++;
            } else {
              break;
            }
          }
          if (i > start) {
            deps.add(envVal.substring(start, i));
          }
        }
        i++;
      }
      return deps;
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

    //Dummy implementation
    @Override
    protected void setStdOut(final Path stdout) throws IOException {
    }

    //Dummy implementation
    @Override
    protected void setStdErr(final Path stderr) throws IOException {
    }

    @Override
    public void env(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      env(key, value);
    }

    @Override
    public void echo(final String echoStr) throws IOException {
      lineWithLenCheck("@echo \"", echoStr, "\"");
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

    /**
     * Parse <code>envVal</code> using cmd/bat-like syntax to extract env
     * variables it depends on.
     */
    public Set<String> getEnvDependencies(final String envVal) {
      if (envVal == null || envVal.isEmpty()) {
        return Collections.emptySet();
      }
      final Set<String> deps = new HashSet<>();
      final int len = envVal.length();
      int i = 0;
      while (i < len) {
        i = envVal.indexOf('%', i); // find beginning of variable
        if (i < 0 || i == (len - 1)) {
          break;
        }
        i++;
        // 3 cases: %var%, %var:...% or %%
        final int j = envVal.indexOf('%', i); // find end of variable
        if (j == i) {
          // %% case, just skip it
          i++;
          continue;
        }
        if (j < 0) {
          break; // even %var:...% syntax ends with a %, so j cannot be negative
        }
        final int k = envVal.indexOf(':', i);
        if (k >= 0 && k < j) {
          // %var:...% syntax
          deps.add(envVal.substring(i, k));
        } else {
          // %var% syntax
          deps.add(envVal.substring(i, j));
        }
        i = j + 1;
      }
      return deps;
    }
  }

  private static void addToEnvMap(
      Map<String, String> envMap, Set<String> envSet,
      String envName, String envValue) {
    envMap.put(envName, envValue);
    envSet.add(envName);
  }

  public void sanitizeEnv(Map<String, String> environment, Path pwd,
      List<Path> appDirs, List<String> userLocalDirs, List<String>
      containerLogDirs, Map<Path, List<String>> resources,
      Path nmPrivateClasspathJarDir,
      Set<String> nmVars) throws IOException {
    // Based on discussion in YARN-7654, for ENTRY_POINT enabled
    // docker container, we forward user defined environment variables
    // without node manager environment variables.  This is the reason
    // that we skip sanitizeEnv method.
    boolean overrideDisable = Boolean.parseBoolean(
        environment.get(
            Environment.
                YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE.
                    name()));
    if (overrideDisable) {
      environment.remove("WORK_DIR");
      return;
    }

    /**
     * Non-modifiable environment variables
     */

    addToEnvMap(environment, nmVars, Environment.CONTAINER_ID.name(),
        container.getContainerId().toString());

    addToEnvMap(environment, nmVars, Environment.NM_PORT.name(),
      String.valueOf(this.context.getNodeId().getPort()));

    addToEnvMap(environment, nmVars, Environment.NM_HOST.name(),
        this.context.getNodeId().getHost());

    addToEnvMap(environment, nmVars, Environment.NM_HTTP_PORT.name(),
      String.valueOf(this.context.getHttpPort()));

    addToEnvMap(environment, nmVars, Environment.LOCAL_DIRS.name(),
        StringUtils.join(",", appDirs));

    addToEnvMap(environment, nmVars, Environment.LOCAL_USER_DIRS.name(),
        StringUtils.join(",", userLocalDirs));

    addToEnvMap(environment, nmVars, Environment.LOG_DIRS.name(),
      StringUtils.join(",", containerLogDirs));

    addToEnvMap(environment, nmVars, Environment.USER.name(),
        container.getUser());

    addToEnvMap(environment, nmVars, Environment.LOGNAME.name(),
        container.getUser());

    addToEnvMap(environment, nmVars, Environment.HOME.name(),
        conf.get(
            YarnConfiguration.NM_USER_HOME_DIR, 
            YarnConfiguration.DEFAULT_NM_USER_HOME_DIR
            )
        );

    addToEnvMap(environment, nmVars, Environment.PWD.name(), pwd.toString());

    addToEnvMap(environment, nmVars, Environment.LOCALIZATION_COUNTERS.name(),
        container.localizationCountersAsString());

    if (!Shell.WINDOWS) {
      addToEnvMap(environment, nmVars, "JVM_PID", "$$");
    }

    // variables here will be forced in, even if the container has
    // specified them.  Note: we do not track these in nmVars, to
    // allow them to be ordered properly if they reference variables
    // defined by the user.
    String defEnvStr = conf.get(YarnConfiguration.DEFAULT_NM_ADMIN_USER_ENV);
    Apps.setEnvFromInputProperty(environment,
        YarnConfiguration.NM_ADMIN_USER_ENV, defEnvStr, conf,
        File.pathSeparator);

    if (!Shell.WINDOWS) {
      // maybe force path components
      String forcePath = conf.get(YarnConfiguration.NM_ADMIN_FORCE_PATH,
          YarnConfiguration.DEFAULT_NM_ADMIN_FORCE_PATH);
      if (!forcePath.isEmpty()) {
        String userPath = environment.get(Environment.PATH.name());
        environment.remove(Environment.PATH.name());
        if (userPath == null || userPath.isEmpty()) {
          Apps.addToEnvironment(environment, Environment.PATH.name(),
              forcePath, File.pathSeparator);
          Apps.addToEnvironment(environment, Environment.PATH.name(),
              "$PATH", File.pathSeparator);
        } else {
          Apps.addToEnvironment(environment, Environment.PATH.name(),
              forcePath, File.pathSeparator);
          Apps.addToEnvironment(environment, Environment.PATH.name(),
              userPath, File.pathSeparator);
        }
      }
    }

    // TODO: Remove Windows check and use this approach on all platforms after
    // additional testing.  See YARN-358.
    if (Shell.WINDOWS) {

      sanitizeWindowsEnv(environment, pwd,
          resources, nmPrivateClasspathJarDir);
    }
    // put AuxiliaryService data to environment
    for (Map.Entry<String, ByteBuffer> meta : containerManager
        .getAuxServiceMetaData().entrySet()) {
      AuxiliaryServiceHelper.setServiceDataIntoEnv(
          meta.getKey(), meta.getValue(), environment);
      nmVars.add(AuxiliaryServiceHelper.getPrefixServiceName(meta.getKey()));
    }
  }

  private void sanitizeWindowsEnv(Map<String, String> environment, Path pwd,
      Map<Path, List<String>> resources, Path nmPrivateClasspathJarDir)
      throws IOException {

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
      for (Map.Entry<Path, List<String>> entry : resources.entrySet()) {
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

  public static String getExitCodeFile(String pidFile) {
    return pidFile + EXIT_CODE_FILE_SUFFIX;
  }

  private void recordContainerLogDir(ContainerId containerId,
      String logDir) throws IOException{
    container.setLogDir(logDir);
    if (container.isRetryContextSet()) {
      context.getNMStateStore().storeContainerLogDir(containerId, logDir);
    }
  }

  private void recordContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException{
    container.setWorkDir(workDir);
    if (container.isRetryContextSet()) {
      context.getNMStateStore().storeContainerWorkDir(containerId, workDir);
    }
  }

  private void recordContainerCsiVolumesRootDir(ContainerId containerId,
      String volumesRoot) throws IOException {
    container.setCsiVolumesRootDir(volumesRoot);
    // TODO persistent to the NM store...
  }

  protected Path getContainerWorkDir() throws IOException {
    String containerWorkDir = container.getWorkDir();
    if (containerWorkDir == null
        || !dirsHandler.isGoodLocalDir(containerWorkDir)) {
      throw new IOException(
          "Could not find a good work dir " + containerWorkDir
              + " for container " + container);
    }

    return new Path(containerWorkDir);
  }

  /**
   * Clean up container's files for container relaunch or cleanup.
   */
  protected void cleanupContainerFiles(Path containerWorkDir) {
    LOG.debug("cleanup container {} files", containerWorkDir);
    // delete ContainerScriptPath
    deleteAsUser(new Path(containerWorkDir, CONTAINER_SCRIPT));
    // delete TokensPath
    deleteAsUser(new Path(containerWorkDir, FINAL_CONTAINER_TOKENS_FILE));
    // delete sysfs dir
    deleteAsUser(new Path(containerWorkDir, SYSFS_DIR));

    // delete symlinks because launch script will create symlinks again
    try {
      exec.cleanupBeforeRelaunch(container);
    } catch (IOException | InterruptedException e) {
      LOG.warn("{} exec failed to cleanup", container.getContainerId(), e);
    }
  }

  private void deleteAsUser(Path path) {
    try {
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(container.getUser())
          .setSubDir(path)
          .build());
    } catch (Exception e) {
      LOG.warn("Failed to delete " + path, e);
    }
  }

  /**
   * Returns the PID File Path.
   */
  Path getPidFilePath() {
    return pidFilePath;
  }

  /**
   * Marks the container to be launched only if it was not launched.
   *
   * @return true if successful; false otherwise.
   */
  boolean markLaunched() {
    return containerAlreadyLaunched.compareAndSet(false, true);
  }

  /**
   * Returns if the launch is completed or not.
   */
  boolean isLaunchCompleted() {
    return completed.get();
  }

}
