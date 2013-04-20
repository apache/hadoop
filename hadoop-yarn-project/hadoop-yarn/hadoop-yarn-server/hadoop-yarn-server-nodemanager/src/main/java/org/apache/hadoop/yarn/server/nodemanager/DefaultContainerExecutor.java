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

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class DefaultContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(DefaultContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  private final FileContext lfs;

  public DefaultContainerExecutor() {
    try {
      this.lfs = FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  DefaultContainerExecutor(FileContext lfs) {
    this.lfs = lfs;
  }

  @Override
  public void init() throws IOException {
    // nothing to do or verify here
  }
  
  @Override
  public synchronized void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      List<String> localDirs, List<String> logDirs)
      throws IOException, InterruptedException {

    ContainerLocalizer localizer =
        new ContainerLocalizer(lfs, user, appId, locId, getPaths(localDirs),
            RecordFactoryProvider.getRecordFactory(getConf()));

    createUserLocalDirs(localDirs, user);
    createUserCacheDirs(localDirs, user);
    createAppDirs(localDirs, user, appId);
    createAppLogDirs(appId, logDirs);

    // TODO: Why pick first app dir. The same in LCE why not random?
    Path appStorageDir = getFirstApplicationDir(localDirs, user, appId);

    String tokenFn = String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    lfs.util().copy(nmPrivateContainerTokensPath, tokenDst);
    LOG.info("Copying from " + nmPrivateContainerTokensPath + " to " + tokenDst);
    lfs.setWorkingDirectory(appStorageDir);
    LOG.info("CWD set to " + appStorageDir + " = " + lfs.getWorkingDirectory());
    // TODO: DO it over RPC for maintaining similarity?
    localizer.runLocalization(nmAddr);
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String userName, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {

    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerID();

    // create container dirs on all disks
    String containerIdStr = ConverterUtils.toString(containerId);
    String appIdStr =
        ConverterUtils.toString(
            container.getContainerID().getApplicationAttemptId().
                getApplicationId());
    for (String sLocalDir : localDirs) {
      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(appCacheDir, appIdStr);
      Path containerDir = new Path(appDir, containerIdStr);
      createDir(containerDir, dirPerm, true);
    }

    // Create the container log-dirs on all disks
    createContainerLogDirs(appIdStr, containerIdStr, logDirs);

    Path tmpDir = new Path(containerWorkDir,
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    createDir(tmpDir, dirPerm, false);

    // copy launch script to work dir
    Path launchDst =
        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
    lfs.util().copy(nmPrivateContainerScriptPath, launchDst);

    // copy container tokens to work dir
    Path tokenDst =
      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    lfs.util().copy(nmPrivateTokensPath, tokenDst);

    // Create new local launch wrapper script
    LocalWrapperScriptBuilder sb = Shell.WINDOWS ?
      new WindowsLocalWrapperScriptBuilder(containerIdStr, containerWorkDir) :
      new UnixLocalWrapperScriptBuilder(containerWorkDir);

    // Fail fast if attempting to launch the wrapper script would fail due to
    // Windows path length limitation.
    if (Shell.WINDOWS &&
        sb.getWrapperScriptPath().toString().length() > WIN_MAX_PATH) {
      throw new IOException(String.format(
        "Cannot launch container using script at path %s, because it exceeds " +
        "the maximum supported path length of %d characters.  Consider " +
        "configuring shorter directories in %s.", sb.getWrapperScriptPath(),
        WIN_MAX_PATH, YarnConfiguration.NM_LOCAL_DIRS));
    }

    Path pidFile = getPidFilePath(containerId);
    if (pidFile != null) {
      sb.writeLocalWrapperScript(launchDst, pidFile);
    } else {
      LOG.info("Container " + containerIdStr
          + " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }

    // create log dir under app
    // fork script
    ShellCommandExecutor shExec = null;
    try {
      lfs.setPermission(launchDst,
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
      lfs.setPermission(sb.getWrapperScriptPath(),
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);

      // Setup command to run
      String[] command = getRunCommand(sb.getWrapperScriptPath().toString(),
        containerIdStr, this.getConf());

      LOG.info("launchContainer: " + Arrays.toString(command));
      shExec = new ShellCommandExecutor(
          command,
          new File(containerWorkDir.toUri().getPath()),
          container.getLaunchContext().getEnvironment());      // sanitized env
      if (isContainerActive(containerId)) {
        shExec.execute();
      }
      else {
        LOG.info("Container " + containerIdStr +
            " was marked as inactive. Returning terminated error");
        return ExitCode.TERMINATED.getExitCode();
      }
    } catch (IOException e) {
      if (null == shExec) {
        return -1;
      }
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from task is : " + exitCode);
      String message = shExec.getOutput();
      logOutput(message);
      container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
          message));
      return exitCode;
    } finally {
      ; //
    }
    return 0;
  }

  private abstract class LocalWrapperScriptBuilder {

    private final Path wrapperScriptPath;

    public Path getWrapperScriptPath() {
      return wrapperScriptPath;
    }

    public void writeLocalWrapperScript(Path launchDst, Path pidFile) throws IOException {
      DataOutputStream out = null;
      PrintStream pout = null;

      try {
        out = lfs.create(wrapperScriptPath, EnumSet.of(CREATE, OVERWRITE));
        pout = new PrintStream(out);
        writeLocalWrapperScript(launchDst, pidFile, pout);
      } finally {
        IOUtils.cleanup(LOG, pout, out);
      }
    }

    protected abstract void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout);

    protected LocalWrapperScriptBuilder(Path wrapperScriptPath) {
      this.wrapperScriptPath = wrapperScriptPath;
    }
  }

  private final class UnixLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {

    public UnixLocalWrapperScriptBuilder(Path containerWorkDir) {
      super(new Path(containerWorkDir, "default_container_executor.sh"));
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {

      // We need to do a move as writing to a file is not atomic
      // Process reading a file being written to may get garbled data
      // hence write pid to tmp file first followed by a mv
      pout.println("#!/bin/bash");
      pout.println();
      pout.println("echo $$ > " + pidFile.toString() + ".tmp");
      pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
      String exec = ContainerExecutor.isSetsidAvailable? "exec setsid" : "exec";
      pout.println(exec + " /bin/bash -c \"" +
        launchDst.toUri().getPath().toString() + "\"");
    }
  }

  private final class WindowsLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {

    private final String containerIdStr;

    public WindowsLocalWrapperScriptBuilder(String containerIdStr,
        Path containerWorkDir) {

      super(new Path(containerWorkDir, "default_container_executor.cmd"));
      this.containerIdStr = containerIdStr;
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {

      // On Windows, the pid is the container ID, so that it can also serve as
      // the name of the job object created by winutils for task management.
      // Write to temp file followed by atomic move.
      String normalizedPidFile = new File(pidFile.toString()).getPath();
      pout.println("@echo " + containerIdStr + " > " + normalizedPidFile +
        ".tmp");
      pout.println("@move /Y " + normalizedPidFile + ".tmp " +
        normalizedPidFile);
      pout.println("@call " + launchDst.toString());
    }
  }

  @Override
  public boolean signalContainer(String user, String pid, Signal signal)
      throws IOException {
    final String sigpid = ContainerExecutor.isSetsidAvailable
        ? "-" + pid
        : pid;
    LOG.debug("Sending signal " + signal.getValue() + " to pid " + sigpid
        + " as user " + user);
    if (!containerIsAlive(sigpid)) {
      return false;
    }
    try {
      killContainer(sigpid, signal);
    } catch (IOException e) {
      if (!containerIsAlive(sigpid)) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Returns true if the process with the specified pid is alive.
   * 
   * @param pid String pid
   * @return boolean true if the process is alive
   */
  private boolean containerIsAlive(String pid) throws IOException {
    try {
      new ShellCommandExecutor(getCheckProcessIsAliveCommand(pid)).execute();
      // successful execution means process is alive
      return true;
    }
    catch (ExitCodeException e) {
      // failure (non-zero exit code) means process is not alive
      return false;
    }
  }

  /**
   * Send a specified signal to the specified pid
   *
   * @param pid the pid of the process [group] to signal.
   * @param signal signal to send
   * (for logging).
   */
  private void killContainer(String pid, Signal signal) throws IOException {
    new ShellCommandExecutor(getSignalKillCommand(signal.getValue(), pid))
      .execute();
  }

  @Override
  public void deleteAsUser(String user, Path subDir, Path... baseDirs)
      throws IOException, InterruptedException {
    if (baseDirs == null || baseDirs.length == 0) {
      LOG.info("Deleting absolute path : " + subDir);
      if (!lfs.delete(subDir, true)) {
        //Maybe retry
        LOG.warn("delete returned false for path: [" + subDir + "]");
      }
      return;
    }
    for (Path baseDir : baseDirs) {
      Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
      LOG.info("Deleting path : " + del);
      if (!lfs.delete(del, true)) {
        LOG.warn("delete returned false for path: [" + del + "]");
      }
    }
  }

  /** Permissions for user dir.
   * $local.dir/usercache/$user */
  static final short USER_PERM = (short)0750;
  /** Permissions for user appcache dir.
   * $local.dir/usercache/$user/appcache */
  static final short APPCACHE_PERM = (short)0710;
  /** Permissions for user filecache dir.
   * $local.dir/usercache/$user/filecache */
  static final short FILECACHE_PERM = (short)0710;
  /** Permissions for user app dir.
   * $local.dir/usercache/$user/appcache/$appId */
  static final short APPDIR_PERM = (short)0710;
  /** Permissions for user log dir.
   * $logdir/$user/$appId */
  static final short LOGDIR_PERM = (short)0710;

  private Path getFirstApplicationDir(List<String> localDirs, String user,
      String appId) {
    return getApplicationDir(new Path(localDirs.get(0)), user, appId);
  }

  private Path getApplicationDir(Path base, String user, String appId) {
    return new Path(getAppcacheDir(base, user), appId);
  }

  private Path getUserCacheDir(Path base, String user) {
    return new Path(new Path(base, ContainerLocalizer.USERCACHE), user);
  }

  private Path getAppcacheDir(Path base, String user) {
    return new Path(getUserCacheDir(base, user),
        ContainerLocalizer.APPCACHE);
  }

  private Path getFileCacheDir(Path base, String user) {
    return new Path(getUserCacheDir(base, user),
        ContainerLocalizer.FILECACHE);
  }

  private void createDir(Path dirPath, FsPermission perms,
      boolean createParent) throws IOException {
    lfs.mkdir(dirPath, perms, createParent);
    if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
      lfs.setPermission(dirPath, perms);
    }
  }

  /**
   * Initialize the local directories for a particular user.
   * <ul>.mkdir
   * <li>$local.dir/usercache/$user</li>
   * </ul>
   */
  void createUserLocalDirs(List<String> localDirs, String user)
      throws IOException {
    boolean userDirStatus = false;
    FsPermission userperms = new FsPermission(USER_PERM);
    for (String localDir : localDirs) {
      // create $local.dir/usercache/$user and its immediate parent
      try {
        createDir(getUserCacheDir(new Path(localDir), user), userperms, true);
      } catch (IOException e) {
        LOG.warn("Unable to create the user directory : " + localDir, e);
        continue;
      }
      userDirStatus = true;
    }
    if (!userDirStatus) {
      throw new IOException("Not able to initialize user directories "
          + "in any of the configured local directories for user " + user);
    }
  }


  /**
   * Initialize the local cache directories for a particular user.
   * <ul>
   * <li>$local.dir/usercache/$user</li>
   * <li>$local.dir/usercache/$user/appcache</li>
   * <li>$local.dir/usercache/$user/filecache</li>
   * </ul>
   */
  void createUserCacheDirs(List<String> localDirs, String user)
      throws IOException {
    LOG.info("Initializing user " + user);

    boolean appcacheDirStatus = false;
    boolean distributedCacheDirStatus = false;
    FsPermission appCachePerms = new FsPermission(APPCACHE_PERM);
    FsPermission fileperms = new FsPermission(FILECACHE_PERM);

    for (String localDir : localDirs) {
      // create $local.dir/usercache/$user/appcache
      Path localDirPath = new Path(localDir);
      final Path appDir = getAppcacheDir(localDirPath, user);
      try {
        createDir(appDir, appCachePerms, true);
        appcacheDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create app cache directory : " + appDir, e);
      }
      // create $local.dir/usercache/$user/filecache
      final Path distDir = getFileCacheDir(localDirPath, user);
      try {
        createDir(distDir, fileperms, true);
        distributedCacheDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create file cache directory : " + distDir, e);
      }
    }
    if (!appcacheDirStatus) {
      throw new IOException("Not able to initialize app-cache directories "
          + "in any of the configured local directories for user " + user);
    }
    if (!distributedCacheDirStatus) {
      throw new IOException(
          "Not able to initialize distributed-cache directories "
              + "in any of the configured local directories for user "
              + user);
    }
  }

  /**
   * Initialize the local directories for a particular user.
   * <ul>
   * <li>$local.dir/usercache/$user/appcache/$appid</li>
   * </ul>
   * @param localDirs 
   */
  void createAppDirs(List<String> localDirs, String user, String appId)
      throws IOException {
    boolean initAppDirStatus = false;
    FsPermission appperms = new FsPermission(APPDIR_PERM);
    for (String localDir : localDirs) {
      Path fullAppDir = getApplicationDir(new Path(localDir), user, appId);
      // create $local.dir/usercache/$user/appcache/$appId
      try {
        createDir(fullAppDir, appperms, true);
        initAppDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create app directory " + fullAppDir.toString(), e);
      }
    }
    if (!initAppDirStatus) {
      throw new IOException("Not able to initialize app directories "
          + "in any of the configured local directories for app "
          + appId.toString());
    }
  }

  /**
   * Create application log directories on all disks.
   */
  void createAppLogDirs(String appId, List<String> logDirs)
      throws IOException {

    boolean appLogDirStatus = false;
    FsPermission appLogDirPerms = new FsPermission(LOGDIR_PERM);
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid
      Path appLogDir = new Path(rootLogDir, appId);
      try {
        createDir(appLogDir, appLogDirPerms, true);
      } catch (IOException e) {
        LOG.warn("Unable to create the app-log directory : " + appLogDir, e);
        continue;
      }
      appLogDirStatus = true;
    }
    if (!appLogDirStatus) {
      throw new IOException("Not able to initialize app-log directories "
          + "in any of the configured local directories for app " + appId);
    }
  }

  /**
   * Create application log directories on all disks.
   */
  void createContainerLogDirs(String appId, String containerId,
      List<String> logDirs) throws IOException {

    boolean containerLogDirStatus = false;
    FsPermission containerLogDirPerms = new FsPermission(LOGDIR_PERM);
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid/$containerid
      Path appLogDir = new Path(rootLogDir, appId);
      Path containerLogDir = new Path(appLogDir, containerId);
      try {
        createDir(containerLogDir, containerLogDirPerms, true);
      } catch (IOException e) {
        LOG.warn("Unable to create the container-log directory : "
            + appLogDir, e);
        continue;
      }
      containerLogDirStatus = true;
    }
    if (!containerLogDirStatus) {
      throw new IOException(
          "Not able to initialize container-log directories "
              + "in any of the configured local directories for container "
              + containerId);
    }
  }

  /**
   * @return the list of paths of given local directories
   */
  private static List<Path> getPaths(List<String> dirs) {
    List<Path> paths = new ArrayList<Path>(dirs.size());
    for (int i = 0; i < dirs.size(); i++) {
      paths.add(new Path(dirs.get(i)));
    }
    return paths;
  }
}
