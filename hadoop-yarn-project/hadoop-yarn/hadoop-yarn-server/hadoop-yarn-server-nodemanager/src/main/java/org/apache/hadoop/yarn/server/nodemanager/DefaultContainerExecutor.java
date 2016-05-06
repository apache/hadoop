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

import com.google.common.base.Optional;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class DefaultContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(DefaultContainerExecutor.class);

  private static final int WIN_MAX_PATH = 260;

  protected final FileContext lfs;

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

  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    lfs.util().copy(src, dst);
  }
  
  protected void setScriptExecutable(Path script, String owner) throws IOException {
    lfs.setPermission(script, ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
  }

  @Override
  public void init() throws IOException {
    // nothing to do or verify here
  }

  @Override
  public void startLocalizer(Path nmPrivateContainerTokensPath,
      InetSocketAddress nmAddr, String user, String appId, String locId,
      LocalDirsHandlerService dirsHandler)
      throws IOException, InterruptedException {

    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();
    
    createUserLocalDirs(localDirs, user);
    createUserCacheDirs(localDirs, user);
    createAppDirs(localDirs, user, appId);
    createAppLogDirs(appId, logDirs, user);

    // randomly choose the local directory
    Path appStorageDir = getWorkingDir(localDirs, user, appId);

    String tokenFn = String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    copyFile(nmPrivateContainerTokensPath, tokenDst, user);
    LOG.info("Copying from " + nmPrivateContainerTokensPath + " to " + tokenDst);


    FileContext localizerFc = FileContext.getFileContext(
        lfs.getDefaultFileSystem(), getConf());
    localizerFc.setUMask(lfs.getUMask());
    localizerFc.setWorkingDirectory(appStorageDir);
    LOG.info("Localizer CWD set to " + appStorageDir + " = " 
        + localizerFc.getWorkingDirectory());
    ContainerLocalizer localizer =
        new ContainerLocalizer(localizerFc, user, appId, locId, 
            getPaths(localDirs), RecordFactoryProvider.getRecordFactory(getConf()));
    // TODO: DO it over RPC for maintaining similarity?
    localizer.runLocalization(nmAddr);
  }

  @Override
  public int launchContainer(Container container,
      Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
      String user, String appId, Path containerWorkDir,
      List<String> localDirs, List<String> logDirs) throws IOException {
    
    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerId();

    // create container dirs on all disks
    String containerIdStr = ConverterUtils.toString(containerId);
    String appIdStr =
        ConverterUtils.toString(
            containerId.getApplicationAttemptId().
                getApplicationId());
    for (String sLocalDir : localDirs) {
      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, user);
      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(appCacheDir, appIdStr);
      Path containerDir = new Path(appDir, containerIdStr);
      createDir(containerDir, dirPerm, true, user);
    }

    // Create the container log-dirs on all disks
    createContainerLogDirs(appIdStr, containerIdStr, logDirs, user);

    Path tmpDir = new Path(containerWorkDir,
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    createDir(tmpDir, dirPerm, false, user);


    // copy container tokens to work dir
    Path tokenDst =
      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    copyFile(nmPrivateTokensPath, tokenDst, user);

    // copy launch script to work dir
    Path launchDst =
        new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
    copyFile(nmPrivateContainerScriptPath, launchDst, user);

    // Create new local launch wrapper script
    LocalWrapperScriptBuilder sb = getLocalWrapperScriptBuilder(
        containerIdStr, containerWorkDir); 

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
    Shell.CommandExecutor shExec = null;
    try {
      setScriptExecutable(launchDst, user);
      setScriptExecutable(sb.getWrapperScriptPath(), user);

      shExec = buildCommandExecutor(sb.getWrapperScriptPath().toString(),
          containerIdStr, user, pidFile, container.getResource(),
          new File(containerWorkDir.toUri().getPath()),
          container.getLaunchContext().getEnvironment());
      
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
      if (shExec != null) shExec.close();
    }
    return 0;
  }

  protected CommandExecutor buildCommandExecutor(String wrapperScriptPath, 
      String containerIdStr, String user, Path pidFile, Resource resource,
      File wordDir, Map<String, String> environment)
          throws IOException {
    
    String[] command = getRunCommand(wrapperScriptPath,
        containerIdStr, user, pidFile, this.getConf(), resource);

      LOG.info("launchContainer: " + Arrays.toString(command));
      return new ShellCommandExecutor(
          command,
          wordDir,
          environment,
          0L,
          false);
  }

  protected LocalWrapperScriptBuilder getLocalWrapperScriptBuilder(
      String containerIdStr, Path containerWorkDir) {
   return  Shell.WINDOWS ?
       new WindowsLocalWrapperScriptBuilder(containerIdStr, containerWorkDir) :
       new UnixLocalWrapperScriptBuilder(containerWorkDir);
  }

  protected abstract class LocalWrapperScriptBuilder {

    private final Path wrapperScriptPath;

    public Path getWrapperScriptPath() {
      return wrapperScriptPath;
    }

    public void writeLocalWrapperScript(Path launchDst, Path pidFile) throws IOException {
      DataOutputStream out = null;
      PrintStream pout = null;

      try {
        out = lfs.create(wrapperScriptPath, EnumSet.of(CREATE, OVERWRITE));
        pout = new PrintStream(out, false, "UTF-8");
        writeLocalWrapperScript(launchDst, pidFile, pout);
      } finally {
        IOUtils.cleanup(LOG, pout, out);
      }
    }

    protected abstract void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout);

    protected LocalWrapperScriptBuilder(Path containerWorkDir) {
      this.wrapperScriptPath = new Path(containerWorkDir,
        Shell.appendScriptExtension("default_container_executor"));
    }
  }

  private final class UnixLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {
    private final Path sessionScriptPath;

    public UnixLocalWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
      this.sessionScriptPath = new Path(containerWorkDir,
          Shell.appendScriptExtension("default_container_executor_session"));
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile)
        throws IOException {
      writeSessionScript(launchDst, pidFile);
      super.writeLocalWrapperScript(launchDst, pidFile);
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {
      String exitCodeFile = ContainerLaunch.getExitCodeFile(
          pidFile.toString());
      String tmpFile = exitCodeFile + ".tmp";
      pout.println("#!/bin/bash");
      pout.println("/bin/bash \"" + sessionScriptPath.toString() + "\"");
      pout.println("rc=$?");
      pout.println("echo $rc > \"" + tmpFile + "\"");
      pout.println("/bin/mv -f \"" + tmpFile + "\" \"" + exitCodeFile + "\"");
      pout.println("exit $rc");
    }

    private void writeSessionScript(Path launchDst, Path pidFile)
        throws IOException {
      DataOutputStream out = null;
      PrintStream pout = null;
      try {
        out = lfs.create(sessionScriptPath, EnumSet.of(CREATE, OVERWRITE));
        pout = new PrintStream(out, false, "UTF-8");
        // We need to do a move as writing to a file is not atomic
        // Process reading a file being written to may get garbled data
        // hence write pid to tmp file first followed by a mv
        pout.println("#!/bin/bash");
        pout.println();
        pout.println("echo $$ > " + pidFile.toString() + ".tmp");
        pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
        String exec = Shell.isSetsidAvailable? "exec setsid" : "exec";
        pout.println(exec + " /bin/bash \"" +
            launchDst.toUri().getPath().toString() + "\"");
      } finally {
        IOUtils.cleanup(LOG, pout, out);
      }
      lfs.setPermission(sessionScriptPath,
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
    }
  }

  private final class WindowsLocalWrapperScriptBuilder
      extends LocalWrapperScriptBuilder {

    private final String containerIdStr;

    public WindowsLocalWrapperScriptBuilder(String containerIdStr,
        Path containerWorkDir) {

      super(containerWorkDir);
      this.containerIdStr = containerIdStr;
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
        PrintStream pout) {
      // TODO: exit code script for Windows

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
    LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
        + " as user " + user);
    if (!containerIsAlive(pid)) {
      return false;
    }
    try {
      killContainer(pid, signal);
    } catch (IOException e) {
      if (!containerIsAlive(pid)) {
        return false;
      }
      throw e;
    }
    return true;
  }

  @Override
  public boolean isContainerProcessAlive(String user, String pid)
      throws IOException {
    return containerIsAlive(pid);
  }

  /**
   * Returns true if the process with the specified pid is alive.
   * 
   * @param pid String pid
   * @return boolean true if the process is alive
   */
  @VisibleForTesting
  public static boolean containerIsAlive(String pid) throws IOException {
    try {
      new ShellCommandExecutor(Shell.getCheckProcessIsAliveCommand(pid))
        .execute();
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
  protected void killContainer(String pid, Signal signal) throws IOException {
    new ShellCommandExecutor(Shell.getSignalKillCommand(signal.getValue(), pid))
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
      try {
        if (!lfs.delete(del, true)) {
          LOG.warn("delete returned false for path: [" + del + "]");
        }
      } catch (FileNotFoundException e) {
        continue;
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

  private long getDiskFreeSpace(Path base) throws IOException {
    return lfs.getFsStatus(base).getRemaining();
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

  protected Path getWorkingDir(List<String> localDirs, String user,
      String appId) throws IOException {
    Path appStorageDir = null;
    long totalAvailable = 0L;
    long[] availableOnDisk = new long[localDirs.size()];
    int i = 0;
    // randomly choose the app directory
    // the chance of picking a directory is proportional to
    // the available space on the directory.
    // firstly calculate the sum of all available space on these directories
    for (String localDir : localDirs) {
      Path curBase = getApplicationDir(new Path(localDir),
          user, appId);
      long space = 0L;
      try {
        space = getDiskFreeSpace(curBase);
      } catch (IOException e) {
        LOG.warn("Unable to get Free Space for " + curBase.toString(), e);
      }
      availableOnDisk[i++] = space;
      totalAvailable += space;
    }

    // throw an IOException if totalAvailable is 0.
    if (totalAvailable <= 0L) {
      throw new IOException("Not able to find a working directory for "
          + user);
    }

    // make probability to pick a directory proportional to
    // the available space on the directory.
    long randomPosition = RandomUtils.nextLong() % totalAvailable;
    int dir = 0;
    // skip zero available space directory,
    // because totalAvailable is greater than 0 and randomPosition
    // is less than totalAvailable, we can find a valid directory
    // with nonzero available space.
    while (availableOnDisk[dir] == 0L) {
      dir++;
    }
    while (randomPosition > availableOnDisk[dir]) {
      randomPosition -= availableOnDisk[dir++];
    }
    appStorageDir = getApplicationDir(new Path(localDirs.get(dir)),
        user, appId);

    return appStorageDir;
  }

  protected void createDir(Path dirPath, FsPermission perms,
      boolean createParent, String user) throws IOException {
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
        createDir(getUserCacheDir(new Path(localDir), user), userperms, true, user);
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
        createDir(appDir, appCachePerms, true, user);
        appcacheDirStatus = true;
      } catch (IOException e) {
        LOG.warn("Unable to create app cache directory : " + appDir, e);
      }
      // create $local.dir/usercache/$user/filecache
      final Path distDir = getFileCacheDir(localDirPath, user);
      try {
        createDir(distDir, fileperms, true, user);
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
        createDir(fullAppDir, appperms, true, user);
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
  void createAppLogDirs(String appId, List<String> logDirs, String user)
      throws IOException {

    boolean appLogDirStatus = false;
    FsPermission appLogDirPerms = new FsPermission(LOGDIR_PERM);
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid
      Path appLogDir = new Path(rootLogDir, appId);
      try {
        createDir(appLogDir, appLogDirPerms, true, user);
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
      List<String> logDirs, String user) throws IOException {

    boolean containerLogDirStatus = false;
    FsPermission containerLogDirPerms = new FsPermission(LOGDIR_PERM);
    for (String rootLogDir : logDirs) {
      // create $log.dir/$appid/$containerid
      Path appLogDir = new Path(rootLogDir, appId);
      Path containerLogDir = new Path(appLogDir, containerId);
      try {
        createDir(containerLogDir, containerLogDirPerms, true, user);
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
