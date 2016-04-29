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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * This executor will launch and run tasks inside Docker containers. It
 * currently only supports simple authentication mode. It shares a lot of code
 * with the DefaultContainerExecutor (and it may make sense to pull out those
 * common pieces later).
 */
public class DockerContainerExecutor extends ContainerExecutor {
  private static final Log LOG = LogFactory
    .getLog(DockerContainerExecutor.class);
  //The name of the script file that will launch the Docker containers
  public static final String DOCKER_CONTAINER_EXECUTOR_SCRIPT =
    "docker_container_executor";
  //The name of the session script that the DOCKER_CONTAINER_EXECUTOR_SCRIPT
  //launches in turn
  public static final String DOCKER_CONTAINER_EXECUTOR_SESSION_SCRIPT =
    "docker_container_executor_session";

  //This validates that the image is a proper docker image and would not crash
  //docker. The image name is not allowed to contain spaces. e.g.
  //registry.somecompany.com:9999/containername:0.1 or
  //containername:0.1 or
  //containername
  public static final String DOCKER_IMAGE_PATTERN =
    "^(([\\w\\.-]+)(:\\d+)*\\/)?[\\w\\.:-]+$";

  private final FileContext lfs;
  private final Pattern dockerImagePattern;

  public DockerContainerExecutor() {
    try {
      this.lfs = FileContext.getLocalFSFileContext();
      this.dockerImagePattern = Pattern.compile(DOCKER_IMAGE_PATTERN);
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    lfs.util().copy(src, dst);
  }

  @Override
  public void init() throws IOException {
    String auth =
      getConf().get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
    if (auth != null && !auth.equals("simple")) {
      throw new IllegalStateException(
        "DockerContainerExecutor only works with simple authentication mode");
    }
    String dockerExecutor = getConf().get(
      YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME,
      YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME);
    if (!new File(dockerExecutor).exists()) {
      throw new IllegalStateException(
        "Invalid docker exec path: " + dockerExecutor);
    }
  }

  @Override
  public synchronized void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException {
    Path nmPrivateContainerTokensPath = ctx.getNmPrivateContainerTokens();
    InetSocketAddress nmAddr = ctx.getNmAddr();
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String locId = ctx.getLocId();
    LocalDirsHandlerService dirsHandler = ctx.getDirsHandler();
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();

    ContainerLocalizer localizer =
      new ContainerLocalizer(lfs, user, appId, locId, getPaths(localDirs),
        RecordFactoryProvider.getRecordFactory(getConf()));

    createUserLocalDirs(localDirs, user);
    createUserCacheDirs(localDirs, user);
    createAppDirs(localDirs, user, appId);
    createAppLogDirs(appId, logDirs, user);

    // randomly choose the local directory
    Path appStorageDir = getWorkingDir(localDirs, user, appId);

    String tokenFn =
      String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
    Path tokenDst = new Path(appStorageDir, tokenFn);
    copyFile(nmPrivateContainerTokensPath, tokenDst, user);
    LOG.info("Copying from " + nmPrivateContainerTokensPath + " to " + tokenDst);
    lfs.setWorkingDirectory(appStorageDir);
    LOG.info("CWD set to " + appStorageDir + " = " + lfs.getWorkingDirectory());
    // TODO: DO it over RPC for maintaining similarity?
    localizer.runLocalization(nmAddr);
  }


  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException {
    Container container = ctx.getContainer();
    Path nmPrivateContainerScriptPath = ctx.getNmPrivateContainerScriptPath();
    Path nmPrivateTokensPath = ctx.getNmPrivateTokensPath();
    String userName = ctx.getUser();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    //Variables for the launch environment can be injected from the command-line
    //while submitting the application
    String containerImageName = container.getLaunchContext().getEnvironment()
      .get(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    if (LOG.isDebugEnabled()) {
      LOG.debug("containerImageName from launchContext: " + containerImageName);
    }
    Preconditions.checkArgument(!Strings.isNullOrEmpty(containerImageName),
      "Container image must not be null");
    containerImageName = containerImageName.replaceAll("['\"]", "");

    Preconditions.checkArgument(saneDockerImage(containerImageName), "Image: "
      + containerImageName + " is not a proper docker image");
    String dockerExecutor = getConf().get(
      YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME,
      YarnConfiguration.NM_DEFAULT_DOCKER_CONTAINER_EXECUTOR_EXEC_NAME);

    FsPermission dirPerm = new FsPermission(APPDIR_PERM);
    ContainerId containerId = container.getContainerId();

    // create container dirs on all disks
    String containerIdStr = ConverterUtils.toString(containerId);
    String appIdStr = ConverterUtils.toString(
      containerId.getApplicationAttemptId().getApplicationId());
    for (String sLocalDir : localDirs) {
      Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(appCacheDir, appIdStr);
      Path containerDir = new Path(appDir, containerIdStr);
      createDir(containerDir, dirPerm, true, userName);
    }

    // Create the container log-dirs on all disks
    createContainerLogDirs(appIdStr, containerIdStr, logDirs, userName);

    Path tmpDir = new Path(containerWorkDir,
      YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    createDir(tmpDir, dirPerm, false, userName);

    // copy launch script to work dir
    Path launchDst =
      new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
    lfs.util().copy(nmPrivateContainerScriptPath, launchDst);

    // copy container tokens to work dir
    Path tokenDst =
      new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
    lfs.util().copy(nmPrivateTokensPath, tokenDst);

    String localDirMount = toMount(localDirs);
    String logDirMount = toMount(logDirs);
    String containerWorkDirMount = toMount(Collections.singletonList(
      containerWorkDir.toUri().getPath()));
    StringBuilder commands = new StringBuilder();
    //Use docker run to launch the docker container. See man pages for
    //docker-run
    //--rm removes the container automatically once the container finishes
    //--net=host allows the container to take on the host's network stack
    //--name sets the Docker Container name to the YARN containerId string
    //-v is used to bind mount volumes for local, log and work dirs.
    String commandStr = commands.append(dockerExecutor)
      .append(" ")
      .append("run")
      .append(" ")
      .append("--rm --net=host")
      .append(" ")
      .append(" --name " + containerIdStr)
      .append(localDirMount)
      .append(logDirMount)
      .append(containerWorkDirMount)
      .append(" ")
      .append(containerImageName)
      .toString();
    //Get the pid of the process which has been launched as a docker container
    //using docker inspect
    String dockerPidScript = "`" + dockerExecutor +
      " inspect --format {{.State.Pid}} " + containerIdStr + "`";

    // Create new local launch wrapper script
    LocalWrapperScriptBuilder sb = new UnixLocalWrapperScriptBuilder(
      containerWorkDir, commandStr, dockerPidScript);
    Path pidFile = getPidFilePath(containerId);
    if (pidFile != null) {
      sb.writeLocalWrapperScript(launchDst, pidFile);
    } else {
      //Although the container was activated by ContainerLaunch before exec()
      //was called, since then deactivateContainer() has been called.
      LOG.info("Container " + containerIdStr
          + " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }
    
    ShellCommandExecutor shExec = null;
    try {
      lfs.setPermission(launchDst,
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
      lfs.setPermission(sb.getWrapperScriptPath(),
          ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);

      // Setup command to run
      String[] command = getRunCommand(sb.getWrapperScriptPath().toString(),
        containerIdStr, userName, pidFile, this.getConf());
      if (LOG.isDebugEnabled()) {
        LOG.debug("launchContainer: " + commandStr + " " +
          Joiner.on(" ").join(command));
      }
      shExec = new ShellCommandExecutor(
        command,
        new File(containerWorkDir.toUri().getPath()),
        container.getLaunchContext().getEnvironment(),      // sanitized env
        0L,
        false);
      if (isContainerActive(containerId)) {
        shExec.execute();
      } else {
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
            + containerId + " and exit code: " + exitCode, e);
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
      if (shExec != null) {
        shExec.close();
      }
    }
    return 0;
  }

  @Override
  /**
   * Filter the environment variables that may conflict with the ones set in
   * the docker image and write them out to an OutputStream.
   */
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
    Map<Path, List<String>> resources, List<String> command, Path logDir)
    throws IOException {
    ContainerLaunch.ShellScriptBuilder sb =
      ContainerLaunch.ShellScriptBuilder.create();

    //Remove environments that may conflict with the ones in Docker image.
    Set<String> exclusionSet = new HashSet<String>();
    exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());

    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        if (!exclusionSet.contains(env.getKey())) {
          sb.env(env.getKey().toString(), env.getValue().toString());
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
      sb.copyDebugInformation(new Path(ContainerLaunch.CONTAINER_SCRIPT),
          new Path(logDir, ContainerLaunch.CONTAINER_SCRIPT));
      sb.listDebugInformation(new Path(logDir, DIRECTORY_CONTENTS));
    }

    sb.command(command);

    PrintStream pout = null;
    PrintStream ps = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      pout = new PrintStream(out, false, "UTF-8");
      if (LOG.isDebugEnabled()) {
        ps = new PrintStream(baos, false, "UTF-8");
        sb.write(ps);
      }
      sb.write(pout);

    } finally {
      if (out != null) {
        out.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Script: " + baos.toString("UTF-8"));
    }
  }

  private boolean saneDockerImage(String containerImageName) {
    return dockerImagePattern.matcher(containerImageName).matches();
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx)
    throws IOException {
    String user = ctx.getUser();
    String pid = ctx.getPid();
    Signal signal = ctx.getSignal();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending signal " + signal.getValue() + " to pid " + pid
        + " as user " + user);
    }
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
  public boolean isContainerAlive(ContainerLivenessContext ctx)
    throws IOException {
    String pid = ctx.getPid();

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
    catch (Shell.ExitCodeException e) {
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
  public void deleteAsUser(DeletionAsUserContext ctx)
    throws IOException, InterruptedException {
    Path subDir = ctx.getSubDir();
    List<Path> baseDirs = ctx.getBasedirs();

    if (baseDirs == null || baseDirs.size() == 0) {
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

  /**
   * Converts a directory list to a docker mount string
   * @param dirs
   * @return a string of mounts for docker
   */
  private String toMount(List<String> dirs) {
    StringBuilder builder = new StringBuilder();
    for (String dir : dirs) {
      builder.append(" -v " + dir + ":" + dir);
    }
    return builder.toString();
  }

  //This class facilitates (only) the creation of platform-specific scripts that
  //will be used to launch the containers
  //TODO: This should be re-used from the DefaultContainerExecutor.
  private abstract class LocalWrapperScriptBuilder {

    private final Path wrapperScriptPath;

    public Path getWrapperScriptPath() {
      return wrapperScriptPath;
    }

    public void writeLocalWrapperScript(Path launchDst, Path pidFile)
      throws IOException {
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

    protected abstract void writeLocalWrapperScript(Path launchDst,
      Path pidFile, PrintStream pout);

    protected LocalWrapperScriptBuilder(Path containerWorkDir) {
      this.wrapperScriptPath = new Path(containerWorkDir,
          Shell.appendScriptExtension(DOCKER_CONTAINER_EXECUTOR_SCRIPT));
    }
  }

  //TODO: This class too should be used from DefaultContainerExecutor.
  private final class UnixLocalWrapperScriptBuilder
    extends LocalWrapperScriptBuilder {
    private final Path sessionScriptPath;
    private final String dockerCommand;
    private final String dockerPidScript;

    public UnixLocalWrapperScriptBuilder(Path containerWorkDir,
      String dockerCommand, String dockerPidScript) {
      super(containerWorkDir);
      this.dockerCommand = dockerCommand;
      this.dockerPidScript = dockerPidScript;
      this.sessionScriptPath = new Path(containerWorkDir,
        Shell.appendScriptExtension(DOCKER_CONTAINER_EXECUTOR_SESSION_SCRIPT));
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
      pout.println("#!/usr/bin/env bash");
      pout.println("bash \"" + sessionScriptPath.toString() + "\"");
      pout.println("rc=$?");
      pout.println("echo $rc > \"" + tmpFile + "\"");
      pout.println("mv -f \"" + tmpFile + "\" \"" + exitCodeFile + "\"");
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
        pout.println("#!/usr/bin/env bash");
        pout.println();
        pout.println("echo "+ dockerPidScript +" > " + pidFile.toString()
          + ".tmp");
        pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
        pout.println(dockerCommand + " bash \"" +
          launchDst.toUri().getPath().toString() + "\"");
      } finally {
        IOUtils.cleanup(LOG, pout, out);
      }
      lfs.setPermission(sessionScriptPath,
        ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
    }
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
        createDir(getUserCacheDir(new Path(localDir), user), userperms, true,
          user);
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
   * Permissions for user dir.
   * $local.dir/usercache/$user
   */
  static final short USER_PERM = (short) 0750;
  /**
   * Permissions for user appcache dir.
   * $local.dir/usercache/$user/appcache
   */
  static final short APPCACHE_PERM = (short) 0710;
  /**
   * Permissions for user filecache dir.
   * $local.dir/usercache/$user/filecache
   */
  static final short FILECACHE_PERM = (short) 0710;
  /**
   * Permissions for user app dir.
   * $local.dir/usercache/$user/appcache/$appId
   */
  static final short APPDIR_PERM = (short) 0710;
  /**
   * Permissions for user log dir.
   * $logdir/$user/$appId
   */
  static final short LOGDIR_PERM = (short) 0710;

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