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
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.fs.FileContext;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.DelayedProcessKiller;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
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

  private final Dispatcher dispatcher;
  private final ContainerExecutor exec;
  private final Application app;
  private final Container container;
  private final Configuration conf;
  private final Context context;
  private final ContainerManagerImpl containerManager;
  
  private volatile AtomicBoolean shouldLaunchContainer = new AtomicBoolean(false);
  private volatile AtomicBoolean completed = new AtomicBoolean(false);

  private long sleepDelayBeforeSigKill = 250;
  private long maxKillWaitTime = 2000;

  private Path pidFilePath = null;

  private final LocalDirsHandlerService dirsHandler;

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
    final ContainerLaunchContext launchContext = container.getLaunchContext();
    Map<Path,List<String>> localResources = null;
    ContainerId containerID = container.getContainerId();
    String containerIdStr = ConverterUtils.toString(containerID);
    final List<String> command = launchContext.getCommands();
    int ret = -1;

    // CONTAINER_KILLED_ON_REQUEST should not be missed if the container
    // is already at KILLING
    if (container.getContainerState() == ContainerState.KILLING) {
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(containerID,
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
              Shell.WINDOWS ? ExitCode.FORCE_KILLED.getExitCode() :
                  ExitCode.TERMINATED.getExitCode(),
              "Container terminated before launch."));
      return 0;
    }

    try {
      localResources = container.getLocalizedResources();
      if (localResources == null) {
        throw RPCUtil.getRemoteException(
            "Unable to get local resources when Container " + containerID +
            " is at " + container.getContainerState());
      }

      final String user = container.getUser();
      // /////////////////////////// Variable expansion
      // Before the container script gets written out.
      List<String> newCmds = new ArrayList<String>(command.size());
      String appIdStr = app.getAppId().toString();
      String relativeContainerLogDir = ContainerLaunch
          .getRelativeContainerLogDir(appIdStr, containerIdStr);
      Path containerLogDir =
          dirsHandler.getLogPathForWrite(relativeContainerLogDir, false);
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

      DataOutputStream containerScriptOutStream = null;
      DataOutputStream tokensOutStream = null;

      // Select the working directory for the container
      Path containerWorkDir =
          dirsHandler.getLocalPathForWrite(ContainerLocalizer.USERCACHE
              + Path.SEPARATOR + user + Path.SEPARATOR
              + ContainerLocalizer.APPCACHE + Path.SEPARATOR + appIdStr
              + Path.SEPARATOR + containerIdStr,
              LocalDirAllocator.SIZE_UNKNOWN, false);

      String pidFileSuffix = String.format(ContainerLaunch.PID_FILE_NAME_FMT,
          containerIdStr);

      // pid file should be in nm private dir so that it is not 
      // accessible by users
      pidFilePath = dirsHandler.getLocalPathForWrite(
          ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR 
          + pidFileSuffix);
      List<String> localDirs = dirsHandler.getLocalDirs();
      List<String> logDirs = dirsHandler.getLogDirs();

      List<String> containerLogDirs = new ArrayList<String>();
      for( String logDir : logDirs) {
        containerLogDirs.add(logDir + Path.SEPARATOR + relativeContainerLogDir);
      }

      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport());
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
          localResources);
        
        // Write out the environment
        writeLaunchEnv(containerScriptOutStream, environment, localResources,
            launchContext.getCommands());
        
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

      // LaunchContainer is a blocking call. We are here almost means the
      // container is launched, so send out the event.
      dispatcher.getEventHandler().handle(new ContainerEvent(
            containerID,
            ContainerEventType.CONTAINER_LAUNCHED));

      // Check if the container is signalled to be killed.
      if (!shouldLaunchContainer.compareAndSet(false, true)) {
        LOG.info("Container " + containerIdStr + " not launched as "
            + "cleanup already called");
        ret = ExitCode.TERMINATED.getExitCode();
      }
      else {
        exec.activateContainer(containerID, pidFilePath);
        ret = exec.launchContainer(container, nmPrivateContainerScriptPath,
                nmPrivateTokensPath, user, appIdStr, containerWorkDir,
                localDirs, logDirs);
      }
    } catch (Throwable e) {
      LOG.warn("Failed to launch container.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      return ret;
    } finally {
      completed.set(true);
      exec.deactivateContainer(containerID);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Container " + containerIdStr + " completed with exit code "
                + ret);
    }
    if (ret == ExitCode.FORCE_KILLED.getExitCode()
        || ret == ExitCode.TERMINATED.getExitCode()) {
      // If the process was killed, Send container_cleanedup_after_kill and
      // just break out of this method.
      dispatcher.getEventHandler().handle(
            new ContainerExitEvent(containerID,
                ContainerEventType.CONTAINER_KILLED_ON_REQUEST, ret,
                "Container exited with a non-zero exit code " + ret));
      return ret;
    }

    if (ret != 0) {
      LOG.warn("Container exited with a non-zero exit code " + ret);
      this.dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID,
          ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          "Container exited with a non-zero exit code " + ret));
      return ret;
    }

    LOG.info("Container " + containerIdStr + " succeeded ");
    dispatcher.getEventHandler().handle(
        new ContainerEvent(containerID,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    return 0;
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
    String containerIdStr = ConverterUtils.toString(containerId);
    LOG.info("Cleaning up container " + containerIdStr);

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

        boolean result = exec.signalContainer(user, processId, signal);

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
      }
    }
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
        ConverterUtils.toString(container.getContainerId());
    String processId = null;
    LOG.debug("Accessing pid for container " + containerIdStr
        + " from pid file " + pidFilePath);
    int sleepCounter = 0;
    final int sleepInterval = 100;

    // loop waiting for pid file to show up 
    // until either the completed flag is set which means something bad 
    // happened or our timer expires in which case we admit defeat
    while (!completed.get()) {
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

  private String getContainerPrivateDir(String appIdStr, String containerIdStr) {
    return getAppPrivateDir(appIdStr) + Path.SEPARATOR + containerIdStr
        + Path.SEPARATOR;
  }

  private String getAppPrivateDir(String appIdStr) {
    return ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR
        + appIdStr;
  }

  @VisibleForTesting
  static abstract class ShellScriptBuilder {
    public static ShellScriptBuilder create() {
      return Shell.WINDOWS ? new WindowsShellScriptBuilder() :
        new UnixShellScriptBuilder();
    }

    private static final String LINE_SEPARATOR =
        System.getProperty("line.separator");
    private final StringBuilder sb = new StringBuilder();

    public abstract void command(List<String> command) throws IOException;

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
    public void env(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      File srcFile = new File(src.toUri().getPath());
      String srcFileStr = srcFile.getPath();
      String dstFileStr = new File(dst.toString()).getPath();
      // If not on Java7+ on Windows, then copy file instead of symlinking.
      // See also FileUtil#symLink for full explanation.
      if (!Shell.isJava7OrAbove() && srcFile.isFile()) {
        lineWithLenCheck(String.format("@copy \"%s\" \"%s\"", srcFileStr, dstFileStr));
        errorCheck();
      } else {
        lineWithLenCheck(String.format("@%s symlink \"%s\" \"%s\"", Shell.WINUTILS,
          dstFileStr, srcFileStr));
        errorCheck();
      }
    }

    @Override
    protected void mkdir(Path path) throws IOException {
      lineWithLenCheck(String.format("@if not exist \"%s\" mkdir \"%s\"",
          path.toString(), path.toString()));
      errorCheck();
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
      Map<Path, List<String>> resources) throws IOException {
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
        StringBuilder newClassPath = new StringBuilder(inputClassPath);

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
            newClassPath.append(File.pathSeparator).append(pwd.toString())
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

        // When the container launches, it takes the parent process's environment
        // and then adds/overwrites with the entries from the container launch
        // context.  Do the same thing here for correct substitution of
        // environment variables in the classpath jar manifest.
        Map<String, String> mergedEnv = new HashMap<String, String>(
          System.getenv());
        mergedEnv.putAll(environment);

        String classPathJar = FileUtil.createJarWithClassPath(
          newClassPath.toString(), pwd, mergedEnv);
        environment.put(Environment.CLASSPATH.name(), classPathJar);
      }
    }
    // put AuxiliaryService data to environment
    for (Map.Entry<String, ByteBuffer> meta : containerManager
        .getAuxServiceMetaData().entrySet()) {
      AuxiliaryServiceHelper.setServiceDataIntoEnv(
          meta.getKey(), meta.getValue(), environment);
    }
  }
    
  static void writeLaunchEnv(OutputStream out,
      Map<String,String> environment, Map<Path,List<String>> resources,
      List<String> command)
      throws IOException {
    ShellScriptBuilder sb = ShellScriptBuilder.create();
    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        sb.env(env.getKey().toString(), env.getValue().toString());
      }
    }
    if (resources != null) {
      for (Map.Entry<Path,List<String>> entry : resources.entrySet()) {
        for (String linkName : entry.getValue()) {
          sb.symlink(entry.getKey(), new Path(linkName));
        }
      }
    }

    sb.command(command);

    PrintStream pout = null;
    try {
      pout = new PrintStream(out);
      sb.write(pout);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

}
