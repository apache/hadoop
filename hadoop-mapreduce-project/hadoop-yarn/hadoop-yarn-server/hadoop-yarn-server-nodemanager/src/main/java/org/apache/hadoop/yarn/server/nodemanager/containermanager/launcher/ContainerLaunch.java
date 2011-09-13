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
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ContainerLaunch implements Callable<Integer> {

  private static final Log LOG = LogFactory.getLog(ContainerLaunch.class);

  public static final String CONTAINER_SCRIPT = "task.sh";
  public static final String FINAL_CONTAINER_TOKENS_FILE = "container_tokens";

  private final Dispatcher dispatcher;
  private final ContainerExecutor exec;
  private final Application app;
  private final Container container;
  private final Configuration conf;
  private final LocalDirAllocator logDirsSelector;

  public ContainerLaunch(Configuration configuration, Dispatcher dispatcher,
      ContainerExecutor exec, Application app, Container container) {
    this.conf = configuration;
    this.app = app;
    this.exec = exec;
    this.container = container;
    this.dispatcher = dispatcher;
    this.logDirsSelector = new LocalDirAllocator(YarnConfiguration.NM_LOG_DIRS);
  }

  @Override
  @SuppressWarnings("unchecked") // dispatcher not typed
  public Integer call() {
    final ContainerLaunchContext launchContext = container.getLaunchContext();
    final Map<Path,String> localResources = container.getLocalizedResources();
    String containerIdStr = ConverterUtils.toString(container.getContainerID());
    final String user = launchContext.getUser();
    final Map<String,String> env = launchContext.getEnv();
    final List<String> command = launchContext.getCommands();
    int ret = -1;

    try {
      // /////////////////////////// Variable expansion
      // Before the container script gets written out.
      List<String> newCmds = new ArrayList<String>(command.size());
      String appIdStr = app.toString();
      Path containerLogDir =
          this.logDirsSelector.getLocalPathForWrite(appIdStr + Path.SEPARATOR
              + containerIdStr, LocalDirAllocator.SIZE_UNKNOWN, this.conf, 
              false);
      for (String str : command) {
        // TODO: Should we instead work via symlinks without this grammar?
        newCmds.add(str.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,
            containerLogDir.toUri().getPath()));
      }
      launchContext.setCommands(newCmds);

      Map<String, String> envs = launchContext.getEnv();
      Map<String, String> newEnvs = new HashMap<String, String>(envs.size());
      for (Entry<String, String> entry : envs.entrySet()) {
        newEnvs.put(
            entry.getKey(),
            entry.getValue().replace(
                ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                containerLogDir.toUri().getPath()));
      }
      launchContext.setEnv(newEnvs);
      // /////////////////////////// End of variable expansion

      FileContext lfs = FileContext.getLocalFSFileContext();
      LocalDirAllocator lDirAllocator =
          new LocalDirAllocator(YarnConfiguration.NM_LOCAL_DIRS); // TODO
      Path nmPrivateContainerScriptPath =
          lDirAllocator.getLocalPathForWrite(
              ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR
                  + appIdStr + Path.SEPARATOR + containerIdStr
                  + Path.SEPARATOR + CONTAINER_SCRIPT, this.conf);
      Path nmPrivateTokensPath =
          lDirAllocator.getLocalPathForWrite(
              ResourceLocalizationService.NM_PRIVATE_DIR
                  + Path.SEPARATOR
                  + containerIdStr
                  + Path.SEPARATOR
                  + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                      containerIdStr), this.conf);
      DataOutputStream containerScriptOutStream = null;
      DataOutputStream tokensOutStream = null;

      // Select the working directory for the container
      Path containerWorkDir =
          lDirAllocator.getLocalPathForWrite(ContainerLocalizer.USERCACHE
              + Path.SEPARATOR + user + Path.SEPARATOR
              + ContainerLocalizer.APPCACHE + Path.SEPARATOR + appIdStr
              + Path.SEPARATOR + containerIdStr,
              LocalDirAllocator.SIZE_UNKNOWN, this.conf, false);
      try {
        // /////////// Write out the container-script in the nmPrivate space.
        String[] localDirs =
            this.conf.getStrings(YarnConfiguration.NM_LOCAL_DIRS,
                YarnConfiguration.DEFAULT_NM_LOCAL_DIRS);
        List<Path> appDirs = new ArrayList<Path>(localDirs.length);
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
        env.put(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME, new Path(
            containerWorkDir, FINAL_CONTAINER_TOKENS_FILE).toUri().getPath());

        writeLaunchEnv(containerScriptOutStream, env, localResources,
            launchContext.getCommands(), appDirs);
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
            container.getContainerID(),
            ContainerEventType.CONTAINER_LAUNCHED));

      ret =
          exec.launchContainer(container, nmPrivateContainerScriptPath,
              nmPrivateTokensPath, user, appIdStr, containerWorkDir);
    } catch (Throwable e) {
      LOG.warn("Failed to launch container", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
            launchContext.getContainerId(),
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret));
      return ret;
    }

    if (ret == ExitCode.KILLED.getExitCode()) {
      // If the process was killed, Send container_cleanedup_after_kill and
      // just break out of this method.
      dispatcher.getEventHandler().handle(
            new ContainerExitEvent(launchContext.getContainerId(),
                ContainerEventType.CONTAINER_KILLED_ON_REQUEST, ret));
      return ret;
    }

    if (ret != 0) {
      LOG.warn("Container exited with a non-zero exit code " + ret);
      this.dispatcher.getEventHandler().handle(new ContainerExitEvent(
              launchContext.getContainerId(),
              ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret));
      return ret;
    }

    LOG.info("Container " + containerIdStr + " succeeded ");
    dispatcher.getEventHandler().handle(
        new ContainerEvent(launchContext.getContainerId(),
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    return 0;
  }

  private static class ShellScriptBuilder {
    
    private final StringBuilder sb;
  
    public ShellScriptBuilder() {
      this(new StringBuilder("#!/bin/bash\n\n"));
    }
  
    protected ShellScriptBuilder(StringBuilder sb) {
      this.sb = sb;
    }
  
    public ShellScriptBuilder env(String key, String value) {
      line("export ", key, "=\"", value, "\"");
      return this;
    }
  
    public ShellScriptBuilder symlink(Path src, String dst) throws IOException {
      return symlink(src, new Path(dst));
    }
  
    public ShellScriptBuilder symlink(Path src, Path dst) throws IOException {
      if (!src.isAbsolute()) {
        throw new IOException("Source must be absolute");
      }
      if (dst.isAbsolute()) {
        throw new IOException("Destination must be relative");
      }
      if (dst.toUri().getPath().indexOf('/') != -1) {
        line("mkdir -p ", dst.getParent().toString());
      }
      line("ln -sf ", src.toUri().getPath(), " ", dst.toString());
      return this;
    }
  
    public void write(PrintStream out) throws IOException {
      out.append(sb);
    }
  
    public void line(String... command) {
      for (String s : command) {
        sb.append(s);
      }
      sb.append("\n");
    }
  
    @Override
    public String toString() {
      return sb.toString();
    }
  
  }

  private static void writeLaunchEnv(OutputStream out,
      Map<String,String> environment, Map<Path,String> resources,
      List<String> command, List<Path> appDirs)
      throws IOException {
    ShellScriptBuilder sb = new ShellScriptBuilder();
    if (System.getenv("YARN_HOME") != null) {
      // TODO: Get from whitelist.
      sb.env("YARN_HOME", System.getenv("YARN_HOME"));
    }
    sb.env(ApplicationConstants.LOCAL_DIR_ENV, StringUtils.join(",", appDirs));
    if (!Shell.WINDOWS) {
      sb.env("JVM_PID", "$$");
    }
    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        sb.env(env.getKey().toString(), env.getValue().toString());
      }
    }
    if (resources != null) {
      for (Map.Entry<Path,String> link : resources.entrySet()) {
        sb.symlink(link.getKey(), link.getValue());
      }
    }
    ArrayList<String> cmd = new ArrayList<String>(2 * command.size() + 5);
    cmd.add(ContainerExecutor.isSetsidAvailable ? "exec setsid " : "exec ");
    cmd.add("/bin/bash ");
    cmd.add("-c ");
    cmd.add("\"");
    for (String cs : command) {
      cmd.add(cs.toString());
      cmd.add(" ");
    }
    cmd.add("\"");
    sb.line(cmd.toArray(new String[cmd.size()]));
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
