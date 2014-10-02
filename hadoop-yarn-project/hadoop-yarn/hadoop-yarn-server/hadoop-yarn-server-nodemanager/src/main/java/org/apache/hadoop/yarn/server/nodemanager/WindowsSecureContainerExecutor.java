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
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor.LocalWrapperScriptBuilder;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;

/**
 * Windows secure container executor. Uses winutils task createAsUser.
 *
 */
public class WindowsSecureContainerExecutor extends DefaultContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(WindowsSecureContainerExecutor.class);

  private class WindowsSecureWrapperScriptBuilder 
    extends LocalWrapperScriptBuilder {

    public WindowsSecureWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
    }

    @Override
    protected void writeLocalWrapperScript(Path launchDst, Path pidFile, PrintStream pout) {
      pout.format("@call \"%s\"", launchDst);
    }
  }

  private String nodeManagerGroup;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    nodeManagerGroup = conf.get(YarnConfiguration.NM_WINDOWS_SECURE_CONTAINER_GROUP);
  }

  @Override
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration conf) {
    return new String[] { Shell.WINUTILS, "task", "createAsUser", groupId, userName,
        pidFile.toString(), "cmd /c " + command };
  }

  @Override
  protected LocalWrapperScriptBuilder getLocalWrapperScriptBuilder(
      String containerIdStr, Path containerWorkDir) {
   return  new WindowsSecureWrapperScriptBuilder(containerWorkDir);
  }
  
  @Override
  protected void copyFile(Path src, Path dst, String owner) throws IOException {
    super.copyFile(src, dst, owner);
    lfs.setOwner(dst,  owner, nodeManagerGroup);
  }

  @Override
  protected void createDir(Path dirPath, FsPermission perms,
      boolean createParent, String owner) throws IOException {
    super.createDir(dirPath, perms, createParent, owner);
    lfs.setOwner(dirPath, owner, nodeManagerGroup);
  }

  @Override
  protected void setScriptExecutable(Path script, String owner) throws IOException {
    super.setScriptExecutable(script, null);
    lfs.setOwner(script, owner, nodeManagerGroup);
  }

  @Override
  public void localizeClasspathJar(Path classpathJar, String owner) throws IOException {
    lfs.setOwner(classpathJar, owner, nodeManagerGroup);
  }

 @Override
 public void startLocalizer(Path nmPrivateContainerTokens,
     InetSocketAddress nmAddr, String user, String appId, String locId,
     List<String> localDirs, List<String> logDirs) throws IOException,
     InterruptedException {

     createUserLocalDirs(localDirs, user);
     createUserCacheDirs(localDirs, user);
     createAppDirs(localDirs, user, appId);
     createAppLogDirs(appId, logDirs, user);
     
     // TODO: Why pick first app dir. The same in LCE why not random?
     Path appStorageDir = getFirstApplicationDir(localDirs, user, appId);

     String tokenFn = String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT, locId);
     Path tokenDst = new Path(appStorageDir, tokenFn);
     LOG.info("Copying from " + nmPrivateContainerTokens + " to " + tokenDst);
     copyFile(nmPrivateContainerTokens, tokenDst, user);

     List<String> command ;
     String[] commandArray;
     ShellCommandExecutor shExec;

     File cwdApp = new File(appStorageDir.toString());
     LOG.info(String.format("cwdApp: %s", cwdApp));

     command = new ArrayList<String>();

     command.add(Shell.WINUTILS);
     command.add("task");
     command.add("createAsUser");
     command.add("START_LOCALIZER_" + locId);
     command.add(user);
     command.add("nul:"); // PID file    
   
   //use same jvm as parent
     File jvm = new File(new File(System.getProperty("java.home"), "bin"), "java.exe");
     command.add(jvm.toString());
     
     
     // Build a temp classpath jar. See ContainerLaunch.sanitizeEnv().
     // Passing CLASSPATH explicitly is *way* too long for command line.
     String classPath = System.getProperty("java.class.path");
     Map<String, String> env = new HashMap<String, String>(System.getenv());
     String classPathJar = FileUtil.createJarWithClassPath(classPath, appStorageDir, env);
     localizeClasspathJar(new Path(classPathJar), user);
     command.add("-classpath");
     command.add(classPathJar);
     
     String javaLibPath = System.getProperty("java.library.path");
     if (javaLibPath != null) {
       command.add("-Djava.library.path=" + javaLibPath);
     }
     
     ContainerLocalizer.buildMainArgs(command, user, appId, locId, nmAddr, localDirs);
     commandArray = command.toArray(new String[command.size()]);

     shExec = new ShellCommandExecutor(
         commandArray, cwdApp);

     shExec.execute();
   }
}

