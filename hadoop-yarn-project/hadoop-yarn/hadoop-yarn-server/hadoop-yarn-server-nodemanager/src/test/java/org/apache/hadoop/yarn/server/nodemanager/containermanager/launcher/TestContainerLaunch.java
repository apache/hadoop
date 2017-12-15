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

import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.ShellScriptBuilder;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestContainerLaunch extends BaseContainerManagerTest {

  private static final String INVALID_JAVA_HOME = "/no/jvm/here";
  private NMContext distContext =
      new NMContext(new NMContainerTokenSecretManager(conf),
          new NMTokenSecretManagerInNM(), null,
          new ApplicationACLsManager(conf), new NMNullStateStoreService(),
          false, conf) {
        public int getHttpPort() {
          return HTTP_PORT;
        };
        public NodeId getNodeId() {
          return NodeId.newInstance("ahost", 1234);
        };
  };

  public TestContainerLaunch() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    conf.setClass(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    super.setup();
  }

  @Test
  public void testSpecialCharSymlinks() throws IOException  {

    File shellFile = null;
    File tempFile = null;
    String badSymlink = Shell.WINDOWS ? "foo@zz_#!-+bar.cmd" :
      "foo@zz%_#*&!-+= bar()";
    File symLinkFile = null;

    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      tempFile = Shell.appendScriptExtension(tmpDir, "temp");
      String timeoutCommand = Shell.WINDOWS ? "@echo \"hello\"" :
        "echo \"hello\"";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
      FileUtil.setExecutable(shellFile, true);
      writer.println(timeoutCommand);
      writer.close();

      Map<Path, List<String>> resources =
          new HashMap<Path, List<String>>();
      Path path = new Path(shellFile.getAbsolutePath());
      resources.put(path, Arrays.asList(badSymlink));

      FileOutputStream fos = new FileOutputStream(tempFile);

      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      if (Shell.WINDOWS) {
        commands.add("cmd");
        commands.add("/c");
        commands.add("\"" + badSymlink + "\"");
      } else {
        commands.add("/bin/sh ./\\\"" + badSymlink + "\\\"");
      }

      DefaultContainerExecutor defaultContainerExecutor =
          new DefaultContainerExecutor();
      defaultContainerExecutor.setConf(new YarnConfiguration());
      defaultContainerExecutor.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), "user", tempFile.getName());
      fos.flush();
      fos.close();
      FileUtil.setExecutable(tempFile, true);

      Shell.ShellCommandExecutor shexc 
      = new Shell.ShellCommandExecutor(new String[]{tempFile.getAbsolutePath()}, tmpDir);

      shexc.execute();
      assertEquals(shexc.getExitCode(), 0);
      //Capture output from prelaunch.out

      List<String> output = Files.readAllLines(Paths.get(localLogDir.getAbsolutePath(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT),
          Charset.forName("UTF-8"));
      assert(output.contains("hello"));

      symLinkFile = new File(tmpDir, badSymlink);
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
      if (tempFile != null 
          && tempFile.exists()) {
        tempFile.delete();
      }
      if (symLinkFile != null
          && symLinkFile.exists()) {
        symLinkFile.delete();
      } 
    }
  }

  // test the diagnostics are generated
  @Test (timeout = 20000)
  public void testInvalidSymlinkDiagnostics() throws IOException  {

    File shellFile = null;
    File tempFile = null;
    String symLink = Shell.WINDOWS ? "test.cmd" :
      "test";
    File symLinkFile = null;

    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      tempFile = Shell.appendScriptExtension(tmpDir, "temp");
      String timeoutCommand = Shell.WINDOWS ? "@echo \"hello\"" :
        "echo \"hello\"";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
      FileUtil.setExecutable(shellFile, true);
      writer.println(timeoutCommand);
      writer.close();

      Map<Path, List<String>> resources =
          new HashMap<Path, List<String>>();
      //This is an invalid path and should throw exception because of No such file.
      Path invalidPath = new Path(shellFile.getAbsolutePath()+"randomPath");
      resources.put(invalidPath, Arrays.asList(symLink));
      FileOutputStream fos = new FileOutputStream(tempFile);

      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      if (Shell.WINDOWS) {
        commands.add("cmd");
        commands.add("/c");
        commands.add("\"" + symLink + "\"");
      } else {
        commands.add("/bin/sh ./\\\"" + symLink + "\\\"");
      }
      DefaultContainerExecutor defaultContainerExecutor =
          new DefaultContainerExecutor();
      defaultContainerExecutor.setConf(new YarnConfiguration());
      defaultContainerExecutor.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), "user");
      fos.flush();
      fos.close();
      FileUtil.setExecutable(tempFile, true);

      Shell.ShellCommandExecutor shexc
      = new Shell.ShellCommandExecutor(new String[]{tempFile.getAbsolutePath()}, tmpDir);
      String diagnostics = null;
      try {
        shexc.execute();
        Assert.fail("Should catch exception");
      } catch(ExitCodeException e){
        diagnostics = e.getMessage();
      }
      Assert.assertNotNull(diagnostics);
      Assert.assertTrue(shexc.getExitCode() != 0);
      symLinkFile = new File(tmpDir, symLink);
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
      if (tempFile != null
          && tempFile.exists()) {
        tempFile.delete();
      }
      if (symLinkFile != null
          && symLinkFile.exists()) {
        symLinkFile.delete();
      }
    }
  }

  @Test(timeout = 20000)
  public void testWriteEnvExport() throws Exception {
    // Valid only for unix
    assumeNotWindows();
    File shellFile = Shell.appendScriptExtension(tmpDir, "hello");
    Map<String, String> env = new HashMap<String, String>();
    env.put("HADOOP_COMMON_HOME", "/opt/hadoopcommon");
    env.put("HADOOP_MAPRED_HOME", "/opt/hadoopbuild");
    Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
    FileOutputStream fos = new FileOutputStream(shellFile);
    List<String> commands = new ArrayList<String>();
    final Map<String, String> nmEnv = new HashMap<>();
    nmEnv.put("HADOOP_COMMON_HOME", "nodemanager_common_home");
    nmEnv.put("HADOOP_HDFS_HOME", "nodemanager_hdfs_home");
    nmEnv.put("HADOOP_YARN_HOME", "nodemanager_yarn_home");
    nmEnv.put("HADOOP_MAPRED_HOME", "nodemanager_mapred_home");
    DefaultContainerExecutor defaultContainerExecutor =
        new DefaultContainerExecutor() {
          @Override
          protected String getNMEnvVar(String varname) {
            return nmEnv.get(varname);
          }
        };
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_ENV_WHITELIST,
        "HADOOP_MAPRED_HOME,HADOOP_YARN_HOME");
    defaultContainerExecutor.setConf(conf);
    defaultContainerExecutor.writeLaunchEnv(fos, env, resources, commands,
        new Path(localLogDir.getAbsolutePath()), "user");
    String shellContent =
        new String(Files.readAllBytes(Paths.get(shellFile.getAbsolutePath())),
            StandardCharsets.UTF_8);
    Assert.assertTrue(shellContent
        .contains("export HADOOP_COMMON_HOME=\"/opt/hadoopcommon\""));
    // Whitelisted variable overridden by container
    Assert.assertTrue(shellContent.contains(
        "export HADOOP_MAPRED_HOME=\"/opt/hadoopbuild\""));
    // Available in env but not in whitelist
    Assert.assertFalse(shellContent.contains("HADOOP_HDFS_HOME"));
    // Available in env and in whitelist
    Assert.assertTrue(shellContent.contains(
        "export HADOOP_YARN_HOME=\"nodemanager_yarn_home\""));
    fos.flush();
    fos.close();
  }

  @Test(timeout = 20000)
  public void testWriteEnvExportDocker() throws Exception {
    // Valid only for unix
    assumeNotWindows();
    File shellFile = Shell.appendScriptExtension(tmpDir, "hello");
    Map<String, String> env = new HashMap<String, String>();
    env.put("HADOOP_COMMON_HOME", "/opt/hadoopcommon");
    env.put("HADOOP_MAPRED_HOME", "/opt/hadoopbuild");
    Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
    FileOutputStream fos = new FileOutputStream(shellFile);
    List<String> commands = new ArrayList<String>();
    final Map<String, String> nmEnv = new HashMap<>();
    nmEnv.put("HADOOP_COMMON_HOME", "nodemanager_common_home");
    nmEnv.put("HADOOP_HDFS_HOME", "nodemanager_hdfs_home");
    nmEnv.put("HADOOP_YARN_HOME", "nodemanager_yarn_home");
    nmEnv.put("HADOOP_MAPRED_HOME", "nodemanager_mapred_home");
    DockerLinuxContainerRuntime dockerRuntime =
        new DockerLinuxContainerRuntime(
            mock(PrivilegedOperationExecutor.class));
    LinuxContainerExecutor lce =
        new LinuxContainerExecutor(dockerRuntime) {
          @Override
          protected String getNMEnvVar(String varname) {
            return nmEnv.get(varname);
          }
        };
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NM_ENV_WHITELIST,
        "HADOOP_MAPRED_HOME,HADOOP_YARN_HOME");
    lce.setConf(conf);
    lce.writeLaunchEnv(fos, env, resources, commands,
        new Path(localLogDir.getAbsolutePath()), "user");
    String shellContent =
        new String(Files.readAllBytes(Paths.get(shellFile.getAbsolutePath())),
            StandardCharsets.UTF_8);
    Assert.assertTrue(shellContent
        .contains("export HADOOP_COMMON_HOME=\"/opt/hadoopcommon\""));
    // Whitelisted variable overridden by container
    Assert.assertTrue(shellContent.contains(
        "export HADOOP_MAPRED_HOME=\"/opt/hadoopbuild\""));
    // Verify no whitelisted variables inherited from NM env
    Assert.assertFalse(shellContent.contains("HADOOP_HDFS_HOME"));
    Assert.assertFalse(shellContent.contains("HADOOP_YARN_HOME"));
    fos.flush();
    fos.close();
  }

  @Test (timeout = 20000)
  public void testInvalidEnvSyntaxDiagnostics() throws IOException  {

    File shellFile = null;
    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      Map<Path, List<String>> resources =
          new HashMap<Path, List<String>>();
      FileOutputStream fos = new FileOutputStream(shellFile);
      FileUtil.setExecutable(shellFile, true);

      Map<String, String> env = new HashMap<String, String>();
      // invalid env
      env.put(
          "APPLICATION_WORKFLOW_CONTEXT", "{\"workflowId\":\"609f91c5cd83\"," +
          "\"workflowName\":\"\n\ninsert table " +
          "\npartition (cd_education_status)\nselect cd_demo_sk, cd_gender, " );
      List<String> commands = new ArrayList<String>();
      DefaultContainerExecutor defaultContainerExecutor =
          new DefaultContainerExecutor();
      defaultContainerExecutor.setConf(new YarnConfiguration());
      defaultContainerExecutor.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), "user");
      fos.flush();
      fos.close();

      // It is supposed that LANG is set as C.
      Map<String, String> cmdEnv = new HashMap<String, String>();
      cmdEnv.put("LANG", "C");
      Shell.ShellCommandExecutor shexc
      = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()},
        tmpDir, cmdEnv);
      String diagnostics = null;
      try {
        shexc.execute();
        Assert.fail("Should catch exception");
      } catch(ExitCodeException e){
        //Capture diagnostics from prelaunch.stderr
        List<String> error = Files.readAllLines(Paths.get(localLogDir.getAbsolutePath(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR),
            Charset.forName("UTF-8"));
        diagnostics = StringUtils.join("\n", error);
      }
      Assert.assertTrue(diagnostics.contains(Shell.WINDOWS ?
          "is not recognized as an internal or external command" :
          "command not found"));
      Assert.assertTrue(shexc.getExitCode() != 0);
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
    }
  }

  @Test(timeout = 10000)
  public void testEnvExpansion() throws IOException {
    Path logPath = new Path("/nm/container/logs");
    String input =
        Apps.crossPlatformify("HADOOP_HOME") + "/share/hadoop/common/*"
            + ApplicationConstants.CLASS_PATH_SEPARATOR
            + Apps.crossPlatformify("HADOOP_HOME") + "/share/hadoop/common/lib/*"
            + ApplicationConstants.CLASS_PATH_SEPARATOR
            + Apps.crossPlatformify("HADOOP_LOG_HOME")
            + ApplicationConstants.LOG_DIR_EXPANSION_VAR;

    String res = ContainerLaunch.expandEnvironment(input, logPath);

    if (Shell.WINDOWS) {
      Assert.assertEquals("%HADOOP_HOME%/share/hadoop/common/*;"
          + "%HADOOP_HOME%/share/hadoop/common/lib/*;"
          + "%HADOOP_LOG_HOME%/nm/container/logs", res);
    } else {
      Assert.assertEquals("$HADOOP_HOME/share/hadoop/common/*:"
          + "$HADOOP_HOME/share/hadoop/common/lib/*:"
          + "$HADOOP_LOG_HOME/nm/container/logs", res);
    }
    System.out.println(res);
  }

  @Test (timeout = 20000)
  public void testContainerLaunchStdoutAndStderrDiagnostics() throws IOException {

    File shellFile = null;
    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      // echo "hello" to stdout and "error" to stderr and exit code with 2;
      String command = Shell.WINDOWS ?
          "@echo \"hello\" & @echo \"error\" 1>&2 & exit /b 2" :
          "echo \"hello\"; echo \"error\" 1>&2; exit 2;";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
      FileUtil.setExecutable(shellFile, true);
      writer.println(command);
      writer.close();
      Map<Path, List<String>> resources =
          new HashMap<Path, List<String>>();
      FileOutputStream fos = new FileOutputStream(shellFile, true);

      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      commands.add(command);
      ContainerExecutor exec = new DefaultContainerExecutor();
      exec.setConf(new YarnConfiguration());
      exec.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), "user");
      fos.flush();
      fos.close();

      Shell.ShellCommandExecutor shexc
      = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()}, tmpDir);
      String diagnostics = null;
      try {
        shexc.execute();
        Assert.fail("Should catch exception");
      } catch(ExitCodeException e){
        diagnostics = e.getMessage();
      }
      // test stderr
      Assert.assertTrue(diagnostics.contains("error"));
      // test stdout
      Assert.assertTrue(shexc.getOutput().contains("hello"));
      Assert.assertTrue(shexc.getExitCode() == 2);
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
    }
  }

  @Test
  public void testPrependDistcache() throws Exception {

    // Test is only relevant on Windows
    assumeWindows();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);

    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    Map<String, String> userSetEnv = new HashMap<String, String>();
    userSetEnv.put(Environment.CONTAINER_ID.name(), "user_set_container_id");
    userSetEnv.put(Environment.NM_HOST.name(), "user_set_NM_HOST");
    userSetEnv.put(Environment.NM_PORT.name(), "user_set_NM_PORT");
    userSetEnv.put(Environment.NM_HTTP_PORT.name(), "user_set_NM_HTTP_PORT");
    userSetEnv.put(Environment.LOCAL_DIRS.name(), "user_set_LOCAL_DIR");
    userSetEnv.put(Environment.USER.key(), "user_set_" +
      Environment.USER.key());
    userSetEnv.put(Environment.LOGNAME.name(), "user_set_LOGNAME");
    userSetEnv.put(Environment.PWD.name(), "user_set_PWD");
    userSetEnv.put(Environment.HOME.name(), "user_set_HOME");
    userSetEnv.put(Environment.CLASSPATH.name(), "APATH");
    containerLaunchContext.setEnvironment(userSetEnv);
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(containerLaunchContext);
    when(container.getLocalizedResources()).thenReturn(null);
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler<Event> eventHandler = new EventHandler<Event>() {
      public void handle(Event event) {
        Assert.assertTrue(event instanceof ContainerExitEvent);
        ContainerExitEvent exitEvent = (ContainerExitEvent) event;
        Assert.assertEquals(ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
            exitEvent.getType());
      }
    };
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);

    Configuration conf = new Configuration();

    ContainerLaunch launch = new ContainerLaunch(distContext, conf,
        dispatcher, exec, null, container, dirsHandler, containerManager);

    String testDir = System.getProperty("test.build.data",
        "target/test-dir");
    Path pwd = new Path(testDir);
    List<Path> appDirs = new ArrayList<Path>();
    List<String> userLocalDirs = new ArrayList<>();
    List<String> containerLogs = new ArrayList<String>();

    Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
    Path userjar = new Path("user.jar");
    List<String> lpaths = new ArrayList<String>();
    lpaths.add("userjarlink.jar");
    resources.put(userjar, lpaths);

    Path nmp = new Path(testDir);

    launch.sanitizeEnv(userSetEnv, pwd, appDirs, userLocalDirs, containerLogs,
        resources, nmp);

    List<String> result =
      getJarManifestClasspath(userSetEnv.get(Environment.CLASSPATH.name()));

    Assert.assertTrue(result.size() > 1);
    Assert.assertTrue(
      result.get(result.size() - 1).endsWith("userjarlink.jar"));

    //Then, with user classpath first
    userSetEnv.put(Environment.CLASSPATH_PREPEND_DISTCACHE.name(), "true");

    cId = ContainerId.newContainerId(appAttemptId, 1);
    when(container.getContainerId()).thenReturn(cId);

    launch = new ContainerLaunch(distContext, conf,
        dispatcher, exec, null, container, dirsHandler, containerManager);

    launch.sanitizeEnv(userSetEnv, pwd, appDirs, userLocalDirs, containerLogs,
        resources, nmp);

    result =
      getJarManifestClasspath(userSetEnv.get(Environment.CLASSPATH.name()));

    Assert.assertTrue(result.size() > 1);
    Assert.assertTrue(
      result.get(0).endsWith("userjarlink.jar"));

  }

  @Test
  public void testErrorLogOnContainerExit() throws Exception {
    verifyTailErrorLogOnContainerExit(new Configuration(), "/stderr", false);
  }

  @Test
  public void testErrorLogOnContainerExitForCase() throws Exception {
    verifyTailErrorLogOnContainerExit(new Configuration(), "/STDERR.log",
        false);
  }

  @Test
  public void testErrorLogOnContainerExitForExt() throws Exception {
    verifyTailErrorLogOnContainerExit(new Configuration(), "/AppMaster.stderr",
        false);
  }

  @Test
  public void testErrorLogOnContainerExitWithCustomPattern() throws Exception {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_CONTAINER_STDERR_PATTERN,
        "{*stderr*,*log*}");
    verifyTailErrorLogOnContainerExit(conf, "/error.log", false);
  }

  @Test
  public void testErrorLogOnContainerExitWithMultipleFiles() throws Exception {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_CONTAINER_STDERR_PATTERN,
        "{*stderr*,*stdout*}");
    verifyTailErrorLogOnContainerExit(conf, "/stderr.log", true);
  }

  private void verifyTailErrorLogOnContainerExit(Configuration conf,
      String errorFileName, boolean testForMultipleErrFiles) throws Exception {
    Container container = mock(Container.class);
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ContainerId containerId = ContainerId
        .newContainerId(ApplicationAttemptId.newInstance(appId, 1), 1);
    when(container.getContainerId()).thenReturn(containerId);
    when(container.getUser()).thenReturn("test");
    String relativeContainerLogDir = ContainerLaunch.getRelativeContainerLogDir(
        appId.toString(), containerId.toString());
    Path containerLogDir =
        dirsHandler.getLogPathForWrite(relativeContainerLogDir, false);

    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    List<String> invalidCommand = new ArrayList<String>();
    invalidCommand.add("$JAVA_HOME/bin/java");
    invalidCommand.add("-Djava.io.tmpdir=$PWD/tmp");
    invalidCommand.add("-Dlog4j.configuration=container-log4j.properties");
    invalidCommand.add("-Dyarn.app.container.log.dir=" + containerLogDir);
    invalidCommand.add("-Dyarn.app.container.log.filesize=0");
    invalidCommand.add("-Dhadoop.root.logger=INFO,CLA");
    invalidCommand.add("-Dhadoop.root.logfile=syslog");
    invalidCommand.add("-Xmx1024m");
    invalidCommand.add("org.apache.hadoop.mapreduce.v2.app.MRAppMaster");
    invalidCommand.add("1>" + containerLogDir + "/stdout");
    invalidCommand.add("2>" + containerLogDir + errorFileName);
    when(clc.getCommands()).thenReturn(invalidCommand);

    Map<String, String> userSetEnv = new HashMap<String, String>();
    userSetEnv.put(Environment.CONTAINER_ID.name(), "user_set_container_id");
    userSetEnv.put("JAVA_HOME", INVALID_JAVA_HOME);
    userSetEnv.put(Environment.NM_HOST.name(), "user_set_NM_HOST");
    userSetEnv.put(Environment.NM_PORT.name(), "user_set_NM_PORT");
    userSetEnv.put(Environment.NM_HTTP_PORT.name(), "user_set_NM_HTTP_PORT");
    userSetEnv.put(Environment.LOCAL_DIRS.name(), "user_set_LOCAL_DIR");
    userSetEnv.put(Environment.USER.key(),
        "user_set_" + Environment.USER.key());
    userSetEnv.put(Environment.LOGNAME.name(), "user_set_LOGNAME");
    userSetEnv.put(Environment.PWD.name(), "user_set_PWD");
    userSetEnv.put(Environment.HOME.name(), "user_set_HOME");
    userSetEnv.put(Environment.CLASSPATH.name(), "APATH");
    when(clc.getEnvironment()).thenReturn(userSetEnv);
    when(container.getLaunchContext()).thenReturn(clc);

    when(container.getLocalizedResources())
        .thenReturn(Collections.<Path, List<String>> emptyMap());
    Dispatcher dispatcher = mock(Dispatcher.class);

    @SuppressWarnings("rawtypes")
    ContainerExitHandler eventHandler =
        new ContainerExitHandler(testForMultipleErrFiles);
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);

    Application app = mock(Application.class);
    when(app.getAppId()).thenReturn(appId);
    when(app.getUser()).thenReturn("test");

    Credentials creds = mock(Credentials.class);
    when(container.getCredentials()).thenReturn(creds);

    ((NMContext) context).setNodeId(NodeId.newInstance("127.0.0.1", HTTP_PORT));

    ContainerLaunch launch = new ContainerLaunch(context, conf, dispatcher,
        exec, app, container, dirsHandler, containerManager);
    launch.call();
    Assert.assertTrue("ContainerExitEvent should have occurred",
        eventHandler.isContainerExitEventOccurred());
  }

  private static class ContainerExitHandler implements EventHandler<Event> {
    private boolean testForMultiFile;

    ContainerExitHandler(boolean testForMultiFile) {
      this.testForMultiFile = testForMultiFile;
    }

    boolean containerExitEventOccurred = false;

    public boolean isContainerExitEventOccurred() {
      return containerExitEventOccurred;
    }

    public void handle(Event event) {
      if (event instanceof ContainerExitEvent) {
        containerExitEventOccurred = true;
        ContainerExitEvent exitEvent = (ContainerExitEvent) event;
        Assert.assertEquals(ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
            exitEvent.getType());
        LOG.info("Diagnostic Info : " + exitEvent.getDiagnosticInfo());
        if (testForMultiFile) {
          Assert.assertTrue("Should contain the Multi file information",
              exitEvent.getDiagnosticInfo().contains("Error files: "));
        }
        Assert.assertTrue(
            "Should contain the error Log message with tail size info",
            exitEvent.getDiagnosticInfo()
                .contains("Last "
                    + YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_BYTES
                    + " bytes of"));
        Assert.assertTrue("Should contain contents of error Log",
            exitEvent.getDiagnosticInfo().contains(
                INVALID_JAVA_HOME + "/bin/java"));
      }
    }
  }

  private static List<String> getJarManifestClasspath(String path)
      throws Exception {
    List<String> classpath = new ArrayList<String>();
    JarFile jarFile = new JarFile(path);
    Manifest manifest = jarFile.getManifest();
    String cps = manifest.getMainAttributes().getValue("Class-Path");
    StringTokenizer cptok = new StringTokenizer(cps);
    while (cptok.hasMoreTokens()) {
      String cpentry = cptok.nextToken();
      classpath.add(cpentry);
    }
    return classpath;
  }

  /**
   * See if environment variable is forwarded using sanitizeEnv.
   * @throws Exception
   */
  @Test (timeout = 60000)
  public void testContainerEnvVariables() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // ////// Construct the Container-id
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);

    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    Map<String, String> userSetEnv = new HashMap<String, String>();
    userSetEnv.put(Environment.CONTAINER_ID.name(), "user_set_container_id");
    userSetEnv.put(Environment.NM_HOST.name(), "user_set_NM_HOST");
    userSetEnv.put(Environment.NM_PORT.name(), "user_set_NM_PORT");
    userSetEnv.put(Environment.NM_HTTP_PORT.name(), "user_set_NM_HTTP_PORT");
    userSetEnv.put(Environment.LOCAL_DIRS.name(), "user_set_LOCAL_DIR");
    userSetEnv.put(Environment.USER.key(), "user_set_" +
    	Environment.USER.key());
    userSetEnv.put(Environment.LOGNAME.name(), "user_set_LOGNAME");
    userSetEnv.put(Environment.PWD.name(), "user_set_PWD");
    userSetEnv.put(Environment.HOME.name(), "user_set_HOME");
    final String userConfDir = "user_set_HADOOP_CONF_DIR";
    userSetEnv.put(Environment.HADOOP_CONF_DIR.name(), userConfDir);
    containerLaunchContext.setEnvironment(userSetEnv);

    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "env_vars.tmp").getAbsoluteFile();
    final File processFinalFile =
        new File(tmpDir, "env_vars.txt").getAbsoluteFile();
    if (Shell.WINDOWS) {
      fileWriter.println("@echo " + Environment.CONTAINER_ID.$() + "> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.NM_HOST.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.NM_PORT.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.NM_HTTP_PORT.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.LOCAL_DIRS.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.USER.$() + ">> "
    	  + processStartFile);
      fileWriter.println("@echo " + Environment.LOGNAME.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.PWD.$() + ">> "
    	  + processStartFile);
      fileWriter.println("@echo " + Environment.HOME.$() + ">> "
          + processStartFile);
      fileWriter.println("@echo " + Environment.HADOOP_CONF_DIR.$() + ">> "
          + processStartFile);
      for (String serviceName : containerManager.getAuxServiceMetaData()
          .keySet()) {
        fileWriter.println("@echo %" + AuxiliaryServiceHelper.NM_AUX_SERVICE
            + serviceName + "%>> "
            + processStartFile);
      }
      fileWriter.println("@echo " + cId + ">> " + processStartFile);
      fileWriter.println("@move /Y " + processStartFile + " "
          + processFinalFile);
      fileWriter.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      fileWriter.write("\numask 0"); // So that start file is readable by the test
      fileWriter.write("\necho $" + Environment.CONTAINER_ID.name() + " > "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.NM_HOST.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.NM_PORT.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.NM_HTTP_PORT.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.LOCAL_DIRS.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.USER.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.LOGNAME.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.PWD.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.HOME.name() + " >> "
          + processStartFile);
      fileWriter.write("\necho $" + Environment.HADOOP_CONF_DIR.name() + " >> "
          + processStartFile);
      for (String serviceName : containerManager.getAuxServiceMetaData()
          .keySet()) {
        fileWriter.write("\necho $" + AuxiliaryServiceHelper.NM_AUX_SERVICE
            + serviceName + " >> "
            + processStartFile);
      }
      fileWriter.write("\necho $$ >> " + processStartFile);
      fileWriter.write("\nmv " + processStartFile + " " + processFinalFile);
      fileWriter.write("\nexec sleep 100");
    }
    fileWriter.close();

    // upload the script file so that the container can run it
    URL resource_alpha =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);

    // set up the rest of the container
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          createContainerToken(cId, Priority.newInstance(0), 0));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return processFinalFile.exists();
      }
    }, 10, 20000);

    // Now verify the contents of the file
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<String> logDirs = dirsHandler.getLogDirs();

    List<Path> appDirs = new ArrayList<Path>(localDirs.size());
    for (String localDir : localDirs) {
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, user);
      Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      appDirs.add(new Path(appsdir, appId.toString()));
    }
    List<String> containerLogDirs = new ArrayList<String>();
    String relativeContainerLogDir = ContainerLaunch
        .getRelativeContainerLogDir(appId.toString(), cId.toString());
    for(String logDir : logDirs){
      containerLogDirs.add(logDir + Path.SEPARATOR + relativeContainerLogDir);
    }
    BufferedReader reader =
        new BufferedReader(new FileReader(processFinalFile));
    Assert.assertEquals(cId.toString(), reader.readLine());
    Assert.assertEquals(context.getNodeId().getHost(), reader.readLine());
    Assert.assertEquals(String.valueOf(context.getNodeId().getPort()),
      reader.readLine());
    Assert.assertEquals(String.valueOf(HTTP_PORT), reader.readLine());
    Assert.assertEquals(StringUtils.join(",", appDirs), reader.readLine());
    Assert.assertEquals(user, reader.readLine());
    Assert.assertEquals(user, reader.readLine());
    String obtainedPWD = reader.readLine();
    boolean found = false;
    for (Path localDir : appDirs) {
      if (new Path(localDir, cId.toString()).toString().equals(obtainedPWD)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue("Wrong local-dir found : " + obtainedPWD, found);
    Assert.assertEquals(
        conf.get(
              YarnConfiguration.NM_USER_HOME_DIR, 
              YarnConfiguration.DEFAULT_NM_USER_HOME_DIR),
        reader.readLine());
    Assert.assertEquals(userConfDir, reader.readLine());
    for (String serviceName : containerManager.getAuxServiceMetaData().keySet()) {
      Assert.assertEquals(
          containerManager.getAuxServiceMetaData().get(serviceName),
          ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes())));
    }

    Assert.assertEquals(cId.toString(), containerLaunchContext
        .getEnvironment().get(Environment.CONTAINER_ID.name()));
    Assert.assertEquals(context.getNodeId().getHost(), containerLaunchContext
      .getEnvironment().get(Environment.NM_HOST.name()));
    Assert.assertEquals(String.valueOf(context.getNodeId().getPort()),
      containerLaunchContext.getEnvironment().get(Environment.NM_PORT.name()));
    Assert.assertEquals(String.valueOf(HTTP_PORT), containerLaunchContext
      .getEnvironment().get(Environment.NM_HTTP_PORT.name()));
    Assert.assertEquals(StringUtils.join(",", appDirs), containerLaunchContext
        .getEnvironment().get(Environment.LOCAL_DIRS.name()));
    Assert.assertEquals(StringUtils.join(",", containerLogDirs),
      containerLaunchContext.getEnvironment().get(Environment.LOG_DIRS.name()));
    Assert.assertEquals(user, containerLaunchContext.getEnvironment()
    	.get(Environment.USER.name()));
    Assert.assertEquals(user, containerLaunchContext.getEnvironment()
    	.get(Environment.LOGNAME.name()));
    found = false;
    obtainedPWD =
        containerLaunchContext.getEnvironment().get(Environment.PWD.name());
    for (Path localDir : appDirs) {
      if (new Path(localDir, cId.toString()).toString().equals(obtainedPWD)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue("Wrong local-dir found : " + obtainedPWD, found);
    Assert.assertEquals(
        conf.get(
    	        YarnConfiguration.NM_USER_HOME_DIR, 
    	        YarnConfiguration.DEFAULT_NM_USER_HOME_DIR),
    	containerLaunchContext.getEnvironment()
    		.get(Environment.HOME.name()));
    Assert.assertEquals(userConfDir, containerLaunchContext.getEnvironment()
        .get(Environment.HADOOP_CONF_DIR.name()));

    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    // Now test the stop functionality.

    // Assert that the process is alive
    Assert.assertTrue("Process is not alive!",
      DefaultContainerExecutor.containerIsAlive(pid));
    // Once more
    Assert.assertTrue("Process is not alive!",
      DefaultContainerExecutor.containerIsAlive(pid));

    // Now test the stop functionality.
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    containerManager.stopContainers(stopRequest);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerStatus containerStatus = 
        containerManager.getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
    int expectedExitCode = ContainerExitStatus.KILLED_BY_APPMASTER;
    Assert.assertEquals(expectedExitCode, containerStatus.getExitStatus());

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
      DefaultContainerExecutor.containerIsAlive(pid));
  }

  @Test (timeout = 5000)
  public void testAuxiliaryServiceHelper() throws Exception {
    Map<String, String> env = new HashMap<String, String>();
    String serviceName = "testAuxiliaryService";
    ByteBuffer bb = ByteBuffer.wrap("testAuxiliaryService".getBytes());
    AuxiliaryServiceHelper.setServiceDataIntoEnv(serviceName, bb, env);
    Assert.assertEquals(bb,
        AuxiliaryServiceHelper.getServiceDataFromEnv(serviceName, env));
  }

  private void internalKillTest(boolean delayed) throws Exception {
    conf.setLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
      delayed ? 1000 : 0);
    containerManager.start();

    // ////// Construct the Container-id
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    File processStartFile =
        new File(tmpDir, "pid.txt").getAbsoluteFile();

    // setup a script that can handle sigterm gracefully
    File scriptFile = Shell.appendScriptExtension(tmpDir, "testscript");
    PrintWriter writer = new PrintWriter(new FileOutputStream(scriptFile));
    if (Shell.WINDOWS) {
      writer.println("@echo \"Running testscript for delayed kill\"");
      writer.println("@echo \"Writing pid to start file\"");
      writer.println("@echo " + cId + "> " + processStartFile);
      writer.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      writer.println("#!/bin/bash\n\n");
      writer.println("echo \"Running testscript for delayed kill\"");
      writer.println("hello=\"Got SIGTERM\"");
      writer.println("umask 0");
      writer.println("trap \"echo $hello >> " + processStartFile + "\" SIGTERM");
      writer.println("echo \"Writing pid to start file\"");
      writer.println("echo $$ >> " + processStartFile);
      writer.println("while true; do\nsleep 1s;\ndone");
    }
    writer.close();
    FileUtil.setExecutable(scriptFile, true);

    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // upload the script file so that the container can run it
    URL resource_alpha =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file.sh";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);

    // set up the rest of the container
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    Priority priority = Priority.newInstance(10);
    long createTime = 1234;
    Token containerToken = createContainerToken(cId, priority, createTime);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          containerToken);
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    NMContainerStatus nmContainerStatus =
        containerManager.getContext().getContainers().get(cId)
          .getNMContainerStatus();
    Assert.assertEquals(priority, nmContainerStatus.getPriority());

    // Now test the stop functionality.
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    containerManager.stopContainers(stopRequest);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    // if delayed container stop sends a sigterm followed by a sigkill
    // otherwise sigkill is sent immediately 
    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    
    ContainerStatus containerStatus = 
        containerManager.getContainerStatuses(gcsRequest)
          .getContainerStatuses().get(0);
    Assert.assertEquals(ContainerExitStatus.KILLED_BY_APPMASTER,
        containerStatus.getExitStatus());

    // Now verify the contents of the file.  Script generates a message when it
    // receives a sigterm so we look for that.  We cannot perform this check on
    // Windows, because the process is not notified when killed by winutils.
    // There is no way for the process to trap and respond.  Instead, we can
    // verify that the job object with ID matching container ID no longer exists.
    if (Shell.WINDOWS || !delayed) {
      Assert.assertFalse("Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(cId.toString()));
    } else {
      BufferedReader reader =
          new BufferedReader(new FileReader(processStartFile));

      boolean foundSigTermMessage = false;
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        if (line.contains("SIGTERM")) {
          foundSigTermMessage = true;
          break;
        }
      }
      Assert.assertTrue("Did not find sigterm message", foundSigTermMessage);
      reader.close();
    }
  }

  @Test (timeout = 30000)
  public void testDelayedKill() throws Exception {
    internalKillTest(true);
  }

  @Test (timeout = 30000)
  public void testImmediateKill() throws Exception {
    internalKillTest(false);
  }

  @SuppressWarnings("rawtypes")
  @Test (timeout = 10000)
  public void testCallFailureWithNullLocalizedResources() {
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            System.currentTimeMillis(), 1), 1), 1));
    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    when(clc.getCommands()).thenReturn(Collections.<String>emptyList());
    when(container.getLaunchContext()).thenReturn(clc);
    when(container.getLocalizedResources()).thenReturn(null);
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler<Event> eventHandler = new EventHandler<Event>() {
      @Override
      public void handle(Event event) {
        Assert.assertTrue(event instanceof ContainerExitEvent);
        ContainerExitEvent exitEvent = (ContainerExitEvent) event;
        Assert.assertEquals(ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
            exitEvent.getType());
      }
    };
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    ContainerLaunch launch = new ContainerLaunch(context, new Configuration(),
        dispatcher, exec, null, container, dirsHandler, containerManager);
    launch.call();
  }

  protected Token createContainerToken(ContainerId cId, Priority priority,
      long createTime) throws InvalidToken {
    Resource r = BuilderUtils.newResource(1024, 1);
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, context.getNodeId().toString(), user,
          r, System.currentTimeMillis() + 10000L, 123, DUMMY_RM_IDENTIFIER,
          priority, createTime);
    Token containerToken =
        BuilderUtils.newContainerToken(
          context.getNodeId(),
          context.getContainerTokenSecretManager().retrievePassword(
            containerTokenIdentifier), containerTokenIdentifier);
    return containerToken;
  }

  /**
   * Test that script exists with non-zero exit code when command fails.
   * @throws IOException
   */
  @Test (timeout = 10000)
  public void testShellScriptBuilderNonZeroExitCode() throws IOException {
    ShellScriptBuilder builder = ShellScriptBuilder.create();
    builder.command(Arrays.asList(new String[] {"unknownCommand"}));
    File shellFile = Shell.appendScriptExtension(tmpDir, "testShellScriptBuilderError");
    PrintStream writer = new PrintStream(new FileOutputStream(shellFile));
    builder.write(writer);
    writer.close();
    try {
      FileUtil.setExecutable(shellFile, true);

      Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(
          new String[]{shellFile.getAbsolutePath()}, tmpDir);
      try {
        shexc.execute();
        fail("builder shell command was expected to throw");
      }
      catch(IOException e) {
        // expected
        System.out.println("Received an expected exception: " + e.getMessage());
      }
    }
    finally {
      FileUtil.fullyDelete(shellFile);
    }
  }

  private static final String expectedMessage = "The command line has a length of";
  
  @Test (timeout = 10000)
  public void testWindowsShellScriptBuilderCommand() throws IOException {
    String callCmd = "@call ";
    
    // Test is only relevant on Windows
    assumeWindows();

    // The tests are built on assuming 8191 max command line length
    assertEquals(8191, Shell.WINDOWS_MAX_SHELL_LENGTH);

    ShellScriptBuilder builder = ShellScriptBuilder.create();

    // Basic tests: less length, exact length, max+1 length 
    builder.command(Arrays.asList(
        org.apache.commons.lang.StringUtils.repeat("A", 1024)));
    builder.command(Arrays.asList(
        org.apache.commons.lang.StringUtils.repeat(
            "E", Shell.WINDOWS_MAX_SHELL_LENGTH - callCmd.length())));
    try {
      builder.command(Arrays.asList(
          org.apache.commons.lang.StringUtils.repeat(
              "X", Shell.WINDOWS_MAX_SHELL_LENGTH -callCmd.length() + 1)));
      fail("longCommand was expected to throw");
    } catch(IOException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString(expectedMessage));
    }

    // Composite tests, from parts: less, exact and +
    builder.command(Arrays.asList(
        org.apache.commons.lang.StringUtils.repeat("A", 1024),
        org.apache.commons.lang.StringUtils.repeat("A", 1024),
        org.apache.commons.lang.StringUtils.repeat("A", 1024)));

    // buildr.command joins the command parts with an extra space
    builder.command(Arrays.asList(
        org.apache.commons.lang.StringUtils.repeat("E", 4095),
        org.apache.commons.lang.StringUtils.repeat("E", 2047),
        org.apache.commons.lang.StringUtils.repeat("E", 2047 - callCmd.length())));

    try {
      builder.command(Arrays.asList(
          org.apache.commons.lang.StringUtils.repeat("X", 4095), 
          org.apache.commons.lang.StringUtils.repeat("X", 2047),
          org.apache.commons.lang.StringUtils.repeat("X", 2048 - callCmd.length())));
      fail("long commands was expected to throw");
    } catch(IOException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString(expectedMessage));
    }
  }
  
  @Test (timeout = 10000)
  public void testWindowsShellScriptBuilderEnv() throws IOException {
    // Test is only relevant on Windows
    assumeWindows();

    // The tests are built on assuming 8191 max command line length
    assertEquals(8191, Shell.WINDOWS_MAX_SHELL_LENGTH);

    ShellScriptBuilder builder = ShellScriptBuilder.create();

    // test env
    builder.env("somekey", org.apache.commons.lang.StringUtils.repeat("A", 1024));
    builder.env("somekey", org.apache.commons.lang.StringUtils.repeat(
        "A", Shell.WINDOWS_MAX_SHELL_LENGTH - ("@set somekey=").length()));
    try {
      builder.env("somekey", org.apache.commons.lang.StringUtils.repeat(
          "A", Shell.WINDOWS_MAX_SHELL_LENGTH - ("@set somekey=").length()) + 1);
      fail("long env was expected to throw");
    } catch(IOException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString(expectedMessage));
    }
  }
    
  @Test (timeout = 10000)
  public void testWindowsShellScriptBuilderMkdir() throws IOException {
    String mkDirCmd = "@if not exist \"\" mkdir \"\"";

    // Test is only relevant on Windows
    assumeWindows();

    // The tests are built on assuming 8191 max command line length
    assertEquals(8191, Shell.WINDOWS_MAX_SHELL_LENGTH);

    ShellScriptBuilder builder = ShellScriptBuilder.create();

    // test mkdir
    builder.mkdir(new Path(org.apache.commons.lang.StringUtils.repeat("A", 1024)));
    builder.mkdir(new Path(org.apache.commons.lang.StringUtils.repeat("E",
        (Shell.WINDOWS_MAX_SHELL_LENGTH - mkDirCmd.length()) / 2)));
    try {
      builder.mkdir(new Path(org.apache.commons.lang.StringUtils.repeat(
          "X", (Shell.WINDOWS_MAX_SHELL_LENGTH - mkDirCmd.length())/2 +1)));
      fail("long mkdir was expected to throw");
    } catch(IOException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString(expectedMessage));
    }    
  }

  @Test (timeout = 10000)
  public void testWindowsShellScriptBuilderLink() throws IOException {
    // Test is only relevant on Windows
    assumeWindows();
    String linkCmd = "@" + Shell.getWinUtilsPath() + " symlink \"\" \"\"";

    // The tests are built on assuming 8191 max command line length
    assertEquals(8191, Shell.WINDOWS_MAX_SHELL_LENGTH);

    ShellScriptBuilder builder = ShellScriptBuilder.create();

    // test link
    builder.link(new Path(org.apache.commons.lang.StringUtils.repeat("A", 1024)),
        new Path(org.apache.commons.lang.StringUtils.repeat("B", 1024)));
    builder.link(
        new Path(org.apache.commons.lang.StringUtils.repeat(
            "E", (Shell.WINDOWS_MAX_SHELL_LENGTH - linkCmd.length())/2)),
        new Path(org.apache.commons.lang.StringUtils.repeat(
            "F", (Shell.WINDOWS_MAX_SHELL_LENGTH - linkCmd.length())/2)));
    try {
      builder.link(
          new Path(org.apache.commons.lang.StringUtils.repeat(
              "X", (Shell.WINDOWS_MAX_SHELL_LENGTH - linkCmd.length())/2 + 1)),
          new Path(org.apache.commons.lang.StringUtils.repeat(
              "Y", (Shell.WINDOWS_MAX_SHELL_LENGTH - linkCmd.length())/2) + 1));
      fail("long link was expected to throw");
    } catch(IOException e) {
      assertThat(e.getMessage(), CoreMatchers.containsString(expectedMessage));
    }
  }

  @Test
  public void testKillProcessGroup() throws Exception {
    Assume.assumeTrue(Shell.isSetsidAvailable);
    containerManager.start();

    // Construct the Container-id
    ApplicationId appId = ApplicationId.newInstance(2, 2);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);
    File processStartFile =
        new File(tmpDir, "pid.txt").getAbsoluteFile();
    File childProcessStartFile =
        new File(tmpDir, "child_pid.txt").getAbsoluteFile();

    // setup a script that can handle sigterm gracefully
    File scriptFile = Shell.appendScriptExtension(tmpDir, "testscript");
    PrintWriter writer = new PrintWriter(new FileOutputStream(scriptFile));
    writer.println("#!/bin/bash\n\n");
    writer.println("echo \"Running testscript for forked process\"");
    writer.println("umask 0");
    writer.println("echo $$ >> " + processStartFile);
    writer.println("while true;\ndo sleep 1s;\ndone > /dev/null 2>&1 &");
    writer.println("echo $! >> " + childProcessStartFile);
    writer.println("while true;\ndo sleep 1s;\ndone");
    writer.close();
    FileUtil.setExecutable(scriptFile, true);

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // upload the script file so that the container can run it
    URL resource_alpha =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file.sh";
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);

    // set up the rest of the container
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    Priority priority = Priority.newInstance(10);
    long createTime = 1234;
    Token containerToken = createContainerToken(cId, priority, createTime);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
            containerToken);
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    BufferedReader reader =
          new BufferedReader(new FileReader(processStartFile));
    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());
    reader.close();

    reader =
          new BufferedReader(new FileReader(childProcessStartFile));
    // Get the pid of the child process
    String child = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());
    reader.close();

    LOG.info("Manually killing pid " + pid + ", but not child pid " + child);
    Shell.execCommand(new String[]{"kill", "-9", pid});

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    Assert.assertFalse("Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(pid));

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);

    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);

    ContainerStatus containerStatus =
        containerManager.getContainerStatuses(gcsRequest)
            .getContainerStatuses().get(0);
    Assert.assertEquals(ExitCode.FORCE_KILLED.getExitCode(),
        containerStatus.getExitStatus());
  }

  @Test
  public void testDebuggingInformation() throws IOException {

    File shellFile = null;
    File tempFile = null;
    Configuration conf = new YarnConfiguration();
    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      tempFile = Shell.appendScriptExtension(tmpDir, "temp");
      String testCommand = Shell.WINDOWS ? "@echo \"hello\"" :
          "echo \"hello\"";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
      FileUtil.setExecutable(shellFile, true);
      writer.println(testCommand);
      writer.close();

      Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      if (Shell.WINDOWS) {
        commands.add("cmd");
        commands.add("/c");
        commands.add("\"" + shellFile.getAbsolutePath() + "\"");
      } else {
        commands.add("/bin/sh \\\"" + shellFile.getAbsolutePath() + "\\\"");
      }

      boolean[] debugLogsExistArray = { false, true };
      for (boolean debugLogsExist : debugLogsExistArray) {

        conf.setBoolean(YarnConfiguration.NM_LOG_CONTAINER_DEBUG_INFO,
          debugLogsExist);
        FileOutputStream fos = new FileOutputStream(tempFile);
        ContainerExecutor exec = new DefaultContainerExecutor();
        exec.setConf(conf);
        exec.writeLaunchEnv(fos, env, resources, commands,
            new Path(localLogDir.getAbsolutePath()), "user",
            tempFile.getName());
        fos.flush();
        fos.close();
        FileUtil.setExecutable(tempFile, true);

        Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(
          new String[] { tempFile.getAbsolutePath() }, tmpDir);

        shexc.execute();
        assertEquals(shexc.getExitCode(), 0);
        File directorInfo =
          new File(localLogDir, ContainerExecutor.DIRECTORY_CONTENTS);
        File scriptCopy = new File(localLogDir, tempFile.getName());

        Assert.assertEquals("Directory info file missing", debugLogsExist,
          directorInfo.exists());
        Assert.assertEquals("Copy of launch script missing", debugLogsExist,
          scriptCopy.exists());
        if (debugLogsExist) {
          Assert.assertTrue("Directory info file size is 0",
            directorInfo.length() > 0);
          Assert.assertTrue("Size of copy of launch script is 0",
            scriptCopy.length() > 0);
        }
      }
    } finally {
      // cleanup
      if (shellFile != null && shellFile.exists()) {
        shellFile.delete();
      }
      if (tempFile != null && tempFile.exists()) {
        tempFile.delete();
      }
    }
  }

  /**
   * Test container launch fault.
   * @throws Exception
   */
  @Test
  public void testContainerLaunchOnConfigurationError() throws Exception {
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler handler = mock(EventHandler.class);
    when(dispatcher.getEventHandler()).thenReturn(handler);
    Application app = mock(Application.class);
    ApplicationId appId = mock(ApplicationId.class);
    when(appId.toString()).thenReturn("1");
    when(app.getAppId()).thenReturn(appId);
    Container container = mock(Container.class);
    ContainerId id = mock(ContainerId.class);
    when(id.toString()).thenReturn("1");
    when(container.getContainerId()).thenReturn(id);
    when(container.getUser()).thenReturn("user");
    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    when(clc.getCommands()).thenReturn(Lists.newArrayList());
    when(container.getLaunchContext()).thenReturn(clc);
    Credentials credentials = mock(Credentials.class);
    when(container.getCredentials()).thenReturn(credentials);

    // Configuration errors should result in node shutdown...
    ContainerExecutor returnConfigError = mock(ContainerExecutor.class);
    when(returnConfigError.launchContainer(any())).
        thenThrow(new ConfigurationException("Mock configuration error"));
    ContainerLaunch launchConfigError = new ContainerLaunch(
        distContext, conf, dispatcher,
        returnConfigError, app, container, dirsHandler, containerManager);
    NodeStatusUpdater updater = mock(NodeStatusUpdater.class);
    distContext.setNodeStatusUpdater(updater);
    launchConfigError.call();
    verify(updater, atLeastOnce()).reportException(any());

    // ... any other error should continue.
    ContainerExecutor returnOtherError = mock(ContainerExecutor.class);
    when(returnOtherError.launchContainer(any())).
        thenThrow(new IOException("Mock configuration error"));
    ContainerLaunch launchOtherError = new ContainerLaunch(
        distContext, conf, dispatcher,
        returnOtherError, app, container, dirsHandler, containerManager);
    NodeStatusUpdater updaterNoCall = mock(NodeStatusUpdater.class);
    distContext.setNodeStatusUpdater(updaterNoCall);
    launchOtherError.call();
    verify(updaterNoCall, never()).reportException(any());

  }

  /**
   * Test that script exists with non-zero exit code when command fails.
   * @throws IOException
   */
  @Test
  public void testShellScriptBuilderStdOutandErrRedirection() throws IOException {
    ShellScriptBuilder builder = ShellScriptBuilder.create();

    Path logDir = new Path(localLogDir.getAbsolutePath());
    File stdout = new File(logDir.toString(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT);
    File stderr = new File(logDir.toString(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR);

    builder.stdout(logDir, ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT);
    builder.stderr(logDir, ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR);

    //should redirect to specified stdout path
    String TEST_STDOUT_ECHO = "Test stdout redirection";
    builder.echo(TEST_STDOUT_ECHO);
    //should fail and redirect to stderr
    builder.mkdir(new Path("/invalidSrcDir"));

    builder.command(Arrays.asList(new String[] {"unknownCommand"}));

    File shellFile = Shell.appendScriptExtension(tmpDir, "testShellScriptBuilderStdOutandErrRedirection");
    PrintStream writer = new PrintStream(new FileOutputStream(shellFile));
    builder.write(writer);
    writer.close();
    try {
      FileUtil.setExecutable(shellFile, true);

      Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(
          new String[]{shellFile.getAbsolutePath()}, tmpDir);
      try {
        shexc.execute();
        fail("builder shell command was expected to throw");
      }
      catch(IOException e) {
        // expected
        System.out.println("Received an expected exception: " + e.getMessage());

        Assert.assertEquals(true, stdout.exists());
        BufferedReader stdoutReader = new BufferedReader(new FileReader(stdout));
        // Get the pid of the process
        String line = stdoutReader.readLine().trim();
        Assert.assertEquals(TEST_STDOUT_ECHO, line);
        // No more lines
        Assert.assertEquals(null, stdoutReader.readLine());
        stdoutReader.close();

        Assert.assertEquals(true, stderr.exists());
        Assert.assertTrue(stderr.length() > 0);
      }
    }
    finally {
      FileUtil.fullyDelete(shellFile);
      FileUtil.fullyDelete(stdout);
      FileUtil.fullyDelete(stderr);
    }
  }

  /**
   * Test that script exists with non-zero exit code when command fails.
   * @throws IOException
   */
  @Test
  public void testShellScriptBuilderWithNoRedirection() throws IOException {
    ShellScriptBuilder builder = ShellScriptBuilder.create();

    Path logDir = new Path(localLogDir.getAbsolutePath());
    File stdout = new File(logDir.toString(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT);
    File stderr = new File(logDir.toString(), ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR);

    //should redirect to specified stdout path
    String TEST_STDOUT_ECHO = "Test stdout redirection";
    builder.echo(TEST_STDOUT_ECHO);
    //should fail and redirect to stderr
    builder.mkdir(new Path("/invalidSrcDir"));

    builder.command(Arrays.asList(new String[]{"unknownCommand"}));

    File shellFile = Shell.appendScriptExtension(tmpDir, "testShellScriptBuilderStdOutandErrRedirection");
    PrintStream writer = new PrintStream(new FileOutputStream(shellFile));
    builder.write(writer);
    writer.close();
    try {
      FileUtil.setExecutable(shellFile, true);

      Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(
          new String[]{shellFile.getAbsolutePath()}, tmpDir);
      try {
        shexc.execute();
        fail("builder shell command was expected to throw");
      } catch (IOException e) {
        // expected
        System.out.println("Received an expected exception: " + e.getMessage());

        Assert.assertEquals(false, stdout.exists());
        Assert.assertEquals(false, stderr.exists());
      }
    } finally {
      FileUtil.fullyDelete(shellFile);
    }
  }
  /*
   * ${foo.version} is substituted to suffix a specific version number
   */
  @Test
  public void testInvalidEnvVariableSubstitutionType1() throws IOException {
    Map<String, String> env = new HashMap<String, String>();
    // invalid env
    env.put("testVar", "version${foo.version}");
    validateShellExecutorForDifferentEnvs(env);
  }

  /*
   * Multiple paths are substituted in a path variable
   */
  @Test
  public void testInvalidEnvVariableSubstitutionType2() throws IOException {
    Map<String, String> env = new HashMap<String, String>();
    // invalid env
    env.put("testPath", "/abc:/${foo.path}:/$bar");
    validateShellExecutorForDifferentEnvs(env);
  }

  private void validateShellExecutorForDifferentEnvs(Map<String, String> env)
      throws IOException {
    File shellFile = null;
    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      Map<Path, List<String>> resources = new HashMap<Path, List<String>>();
      FileOutputStream fos = new FileOutputStream(shellFile);
      FileUtil.setExecutable(shellFile, true);

      List<String> commands = new ArrayList<String>();
      DefaultContainerExecutor executor = new DefaultContainerExecutor();
      executor.setConf(new Configuration());
      executor.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), user);
      fos.flush();
      fos.close();

      // It is supposed that LANG is set as C.
      Map<String, String> cmdEnv = new HashMap<String, String>();
      cmdEnv.put("LANG", "C");
      Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(
          new String[] { shellFile.getAbsolutePath() }, tmpDir, cmdEnv);
      try {
        shexc.execute();
        Assert.fail("Should catch exception");
      } catch (ExitCodeException e) {
        Assert.assertTrue(shexc.getExitCode() != 0);
      }
    } finally {
      // cleanup
      if (shellFile != null && shellFile.exists()) {
        shellFile.delete();
      }
    }
  }

  @Test
  public void testValidEnvVariableSubstitution() throws IOException  {
    File shellFile = null;
    try {
      shellFile = Shell.appendScriptExtension(tmpDir, "hello");
      Map<Path, List<String>> resources =
          new HashMap<Path, List<String>>();
      FileOutputStream fos = new FileOutputStream(shellFile);
      FileUtil.setExecutable(shellFile, true);

      Map<String, String> env = new LinkedHashMap<String, String>();
      // valid env
      env.put(
          "foo", "2.4.6" );
      env.put(
          "testVar", "version${foo}" );
      List<String> commands = new ArrayList<String>();
      DefaultContainerExecutor executor = new DefaultContainerExecutor();
      Configuration execConf = new Configuration();
      execConf.setBoolean(YarnConfiguration.NM_LOG_CONTAINER_DEBUG_INFO, false);
      executor.setConf(execConf);
      executor.writeLaunchEnv(fos, env, resources, commands,
          new Path(localLogDir.getAbsolutePath()), user);
      fos.flush();
      fos.close();

      // It is supposed that LANG is set as C.
      Map<String, String> cmdEnv = new HashMap<String, String>();
      cmdEnv.put("LANG", "C");
      Shell.ShellCommandExecutor shexc
      = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()},
        tmpDir, cmdEnv);
      try {
        shexc.execute();
      } catch(ExitCodeException e){
        Assert.fail("Should not catch exception");
      }
      Assert.assertTrue(shexc.getExitCode() == 0);
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
    }
  }
}
