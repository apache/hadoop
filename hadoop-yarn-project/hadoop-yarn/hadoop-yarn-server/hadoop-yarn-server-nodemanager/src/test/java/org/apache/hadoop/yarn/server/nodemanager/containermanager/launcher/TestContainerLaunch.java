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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Before;
import org.junit.Test;

public class TestContainerLaunch extends BaseContainerManagerTest {

  public TestContainerLaunch() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    conf.setClass(
        YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
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

      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
      fos.flush();
      fos.close();
      FileUtil.setExecutable(tempFile, true);

      Shell.ShellCommandExecutor shexc 
      = new Shell.ShellCommandExecutor(new String[]{tempFile.getAbsolutePath()}, tmpDir);

      shexc.execute();
      assertEquals(shexc.getExitCode(), 0);
      assert(shexc.getOutput().contains("hello"));

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
      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
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
      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
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
        diagnostics = e.getMessage();
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
      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
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

    ContainerId cId = ContainerId.newInstance(appAttemptId, 0);
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
    containerLaunchContext.setEnvironment(userSetEnv);

    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
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
      for (String serviceName : containerManager.getAuxServiceMetaData()
          .keySet()) {
        fileWriter.println("@echo %" + AuxiliaryServiceHelper.NM_AUX_SERVICE
            + serviceName + "%>> "
            + processStartFile);
      }
      fileWriter.println("@echo " + cId + ">> " + processStartFile);
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
      for (String serviceName : containerManager.getAuxServiceMetaData()
          .keySet()) {
        fileWriter.write("\necho $" + AuxiliaryServiceHelper.NM_AUX_SERVICE
            + serviceName + " >> "
            + processStartFile);
      }
      fileWriter.write("\necho $$ >> " + processStartFile);
      fileWriter.write("\nexec sleep 100");
    }
    fileWriter.close();

    // upload the script file so that the container can run it
    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
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
          createContainerToken(cId));
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
        new BufferedReader(new FileReader(processStartFile));
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
    int expectedExitCode = Shell.WINDOWS ? ExitCode.FORCE_KILLED.getExitCode() :
      ExitCode.TERMINATED.getExitCode();
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
    ContainerId cId = ContainerId.newInstance(appAttemptId, 0);
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
        ConverterUtils.getYarnUrlFromPath(localFS
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
    Token containerToken = createContainerToken(cId);

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
    Assert.assertEquals(ExitCode.FORCE_KILLED.getExitCode(),
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

  @Test
  public void testDelayedKill() throws Exception {
    internalKillTest(true);
  }

  @Test
  public void testImmediateKill() throws Exception {
    internalKillTest(false);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCallFailureWithNullLocalizedResources() {
    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(ContainerId.newInstance(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(
            System.currentTimeMillis(), 1), 1), 1));
    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    when(clc.getCommands()).thenReturn(Collections.<String>emptyList());
    when(container.getLaunchContext()).thenReturn(clc);
    when(container.getLocalizedResources()).thenReturn(null);
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = new EventHandler() {
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

  protected Token createContainerToken(ContainerId cId) throws InvalidToken {
    Resource r = BuilderUtils.newResource(1024, 1);
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, context.getNodeId().toString(), user,
          r, System.currentTimeMillis() + 10000L, 123, DUMMY_RM_IDENTIFIER);
    Token containerToken =
        BuilderUtils.newContainerToken(
          context.getNodeId(),
          context.getContainerTokenSecretManager().retrievePassword(
            containerTokenIdentifier), containerTokenIdentifier);
    return containerToken;
  }

}
