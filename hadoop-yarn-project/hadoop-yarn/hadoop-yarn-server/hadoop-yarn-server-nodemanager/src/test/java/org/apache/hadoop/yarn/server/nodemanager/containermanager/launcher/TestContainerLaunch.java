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

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import junit.framework.Assert;

public class TestContainerLaunch extends BaseContainerManagerTest {

  public TestContainerLaunch() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    conf.setClass(
        YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    conf.setLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS, 1000);
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

  /**
   * See if environment variable is forwarded using sanitizeEnv.
   * @throws Exception
   */
  @Test (timeout = 5000)
  public void testContainerEnvVariables() throws Exception {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    Container mockContainer = mock(Container.class);
    // ////// Construct the Container-id
    ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(0);
    appId.setId(0);
    ApplicationAttemptId appAttemptId = 
        recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(1);
    ContainerId cId = 
        recordFactory.newRecordInstance(ContainerId.class);
    cId.setApplicationAttemptId(appAttemptId);
    when(mockContainer.getId()).thenReturn(cId);

    when(mockContainer.getNodeId()).thenReturn(context.getNodeId());
    when(mockContainer.getNodeHttpAddress()).thenReturn(
        context.getNodeId().getHost() + ":12345");
    when(mockContainer.getRMIdentifer()).thenReturn(super.DUMMY_RM_IDENTIFIER);

    Map<String, String> userSetEnv = new HashMap<String, String>();
    userSetEnv.put(Environment.CONTAINER_ID.name(), "user_set_container_id");
    userSetEnv.put(Environment.NM_HOST.name(), "user_set_NM_HOST");
    userSetEnv.put(Environment.NM_PORT.name(), "user_set_NM_PORT");
    userSetEnv.put(Environment.NM_HTTP_PORT.name(), "user_set_NM_HTTP_PORT");
    userSetEnv.put(Environment.LOCAL_DIRS.name(), "user_set_LOCAL_DIR");
    containerLaunchContext.setUser(user);
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
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    when(mockContainer.getResource()).thenReturn(
        BuilderUtils.newResource(1024, 1));
    StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    startRequest.setContainer(mockContainer);
    containerManager.startContainer(startRequest);

    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    // Now verify the contents of the file
    List<String> localDirs = dirsHandler.getLocalDirs();
    List<Path> appDirs = new ArrayList<Path>(localDirs.size());
    for (String localDir : localDirs) {
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, user);
      Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      appDirs.add(new Path(appsdir, appId.toString()));
    }
    BufferedReader reader =
        new BufferedReader(new FileReader(processStartFile));
    Assert.assertEquals(cId.toString(), reader.readLine());
    Assert.assertEquals(mockContainer.getNodeId().getHost(),
        reader.readLine());
    Assert.assertEquals(String.valueOf(mockContainer.getNodeId().getPort()),
        reader.readLine());
    Assert.assertEquals(
        String.valueOf(mockContainer.getNodeHttpAddress().split(":")[1]),
        reader.readLine());
    Assert.assertEquals(StringUtils.join(",", appDirs), reader.readLine());

    Assert.assertEquals(cId.toString(), containerLaunchContext
        .getEnvironment().get(Environment.CONTAINER_ID.name()));
    Assert.assertEquals(mockContainer.getNodeId().getHost(),
        containerLaunchContext.getEnvironment()
        .get(Environment.NM_HOST.name()));
    Assert.assertEquals(String.valueOf(mockContainer.getNodeId().getPort()),
        containerLaunchContext.getEnvironment().get(
            Environment.NM_PORT.name()));
    Assert.assertEquals(
        mockContainer.getNodeHttpAddress().split(":")[1],
        containerLaunchContext.getEnvironment().get(
            Environment.NM_HTTP_PORT.name()));
    Assert.assertEquals(StringUtils.join(",", appDirs), containerLaunchContext
        .getEnvironment().get(Environment.LOCAL_DIRS.name()));
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

    StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
    stopRequest.setContainerId(cId);
    containerManager.stopContainer(stopRequest);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    GetContainerStatusRequest gcsRequest = 
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    gcsRequest.setContainerId(cId);
    ContainerStatus containerStatus = 
        containerManager.getContainerStatus(gcsRequest).getStatus();
    int expectedExitCode = Shell.WINDOWS ? ExitCode.FORCE_KILLED.getExitCode() :
      ExitCode.TERMINATED.getExitCode();
    Assert.assertEquals(expectedExitCode, containerStatus.getExitStatus());

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
      DefaultContainerExecutor.containerIsAlive(pid));
  }

  @Test
  public void testDelayedKill() throws Exception {
    containerManager.start();

    Container mockContainer = mock(Container.class);
    // ////// Construct the Container-id
    ApplicationId appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(1);
    appId.setId(1);
    ApplicationAttemptId appAttemptId = 
        recordFactory.newRecordInstance(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(1);
    ContainerId cId = 
        recordFactory.newRecordInstance(ContainerId.class);
    cId.setApplicationAttemptId(appAttemptId);

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
    when(mockContainer.getId()).thenReturn(cId);
    when(mockContainer.getNodeId()).thenReturn(context.getNodeId());
    when(mockContainer.getNodeHttpAddress()).thenReturn(
        context.getNodeId().getHost() + ":12345");
    when(mockContainer.getRMIdentifer()).thenReturn(super.DUMMY_RM_IDENTIFIER);

    containerLaunchContext.setUser(user);

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
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    when(mockContainer.getResource()).thenReturn(
        BuilderUtils.newResource(1024, 1));
    StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    startRequest.setContainer(mockContainer);
    containerManager.startContainer(startRequest);

    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    // Now test the stop functionality.
    StopContainerRequest stopRequest = recordFactory.newRecordInstance(StopContainerRequest.class);
    stopRequest.setContainerId(cId);
    containerManager.stopContainer(stopRequest);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    // container stop sends a sigterm followed by a sigkill
    GetContainerStatusRequest gcsRequest = 
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
    gcsRequest.setContainerId(cId);
    ContainerStatus containerStatus = 
        containerManager.getContainerStatus(gcsRequest).getStatus();
    Assert.assertEquals(ExitCode.FORCE_KILLED.getExitCode(),
        containerStatus.getExitStatus());

    // Now verify the contents of the file.  Script generates a message when it
    // receives a sigterm so we look for that.  We cannot perform this check on
    // Windows, because the process is not notified when killed by winutils.
    // There is no way for the process to trap and respond.  Instead, we can
    // verify that the job object with ID matching container ID no longer exists.
    if (Shell.WINDOWS) {
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

}
