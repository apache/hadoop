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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
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
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Before;
import org.junit.Test;
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
    String badSymlink = "foo@zz%_#*&!-+= bar()";
    File symLinkFile = null;

    try {
      shellFile = new File(tmpDir, "hello.sh");
      tempFile = new File(tmpDir, "temp.sh");
      String timeoutCommand = "echo \"hello\"";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));    
      shellFile.setExecutable(true);
      writer.println(timeoutCommand);
      writer.close();

      Map<Path, String> resources = new HashMap<Path, String>();
      Path path = new Path(shellFile.getAbsolutePath());
      resources.put(path, badSymlink);

      FileOutputStream fos = new FileOutputStream(tempFile);

      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      commands.add("/bin/sh ./\\\"" + badSymlink + "\\\"");

      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
      fos.flush();
      fos.close();
      tempFile.setExecutable(true);

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
  
  // this is a dirty hack - but should be ok for a unittest.
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void setNewEnvironmentHack(Map<String, String> newenv) throws Exception {
    Class[] classes = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object obj = field.get(env);
        Map<String, String> map = (Map<String, String>) obj;
        map.clear();
        map.putAll(newenv);
      }
    }
  }

  /**
   * See if environment variable is forwarded using sanitizeEnv.
   * @throws Exception
   */
  @Test
  public void testContainerEnvVariables() throws Exception {
    containerManager.start();

    Map<String, String> envWithDummy = new HashMap<String, String>();
    envWithDummy.putAll(System.getenv());
    envWithDummy.put(Environment.MALLOC_ARENA_MAX.name(), "99");
    setNewEnvironmentHack(envWithDummy);

    String malloc = System.getenv(Environment.MALLOC_ARENA_MAX.name());
    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "env_vars.txt").getAbsoluteFile();
    fileWriter.write("\numask 0"); // So that start file is readable by the test
    fileWriter.write("\necho $" + Environment.MALLOC_ARENA_MAX.name() + " > " + processStartFile);
    fileWriter.write("\necho $$ >> " + processStartFile);
    fileWriter.write("\nexec sleep 100");
    fileWriter.close();

    assert(malloc != null && !"".equals(malloc));

    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

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
    containerLaunchContext.setContainerId(cId);

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
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);

    // set up the rest of the container
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/bash");
    commands.add(scriptFile.getAbsolutePath());
    containerLaunchContext.setCommands(commands);
    containerLaunchContext.setResource(recordFactory
        .newRecordInstance(Resource.class));
    containerLaunchContext.getResource().setMemory(1024);
    StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    containerManager.startContainer(startRequest);

    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(processStartFile));
    Assert.assertEquals(malloc, reader.readLine());
    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    // Now test the stop functionality.

    // Assert that the process is alive
    Assert.assertTrue("Process is not alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));
    // Once more
    Assert.assertTrue("Process is not alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));

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
    Assert.assertEquals(ExitCode.TERMINATED.getExitCode(),
        containerStatus.getExitStatus());

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        exec.signalContainer(user,
            pid, Signal.NULL));
  }

  @Test
  public void testDelayedKill() throws Exception {
    containerManager.start();

    File processStartFile =
        new File(tmpDir, "pid.txt").getAbsoluteFile();

    // setup a script that can handle sigterm gracefully
    File scriptFile = new File(tmpDir, "testscript.sh");
    PrintWriter writer = new PrintWriter(new FileOutputStream(scriptFile));
    writer.println("#!/bin/bash\n\n");
    writer.println("echo \"Running testscript for delayed kill\"");
    writer.println("hello=\"Got SIGTERM\"");
    writer.println("umask 0");
    writer.println("trap \"echo $hello >> " + processStartFile + "\" SIGTERM");
    writer.println("echo \"Writing pid to start file\"");
    writer.println("echo $$ >> " + processStartFile);
    writer.println("while true; do\nsleep 1s;\ndone");
    writer.close();
    scriptFile.setExecutable(true);

    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

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
    containerLaunchContext.setContainerId(cId);

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
    List<String> commands = new ArrayList<String>();
    commands.add(scriptFile.getAbsolutePath());
    containerLaunchContext.setCommands(commands);
    containerLaunchContext.setResource(recordFactory
        .newRecordInstance(Resource.class));
    containerLaunchContext.getResource().setMemory(1024);
    StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
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

    // Now verify the contents of the file
    // Script generates a message when it receives a sigterm
    // so we look for that
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
