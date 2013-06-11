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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.util.Shell;
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
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Test;

public class TestContainerManager extends BaseContainerManagerTest {

  public TestContainerManager() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LogFactory.getLog(TestContainerManager.class);
  }

  private ContainerId createContainerId() {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 0);
    return containerId;
  }
  
  @Test
  public void testContainerManagerInitialization() throws IOException {

    containerManager.start();

    // Just do a query for a non-existing container.
    boolean throwsException = false;
    try {
      GetContainerStatusRequest request = 
          recordFactory.newRecordInstance(GetContainerStatusRequest.class);
      ContainerId cId = createContainerId();
      request.setContainerId(cId);
      containerManager.getContainerStatus(request);
    } catch (YarnException e) {
      throwsException = true;
    }
    Assert.assertTrue(throwsException);
  }

  @Test
  public void testContainerSetup() throws IOException, InterruptedException,
      YarnException {

    containerManager.start();

    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, "file");
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    // ////// Construct the Container-id
    ContainerId cId = createContainerId();

    // ////// Construct the container-spec.
    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
            .makeQualified(new Path(file.getAbsolutePath())));
    LocalResource rsrc_alpha = recordFactory.newRecordInstance(LocalResource.class);    
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(file.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);
    Resource r = BuilderUtils.newResource(512, 1);
    int port = 12345;
    Token containerToken =
        BuilderUtils.newContainerToken(cId, context.getNodeId().getHost(),
          port, user, r, System.currentTimeMillis() + 10000L, 123,
          "password".getBytes(), super.DUMMY_RM_IDENTIFIER);
    StartContainerRequest startRequest = 
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    startRequest.setContainerToken(containerToken);

    containerManager.startContainer(startRequest);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    // Now ascertain that the resources are localised correctly.
    ApplicationId appId = cId.getApplicationAttemptId().getApplicationId();
    String appIDStr = ConverterUtils.toString(appId);
    String containerIDStr = ConverterUtils.toString(cId);
    File userCacheDir = new File(localDir, ContainerLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ContainerLocalizer.APPCACHE);
    File appDir = new File(appCache, appIDStr);
    File containerDir = new File(appDir, containerIDStr);
    File targetFile = new File(containerDir, destinationFile);
    File sysDir =
        new File(localDir,
            ResourceLocalizationService.NM_PRIVATE_DIR);
    File appSysDir = new File(sysDir, appIDStr);
    File containerSysDir = new File(appSysDir, containerIDStr);

    for (File f : new File[] { localDir, sysDir, userCacheDir, appDir,
        appSysDir,
        containerDir, containerSysDir }) {
      Assert.assertTrue(f.getAbsolutePath() + " doesn't exist!!", f.exists());
      Assert.assertTrue(f.getAbsolutePath() + " is not a directory!!",
          f.isDirectory());
    }
    Assert.assertTrue(targetFile.getAbsolutePath() + " doesn't exist!!",
        targetFile.exists());

    // Now verify the contents of the file
    BufferedReader reader = new BufferedReader(new FileReader(targetFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    Assert.assertEquals(null, reader.readLine());
  }

  @Test
  public void testContainerLaunchAndStop() throws IOException,
      InterruptedException, YarnException {
    containerManager.start();

    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "start_file.txt").getAbsoluteFile();

    // ////// Construct the Container-id
    ContainerId cId = createContainerId();

    if (Shell.WINDOWS) {
      fileWriter.println("@echo Hello World!> " + processStartFile);
      fileWriter.println("@echo " + cId + ">> " + processStartFile);
      fileWriter.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      fileWriter.write("\numask 0"); // So that start file is readable by the test
      fileWriter.write("\necho Hello World! > " + processStartFile);
      fileWriter.write("\necho $$ >> " + processStartFile);
      fileWriter.write("\nexec sleep 100");
    }
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext = 
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

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
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    Resource r = BuilderUtils.newResource(100, 1);
    int port = 12345;
    Token containerToken =
        BuilderUtils.newContainerToken(cId, context.getNodeId().getHost(),
          port, user, r, System.currentTimeMillis() + 10000L, 123,
          "password".getBytes(), super.DUMMY_RM_IDENTIFIER);

    StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    startRequest.setContainerToken(containerToken);
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
    Assert.assertEquals("Hello World!", reader.readLine());
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
  
  private void testContainerLaunchAndExit(int exitCode) throws IOException,
      InterruptedException, YarnException {

	  File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
	  PrintWriter fileWriter = new PrintWriter(scriptFile);
	  File processStartFile =
			  new File(tmpDir, "start_file.txt").getAbsoluteFile();

	  // ////// Construct the Container-id
	  ContainerId cId = createContainerId();

	  if (Shell.WINDOWS) {
	    fileWriter.println("@echo Hello World!> " + processStartFile);
	    fileWriter.println("@echo " + cId + ">> " + processStartFile);
	    if (exitCode != 0) {
	      fileWriter.println("@exit " + exitCode);
	    }
	  } else {
	    fileWriter.write("\numask 0"); // So that start file is readable by the test
	    fileWriter.write("\necho Hello World! > " + processStartFile);
	    fileWriter.write("\necho $$ >> " + processStartFile); 
	    // Have script throw an exit code at the end
	    if (exitCode != 0) {
	      fileWriter.write("\nexit "+exitCode);
	    }
	  }
	  
	  fileWriter.close();

	  ContainerLaunchContext containerLaunchContext = 
			  recordFactory.newRecordInstance(ContainerLaunchContext.class);

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
	  List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
	  containerLaunchContext.setCommands(commands);
    Resource r = BuilderUtils.newResource(100, 1);
    int port = 12345;
    Token containerToken =
        BuilderUtils.newContainerToken(cId, context.getNodeId().getHost(),
          port, user, r, System.currentTimeMillis() + 10000L, 123,
          "password".getBytes(), super.DUMMY_RM_IDENTIFIER);
	  StartContainerRequest startRequest = recordFactory.newRecordInstance(StartContainerRequest.class);
	  startRequest.setContainerLaunchContext(containerLaunchContext);
	  startRequest.setContainerToken(containerToken);
	  containerManager.startContainer(startRequest);

	  BaseContainerManagerTest.waitForContainerState(containerManager, cId,
			  ContainerState.COMPLETE);

	  GetContainerStatusRequest gcsRequest = 
			  recordFactory.newRecordInstance(GetContainerStatusRequest.class);
	  gcsRequest.setContainerId(cId);
	  ContainerStatus containerStatus = 
			  containerManager.getContainerStatus(gcsRequest).getStatus();

	  // Verify exit status matches exit state of script
	  Assert.assertEquals(exitCode,
			  containerStatus.getExitStatus());	    
  }
  
  @Test
  public void testContainerLaunchAndExitSuccess() throws IOException,
      InterruptedException, YarnException {
	  containerManager.start();
	  int exitCode = 0; 

	  // launch context for a command that will return exit code 0 
	  // and verify exit code returned 
	  testContainerLaunchAndExit(exitCode);	  
  }

  @Test
  public void testContainerLaunchAndExitFailure() throws IOException,
      InterruptedException, YarnException {
	  containerManager.start();
	  int exitCode = 50; 

	  // launch context for a command that will return exit code 0 
	  // and verify exit code returned 
	  testContainerLaunchAndExit(exitCode);	  
  }
  
  @Test
  public void testLocalFilesCleanup() throws InterruptedException,
      IOException, YarnException {
    // Real del service
    delSrvc = new DeletionService(exec);
    delSrvc.init(conf);

    containerManager = createContainerManager(delSrvc);
    containerManager.init(conf);
    containerManager.start();

    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, "file");
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    // ////// Construct the Container-id
    ContainerId cId = createContainerId();
    ApplicationId appId = cId.getApplicationAttemptId().getApplicationId();

    // ////// Construct the container-spec.
    ContainerLaunchContext containerLaunchContext = recordFactory.newRecordInstance(ContainerLaunchContext.class);
//    containerLaunchContext.resources =
//        new HashMap<CharSequence, LocalResource>();
    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(FileContext.getLocalFSFileContext()
            .makeQualified(new Path(file.getAbsolutePath())));
    LocalResource rsrc_alpha = recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(file.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);
    Resource r = BuilderUtils.newResource(100, 1);
    int port = 12345;

    Token containerToken =
        BuilderUtils.newContainerToken(cId, context.getNodeId().getHost(),
          port, user, r, System.currentTimeMillis() + 10000L, 123,
          "password".getBytes(), super.DUMMY_RM_IDENTIFIER);
    StartContainerRequest request = recordFactory.newRecordInstance(StartContainerRequest.class);
    request.setContainerLaunchContext(containerLaunchContext);
    request.setContainerToken(containerToken);
    containerManager.startContainer(request);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE);

    BaseContainerManagerTest.waitForApplicationState(containerManager, 
        cId.getApplicationAttemptId().getApplicationId(),
        ApplicationState.RUNNING);

    // Now ascertain that the resources are localised correctly.
    String appIDStr = ConverterUtils.toString(appId);
    String containerIDStr = ConverterUtils.toString(cId);
    File userCacheDir = new File(localDir, ContainerLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ContainerLocalizer.APPCACHE);
    File appDir = new File(appCache, appIDStr);
    File containerDir = new File(appDir, containerIDStr);
    File targetFile = new File(containerDir, destinationFile);
    File sysDir =
        new File(localDir,
            ResourceLocalizationService.NM_PRIVATE_DIR);
    File appSysDir = new File(sysDir, appIDStr);
    File containerSysDir = new File(appSysDir, containerIDStr);
    // AppDir should still exist
    Assert.assertTrue("AppDir " + appDir.getAbsolutePath()
        + " doesn't exist!!", appDir.exists());
    Assert.assertTrue("AppSysDir " + appSysDir.getAbsolutePath()
        + " doesn't exist!!", appSysDir.exists());
    for (File f : new File[] { containerDir, containerSysDir }) {
      Assert.assertFalse(f.getAbsolutePath() + " exists!!", f.exists());
    }
    Assert.assertFalse(targetFile.getAbsolutePath() + " exists!!",
        targetFile.exists());

    // Simulate RM sending an AppFinish event.
    containerManager.handle(new CMgrCompletedAppsEvent(Arrays
        .asList(new ApplicationId[] { appId })));

    BaseContainerManagerTest.waitForApplicationState(containerManager, 
        cId.getApplicationAttemptId().getApplicationId(),
        ApplicationState.FINISHED);

    // Now ascertain that the resources are localised correctly.
    for (File f : new File[] { appDir, containerDir, appSysDir,
        containerSysDir }) {
      // Wait for deletion. Deletion can happen long after AppFinish because of
      // the async DeletionService
      int timeout = 0;
      while (f.exists() && timeout++ < 15) {
        Thread.sleep(1000);
      }
      Assert.assertFalse(f.getAbsolutePath() + " exists!!", f.exists());
    }
    // Wait for deletion
    int timeout = 0;
    while (targetFile.exists() && timeout++ < 15) {
      Thread.sleep(1000);
    }
    Assert.assertFalse(targetFile.getAbsolutePath() + " exists!!",
        targetFile.exists());
  }

  @Test
  public void testContainerLaunchFromPreviousRM() throws IOException,
      InterruptedException {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    String host = "127.0.0.1";
    int port = 1234;
    ContainerId cId1 = createContainerId();
    ContainerId cId2 = createContainerId();
    containerLaunchContext
      .setLocalResources(new HashMap<String, LocalResource>());
    Resource mockResource = BuilderUtils.newResource(1024, 1);

    // Construct the Container with Invalid RMIdentifier
    StartContainerRequest startRequest1 =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest1.setContainerLaunchContext(containerLaunchContext);
    
    Token containerToken1 =
        BuilderUtils.newContainerToken(cId1, host, port, user, mockResource,
          System.currentTimeMillis() + 10000, 123, "password".getBytes(), 
          (long) ResourceManagerConstants.RM_INVALID_IDENTIFIER);
    startRequest1.setContainerToken(containerToken1);
    boolean catchException = false;
    try {
      containerManager.startContainer(startRequest1);
    } catch (YarnException e) {
      catchException = true;
      Assert.assertTrue(e.getMessage().contains(
        "Container " + cId1 + " rejected as it is allocated by a previous RM"));
      Assert.assertEquals(InvalidContainerException.class.getName(), e
        .getClass().getName());
    }

    // Verify that startContainer fail because of invalid container request
    Assert.assertTrue(catchException);

    // Construct the Container with a RMIdentifier within current RM
    StartContainerRequest startRequest2 =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest2.setContainerLaunchContext(containerLaunchContext);
    Token containerToken2 =
        BuilderUtils.newContainerToken(cId1, host, port, user, mockResource,
          System.currentTimeMillis() + 10000, 123, "password".getBytes(),
          super.DUMMY_RM_IDENTIFIER);
    startRequest2.setContainerToken(containerToken2);
    boolean noException = true;
    try {
      containerManager.startContainer(startRequest2);
    } catch (YarnException e) {
      noException = false;
    }
    // Verify that startContainer get no YarnException
    Assert.assertTrue(noException);
  }
}
