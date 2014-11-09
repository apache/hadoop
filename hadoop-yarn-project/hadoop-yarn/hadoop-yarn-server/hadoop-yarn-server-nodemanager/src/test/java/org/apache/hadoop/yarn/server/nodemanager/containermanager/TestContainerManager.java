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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
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
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestAuxServices.ServiceA;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestContainerManager extends BaseContainerManagerTest {

  public TestContainerManager() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LogFactory.getLog(TestContainerManager.class);
  }
  
  @Override
  @Before
  public void setup() throws IOException {
    super.setup();
  }

  private ContainerId createContainerId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, id);
    return containerId;
  }
  
  @Override
  protected ContainerManagerImpl
      createContainerManager(DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
      metrics, new ApplicationACLsManager(conf), dirsHandler) {
      @Override
      public void
          setBlockNewContainerRequests(boolean blockNewContainerRequests) {
        // do nothing
      }

      @Override
      protected UserGroupInformation getRemoteUgi() throws YarnException {
        ApplicationId appId = ApplicationId.newInstance(0, 0);
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, 1);
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
        ugi.addTokenIdentifier(new NMTokenIdentifier(appAttemptId, context
          .getNodeId(), user, context.getNMTokenSecretManager().getCurrentKey()
          .getKeyId()));
        return ugi;
      }

      @Override
      protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
          Container container, boolean stopRequest, NMTokenIdentifier identifier) throws YarnException {
        if(container == null || container.getUser().equals("Fail")){
          throw new YarnException("Reject this container");
        }
      }
    };
  }
  
  @Test
  public void testContainerManagerInitialization() throws IOException {

    containerManager.start();

    InetAddress localAddr = InetAddress.getLocalHost();
    String fqdn = localAddr.getCanonicalHostName();
    if (!localAddr.getHostAddress().equals(fqdn)) {
      // only check if fqdn is not same as ip
      // api returns ip in case of resolution failure
      Assert.assertEquals(fqdn, context.getNodeId().getHost());
    }
  
    // Just do a query for a non-existing container.
    boolean throwsException = false;
    try {
      List<ContainerId> containerIds = new ArrayList<ContainerId>();
      ContainerId id =createContainerId(0);
      containerIds.add(id);
      GetContainerStatusesRequest request =
          GetContainerStatusesRequest.newInstance(containerIds);
      GetContainerStatusesResponse response =
          containerManager.getContainerStatuses(request);
      if(response.getFailedRequests().containsKey(id)){
        throw response.getFailedRequests().get(id).deSerialize();
      }
    } catch (Throwable e) {
      throwsException = true;
    }
    Assert.assertTrue(throwsException);
  }

  @Test
  public void testContainerSetup() throws Exception {

    containerManager.start();

    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, "file");
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);

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

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

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
    ContainerId cId = createContainerId(0);

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

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          createContainerToken(cId,
            DUMMY_RM_IDENTIFIER, context.getNodeId(), user,
            context.getContainerTokenSecretManager()));
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

  private void testContainerLaunchAndExit(int exitCode) throws IOException,
      InterruptedException, YarnException {

	  File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
	  PrintWriter fileWriter = new PrintWriter(scriptFile);
	  File processStartFile =
			  new File(tmpDir, "start_file.txt").getAbsoluteFile();

	  // ////// Construct the Container-id
	  ContainerId cId = createContainerId(0);

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

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

	  BaseContainerManagerTest.waitForContainerState(containerManager, cId,
			  ContainerState.COMPLETE);

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
	  ContainerStatus containerStatus = 
			  containerManager.getContainerStatuses(gcsRequest).getContainerStatuses().get(0);

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
    ContainerId cId = createContainerId(0);
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

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

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
        .asList(new ApplicationId[] { appId }), CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));

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
      InterruptedException, YarnException {
    containerManager.start();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    ContainerId cId1 = createContainerId(0);
    ContainerId cId2 = createContainerId(0);
    containerLaunchContext
      .setLocalResources(new HashMap<String, LocalResource>());

    // Construct the Container with Invalid RMIdentifier
    StartContainerRequest startRequest1 =
        StartContainerRequest.newInstance(containerLaunchContext,
          createContainerToken(cId1,
            ResourceManagerConstants.RM_INVALID_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(startRequest1);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);
    
    boolean catchException = false;
    try {
      StartContainersResponse response = containerManager.startContainers(allRequests);
      if(response.getFailedRequests().containsKey(cId1)) {
        throw response.getFailedRequests().get(cId1).deSerialize();
      }
    } catch (Throwable e) {
      e.printStackTrace();
      catchException = true;
      Assert.assertTrue(e.getMessage().contains(
        "Container " + cId1 + " rejected as it is allocated by a previous RM"));
      Assert.assertTrue(e.getClass().getName()
        .equalsIgnoreCase(InvalidContainerException.class.getName()));
    }

    // Verify that startContainer fail because of invalid container request
    Assert.assertTrue(catchException);

    // Construct the Container with a RMIdentifier within current RM
    StartContainerRequest startRequest2 =
        StartContainerRequest.newInstance(containerLaunchContext,
          createContainerToken(cId2,
            DUMMY_RM_IDENTIFIER, context.getNodeId(), user,
            context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list2 = new ArrayList<StartContainerRequest>();
    list.add(startRequest2);
    StartContainersRequest allRequests2 =
        StartContainersRequest.newInstance(list2);
    containerManager.startContainers(allRequests2);
    
    boolean noException = true;
    try {
      containerManager.startContainers(allRequests2);
    } catch (YarnException e) {
      noException = false;
    }
    // Verify that startContainer get no YarnException
    Assert.assertTrue(noException);
  }

  @Test
  public void testMultipleContainersLaunch() throws Exception {
    containerManager.start();

    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    for (int i = 0; i < 10; i++) {
      ContainerId cId = createContainerId(i);
      long identifier = 0;
      if ((i & 1) == 0)
        // container with even id fail
        identifier = ResourceManagerConstants.RM_INVALID_IDENTIFIER;
      else
        identifier = DUMMY_RM_IDENTIFIER;
      Token containerToken =
          createContainerToken(cId, identifier, context.getNodeId(), user,
            context.getContainerTokenSecretManager());
      StartContainerRequest request =
          StartContainerRequest.newInstance(containerLaunchContext,
            containerToken);
      list.add(request);
    }
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(list);

    StartContainersResponse response =
        containerManager.startContainers(requestList);

    Assert.assertEquals(5, response.getSuccessfullyStartedContainers().size());
    for (ContainerId id : response.getSuccessfullyStartedContainers()) {
      // Containers with odd id should succeed.
      Assert.assertEquals(1, id.getContainerId() & 1);
    }
    Assert.assertEquals(5, response.getFailedRequests().size());
    for (Map.Entry<ContainerId, SerializedException> entry : response
      .getFailedRequests().entrySet()) {
      // Containers with even id should fail.
      Assert.assertEquals(0, entry.getKey().getContainerId() & 1);
      Assert.assertTrue(entry.getValue().getMessage()
        .contains(
          "Container " + entry.getKey() + " rejected as it is allocated by a previous RM"));
    }
  }

  @Test
  public void testMultipleContainersStopAndGetStatus() throws Exception {
    containerManager.start();
    List<StartContainerRequest> startRequest =
        new ArrayList<StartContainerRequest>();
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    for (int i = 0; i < 10; i++) {
      ContainerId cId = createContainerId(i);
      String user = null;
      if ((i & 1) == 0) {
        // container with even id fail
        user = "Fail";
      } else {
        user = "Pass";
      }
      Token containerToken =
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
      StartContainerRequest request =
          StartContainerRequest.newInstance(containerLaunchContext,
            containerToken);
      startRequest.add(request);
      containerIds.add(cId);
    }
    // start containers
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);
    containerManager.startContainers(requestList);

    // Get container statuses
    GetContainerStatusesRequest statusRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    GetContainerStatusesResponse statusResponse =
        containerManager.getContainerStatuses(statusRequest);
    Assert.assertEquals(5, statusResponse.getContainerStatuses().size());
    for (ContainerStatus status : statusResponse.getContainerStatuses()) {
      // Containers with odd id should succeed
      Assert.assertEquals(1, status.getContainerId().getContainerId() & 1);
    }
    Assert.assertEquals(5, statusResponse.getFailedRequests().size());
    for (Map.Entry<ContainerId, SerializedException> entry : statusResponse
      .getFailedRequests().entrySet()) {
      // Containers with even id should fail.
      Assert.assertEquals(0, entry.getKey().getContainerId() & 1);
      Assert.assertTrue(entry.getValue().getMessage()
        .contains("Reject this container"));
    }

    // stop containers
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    StopContainersResponse stopResponse =
        containerManager.stopContainers(stopRequest);
    Assert.assertEquals(5, stopResponse.getSuccessfullyStoppedContainers()
      .size());
    for (ContainerId id : stopResponse.getSuccessfullyStoppedContainers()) {
      // Containers with odd id should succeed.
      Assert.assertEquals(1, id.getContainerId() & 1);
    }
    Assert.assertEquals(5, stopResponse.getFailedRequests().size());
    for (Map.Entry<ContainerId, SerializedException> entry : stopResponse
      .getFailedRequests().entrySet()) {
      // Containers with even id should fail.
      Assert.assertEquals(0, entry.getKey().getContainerId() & 1);
      Assert.assertTrue(entry.getValue().getMessage()
        .contains("Reject this container"));
    }
  }

  @Test
  public void testStartContainerFailureWithUnknownAuxService() throws Exception {
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] { "existService" });
    conf.setClass(
        String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "existService"),
        ServiceA.class, Service.class);
    containerManager.start();

    List<StartContainerRequest> startRequest =
        new ArrayList<StartContainerRequest>();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();
    String serviceName = "non_exist_auxService";
    serviceData.put(serviceName, ByteBuffer.wrap(serviceName.getBytes()));
    containerLaunchContext.setServiceData(serviceData);

    ContainerId cId = createContainerId(0);
    String user = "start_container_fail";
    Token containerToken =
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
    StartContainerRequest request =
        StartContainerRequest.newInstance(containerLaunchContext,
            containerToken);

    // start containers
    startRequest.add(request);
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);

    StartContainersResponse response =
        containerManager.startContainers(requestList);
    Assert.assertTrue(response.getFailedRequests().size() == 1);
    Assert.assertTrue(response.getSuccessfullyStartedContainers().size() == 0);
    Assert.assertTrue(response.getFailedRequests().containsKey(cId));
    Assert.assertTrue(response.getFailedRequests().get(cId).getMessage()
        .contains("The auxService:" + serviceName + " does not exist"));
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager)
      throws IOException {
    return createContainerToken(cId, rmIdentifier, nodeId, user,
      containerTokenSecretManager, null);
  }

  public static Token createContainerToken(ContainerId cId, long rmIdentifier,
      NodeId nodeId, String user,
      NMContainerTokenSecretManager containerTokenSecretManager,
      LogAggregationContext logAggregationContext)
      throws IOException {
    Resource r = BuilderUtils.newResource(1024, 1);
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(cId, nodeId.toString(), user, r,
          System.currentTimeMillis() + 100000L, 123, rmIdentifier,
          Priority.newInstance(0), 0, logAggregationContext);
    Token containerToken =
        BuilderUtils
          .newContainerToken(nodeId, containerTokenSecretManager
            .retrievePassword(containerTokenIdentifier),
            containerTokenIdentifier);
    return containerToken;
  }
}
