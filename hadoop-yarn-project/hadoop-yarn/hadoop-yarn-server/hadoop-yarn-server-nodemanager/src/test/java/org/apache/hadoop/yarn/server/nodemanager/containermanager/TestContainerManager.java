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

import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetContainerStatusesRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StartContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.StopContainersRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ResourceManagerConstants;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrSignalContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.ContainerStateTransitionListener;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestAuxServices.ServiceA;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.slf4j.LoggerFactory;

public class TestContainerManager extends BaseContainerManagerTest {

  public TestContainerManager() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LoggerFactory.getLogger(TestContainerManager.class);
  }

  private static class Listener implements ContainerStateTransitionListener {

    private final Map<ContainerId,
        List<org.apache.hadoop.yarn.server.nodemanager.containermanager.
            container.ContainerState>> states = new HashMap<>();
    private final Map<ContainerId, List<ContainerEventType>> events =
        new HashMap<>();

    @Override
    public void init(Context context) {}

    @Override
    public void preTransition(ContainerImpl op,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState beforeState,
        ContainerEvent eventToBeProcessed) {
      if (!states.containsKey(op.getContainerId())) {
        states.put(op.getContainerId(), new ArrayList<>());
        states.get(op.getContainerId()).add(beforeState);
        events.put(op.getContainerId(), new ArrayList<>());
      }
    }

    @Override
    public void postTransition(ContainerImpl op,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState beforeState,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState afterState,
        ContainerEvent processedEvent) {
      states.get(op.getContainerId()).add(afterState);
      events.get(op.getContainerId()).add(processedEvent.getType());
    }
  }

  private boolean delayContainers = false;

  @Override
  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec = new DefaultContainerExecutor() {
      @Override
      public int launchContainer(ContainerStartContext ctx)
          throws IOException, ConfigurationException {
        if (delayContainers) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            // Nothing..
          }
        }
        return super.launchContainer(ctx);
      }
    };
    exec.setConf(conf);
    return spy(exec);
  }
  
  @Override
  protected ContainerManagerImpl
      createContainerManager(DeletionService delSrvc) {
    return  new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
      metrics, dirsHandler) {

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
      List<ContainerId> containerIds = new ArrayList<>();
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
        URL.fromPath(localFS
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
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE, 40);

    // Now ascertain that the resources are localised correctly.
    ApplicationId appId = cId.getApplicationAttemptId().getApplicationId();
    String appIDStr = appId.toString();
    String containerIDStr = cId.toString();
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

  @Test (timeout = 10000L)
  public void testAuxPathHandler() throws Exception {
    File testDir = GenericTestUtils
        .getTestDir(TestContainerManager.class.getSimpleName() + "LocDir");
    testDir.mkdirs();
    File testFile = new File(testDir, "test");
    testFile.createNewFile();
    YarnConfiguration configuration = new YarnConfiguration();
    configuration.set(YarnConfiguration.NM_LOCAL_DIRS,
        testDir.getAbsolutePath());
    LocalDirsHandlerService spyDirHandlerService =
        Mockito.spy(new LocalDirsHandlerService());
    spyDirHandlerService.init(configuration);
    when(spyDirHandlerService.getConfig()).thenReturn(configuration);
    AuxiliaryLocalPathHandler auxiliaryLocalPathHandler =
        new ContainerManagerImpl.AuxiliaryLocalPathHandlerImpl(
            spyDirHandlerService);
    Path p = auxiliaryLocalPathHandler.getLocalPathForRead("test");
    assertTrue(p != null &&
        !spyDirHandlerService.getLocalDirsForRead().isEmpty());

    when(spyDirHandlerService.getLocalDirsForRead()).thenReturn(
        new ArrayList<String>());
    try {
      auxiliaryLocalPathHandler.getLocalPathForRead("test");
      fail("Should not have passed!");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Could not find"));
    } finally {
      testFile.delete();
      testDir.delete();
    }
  }

  //@Test
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
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          createContainerToken(cId,
            DUMMY_RM_IDENTIFIER, context.getNodeId(), user,
            context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
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

    List<ContainerId> containerIds = new ArrayList<>();
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

  @Test
  public void testContainerRestart() throws IOException, InterruptedException,
      YarnException {
    containerManager.start();
    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    String pid = prepareInitialContainer(cId, oldStartFile);

    // Test that the container can restart
    // Also, Since there was no rollback context present before the
    // restart, rollback should NOT be possible after the restart
    doRestartTests(cId, oldStartFile, "Hello World!", pid, false);
  }

  private String doRestartTests(ContainerId cId, File oldStartFile,
      String testString, String pid, boolean canRollback)
      throws YarnException, IOException, InterruptedException {
    int beforeRestart = metrics.getRunningContainers();
    Container container =
        containerManager.getContext().getContainers().get(cId);
    Assert.assertFalse(container.isReInitializing());
    containerManager.restartContainer(cId);
    Assert.assertTrue(container.isReInitializing());

    // Wait for original process to die and the new process to restart
    int timeoutSecs = 0;
    while (DefaultContainerExecutor.containerIsAlive(pid)
        && (metrics.getRunningContainers() == beforeRestart)
        && container.isReInitializing()
        && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for Original process to die.." +
          "and new process to start!!");
    }

    Assert.assertFalse("Old Process Still alive!!",
        DefaultContainerExecutor.containerIsAlive(pid));

    String newPid = null;
    timeoutSecs = 0;
    while (timeoutSecs++ < 20) {
      LOG.info("Waiting for New process file to be created!!");
      // Now verify the contents of the file
      BufferedReader reader =
          new BufferedReader(new FileReader(oldStartFile));
      Assert.assertEquals(testString, reader.readLine());
      // Get the pid of the process
      newPid = reader.readLine().trim();
      // No more lines
      Assert.assertEquals(null, reader.readLine());
      reader.close();
      if (!newPid.equals(pid)) {
        break;
      }
      Thread.sleep(1000);
    }

    // Assert both pids are different
    Assert.assertNotEquals(pid, newPid);

    // Container cannot rollback from a restart
    Assert.assertEquals(canRollback, container.canRollback());

    return newPid;
  }

  private String[] testContainerReInitSuccess(boolean autoCommit)
      throws IOException, InterruptedException, YarnException {
    containerManager.start();
    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    String pid = prepareInitialContainer(cId, oldStartFile);

    File newStartFile = new File(tmpDir, "start_file_n.txt").getAbsoluteFile();

    ResourceUtilization beforeUpgrade =
        ResourceUtilization.newInstance(
            containerManager.getContainerScheduler().getCurrentUtilization());
    prepareContainerUpgrade(autoCommit, false, false, cId, newStartFile);
    ResourceUtilization afterUpgrade =
        ResourceUtilization.newInstance(
            containerManager.getContainerScheduler().getCurrentUtilization());
    Assert.assertEquals("Possible resource leak detected !!",
        beforeUpgrade, afterUpgrade);

    // Assert that the First process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(pid));

    BufferedReader reader =
        new BufferedReader(new FileReader(newStartFile));
    Assert.assertEquals("Upgrade World!", reader.readLine());

    // Get the pid of the process
    String newPid = reader.readLine().trim();
    Assert.assertNotEquals("Old and New Pids must be different !", pid, newPid);
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    reader.close();

    // Verify old file still exists and is accessible by
    // the new process...
    reader = new BufferedReader(new FileReader(oldStartFile));
    Assert.assertEquals("Hello World!", reader.readLine());

    // Assert that the New process is alive
    Assert.assertTrue("New Process is not alive!",
        DefaultContainerExecutor.containerIsAlive(newPid));
    return new String[]{pid, newPid};
  }

  @Test
  public void testContainerUpgradeSuccessAutoCommit() throws IOException,
      InterruptedException, YarnException {
    Listener listener = new Listener();
    ((NodeManager.DefaultContainerStateListener)containerManager.context.
        getContainerStateTransitionListener()).addListener(listener);
    testContainerReInitSuccess(true);
    // Should not be able to Commit (since already auto committed)
    try {
      containerManager.commitLastReInitialization(createContainerId(0));
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Nothing to Commit"));
    }

    List<org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
        ContainerState> containerStates =
        listener.states.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.NEW,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.LOCALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING), containerStates);

    List<ContainerEventType> containerEventTypes =
        listener.events.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        ContainerEventType.INIT_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.REINITIALIZE_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        ContainerEventType.CONTAINER_LAUNCHED), containerEventTypes);
  }

  @Test
  public void testContainerUpgradeSuccessExplicitCommit() throws IOException,
      InterruptedException, YarnException {
    testContainerReInitSuccess(false);
    ContainerId cId = createContainerId(0);
    containerManager.commitLastReInitialization(cId);
    // Should not be able to Rollback once committed
    try {
      containerManager.rollbackLastReInitialization(cId);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Nothing to rollback to"));
    }
  }

  @Test
  public void testContainerUpgradeSuccessExplicitRollback() throws IOException,
      InterruptedException, YarnException {
    Listener listener = new Listener();
    ((NodeManager.DefaultContainerStateListener)containerManager.context.
        getContainerStateTransitionListener()).addListener(listener);
    String[] pids = testContainerReInitSuccess(false);

    // Test that the container can be Restarted after the successful upgrrade.
    // Also, since there is a rollback context present before the restart, it
    // should be possible to rollback the container AFTER the restart.
    pids[1] = doRestartTests(createContainerId(0),
        new File(tmpDir, "start_file_n.txt").getAbsoluteFile(),
        "Upgrade World!", pids[1], true);

    // Delete the old start File..
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    oldStartFile.delete();

    ContainerId cId = createContainerId(0);
    // Explicit Rollback
    containerManager.rollbackLastReInitialization(cId);

    Container container =
        containerManager.getContext().getContainers().get(cId);
    Assert.assertTrue(container.isReInitializing());
    // Original should be dead anyway
    Assert.assertFalse("Original Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(pids[0]));

    // Wait for new container to startup
    int timeoutSecs = 0;
    while (container.isReInitializing() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for ReInitialization to complete..");
    }
    Assert.assertFalse(container.isReInitializing());

    timeoutSecs = 0;
    // Wait for new processStartfile to be created
    while (!oldStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for New process start-file to be created");
    }

    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(oldStartFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    // Get the pid of the process
    String rolledBackPid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    Assert.assertNotEquals("The Rolled-back process should be a different pid",
        pids[0], rolledBackPid);

    List<org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
        ContainerState> containerStates =
        listener.states.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.NEW,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.LOCALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        // This is the successful restart
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        // This is the rollback
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING), containerStates);

    List<ContainerEventType> containerEventTypes =
        listener.events.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        ContainerEventType.INIT_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.REINITIALIZE_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.REINITIALIZE_CONTAINER,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.ROLLBACK_REINIT,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        ContainerEventType.CONTAINER_LAUNCHED), containerEventTypes);
  }

  @Test
  public void testContainerUpgradeLocalizationFailure() throws IOException,
      InterruptedException, YarnException {
    if (Shell.WINDOWS) {
      return;
    }
    containerManager.start();
    Listener listener = new Listener();
    ((NodeManager.DefaultContainerStateListener)containerManager.context.
        getContainerStateTransitionListener()).addListener(listener);
    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    String pid = prepareInitialContainer(cId, oldStartFile);

    File newStartFile = new File(tmpDir, "start_file_n.txt").getAbsoluteFile();

    prepareContainerUpgrade(false, true, true, cId, newStartFile);

    // Assert that the First process is STILL alive
    // since upgrade was terminated..
    Assert.assertTrue("Process is NOT alive!",
        DefaultContainerExecutor.containerIsAlive(pid));

    List<org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
        ContainerState> containerStates =
        listener.states.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.NEW,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.LOCALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING), containerStates);

    List<ContainerEventType> containerEventTypes =
        listener.events.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        ContainerEventType.INIT_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.REINITIALIZE_CONTAINER,
        ContainerEventType.RESOURCE_FAILED), containerEventTypes);
  }

  @Test
  public void testContainerUpgradeProcessFailure() throws IOException,
      InterruptedException, YarnException {
    if (Shell.WINDOWS) {
      return;
    }
    containerManager.start();
    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    String pid = prepareInitialContainer(cId, oldStartFile);

    File newStartFile = new File(tmpDir, "start_file_n.txt").getAbsoluteFile();

    // Since Autocommit is true, there is also no rollback context...
    // which implies that if the new process fails, since there is no
    // rollback, it is terminated.
    prepareContainerUpgrade(true, true, false, cId, newStartFile);

    // Assert that the First process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(pid));
  }

  @Test
  public void testContainerUpgradeRollbackDueToFailure() throws IOException,
      InterruptedException, YarnException {
    if (Shell.WINDOWS) {
      return;
    }
    containerManager.start();
    Listener listener = new Listener();
    ((NodeManager.DefaultContainerStateListener)containerManager.context.
        getContainerStateTransitionListener()).addListener(listener);
    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);
    File oldStartFile = new File(tmpDir, "start_file_o.txt").getAbsoluteFile();

    String pid = prepareInitialContainer(cId, oldStartFile);

    File newStartFile = new File(tmpDir, "start_file_n.txt").getAbsoluteFile();

    prepareContainerUpgrade(false, true, false, cId, newStartFile);

    // Assert that the First process is not alive anymore
    Assert.assertFalse("Original Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(pid));

    int timeoutSecs = 0;
    // Wait for oldStartFile to be created
    while (!oldStartFile.exists() && timeoutSecs++ < 20) {
      System.out.println("\nFiles: " +
          Arrays.toString(oldStartFile.getParentFile().list()));
      Thread.sleep(1000);
      LOG.info("Waiting for New process start-file to be created");
    }

    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(oldStartFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    // Get the pid of the process
    String rolledBackPid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    Assert.assertNotEquals("The Rolled-back process should be a different pid",
        pid, rolledBackPid);

    List<org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
        ContainerState> containerStates =
        listener.states.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.NEW,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.LOCALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.REINITIALIZING_AWAITING_KILL,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.SCHEDULED,
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.
            ContainerState.RUNNING), containerStates);

    List<ContainerEventType> containerEventTypes =
        listener.events.get(createContainerId(0));
    Assert.assertEquals(Arrays.asList(
        ContainerEventType.INIT_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.REINITIALIZE_CONTAINER,
        ContainerEventType.RESOURCE_LOCALIZED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        ContainerEventType.CONTAINER_LAUNCHED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_LAUNCHED), containerEventTypes);
  }

  /**
   * Prepare a launch Context for container upgrade and request the
   * Container Manager to re-initialize a running container using the
   * new launch context.
   * @param autoCommit Enable autoCommit.
   * @param failCmd injects a start script that intentionally fails.
   * @param failLoc injects a bad file Location that will fail localization.
   */
  private void prepareContainerUpgrade(boolean autoCommit, boolean failCmd,
      boolean failLoc, ContainerId cId, File startFile)
      throws FileNotFoundException, YarnException, InterruptedException {
    // Re-write scriptfile and processStartFile
    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile_new");
    PrintWriter fileWriter = new PrintWriter(scriptFile);

    writeScriptFile(fileWriter, "Upgrade World!", startFile, cId, failCmd);

    ContainerLaunchContext containerLaunchContext =
        prepareContainerLaunchContext(scriptFile, "dest_file_new", failLoc, 0);

    containerManager.reInitializeContainer(cId, containerLaunchContext,
        autoCommit);
    try {
      containerManager.reInitializeContainer(cId, containerLaunchContext,
          autoCommit);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Cannot perform RE_INIT"));
    }
    int timeoutSecs = 0;
    int maxTimeToWait = failLoc ? 10 : 20;
    // Wait for new processStartfile to be created
    while (!startFile.exists() && timeoutSecs++ < maxTimeToWait) {
      Thread.sleep(1000);
      LOG.info("Waiting for New process start-file to be created");
    }
  }

  /**
   * Prepare and start an initial container. This container will be subsequently
   * re-initialized for upgrade. It also waits for the container to start and
   * returns the Pid of the running container.
   */
  private String prepareInitialContainer(ContainerId cId, File startFile)
      throws IOException, YarnException, InterruptedException {
    File scriptFileOld = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriterOld = new PrintWriter(scriptFileOld);

    writeScriptFile(fileWriterOld, "Hello World!", startFile, cId, false);

    ContainerLaunchContext containerLaunchContext =
        prepareContainerLaunchContext(scriptFileOld, "dest_file", false, 4);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
            createContainerToken(cId,
                DUMMY_RM_IDENTIFIER, context.getNodeId(), user,
                context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

    int timeoutSecs = 0;
    while (!startFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        startFile.exists());

    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(startFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    // Assert that the process is alive
    Assert.assertTrue("Process is not alive!",
        DefaultContainerExecutor.containerIsAlive(pid));
    // Once more
    Assert.assertTrue("Process is not alive!",
        DefaultContainerExecutor.containerIsAlive(pid));
    return pid;
  }

  private void writeScriptFile(PrintWriter fileWriter, String startLine,
      File processStartFile, ContainerId cId, boolean isFailure) {
    if (Shell.WINDOWS) {
      fileWriter.println("@echo " + startLine + "> " + processStartFile);
      fileWriter.println("@echo " + cId + ">> " + processStartFile);
      fileWriter.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      fileWriter.write("\numask 0"); // So that start file is readable by test
      if (isFailure) {
        // Echo PID and throw some error code
        fileWriter.write("\necho $$ >> " + processStartFile);
        fileWriter.write("\nexit 111");
      } else {
        fileWriter.write("\necho " + startLine + " > " + processStartFile);
        fileWriter.write("\necho $$ >> " + processStartFile);
        fileWriter.write("\nexec sleep 100");
      }
    }
    fileWriter.close();
  }

  private ContainerLaunchContext prepareContainerLaunchContext(File scriptFile,
      String destFName, boolean putBadFile, int numRetries) {
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    URL resourceAlpha = null;
    if (putBadFile) {
      File fileToDelete = new File(tmpDir, "fileToDelete")
          .getAbsoluteFile();
      resourceAlpha =
          URL.fromPath(localFS
              .makeQualified(new Path(fileToDelete.getAbsolutePath())));
      fileToDelete.delete();
    } else {
      resourceAlpha =
          URL.fromPath(localFS
              .makeQualified(new Path(scriptFile.getAbsolutePath())));
    }
    LocalResource rsrcAlpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrcAlpha.setResource(resourceAlpha);
    rsrcAlpha.setSize(-1);
    rsrcAlpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrcAlpha.setType(LocalResourceType.FILE);
    rsrcAlpha.setTimestamp(scriptFile.lastModified());
    Map<String, LocalResource> localResources = new HashMap<>();
    localResources.put(destFName, rsrcAlpha);
    containerLaunchContext.setLocalResources(localResources);

    ContainerRetryContext containerRetryContext = ContainerRetryContext
        .newInstance(
            ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES,
            new HashSet<>(Arrays.asList(Integer.valueOf(111))), numRetries, 0);
    containerLaunchContext.setContainerRetryContext(containerRetryContext);
    List<String> commands = Arrays.asList(
        Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    return containerLaunchContext;
  }

  protected void testContainerLaunchAndExit(int exitCode) throws IOException,
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
	  List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
	  containerLaunchContext.setCommands(commands);

    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
          containerLaunchContext,
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);

	  BaseContainerManagerTest.waitForContainerState(containerManager, cId,
			  ContainerState.COMPLETE);

    List<ContainerId> containerIds = new ArrayList<>();
    containerIds.add(cId);
    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerStatus containerStatus = containerManager.
        getContainerStatuses(gcsRequest).getContainerStatuses().get(0);

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

  private Map<String, LocalResource> setupLocalResources(String fileName,
      String symLink) throws Exception {
    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, fileName);
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    URL resourceURL = URL.fromPath(FileContext.getLocalFSFileContext()
        .makeQualified(new Path(file.getAbsolutePath())));
    LocalResource resource =
        recordFactory.newRecordInstance(LocalResource.class);
    resource.setResource(resourceURL);
    resource.setSize(-1);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    resource.setType(LocalResourceType.FILE);
    resource.setTimestamp(file.lastModified());
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put(symLink, resource);
    return localResources;
  }

  // Start the container
  // While the container is running, localize new resources.
  // Verify the symlink is created properly
  @Test
  public void testLocalingResourceWhileContainerRunning() throws Exception {
    // Real del service
    delSrvc = new DeletionService(exec);
    delSrvc.init(conf);

    ((NodeManager.NMContext)context).setContainerExecutor(exec);
    containerManager = createContainerManager(delSrvc);
    containerManager.init(conf);
    containerManager.start();
    // set up local resources
    Map<String, LocalResource> localResource =
        setupLocalResources("file", "symLink1");
    ContainerLaunchContext context =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    context.setLocalResources(localResource);

    // a long running container - sleep
    context.setCommands(Arrays.asList("sleep 6"));
    ContainerId cId = createContainerId(0);

    // start the container
    StartContainerRequest scRequest = StartContainerRequest.newInstance(context,
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, this.context.getNodeId(),
            user, this.context.getContainerTokenSecretManager()));
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(Arrays.asList(scRequest));
    containerManager.startContainers(allRequests);
    BaseContainerManagerTest
        .waitForContainerState(containerManager, cId, ContainerState.RUNNING);

    BaseContainerManagerTest.waitForApplicationState(containerManager,
        cId.getApplicationAttemptId().getApplicationId(),
        ApplicationState.RUNNING);
    checkResourceLocalized(cId, "symLink1");

    // Localize new local resources while container is running
    Map<String, LocalResource> localResource2 =
        setupLocalResources("file2", "symLink2");

    ResourceLocalizationRequest request =
        ResourceLocalizationRequest.newInstance(cId, localResource2);
    containerManager.localize(request);

    // Verify resource is localized and symlink is created.
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        try {
          checkResourceLocalized(cId, "symLink2");
          return true;
        } catch (Throwable e) {
          return false;
        }
      }
    }, 500, 20000);

    BaseContainerManagerTest
        .waitForContainerState(containerManager, cId, ContainerState.COMPLETE);
    // Verify container cannot localize resources while at non-running state.
    try{
      containerManager.localize(request);
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().contains("Cannot perform LOCALIZE"));
    }
  }

  private void checkResourceLocalized(ContainerId containerId, String symLink) {
    String appId =
        containerId.getApplicationAttemptId().getApplicationId().toString();
    File userCacheDir = new File(localDir, ContainerLocalizer.USERCACHE);
    File userDir = new File(userCacheDir, user);
    File appCache = new File(userDir, ContainerLocalizer.APPCACHE);
    // localDir/usercache/nobody/appcache/application_0_0000
    File appDir = new File(appCache, appId);
    // localDir/usercache/nobody/appcache/application_0_0000/container_0_0000_01_000000
    File containerDir = new File(appDir, containerId.toString());
    // localDir/usercache/nobody/appcache/application_0_0000/container_0_0000_01_000000/symLink1
    File targetFile = new File(containerDir, symLink);

    File sysDir =
        new File(localDir, ResourceLocalizationService.NM_PRIVATE_DIR);
    // localDir/nmPrivate/application_0_0000
    File appSysDir = new File(sysDir, appId);
    // localDir/nmPrivate/application_0_0000/container_0_0000_01_000000
    File containerSysDir = new File(appSysDir, containerId.toString());

    Assert.assertTrue("AppDir " + appDir.getAbsolutePath() + " doesn't exist!!",
        appDir.exists());
    Assert.assertTrue(
        "AppSysDir " + appSysDir.getAbsolutePath() + " doesn't exist!!",
        appSysDir.exists());
    Assert.assertTrue(
        "containerDir " + containerDir.getAbsolutePath() + " doesn't exist !",
        containerDir.exists());
    Assert.assertTrue("containerSysDir " + containerSysDir.getAbsolutePath()
        + " doesn't exist !", containerDir.exists());
    Assert.assertTrue(
        "targetFile " + targetFile.getAbsolutePath() + " doesn't exist !!",
        targetFile.exists());
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
        URL.fromPath(FileContext.getLocalFSFileContext()
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
    List<StartContainerRequest> list = new ArrayList<>();
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
    String appIDStr = appId.toString();
    String containerIDStr = cId.toString();
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
    List<StartContainerRequest> list = new ArrayList<>();
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
    List<StartContainerRequest> list2 = new ArrayList<>();
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

    List<StartContainerRequest> list = new ArrayList<>();
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
          StartContainerRequest.newInstance(
              recordFactory.newRecordInstance(ContainerLaunchContext.class),
              containerToken);
      list.add(request);
    }
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(list);

    StartContainersResponse response =
        containerManager.startContainers(requestList);
    Thread.sleep(5000);

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
    List<StartContainerRequest> startRequest = new ArrayList<>();
    List<ContainerId> containerIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ContainerId cId;
      if ((i & 1) == 0) {
        // Containers with even id belong to an unauthorized app
        cId = createContainerId(i, 1);
      } else {
        cId = createContainerId(i, 0);
      }
      Token containerToken =
          createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
      StartContainerRequest request =
          StartContainerRequest.newInstance(
              recordFactory.newRecordInstance(ContainerLaunchContext.class),
              containerToken);
      startRequest.add(request);
      containerIds.add(cId);
    }
    // start containers
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);
    containerManager.startContainers(requestList);
    Thread.sleep(5000);

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
          .contains("attempted to get status for non-application container"));
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
          .contains("attempted to stop non-application container"));
    }
  }

  @Test
  public void testUnauthorizedRequests() throws IOException, YarnException {
    containerManager.start();

    // Create a containerId that belongs to an unauthorized appId
    ContainerId cId = createContainerId(0, 1);

    // startContainers()
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
            createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
                user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    StartContainersResponse startResponse =
        containerManager.startContainers(allRequests);

    Assert.assertFalse("Should not be authorized to start container",
        startResponse.getSuccessfullyStartedContainers().contains(cId));
    Assert.assertTrue("Start container request should fail",
        startResponse.getFailedRequests().containsKey(cId));

    // Insert the containerId into context, make it as if it is running
    ContainerTokenIdentifier containerTokenIdentifier =
        BuilderUtils.newContainerTokenIdentifier(scRequest.getContainerToken());
    Container container = new ContainerImpl(conf, null, containerLaunchContext,
        null, metrics, containerTokenIdentifier, context);
    context.getContainers().put(cId, container);

    // stopContainers()
    List<ContainerId> containerIds = new ArrayList<>();
    containerIds.add(cId);
    StopContainersRequest stopRequest =
        StopContainersRequest.newInstance(containerIds);
    StopContainersResponse stopResponse =
        containerManager.stopContainers(stopRequest);

    Assert.assertFalse("Should not be authorized to stop container",
        stopResponse.getSuccessfullyStoppedContainers().contains(cId));
    Assert.assertTrue("Stop container request should fail",
        stopResponse.getFailedRequests().containsKey(cId));

    // getContainerStatuses()
    containerIds = new ArrayList<>();
    containerIds.add(cId);
    GetContainerStatusesRequest request =
        GetContainerStatusesRequest.newInstance(containerIds);
    GetContainerStatusesResponse response =
        containerManager.getContainerStatuses(request);

    Assert.assertEquals("Should not be authorized to get container status",
        response.getContainerStatuses().size(), 0);
    Assert.assertTrue("Get status request should fail",
        response.getFailedRequests().containsKey(cId));
  }

  @Test
  public void testStartContainerFailureWithUnknownAuxService() throws Exception {
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES,
        new String[] { "existService" });
    conf.setClass(
        String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "existService"),
        ServiceA.class, Service.class);
    containerManager.start();

    List<StartContainerRequest> startRequest = new ArrayList<>();

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
    Assert.assertEquals(1, response.getFailedRequests().size());
    Assert.assertEquals(0, response.getSuccessfullyStartedContainers().size());
    Assert.assertTrue(response.getFailedRequests().containsKey(cId));
    Assert.assertTrue(response.getFailedRequests().get(cId).getMessage()
        .contains("The auxService:" + serviceName + " does not exist"));
  }

  /* Test added to verify fix in YARN-644 */
  @Test
  public void testNullTokens() throws Exception {
    ContainerManagerImpl cMgrImpl =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
        metrics, dirsHandler);
    String strExceptionMsg = "";
    try {
      cMgrImpl.authorizeStartAndResourceIncreaseRequest(
          null, new ContainerTokenIdentifier(), true);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeStartAndResourceIncreaseRequest(
          new NMTokenIdentifier(), null, true);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_CONTAINERTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeGetAndStopContainerRequest(null, null, true, null);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      cMgrImpl.authorizeUser(null, null);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    ContainerManagerImpl spyContainerMgr = spy(cMgrImpl);
    UserGroupInformation ugInfo = UserGroupInformation.createRemoteUser("a");
    Mockito.when(spyContainerMgr.getRemoteUgi()).thenReturn(ugInfo);
    Mockito.when(spyContainerMgr.
        selectNMTokenIdentifier(ugInfo)).thenReturn(null);

    strExceptionMsg = "";
    try {
      spyContainerMgr.stopContainers(new StopContainersRequestPBImpl());
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    strExceptionMsg = "";
    try {
      spyContainerMgr.getContainerStatuses(
          new GetContainerStatusesRequestPBImpl());
    } catch(YarnException ye) {
      strExceptionMsg = ye.getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_NMTOKEN_MSG);

    Mockito.doNothing().when(spyContainerMgr).authorizeUser(ugInfo, null);
    List<StartContainerRequest> reqList = new ArrayList<>();
    reqList.add(StartContainerRequest.newInstance(null, null));
    StartContainersRequest reqs = new StartContainersRequestPBImpl();
    reqs.setStartContainerRequests(reqList);
    strExceptionMsg = "";
    try {
      spyContainerMgr.startContainers(reqs);
    } catch(YarnException ye) {
      strExceptionMsg = ye.getCause().getMessage();
    }
    Assert.assertEquals(strExceptionMsg,
        ContainerManagerImpl.INVALID_CONTAINERTOKEN_MSG);
  }

  @Test
  public void testIncreaseContainerResourceWithInvalidRequests() throws Exception {
    containerManager.start();
    // Start 4 containers 0..4 with default resource (1024, 1)
    List<StartContainerRequest> list = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      ContainerId cId = createContainerId(i);
      long identifier = DUMMY_RM_IDENTIFIER;
      Token containerToken = createContainerToken(cId, identifier,
          context.getNodeId(), user, context.getContainerTokenSecretManager());
      StartContainerRequest request = StartContainerRequest.newInstance(
          recordFactory.newRecordInstance(ContainerLaunchContext.class),
          containerToken);
      list.add(request);
    }
    StartContainersRequest requestList = StartContainersRequest
        .newInstance(list);
    StartContainersResponse response = containerManager
        .startContainers(requestList);

    Assert.assertEquals(4, response.getSuccessfullyStartedContainers().size());
    int i = 0;
    for (ContainerId id : response.getSuccessfullyStartedContainers()) {
      Assert.assertEquals(i, id.getContainerId());
      i++;
    }

    Thread.sleep(2000);
    // Construct container resource increase request,
    List<Token> increaseTokens = new ArrayList<>();
    // Add increase request for container-0, the request will fail as the
    // container will have exited, and won't be in RUNNING state
    ContainerId cId0 = createContainerId(0);
    Token containerToken =
        createContainerToken(cId0, 1, DUMMY_RM_IDENTIFIER,
            context.getNodeId(), user,
                Resource.newInstance(1234, 3),
                    context.getContainerTokenSecretManager(), null);
    increaseTokens.add(containerToken);
    // Add increase request for container-7, the request will fail as the
    // container does not exist
    ContainerId cId7 = createContainerId(7);
    containerToken =
        createContainerToken(cId7, DUMMY_RM_IDENTIFIER,
            context.getNodeId(), user,
            Resource.newInstance(1234, 3),
            context.getContainerTokenSecretManager(), null);
    increaseTokens.add(containerToken);

    ContainerUpdateRequest updateRequest =
        ContainerUpdateRequest.newInstance(increaseTokens);
    ContainerUpdateResponse updateResponse =
        containerManager.updateContainer(updateRequest);
    // Check response
    Assert.assertEquals(
        1, updateResponse.getSuccessfullyUpdatedContainers().size());
    Assert.assertEquals(1, updateResponse.getFailedRequests().size());
    for (Map.Entry<ContainerId, SerializedException> entry : updateResponse
        .getFailedRequests().entrySet()) {
      Assert.assertNotNull("Failed message", entry.getValue().getMessage());
      if (cId7.equals(entry.getKey())) {
        Assert.assertTrue(entry.getValue().getMessage()
            .contains("Container " + cId7.toString()
                + " is not handled by this NodeManager"));
      } else {
        throw new YarnException("Received failed request from wrong"
            + " container: " + entry.getKey().toString());
      }
    }
  }

  @Test
  public void testChangeContainerResource() throws Exception {
    containerManager.start();
    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    // Construct the Container-id
    ContainerId cId = createContainerId(0);
    if (Shell.WINDOWS) {
      fileWriter.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      fileWriter.write("\numask 0");
      fileWriter.write("\nexec sleep 100");
    }
    fileWriter.close();
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
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
    List<String> commands =
        Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
            containerLaunchContext,
                createContainerToken(cId, DUMMY_RM_IDENTIFIER,
                    context.getNodeId(), user,
                        context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);
    // Make sure the container reaches RUNNING state
    BaseContainerManagerTest.waitForNMContainerState(containerManager, cId,
        org.apache.hadoop.yarn.server.nodemanager.
            containermanager.container.ContainerState.RUNNING);
    // Construct container resource increase request,
    List<Token> increaseTokens = new ArrayList<>();
    // Add increase request.
    Resource targetResource = Resource.newInstance(4096, 2);
    Token containerToken = createContainerToken(cId, 1, DUMMY_RM_IDENTIFIER,
        context.getNodeId(), user, targetResource,
            context.getContainerTokenSecretManager(), null);
    increaseTokens.add(containerToken);
    ContainerUpdateRequest updateRequest =
        ContainerUpdateRequest.newInstance(increaseTokens);
    ContainerUpdateResponse updateResponse =
        containerManager.updateContainer(updateRequest);
    Assert.assertEquals(
        1, updateResponse.getSuccessfullyUpdatedContainers().size());
    Assert.assertTrue(updateResponse.getFailedRequests().isEmpty());
    // Check status
    List<ContainerId> containerIds = new ArrayList<>();
    containerIds.add(cId);
    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerStatus containerStatus = containerManager
        .getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
    // Check status immediately as resource increase is blocking
    assertEquals(targetResource, containerStatus.getCapability());
    // Simulate a decrease request
    List<Token> decreaseTokens = new ArrayList<>();
    targetResource = Resource.newInstance(2048, 2);
    Token token = createContainerToken(cId, 2, DUMMY_RM_IDENTIFIER,
        context.getNodeId(), user, targetResource,
        context.getContainerTokenSecretManager(), null);
    decreaseTokens.add(token);
    updateRequest = ContainerUpdateRequest.newInstance(decreaseTokens);
    updateResponse = containerManager.updateContainer(updateRequest);

    Assert.assertEquals(
        1, updateResponse.getSuccessfullyUpdatedContainers().size());
    Assert.assertTrue(updateResponse.getFailedRequests().isEmpty());

    // Check status with retry
    containerStatus = containerManager
        .getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
    int retry = 0;
    while (!targetResource.equals(containerStatus.getCapability()) &&
        (retry++ < 5)) {
      Thread.sleep(200);
      containerStatus = containerManager.getContainerStatuses(gcsRequest)
          .getContainerStatuses().get(0);
    }
    assertEquals(targetResource, containerStatus.getCapability());
  }

  @Test
  public void testOutputThreadDumpSignal() throws IOException,
      InterruptedException, YarnException {
    testContainerLaunchAndSignal(SignalContainerCommand.OUTPUT_THREAD_DUMP);
  }

  @Test
  public void testGracefulShutdownSignal() throws IOException,
      InterruptedException, YarnException {
    testContainerLaunchAndSignal(SignalContainerCommand.GRACEFUL_SHUTDOWN);
  }

  @Test
  public void testForcefulShutdownSignal() throws IOException,
      InterruptedException, YarnException {
    testContainerLaunchAndSignal(SignalContainerCommand.FORCEFUL_SHUTDOWN);
  }

  // Verify signal container request can be delivered from
  // NodeStatusUpdaterImpl to ContainerExecutor.
  private void testContainerLaunchAndSignal(SignalContainerCommand command)
      throws IOException, InterruptedException, YarnException {

    Signal signal = ContainerLaunch.translateCommandToSignal(command);
    containerManager.start();

    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "start_file.txt").getAbsoluteFile();
    writeScriptFile(fileWriter, "Hello world!", processStartFile, null, false);

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // ////// Construct the Container-id
    ContainerId cId = createContainerId(0);

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
    List<String> commands =
        Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(
            containerLaunchContext,
            createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<>();
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

    // Simulate NodeStatusUpdaterImpl sending CMgrSignalContainersEvent
    SignalContainerRequest signalReq =
        SignalContainerRequest.newInstance(cId, command);
    List<SignalContainerRequest> reqs = new ArrayList<>();
    reqs.add(signalReq);
    containerManager.handle(new CMgrSignalContainersEvent(reqs));

    final ArgumentCaptor<ContainerSignalContext> signalContextCaptor =
        ArgumentCaptor.forClass(ContainerSignalContext.class);
    if (signal.equals(Signal.NULL)) {
      verify(exec, never()).signalContainer(signalContextCaptor.capture());
    } else {
      verify(exec, timeout(10000).atLeastOnce()).signalContainer(signalContextCaptor.capture());
      ContainerSignalContext signalContext = signalContextCaptor.getAllValues().get(0);
      Assert.assertEquals(cId, signalContext.getContainer().getContainerId());
      Assert.assertEquals(signal, signalContext.getSignal());
    }
  }

  @Test
  public void testStartContainerFailureWithInvalidLocalResource()
      throws Exception {
    containerManager.start();
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(null);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(System.currentTimeMillis());
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put("invalid_resource", rsrc_alpha);
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ContainerLaunchContext spyContainerLaunchContext =
        spy(containerLaunchContext);
    Mockito.when(spyContainerLaunchContext.getLocalResources())
        .thenReturn(localResources);

    ContainerId cId = createContainerId(0);
    String user = "start_container_fail";
    Token containerToken =
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
    StartContainerRequest request = StartContainerRequest
        .newInstance(spyContainerLaunchContext, containerToken);

    // start containers
    List<StartContainerRequest> startRequest =
        new ArrayList<StartContainerRequest>();
    startRequest.add(request);
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);

    StartContainersResponse response =
        containerManager.startContainers(requestList);
    Assert.assertTrue(response.getFailedRequests().size() == 1);
    Assert.assertTrue(response.getSuccessfullyStartedContainers().size() == 0);
    Assert.assertTrue(response.getFailedRequests().containsKey(cId));
    Assert.assertTrue(response.getFailedRequests().get(cId).getMessage()
        .contains("Null resource URL for local resource"));
  }

  @Test
  public void testStartContainerFailureWithNullTypeLocalResource()
      throws Exception {
    containerManager.start();
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(URL.fromPath(new Path("./")));
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(null);
    rsrc_alpha.setTimestamp(System.currentTimeMillis());
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put("null_type_resource", rsrc_alpha);
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ContainerLaunchContext spyContainerLaunchContext =
        spy(containerLaunchContext);
    Mockito.when(spyContainerLaunchContext.getLocalResources())
        .thenReturn(localResources);

    ContainerId cId = createContainerId(0);
    String user = "start_container_fail";
    Token containerToken =
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
    StartContainerRequest request = StartContainerRequest
        .newInstance(spyContainerLaunchContext, containerToken);

    // start containers
    List<StartContainerRequest> startRequest =
        new ArrayList<StartContainerRequest>();
    startRequest.add(request);
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);

    StartContainersResponse response =
        containerManager.startContainers(requestList);
    Assert.assertTrue(response.getFailedRequests().size() == 1);
    Assert.assertTrue(response.getSuccessfullyStartedContainers().size() == 0);
    Assert.assertTrue(response.getFailedRequests().containsKey(cId));
    Assert.assertTrue(response.getFailedRequests().get(cId).getMessage()
        .contains("Null resource type for local resource"));
  }

  @Test
  public void testStartContainerFailureWithNullVisibilityLocalResource()
      throws Exception {
    containerManager.start();
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(URL.fromPath(new Path("./")));
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(null);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(System.currentTimeMillis());
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put("null_visibility_resource", rsrc_alpha);
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    ContainerLaunchContext spyContainerLaunchContext =
        spy(containerLaunchContext);
    Mockito.when(spyContainerLaunchContext.getLocalResources())
        .thenReturn(localResources);

    ContainerId cId = createContainerId(0);
    String user = "start_container_fail";
    Token containerToken =
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, context.getNodeId(),
            user, context.getContainerTokenSecretManager());
    StartContainerRequest request = StartContainerRequest
        .newInstance(spyContainerLaunchContext, containerToken);

    // start containers
    List<StartContainerRequest> startRequest =
        new ArrayList<StartContainerRequest>();
    startRequest.add(request);
    StartContainersRequest requestList =
        StartContainersRequest.newInstance(startRequest);

    StartContainersResponse response =
        containerManager.startContainers(requestList);
    Assert.assertTrue(response.getFailedRequests().size() == 1);
    Assert.assertTrue(response.getSuccessfullyStartedContainers().size() == 0);
    Assert.assertTrue(response.getFailedRequests().containsKey(cId));
    Assert.assertTrue(response.getFailedRequests().get(cId).getMessage()
        .contains("Null resource visibility for local resource"));
  }
}
