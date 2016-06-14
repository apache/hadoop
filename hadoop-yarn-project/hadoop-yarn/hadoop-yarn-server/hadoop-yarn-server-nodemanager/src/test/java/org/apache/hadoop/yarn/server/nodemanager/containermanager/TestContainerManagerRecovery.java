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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Before;
import org.junit.Test;

public class TestContainerManagerRecovery extends BaseContainerManagerTest {

  public TestContainerManagerRecovery() throws UnsupportedFileSystemException {
    super();
  }

  @Override
  @Before
  public void setup() throws IOException {
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localFS.delete(new Path(localLogDir.getAbsolutePath()), true);
    localFS.delete(new Path(remoteLogDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    localLogDir.mkdir();
    remoteLogDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:"+ServerSocketUtil.getPort(49160, 10);
    conf.set(YarnConfiguration.NM_ADDRESS, bindAddress);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    // Default delSrvc
    delSrvc = createDeletionService();
    delSrvc.init(conf);
    exec = createContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    nodeHealthChecker = new NodeHealthCheckerService(
        NodeManager.getNodeHealthScriptRunner(conf), dirsHandler);
    nodeHealthChecker.init(conf);
  }

  @Test
  public void testApplicationRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "yarn_admin_user");
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    // add an application by starting a container
    String appUser = "app_user1";
    String modUser = "modify_user1";
    String viewUser = "view_user1";
    String enemyUser = "enemy_user";
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = Collections.emptyMap();
    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.MODIFY_APP, modUser);
    acls.put(ApplicationAccessType.VIEW_APP, viewUser);
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, containerCmds, serviceData,
        containerTokens, acls);
    // create the logAggregationContext
    LogAggregationContext logAggregationContext =
        LogAggregationContext.newInstance("includePattern", "excludePattern",
          "includePatternInRollingAggregation",
          "excludePatternInRollingAggregation");
   StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    Application app = context.getApplications().get(appId);
    assertNotNull(app);
    waitForAppState(app, ApplicationState.INITING);
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(modUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(enemyUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));

    // reset container manager and verify app recovered with proper acls
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);

    // check whether LogAggregationContext is recovered correctly
    LogAggregationContext recovered =
        ((ApplicationImpl) app).getLogAggregationContext();
    assertNotNull(recovered);
    assertEquals(logAggregationContext.getIncludePattern(),
      recovered.getIncludePattern());
    assertEquals(logAggregationContext.getExcludePattern(),
      recovered.getExcludePattern());
    assertEquals(logAggregationContext.getRolledLogsIncludePattern(),
      recovered.getRolledLogsIncludePattern());
    assertEquals(logAggregationContext.getRolledLogsExcludePattern(),
      recovered.getRolledLogsExcludePattern());

    waitForAppState(app, ApplicationState.INITING);
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(modUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(enemyUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));

    // simulate application completion
    List<ApplicationId> finishedApps = new ArrayList<ApplicationId>();
    finishedApps.add(appId);
    cm.handle(new CMgrCompletedAppsEvent(finishedApps,
        CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);

    // restart and verify app is marked for finishing
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    // no longer saving FINISH_APP event in NM stateStore,
    // simulate by resending FINISH_APP event
    cm.handle(new CMgrCompletedAppsEvent(finishedApps,
        CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(modUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.MODIFY_APP, appUser, appId));
    assertTrue(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(viewUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));
    assertFalse(context.getApplicationACLsManager().checkAccess(
        UserGroupInformation.createRemoteUser(enemyUser),
        ApplicationAccessType.VIEW_APP, appUser, appId));

    // simulate log aggregation completion
    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
    assertEquals(app.getApplicationState(), ApplicationState.FINISHED);
    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED));

    // restart and verify app is no longer present after recovery
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertTrue(context.getApplications().isEmpty());
    cm.stop();
  }

  @Test
  public void testNMRecoveryForAppFinishedWithLogAggregationFailure()
      throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);

    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = Collections.emptyMap();
    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();

    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, containerCmds, serviceData,
        null, null);

    StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, null);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    Application app = context.getApplications().get(appId);
    assertNotNull(app);
    waitForAppState(app, ApplicationState.INITING);

    // simulate application completion
    List<ApplicationId> finishedApps = new ArrayList<ApplicationId>();
    finishedApps.add(appId);
    cm.handle(new CMgrCompletedAppsEvent(finishedApps,
        CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);

    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
    assertEquals(app.getApplicationState(), ApplicationState.FINISHED);
    // application is still in NM context.
    assertEquals(1, context.getApplications().size());

    // restart and verify app is still there and marked as finished.
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);

    // no longer saving FINISH_APP event in NM stateStore,
    // simulate by resending FINISH_APP event
    cm.handle(new CMgrCompletedAppsEvent(finishedApps,
        CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER));
    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);
    // TODO need to figure out why additional APPLICATION_RESOURCES_CLEANEDUP
    // is needed.
    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
    assertEquals(app.getApplicationState(), ApplicationState.FINISHED);

    // simulate log aggregation failed.
    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));

    // restart and verify app is no longer present after recovery
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertTrue(context.getApplications().isEmpty());
    cm.stop();
  }

  @Test
  public void testContainerResizeRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    cm.init(conf);
    cm.start();
    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, String> containerEnv = Collections.emptyMap();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    Map<ApplicationAccessType, String> acls = Collections.emptyMap();
    File tmpDir = new File("target",
        this.getClass().getSimpleName() + "-tmpDir");
    File scriptFile = Shell.appendScriptExtension(tmpDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    if (Shell.WINDOWS) {
      fileWriter.println("@ping -n 100 127.0.0.1 >nul");
    } else {
      fileWriter.write("\numask 0");
      fileWriter.write("\nexec sleep 100");
    }
    fileWriter.close();
    FileContext localFS = FileContext.getLocalFSFileContext();
    URL resource_alpha =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha = RecordFactoryProvider
        .getRecordFactory(null).newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = new HashMap<>();
    localResources.put(destinationFile, rsrc_alpha);
    List<String> commands =
        Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, commands, serviceData,
        containerTokens, acls);
    StartContainersResponse startResponse = startContainer(
        context, cm, cid, clc, null);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    Application app = context.getApplications().get(appId);
    assertNotNull(app);
    // make sure the container reaches RUNNING state
    waitForNMContainerState(cm, cid,
        org.apache.hadoop.yarn.server.nodemanager
            .containermanager.container.ContainerState.RUNNING);
    Resource targetResource = Resource.newInstance(2048, 2);
    IncreaseContainersResourceResponse increaseResponse =
        increaseContainersResource(context, cm, cid, targetResource);
    assertTrue(increaseResponse.getFailedRequests().isEmpty());
    // check status
    ContainerStatus containerStatus = getContainerStatus(context, cm, cid);
    assertEquals(targetResource, containerStatus.getCapability());
    // restart and verify container is running and recovered
    // to the correct size
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    containerStatus = getContainerStatus(context, cm, cid);
    assertEquals(targetResource, containerStatus.getCapability());
  }

  @Test
  public void testContainerCleanupOnShutdown() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = Collections.emptyMap();
    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens = ByteBuffer.wrap(dob.getData(), 0,
        dob.getLength());
    Map<ApplicationAccessType, String> acls = Collections.emptyMap();
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, containerCmds, serviceData,
        containerTokens, acls);
    // create the logAggregationContext
    LogAggregationContext logAggregationContext =
        LogAggregationContext.newInstance("includePattern", "excludePattern");

    // verify containers are stopped on shutdown without recovery
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, false);
    Context context = createContext(conf, new NMNullStateStoreService());
    ContainerManagerImpl cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    verify(cm).handle(isA(CMgrCompletedAppsEvent.class));

    // verify containers are stopped on shutdown with unsupervised recovery
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, false);
    NMMemoryStateStoreService memStore = new NMMemoryStateStoreService();
    memStore.init(conf);
    memStore.start();
    context = createContext(conf, memStore);
    cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    memStore.close();
    verify(cm).handle(isA(CMgrCompletedAppsEvent.class));

    // verify containers are not stopped on shutdown with supervised recovery
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    memStore = new NMMemoryStateStoreService();
    memStore.init(conf);
    memStore.start();
    context = createContext(conf, memStore);
    cm = spy(createContainerManager(context));
    cm.init(conf);
    cm.start();
    startResponse = startContainer(context, cm, cid,
        clc, logAggregationContext);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    memStore.close();
    verify(cm, never()).handle(isA(CMgrCompletedAppsEvent.class));
  }

  private ContainerManagerImpl createContainerManager(Context context,
      DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        mock(NodeStatusUpdater.class), metrics, dirsHandler) {
      @Override
      public void
      setBlockNewContainerRequests(boolean blockNewContainerRequests) {
        // do nothing
      }
      @Override
      protected void authorizeGetAndStopContainerRequest(
          ContainerId containerId, Container container,
          boolean stopRequest, NMTokenIdentifier identifier)
          throws YarnException {
        if(container == null || container.getUser().equals("Fail")){
          throw new YarnException("Reject this container");
        }
      }
    };
  }

  private NMContext createContext(Configuration conf,
      NMStateStoreService stateStore) {
    NMContext context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore, false){
      public int getHttpPort() {
        return HTTP_PORT;
      }
    };
    // simulate registration with RM
    MasterKey masterKey = new MasterKeyPBImpl();
    masterKey.setKeyId(123);
    masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
      .byteValue() }));
    context.getContainerTokenSecretManager().setMasterKey(masterKey);
    context.getNMTokenSecretManager().setMasterKey(masterKey);
    return context;
  }

  private StartContainersResponse startContainer(Context context,
      final ContainerManagerImpl cm, ContainerId cid,
      ContainerLaunchContext clc, LogAggregationContext logAggregationContext)
          throws Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        cid.getApplicationAttemptId().toString());
    StartContainerRequest scReq = StartContainerRequest.newInstance(
        clc, TestContainerManager.createContainerToken(cid, 0,
            context.getNodeId(), user.getShortUserName(),
            context.getContainerTokenSecretManager(), logAggregationContext));
    final List<StartContainerRequest> scReqList =
        new ArrayList<StartContainerRequest>();
    scReqList.add(scReq);
    NMTokenIdentifier nmToken = new NMTokenIdentifier(
        cid.getApplicationAttemptId(), context.getNodeId(),
        user.getShortUserName(),
        context.getNMTokenSecretManager().getCurrentKey().getKeyId());
    user.addTokenIdentifier(nmToken);
    return user.doAs(new PrivilegedExceptionAction<StartContainersResponse>() {
      @Override
      public StartContainersResponse run() throws Exception {
        return cm.startContainers(
            StartContainersRequest.newInstance(scReqList));
      }
    });
  }

  private IncreaseContainersResourceResponse increaseContainersResource(
      Context context, final ContainerManagerImpl cm, ContainerId cid,
      Resource capability) throws Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        cid.getApplicationAttemptId().toString());
    // construct container resource increase request
    final List<Token> increaseTokens = new ArrayList<Token>();
    // add increase request
    Token containerToken = TestContainerManager.createContainerToken(
        cid, 0, context.getNodeId(), user.getShortUserName(),
        capability, context.getContainerTokenSecretManager(), null);
    increaseTokens.add(containerToken);
    final IncreaseContainersResourceRequest increaseRequest =
        IncreaseContainersResourceRequest.newInstance(increaseTokens);
    NMTokenIdentifier nmToken = new NMTokenIdentifier(
        cid.getApplicationAttemptId(), context.getNodeId(),
        user.getShortUserName(),
        context.getNMTokenSecretManager().getCurrentKey().getKeyId());
    user.addTokenIdentifier(nmToken);
    return user.doAs(
        new PrivilegedExceptionAction<IncreaseContainersResourceResponse>() {
          @Override
          public IncreaseContainersResourceResponse run() throws Exception {
            return cm.increaseContainersResource(increaseRequest);
          }
        });
  }

  private ContainerStatus getContainerStatus(
      Context context, final ContainerManagerImpl cm, ContainerId cid)
      throws  Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        cid.getApplicationAttemptId().toString());
    NMTokenIdentifier nmToken = new NMTokenIdentifier(
        cid.getApplicationAttemptId(), context.getNodeId(),
        user.getShortUserName(),
        context.getNMTokenSecretManager().getCurrentKey().getKeyId());
    user.addTokenIdentifier(nmToken);
    List<ContainerId> containerIds = new ArrayList<>();
    containerIds.add(cid);
    final GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    return user.doAs(
        new PrivilegedExceptionAction<ContainerStatus>() {
          @Override
          public ContainerStatus run() throws Exception {
            return cm.getContainerStatuses(gcsRequest)
                .getContainerStatuses().get(0);
          }
        });
  }

  private void waitForAppState(Application app, ApplicationState state)
      throws Exception {
    final int msecPerSleep = 10;
    int msecLeft = 5000;
    while (app.getApplicationState() != state && msecLeft > 0) {
      Thread.sleep(msecPerSleep);
      msecLeft -= msecPerSleep;
    }
    assertEquals(state, app.getApplicationState());
  }

  private ContainerManagerImpl createContainerManager(Context context) {
    final LogHandler logHandler = mock(LogHandler.class);
    final ResourceLocalizationService rsrcSrv =
        new ResourceLocalizationService(null, null, null, null, context) {
          @Override
          public void serviceInit(Configuration conf) throws Exception {
          }

          @Override
          public void serviceStart() throws Exception {
            // do nothing
          }

          @Override
          public void serviceStop() throws Exception {
            // do nothing
          }

          @Override
          public void handle(LocalizationEvent event) {
            // do nothing
          }
    };

    final ContainersLauncher launcher = new ContainersLauncher(context, null,
        null, null, null) {
          @Override
          public void handle(ContainersLauncherEvent event) {
            // do nothing
          }
    };

    return new ContainerManagerImpl(context,
        mock(ContainerExecutor.class), mock(DeletionService.class),
        mock(NodeStatusUpdater.class), metrics, null) {
          @Override
          protected LogHandler createLogHandler(Configuration conf,
              Context context, DeletionService deletionService) {
            return logHandler;
          }

          @Override
          protected ResourceLocalizationService createResourceLocalizationService(
              ContainerExecutor exec, DeletionService deletionContext, Context context) {
            return rsrcSrv;
          }

          @Override
          protected ContainersLauncher createContainersLauncher(
              Context context, ContainerExecutor exec) {
            return launcher;
          }

          @Override
          public void setBlockNewContainerRequests(
              boolean blockNewContainerRequests) {
            // do nothing
          }
    };
  }
}
