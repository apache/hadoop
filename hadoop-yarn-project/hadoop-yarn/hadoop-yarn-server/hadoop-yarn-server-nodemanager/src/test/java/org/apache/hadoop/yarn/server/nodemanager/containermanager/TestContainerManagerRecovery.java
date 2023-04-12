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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
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
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.health.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;

import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.metrics.TestNodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Assert;
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

    // enable atsv2 by default in test
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);

    // Default delSrvc
    delSrvc = createDeletionService();
    delSrvc.init(conf);
    exec = createContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    nodeHealthChecker = new NodeHealthCheckerService(dirsHandler);
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
    String appName = "app_name1";
    String appUser = "app_user1";
    String modUser = "modify_user1";
    String viewUser = "view_user1";
    String enemyUser = "enemy_user";
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = new HashMap<>();
    setFlowContext(containerEnv, appName, appId);
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
        clc, logAggregationContext, ContainerType.TASK);
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
    app.handle(new ApplicationFinishEvent(
        appId, "Application killed by ResourceManager"));
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
    app.handle(new ApplicationFinishEvent(
        appId, "Application killed by ResourceManager"));
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
    assertThat(app.getApplicationState()).isEqualTo(ApplicationState.FINISHED);
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
    Map<String, String> containerEnv = new HashMap<>();
    setFlowContext(containerEnv, "app_name1", appId);
    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();

    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, containerCmds, serviceData,
        null, null);

    StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, null, ContainerType.TASK);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    Application app = context.getApplications().get(appId);
    assertNotNull(app);
    waitForAppState(app, ApplicationState.INITING);

    // simulate application completion
    List<ApplicationId> finishedApps = new ArrayList<ApplicationId>();
    finishedApps.add(appId);
    app.handle(new ApplicationFinishEvent(
        appId, "Application killed by ResourceManager"));
    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);

    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
    assertThat(app.getApplicationState()).isEqualTo(ApplicationState.FINISHED);
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
    app.handle(new ApplicationFinishEvent(
        appId, "Application killed by ResourceManager"));

    waitForAppState(app, ApplicationState.APPLICATION_RESOURCES_CLEANINGUP);
    // TODO need to figure out why additional APPLICATION_RESOURCES_CLEANEDUP
    // is needed.
    app.handle(new ApplicationEvent(app.getAppId(),
        ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
    assertThat(app.getApplicationState()).isEqualTo(ApplicationState.FINISHED);

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
  public void testNodeManagerMetricsRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);

    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    cm.init(conf);
    cm.start();
    metrics.addResource(Resource.newInstance(10240, 8));

    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, String> containerEnv = Collections.emptyMap();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Map<String, LocalResource> localResources = Collections.emptyMap();
    List<String> commands = Arrays.asList("sleep 60s".split(" "));
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, containerEnv, commands, serviceData,
        null, null);
    StartContainersResponse startResponse = startContainer(context, cm, cid,
        clc, null, ContainerType.TASK);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    Application app = context.getApplications().get(appId);
    assertNotNull(app);

    // make sure the container reaches RUNNING state
    waitForNMContainerState(cm, cid,
        org.apache.hadoop.yarn.server.nodemanager
            .containermanager.container.ContainerState.RUNNING);
    TestNodeManagerMetrics.checkMetrics(1, 0, 0, 0, 0,
        1, 1, 1, 9, 1, 7, 0F, 1);

    // restart and verify metrics could be recovered
    cm.stop();
    DefaultMetricsSystem.shutdown();
    metrics = NodeManagerMetrics.create();
    metrics.addResource(Resource.newInstance(10240, 8));
    TestNodeManagerMetrics.checkMetrics(0, 0, 0, 0, 0, 0,
        0, 0, 10, 0, 8, 0F, 0);
    context = createContext(conf, stateStore);
    cm = createContainerManager(context, delSrvc);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    TestNodeManagerMetrics.checkMetrics(1, 0, 0, 0, 0,
        1, 1, 1, 9, 1, 7, 0F, 1);
    cm.stop();
  }

  @Test
  public void testContainerResizeRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);

    commonLaunchContainer(appId, cid, cm);

    Application app = context.getApplications().get(appId);
    assertNotNull(app);

    Resource targetResource = Resource.newInstance(2048, 2);
    ContainerUpdateResponse updateResponse =
        updateContainers(context, cm, cid, targetResource);
    assertTrue(updateResponse.getFailedRequests().isEmpty());
    // check status
    ContainerStatus containerStatus = getContainerStatus(context, cm, cid);
    assertEquals(targetResource, containerStatus.getCapability());
    // restart and verify container is running and recovered
    // to the correct size
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    containerStatus = getContainerStatus(context, cm, cid);
    assertEquals(targetResource, containerStatus.getCapability());
    cm.stop();
  }

  @Test
  public void testContainerSchedulerRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);

    commonLaunchContainer(appId, cid, cm);

    Application app = context.getApplications().get(appId);
    assertNotNull(app);

    ResourceUtilization utilization =
        ResourceUtilization.newInstance(1024, 2048, 1.0F);
    assertThat(cm.getContainerScheduler().getNumRunningContainers()).
        isEqualTo(1);
    assertEquals(utilization,
        cm.getContainerScheduler().getCurrentUtilization());

    // restart and verify container scheduler has recovered correctly
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    waitForNMContainerState(cm, cid, ContainerState.RUNNING);

    assertThat(cm.getContainerScheduler().getNumRunningContainers()).
        isEqualTo(1);
    assertEquals(utilization,
        cm.getContainerScheduler().getCurrentUtilization());
    cm.stop();
  }

  @Test
  public void testResourceMappingRecoveryForContainer() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();

    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);

    commonLaunchContainer(appId, cid, cm);

    Container nmContainer = context.getContainers().get(cid);

    Application app = context.getApplications().get(appId);
    assertNotNull(app);

    // store resource mapping of the container
    List<Serializable> gpuResources = Arrays.asList("1", "2", "3");
    stateStore.storeAssignedResources(nmContainer, "gpu", gpuResources);
    List<Serializable> numaResources = Arrays.asList("numa1");
    stateStore.storeAssignedResources(nmContainer, "numa", numaResources);
    List<Serializable> fpgaResources = Arrays.asList("fpga1", "fpga2");
    stateStore.storeAssignedResources(nmContainer, "fpga", fpgaResources);

    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = context.getApplications().get(appId);
    assertNotNull(app);

    Assert.assertNotNull(nmContainer);
    ResourceMappings resourceMappings = nmContainer.getResourceMappings();
    List<Serializable> assignedResource = resourceMappings
        .getAssignedResources("gpu");
    Assert.assertTrue(assignedResource.equals(gpuResources));
    Assert.assertTrue(
        resourceMappings.getAssignedResources("numa").equals(numaResources));
    Assert.assertTrue(
        resourceMappings.getAssignedResources("fpga").equals(fpgaResources));
    cm.stop();
  }

  @Test
  public void testContainerCleanupOnShutdown() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = new HashMap<>();
    setFlowContext(containerEnv, "app_name1", appId);
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
        clc, logAggregationContext, ContainerType.TASK);
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
        clc, logAggregationContext, ContainerType.TASK);
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
        clc, logAggregationContext, ContainerType.TASK);
    assertEquals(1, startResponse.getSuccessfullyStartedContainers().size());
    cm.stop();
    memStore.close();
    verify(cm, never()).handle(isA(CMgrCompletedAppsEvent.class));
  }

  @Test
  public void testKilledContainerInQueuedStateRecovery() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED, true);
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    context = createContext(conf, stateStore);
    ContainerManagerImpl cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();

    // add an application by starting a container
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cid = ContainerId.newContainerId(attemptId, 1);
    createStartContainerRequest(appId, cid, cm);

    Application app = context.getApplications().get(appId);
    assertEquals(1, context.getApplications().size());
    assertNotNull(app);

    stateStore.storeContainerKilled(cid);
    // restart and verify container scheduler has recovered correctly
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context, delSrvc);
    ((NMContext) context).setContainerManager(cm);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());

    ConcurrentMap<ContainerId, Container> containers = context.getContainers();
    Container c = containers.get(cid);
    assertEquals(ContainerState.DONE, c.getContainerState());
    app = context.getApplications().get(appId);
    assertNotNull(app);
    cm.stop();
  }

  private void createStartContainerRequest(ApplicationId appId, ContainerId cid,
      ContainerManagerImpl cm) throws Exception {
    Map<String, String> containerEnv = new HashMap<>();
    setFlowContext(containerEnv, "app_name1", appId);
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
        context, cm, cid, clc, null, ContainerType.TASK);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
  }

  private void commonLaunchContainer(ApplicationId appId, ContainerId cid,
      ContainerManagerImpl cm) throws Exception {
    createStartContainerRequest(appId, cid, cm);
    // make sure the container reaches RUNNING state
    waitForNMContainerState(cm, cid,
        org.apache.hadoop.yarn.server.nodemanager
            .containermanager.container.ContainerState.RUNNING);
  }

  private ContainerManagerImpl createContainerManager(Context context,
      DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        mock(NodeStatusUpdater.class), metrics, dirsHandler) {
      @Override
      protected void authorizeGetAndStopContainerRequest(
          ContainerId containerId, Container container,
          boolean stopRequest, NMTokenIdentifier identifier,
          String remoteUser)
          throws YarnException {
        if(container == null || container.getUser().equals("Fail")){
          throw new YarnException("Reject this container");
        }
      }
      @Override
      protected ContainerScheduler createContainerScheduler(Context context) {
        return new ContainerScheduler(context, getDispatcher(), metrics){
          @Override
          public ContainersMonitor getContainersMonitor() {
            return new ContainersMonitorImpl(null, null, null) {
              @Override
              public float getVmemRatio() {
                return 2.0f;
              }

              @Override
              public long getVmemAllocatedForContainers() {
                return 20480;
              }

              @Override
              public long getPmemAllocatedForContainers() {
                return (long) 2048 << 20;
              }

              @Override
              public long getVCoresAllocatedForContainers() {
                return 4;
              }
            };
          }
        };
      }
    };
  }

  private NMContext createContext(Configuration conf,
      NMStateStoreService stateStore) {
    NMContext context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore, false, conf) {
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
    context.setContainerExecutor(exec);
    return context;
  }

  private StartContainersResponse startContainer(Context context,
      final ContainerManagerImpl cm, ContainerId cid,
      ContainerLaunchContext clc, LogAggregationContext logAggregationContext,
      ContainerType containerType)
          throws Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        cid.getApplicationAttemptId().toString());
    StartContainerRequest scReq = StartContainerRequest.newInstance(
        clc, TestContainerManager.createContainerToken(cid, 0,
            context.getNodeId(), user.getShortUserName(),
            context.getContainerTokenSecretManager(), logAggregationContext, containerType));
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

  private ContainerUpdateResponse updateContainers(
      Context context, final ContainerManagerImpl cm, ContainerId cid,
      Resource capability) throws Exception {
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        cid.getApplicationAttemptId().toString());
    // construct container resource increase request
    final List<Token> increaseTokens = new ArrayList<Token>();
    // add increase request
    Token containerToken = TestContainerManager.createContainerToken(
        cid, 1, 0, context.getNodeId(), user.getShortUserName(),
        capability, context.getContainerTokenSecretManager(), null);
    increaseTokens.add(containerToken);
    final ContainerUpdateRequest updateRequest =
        ContainerUpdateRequest.newInstance(increaseTokens);
    NMTokenIdentifier nmToken = new NMTokenIdentifier(
        cid.getApplicationAttemptId(), context.getNodeId(),
        user.getShortUserName(),
        context.getNMTokenSecretManager().getCurrentKey().getKeyId());
    user.addTokenIdentifier(nmToken);
    return user.doAs(
        new PrivilegedExceptionAction<ContainerUpdateResponse>() {
          @Override
          public ContainerUpdateResponse run() throws Exception {
            return cm.updateContainer(updateRequest);
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
    final NodeManagerMetrics metrics = mock(NodeManagerMetrics.class);
    final ResourceLocalizationService rsrcSrv =
        new ResourceLocalizationService(null, null, null, null, context,
            metrics) {
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

    ContainerManagerImpl containerManager = new ContainerManagerImpl(context,
        mock(ContainerExecutor.class), mock(DeletionService.class),
        mock(NodeStatusUpdater.class), metrics, null) {
          @Override
          protected LogHandler createLogHandler(Configuration conf,
              Context context, DeletionService deletionService) {
            return logHandler;
          }

          @Override
          protected ResourceLocalizationService
              createResourceLocalizationService(
              ContainerExecutor exec, DeletionService deletionContext,
              Context context, NodeManagerMetrics metrics) {
            return rsrcSrv;
          }

          @Override
          protected ContainersLauncher createContainersLauncher(
              Context context, ContainerExecutor exec) {
            return launcher;
          }

          @Override
          public NMTimelinePublisher
              createNMTimelinePublisher(Context context) {
            return null;
          }
    };
    containerManager.getDispatcher().disableExitOnDispatchException();
    return containerManager;
  }

  private void setFlowContext(Map<String, String> containerEnv, String appName,
      ApplicationId appId) {
    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      setFlowTags(containerEnv, TimelineUtils.FLOW_NAME_TAG_PREFIX,
          TimelineUtils.generateDefaultFlowName(appName, appId));
      setFlowTags(containerEnv, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
          TimelineUtils.DEFAULT_FLOW_VERSION);
      setFlowTags(containerEnv, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
          String.valueOf(System.currentTimeMillis()));
    }
  }

  private static void setFlowTags(Map<String, String> environment,
      String tagPrefix, String value) {
    if (!value.isEmpty()) {
      environment.put(tagPrefix, value);
    }
  }

  @Test
  public void testApplicationRecoveryAfterFlowContextUpdated()
      throws Exception {
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
    String appName = "app_name1";
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    // create 1nd attempt container with containerId 2
    ContainerId cid = ContainerId.newContainerId(attemptId, 2);
    Map<String, LocalResource> localResources = Collections.emptyMap();
    Map<String, String> containerEnv = new HashMap<>();

    List<String> containerCmds = Collections.emptyList();
    Map<String, ByteBuffer> serviceData = Collections.emptyMap();
    Credentials containerCreds = new Credentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    containerCreds.writeTokenStorageToStream(dob);
    ByteBuffer containerTokens =
        ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    ContainerLaunchContext clc = ContainerLaunchContext
        .newInstance(localResources, containerEnv, containerCmds, serviceData,
            containerTokens, acls);
    // create the logAggregationContext
    LogAggregationContext logAggregationContext = LogAggregationContext
        .newInstance("includePattern", "excludePattern",
            "includePatternInRollingAggregation",
            "excludePatternInRollingAggregation");

    StartContainersResponse startResponse =
        startContainer(context, cm, cid, clc, logAggregationContext,
            ContainerType.TASK);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    ApplicationImpl app =
        (ApplicationImpl) context.getApplications().get(appId);
    assertNotNull(app);
    waitForAppState(app, ApplicationState.INITING);
    assertNull(app.getFlowName());

    // 2nd attempt
    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(appId, 2);
    // create 2nd attempt master container
    ContainerId cid2 = ContainerId.newContainerId(attemptId, 1);
    setFlowContext(containerEnv, appName, appId);
    // once again create for updating launch context
    clc = ContainerLaunchContext
        .newInstance(localResources, containerEnv, containerCmds, serviceData,
            containerTokens, acls);
    // start container with container type AM.
    startResponse =
        startContainer(context, cm, cid2, clc, logAggregationContext,
            ContainerType.APPLICATION_MASTER);
    assertTrue(startResponse.getFailedRequests().isEmpty());
    assertEquals(1, context.getApplications().size());
    waitForAppState(app, ApplicationState.INITING);
    assertEquals(appName, app.getFlowName());

    // reset container manager and verify flow context information
    cm.stop();
    context = createContext(conf, stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertEquals(1, context.getApplications().size());
    app = (ApplicationImpl) context.getApplications().get(appId);
    assertNotNull(app);
    assertEquals(appName, app.getFlowName());
    waitForAppState(app, ApplicationState.INITING);

    cm.stop();
  }
}
