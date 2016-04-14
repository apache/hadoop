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
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.junit.Test;

public class TestContainerManagerRecovery {

  private NodeManagerMetrics metrics = NodeManagerMetrics.create();

  @Test
  public void testApplicationRecovery() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.NM_ADDRESS, "localhost:1234");
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, "yarn_admin_user");
    NMStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    Context context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore);
    ContainerManagerImpl cm = createContainerManager(context);
    cm.init(conf);
    cm.start();

    // simulate registration with RM
    MasterKey masterKey = new MasterKeyPBImpl();
    masterKey.setKeyId(123);
    masterKey.setBytes(ByteBuffer.wrap(new byte[] { new Integer(123)
      .byteValue() }));
    context.getContainerTokenSecretManager().setMasterKey(masterKey);
    context.getNMTokenSecretManager().setMasterKey(masterKey);

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
    context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore);
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
    context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore);
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
    context = new NMContext(new NMContainerTokenSecretManager(
        conf), new NMTokenSecretManagerInNM(), null,
        new ApplicationACLsManager(conf), stateStore);
    cm = createContainerManager(context);
    cm.init(conf);
    cm.start();
    assertTrue(context.getApplications().isEmpty());
    cm.stop();
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
        mock(NodeStatusUpdater.class), metrics,
        context.getApplicationACLsManager(), null) {
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
