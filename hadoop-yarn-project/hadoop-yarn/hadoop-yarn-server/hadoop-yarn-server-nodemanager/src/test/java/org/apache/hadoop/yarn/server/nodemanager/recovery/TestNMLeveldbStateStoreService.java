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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LogDeleterProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredApplicationsState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerTokensState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredDeletionServiceState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLogDeleterState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredNMTokensState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.iq80.leveldb.DB;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNMLeveldbStateStoreService {
  private static final File TMP_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestNMLeveldbStateStoreService.class.getName());

  YarnConfiguration conf;
  NMLeveldbStateStoreService stateStore;

  @Before
  public void setup() throws IOException {
    FileUtil.fullyDelete(TMP_DIR);
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.NM_RECOVERY_DIR, TMP_DIR.toString());
    restartStateStore();
  }

  @After
  public void cleanup() throws IOException {
    if (stateStore != null) {
      stateStore.close();
    }
    FileUtil.fullyDelete(TMP_DIR);
  }

  private void restartStateStore() throws IOException {
    // need to close so leveldb releases database lock
    if (stateStore != null) {
      stateStore.close();
    }
    stateStore = new NMLeveldbStateStoreService();
    stateStore.init(conf);
    stateStore.start();
  }

  private void verifyEmptyState() throws IOException {
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    assertNotNull(state);
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    assertNotNull(pubts);
    assertTrue(pubts.getLocalizedResources().isEmpty());
    assertTrue(pubts.getInProgressResources().isEmpty());
    assertTrue(state.getUserResources().isEmpty());
  }

  @Test
  public void testIsNewlyCreated() throws IOException {
    assertTrue(stateStore.isNewlyCreated());
    restartStateStore();
    assertFalse(stateStore.isNewlyCreated());
  }

  @Test
  public void testEmptyState() throws IOException {
    assertTrue(stateStore.canRecover());
    verifyEmptyState();
  }
  
  @Test
  public void testCheckVersion() throws IOException {
    // default version
    Version defaultVersion = stateStore.getCurrentVersion();
    Assert.assertEquals(defaultVersion, stateStore.loadVersion());

    // compatible version
    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
          defaultVersion.getMinorVersion() + 2);
    stateStore.storeVersion(compatibleVersion);
    Assert.assertEquals(compatibleVersion, stateStore.loadVersion());
    restartStateStore();
    // overwrite the compatible version
    Assert.assertEquals(defaultVersion, stateStore.loadVersion());

    // incompatible version
    Version incompatibleVersion =
      Version.newInstance(defaultVersion.getMajorVersion() + 1,
          defaultVersion.getMinorVersion());
    stateStore.storeVersion(incompatibleVersion);
    try {
      restartStateStore();
      Assert.fail("Incompatible version, should expect fail here.");
    } catch (ServiceStateException e) {
      Assert.assertTrue("Exception message mismatch", 
        e.getMessage().contains("Incompatible version for NM state:"));
    }
  }

  @Test
  public void testApplicationStorage() throws IOException {
    // test empty when no state
    RecoveredApplicationsState state = stateStore.loadApplicationsState();
    assertTrue(state.getApplications().isEmpty());

    // store an application and verify recovered
    final ApplicationId appId1 = ApplicationId.newInstance(1234, 1);
    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId1).getProto());
    builder.setUser("user1");
    ContainerManagerApplicationProto appProto1 = builder.build();
    stateStore.storeApplication(appId1, appProto1);
    restartStateStore();
    state = stateStore.loadApplicationsState();
    assertEquals(1, state.getApplications().size());
    assertEquals(appProto1, state.getApplications().get(0));

    // add a new app
    final ApplicationId appId2 = ApplicationId.newInstance(1234, 2);
    builder = ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId2).getProto());
    builder.setUser("user2");
    ContainerManagerApplicationProto appProto2 = builder.build();
    stateStore.storeApplication(appId2, appProto2);
    restartStateStore();
    state = stateStore.loadApplicationsState();
    assertEquals(2, state.getApplications().size());
    assertTrue(state.getApplications().contains(appProto1));
    assertTrue(state.getApplications().contains(appProto2));

    // test removing an application
    stateStore.removeApplication(appId2);
    restartStateStore();
    state = stateStore.loadApplicationsState();
    assertEquals(1, state.getApplications().size());
    assertEquals(appProto1, state.getApplications().get(0));
  }

  @Test
  public void testContainerStorage() throws IOException {
    // test empty when no state
    List<RecoveredContainerState> recoveredContainers =
        stateStore.loadContainersState();
    assertTrue(recoveredContainers.isEmpty());

    // create a container request
    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 4);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 5);
    StartContainerRequest containerReq = createContainerRequest(containerId);

    // store a container and verify recovered
    stateStore.storeContainer(containerId, 0, containerReq);

    // verify the container version key is not stored for new containers
    DB db = stateStore.getDB();
    assertNull("version key present for new container", db.get(bytes(
        stateStore.getContainerVersionKey(containerId.toString()))));

    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(0, rcs.getVersion());
    assertEquals(RecoveredContainerStatus.REQUESTED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertTrue(rcs.getDiagnostics().isEmpty());

    // store a new container record without StartContainerRequest
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 6);
    stateStore.storeContainerLaunched(containerId1);
    recoveredContainers = stateStore.loadContainersState();
    // check whether the new container record is discarded
    assertEquals(1, recoveredContainers.size());

    // queue the container, and verify recovered
    stateStore.storeContainerQueued(containerId);
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.QUEUED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertTrue(rcs.getDiagnostics().isEmpty());

    // launch the container, add some diagnostics, and verify recovered
    StringBuilder diags = new StringBuilder();
    stateStore.storeContainerLaunched(containerId);
    diags.append("some diags for container");
    stateStore.storeContainerDiagnostics(containerId, diags);
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertEquals(diags.toString(), rcs.getDiagnostics());

    // increase the container size, and verify recovered
    stateStore.storeContainerResourceChanged(containerId, 2,
        Resource.newInstance(2468, 4));
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(2, rcs.getVersion());
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(Resource.newInstance(2468, 4), rcs.getCapability());

    // mark the container killed, add some more diags, and verify recovered
    diags.append("some more diags for container");
    stateStore.storeContainerDiagnostics(containerId, diags);
    stateStore.storeContainerKilled(containerId);
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertTrue(rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertEquals(diags.toString(), rcs.getDiagnostics());

    // add yet more diags, mark container completed, and verify recovered
    diags.append("some final diags");
    stateStore.storeContainerDiagnostics(containerId, diags);
    stateStore.storeContainerCompleted(containerId, 21);
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.COMPLETED, rcs.getStatus());
    assertEquals(21, rcs.getExitCode());
    assertTrue(rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertEquals(diags.toString(), rcs.getDiagnostics());

    // store remainingRetryAttempts, workDir and logDir
    stateStore.storeContainerRemainingRetryAttempts(containerId, 6);
    stateStore.storeContainerWorkDir(containerId, "/test/workdir");
    stateStore.storeContainerLogDir(containerId, "/test/logdir");
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertEquals(1, recoveredContainers.size());
    rcs = recoveredContainers.get(0);
    assertEquals(6, rcs.getRemainingRetryAttempts());
    assertEquals("/test/workdir", rcs.getWorkDir());
    assertEquals("/test/logdir", rcs.getLogDir());

    // remove the container and verify not recovered
    stateStore.removeContainer(containerId);
    restartStateStore();
    recoveredContainers = stateStore.loadContainersState();
    assertTrue(recoveredContainers.isEmpty());
  }

  private StartContainerRequest createContainerRequest(
      ContainerId containerId) {
    LocalResource lrsrc = LocalResource.newInstance(
        URL.newInstance("hdfs", "somehost", 12345, "/some/path/to/rsrc"),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, 123L,
        1234567890L);
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put("rsrc", lrsrc);
    Map<String, String> env = new HashMap<String, String>();
    env.put("somevar", "someval");
    List<String> containerCmds = new ArrayList<String>();
    containerCmds.add("somecmd");
    containerCmds.add("somearg");
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();
    serviceData.put("someservice",
        ByteBuffer.wrap(new byte[] { 0x1, 0x2, 0x3 }));
    ByteBuffer containerTokens =
        ByteBuffer.wrap(new byte[] { 0x7, 0x8, 0x9, 0xa });
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, "viewuser");
    acls.put(ApplicationAccessType.MODIFY_APP, "moduser");
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, env, containerCmds, serviceData, containerTokens,
        acls);
    Resource containerRsrc = Resource.newInstance(1357, 3);
    ContainerTokenIdentifier containerTokenId =
        new ContainerTokenIdentifier(containerId, "host", "user",
            containerRsrc, 9876543210L, 42, 2468, Priority.newInstance(7),
            13579);
    Token containerToken = Token.newInstance(containerTokenId.getBytes(),
        ContainerTokenIdentifier.KIND.toString(), "password".getBytes(),
        "tokenservice");
    return StartContainerRequest.newInstance(clc, containerToken);
  }

  @Test
  public void testStartResourceLocalization() throws IOException {
    String user = "somebody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    // start a local resource for an application
    Path appRsrcPath = new Path("hdfs://some/app/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto = rsrcPb.getProto();
    Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
    stateStore.startResourceLocalization(user, appId, appRsrcProto,
        appRsrcLocalPath);

    // restart and verify only app resource is marked in-progress
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    assertTrue(pubts.getLocalizedResources().isEmpty());
    assertTrue(pubts.getInProgressResources().isEmpty());
    Map<String, RecoveredUserResources> userResources =
        state.getUserResources();
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    assertTrue(privts.getLocalizedResources().isEmpty());
    assertTrue(privts.getInProgressResources().isEmpty());
    assertEquals(1, rur.getAppTrackerStates().size());
    LocalResourceTrackerState appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    assertTrue(appts.getLocalizedResources().isEmpty());
    assertEquals(1, appts.getInProgressResources().size());
    assertEquals(appRsrcLocalPath,
        appts.getInProgressResources().get(appRsrcProto));

    // start some public and private resources
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath1),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto1 = rsrcPb.getProto();
    Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
    stateStore.startResourceLocalization(null, null, pubRsrcProto1,
        pubRsrcLocalPath1);
    Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath2),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto2 = rsrcPb.getProto();
    Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
    stateStore.startResourceLocalization(null, null, pubRsrcProto2,
        pubRsrcLocalPath2);
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(privRsrcPath),
            LocalResourceType.PATTERN, LocalResourceVisibility.PRIVATE,
            789L, 680L, "*pattern*");
    LocalResourceProto privRsrcProto = rsrcPb.getProto();
    Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
    stateStore.startResourceLocalization(user, null, privRsrcProto,
        privRsrcLocalPath);

    // restart and verify resources are marked in-progress
    restartStateStore();
    state = stateStore.loadLocalizationState();
    pubts = state.getPublicTrackerState();
    assertTrue(pubts.getLocalizedResources().isEmpty());
    assertEquals(2, pubts.getInProgressResources().size());
    assertEquals(pubRsrcLocalPath1,
        pubts.getInProgressResources().get(pubRsrcProto1));
    assertEquals(pubRsrcLocalPath2,
        pubts.getInProgressResources().get(pubRsrcProto2));
    userResources = state.getUserResources();
    assertEquals(1, userResources.size());
    rur = userResources.get(user);
    privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    assertTrue(privts.getLocalizedResources().isEmpty());
    assertEquals(1, privts.getInProgressResources().size());
    assertEquals(privRsrcLocalPath,
        privts.getInProgressResources().get(privRsrcProto));
    assertEquals(1, rur.getAppTrackerStates().size());
    appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    assertTrue(appts.getLocalizedResources().isEmpty());
    assertEquals(1, appts.getInProgressResources().size());
    assertEquals(appRsrcLocalPath,
        appts.getInProgressResources().get(appRsrcProto));
  }

  @Test
  public void testFinishResourceLocalization() throws IOException {
    String user = "somebody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    // start and finish a local resource for an application
    Path appRsrcPath = new Path("hdfs://some/app/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto = rsrcPb.getProto();
    Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
    stateStore.startResourceLocalization(user, appId, appRsrcProto,
        appRsrcLocalPath);
    LocalizedResourceProto appLocalizedProto =
        LocalizedResourceProto.newBuilder()
          .setResource(appRsrcProto)
          .setLocalPath(appRsrcLocalPath.toString())
          .setSize(1234567L)
          .build();
    stateStore.finishResourceLocalization(user, appId, appLocalizedProto);

    // restart and verify only app resource is completed
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    assertTrue(pubts.getLocalizedResources().isEmpty());
    assertTrue(pubts.getInProgressResources().isEmpty());
    Map<String, RecoveredUserResources> userResources =
        state.getUserResources();
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    assertTrue(privts.getLocalizedResources().isEmpty());
    assertTrue(privts.getInProgressResources().isEmpty());
    assertEquals(1, rur.getAppTrackerStates().size());
    LocalResourceTrackerState appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    assertTrue(appts.getInProgressResources().isEmpty());
    assertEquals(1, appts.getLocalizedResources().size());
    assertEquals(appLocalizedProto,
        appts.getLocalizedResources().iterator().next());

    // start some public and private resources
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath1),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto1 = rsrcPb.getProto();
    Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
    stateStore.startResourceLocalization(null, null, pubRsrcProto1,
        pubRsrcLocalPath1);
    Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath2),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto2 = rsrcPb.getProto();
    Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
    stateStore.startResourceLocalization(null, null, pubRsrcProto2,
        pubRsrcLocalPath2);
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(privRsrcPath),
            LocalResourceType.PATTERN, LocalResourceVisibility.PRIVATE,
            789L, 680L, "*pattern*");
    LocalResourceProto privRsrcProto = rsrcPb.getProto();
    Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
    stateStore.startResourceLocalization(user, null, privRsrcProto,
        privRsrcLocalPath);

    // finish some of the resources
    LocalizedResourceProto pubLocalizedProto1 =
        LocalizedResourceProto.newBuilder()
          .setResource(pubRsrcProto1)
          .setLocalPath(pubRsrcLocalPath1.toString())
          .setSize(pubRsrcProto1.getSize())
          .build();
    stateStore.finishResourceLocalization(null, null, pubLocalizedProto1);
    LocalizedResourceProto privLocalizedProto =
        LocalizedResourceProto.newBuilder()
          .setResource(privRsrcProto)
          .setLocalPath(privRsrcLocalPath.toString())
          .setSize(privRsrcProto.getSize())
          .build();
    stateStore.finishResourceLocalization(user, null, privLocalizedProto);

    // restart and verify state
    restartStateStore();
    state = stateStore.loadLocalizationState();
    pubts = state.getPublicTrackerState();
    assertEquals(1, pubts.getLocalizedResources().size());
    assertEquals(pubLocalizedProto1,
        pubts.getLocalizedResources().iterator().next());
    assertEquals(1, pubts.getInProgressResources().size());
    assertEquals(pubRsrcLocalPath2,
        pubts.getInProgressResources().get(pubRsrcProto2));
    userResources = state.getUserResources();
    assertEquals(1, userResources.size());
    rur = userResources.get(user);
    privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    assertEquals(1, privts.getLocalizedResources().size());
    assertEquals(privLocalizedProto,
        privts.getLocalizedResources().iterator().next());
    assertTrue(privts.getInProgressResources().isEmpty());
    assertEquals(1, rur.getAppTrackerStates().size());
    appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    assertTrue(appts.getInProgressResources().isEmpty());
    assertEquals(1, appts.getLocalizedResources().size());
    assertEquals(appLocalizedProto,
        appts.getLocalizedResources().iterator().next());
  }

  @Test
  public void testRemoveLocalizedResource() throws IOException {
    String user = "somebody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    // go through the complete lifecycle for an application local resource
    Path appRsrcPath = new Path("hdfs://some/app/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto = rsrcPb.getProto();
    Path appRsrcLocalPath = new Path("/some/local/dir/for/apprsrc");
    stateStore.startResourceLocalization(user, appId, appRsrcProto,
        appRsrcLocalPath);
    LocalizedResourceProto appLocalizedProto =
        LocalizedResourceProto.newBuilder()
          .setResource(appRsrcProto)
          .setLocalPath(appRsrcLocalPath.toString())
          .setSize(1234567L)
          .build();
    stateStore.finishResourceLocalization(user, appId, appLocalizedProto);
    stateStore.removeLocalizedResource(user, appId, appRsrcLocalPath);

    restartStateStore();
    verifyEmptyState();

    // remove an app resource that didn't finish
    stateStore.startResourceLocalization(user, appId, appRsrcProto,
        appRsrcLocalPath);
    stateStore.removeLocalizedResource(user, appId, appRsrcLocalPath);

    restartStateStore();
    verifyEmptyState();

    // add public and private resources and remove some
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath1),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto1 = rsrcPb.getProto();
    Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
    stateStore.startResourceLocalization(null, null, pubRsrcProto1,
        pubRsrcLocalPath1);
    LocalizedResourceProto pubLocalizedProto1 =
        LocalizedResourceProto.newBuilder()
          .setResource(pubRsrcProto1)
          .setLocalPath(pubRsrcLocalPath1.toString())
          .setSize(789L)
          .build();
    stateStore.finishResourceLocalization(null, null, pubLocalizedProto1);
    Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(pubRsrcPath2),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto2 = rsrcPb.getProto();
    Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
    stateStore.startResourceLocalization(null, null, pubRsrcProto2,
        pubRsrcLocalPath2);
    LocalizedResourceProto pubLocalizedProto2 =
        LocalizedResourceProto.newBuilder()
          .setResource(pubRsrcProto2)
          .setLocalPath(pubRsrcLocalPath2.toString())
          .setSize(7654321L)
          .build();
    stateStore.finishResourceLocalization(null, null, pubLocalizedProto2);
    stateStore.removeLocalizedResource(null, null, pubRsrcLocalPath2);
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            URL.fromPath(privRsrcPath),
            LocalResourceType.PATTERN, LocalResourceVisibility.PRIVATE,
            789L, 680L, "*pattern*");
    LocalResourceProto privRsrcProto = rsrcPb.getProto();
    Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
    stateStore.startResourceLocalization(user, null, privRsrcProto,
        privRsrcLocalPath);
    stateStore.removeLocalizedResource(user, null, privRsrcLocalPath);

    // restart and verify state
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    assertTrue(pubts.getInProgressResources().isEmpty());
    assertEquals(1, pubts.getLocalizedResources().size());
    assertEquals(pubLocalizedProto1,
        pubts.getLocalizedResources().iterator().next());
    Map<String, RecoveredUserResources> userResources =
        state.getUserResources();
    assertTrue(userResources.isEmpty());
  }

  @Test
  public void testDeletionTaskStorage() throws IOException {
    // test empty when no state
    RecoveredDeletionServiceState state =
        stateStore.loadDeletionServiceState();
    assertTrue(state.getTasks().isEmpty());

    // store a deletion task and verify recovered
    DeletionServiceDeleteTaskProto proto =
        DeletionServiceDeleteTaskProto.newBuilder()
        .setId(7)
        .setUser("someuser")
        .setSubdir("some/subdir")
        .addBasedirs("some/dir/path")
        .addBasedirs("some/other/dir/path")
        .setDeletionTime(123456L)
        .addSuccessorIds(8)
        .addSuccessorIds(9)
        .build();
    stateStore.storeDeletionTask(proto.getId(), proto);
    restartStateStore();
    state = stateStore.loadDeletionServiceState();
    assertEquals(1, state.getTasks().size());
    assertEquals(proto, state.getTasks().get(0));

    // store another deletion task
    DeletionServiceDeleteTaskProto proto2 =
        DeletionServiceDeleteTaskProto.newBuilder()
        .setId(8)
        .setUser("user2")
        .setSubdir("subdir2")
        .setDeletionTime(789L)
        .build();
    stateStore.storeDeletionTask(proto2.getId(), proto2);
    restartStateStore();
    state = stateStore.loadDeletionServiceState();
    assertEquals(2, state.getTasks().size());
    assertTrue(state.getTasks().contains(proto));
    assertTrue(state.getTasks().contains(proto2));

    // delete a task and verify gone after recovery
    stateStore.removeDeletionTask(proto2.getId());
    restartStateStore();
    state = stateStore.loadDeletionServiceState();
    assertEquals(1, state.getTasks().size());
    assertEquals(proto, state.getTasks().get(0));

    // delete the last task and verify none left
    stateStore.removeDeletionTask(proto.getId());
    restartStateStore();
    state = stateStore.loadDeletionServiceState();
    assertTrue(state.getTasks().isEmpty());
  }

  @Test
  public void testNMTokenStorage() throws IOException {
    // test empty when no state
    RecoveredNMTokensState state = stateStore.loadNMTokensState();
    assertNull(state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(state.getApplicationMasterKeys().isEmpty());

    // store a master key and verify recovered
    NMTokenSecretManagerForTest secretMgr = new NMTokenSecretManagerForTest();
    MasterKey currentKey = secretMgr.generateKey();
    stateStore.storeNMTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(state.getApplicationMasterKeys().isEmpty());

    // store a previous key and verify recovered
    MasterKey prevKey = secretMgr.generateKey();
    stateStore.storeNMTokenPreviousMasterKey(prevKey);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    assertTrue(state.getApplicationMasterKeys().isEmpty());

    // store a few application keys and verify recovered
    ApplicationAttemptId attempt1 = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    MasterKey attemptKey1 = secretMgr.generateKey();
    stateStore.storeNMTokenApplicationMasterKey(attempt1, attemptKey1);
    ApplicationAttemptId attempt2 = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(2, 3), 4);
    MasterKey attemptKey2 = secretMgr.generateKey();
    stateStore.storeNMTokenApplicationMasterKey(attempt2, attemptKey2);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    Map<ApplicationAttemptId, MasterKey> loadedAppKeys =
        state.getApplicationMasterKeys();
    assertEquals(2, loadedAppKeys.size());
    assertEquals(attemptKey1, loadedAppKeys.get(attempt1));
    assertEquals(attemptKey2, loadedAppKeys.get(attempt2));

    // add/update/remove keys and verify recovered
    ApplicationAttemptId attempt3 = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(5, 6), 7);
    MasterKey attemptKey3 = secretMgr.generateKey();
    stateStore.storeNMTokenApplicationMasterKey(attempt3, attemptKey3);
    stateStore.removeNMTokenApplicationMasterKey(attempt1);
    attemptKey2 = prevKey;
    stateStore.storeNMTokenApplicationMasterKey(attempt2, attemptKey2);
    prevKey = currentKey;
    stateStore.storeNMTokenPreviousMasterKey(prevKey);
    currentKey = secretMgr.generateKey();
    stateStore.storeNMTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    loadedAppKeys = state.getApplicationMasterKeys();
    assertEquals(2, loadedAppKeys.size());
    assertNull(loadedAppKeys.get(attempt1));
    assertEquals(attemptKey2, loadedAppKeys.get(attempt2));
    assertEquals(attemptKey3, loadedAppKeys.get(attempt3));
  }

  @Test
  public void testContainerTokenStorage() throws IOException {
    // test empty when no state
    RecoveredContainerTokensState state =
        stateStore.loadContainerTokensState();
    assertNull(state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(state.getActiveTokens().isEmpty());

    // store a master key and verify recovered
    ContainerTokenKeyGeneratorForTest keygen =
        new ContainerTokenKeyGeneratorForTest(new YarnConfiguration());
    MasterKey currentKey = keygen.generateKey();
    stateStore.storeContainerTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(state.getActiveTokens().isEmpty());

    // store a previous key and verify recovered
    MasterKey prevKey = keygen.generateKey();
    stateStore.storeContainerTokenPreviousMasterKey(prevKey);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    assertTrue(state.getActiveTokens().isEmpty());

    // store a few container tokens and verify recovered
    ContainerId cid1 = BuilderUtils.newContainerId(1, 1, 1, 1);
    Long expTime1 = 1234567890L;
    ContainerId cid2 = BuilderUtils.newContainerId(2, 2, 2, 2);
    Long expTime2 = 9876543210L;
    stateStore.storeContainerToken(cid1, expTime1);
    stateStore.storeContainerToken(cid2, expTime2);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    Map<ContainerId, Long> loadedActiveTokens =
        state.getActiveTokens();
    assertEquals(2, loadedActiveTokens.size());
    assertEquals(expTime1, loadedActiveTokens.get(cid1));
    assertEquals(expTime2, loadedActiveTokens.get(cid2));

    // add/update/remove tokens and verify recovered
    ContainerId cid3 = BuilderUtils.newContainerId(3, 3, 3, 3);
    Long expTime3 = 135798642L;
    stateStore.storeContainerToken(cid3, expTime3);
    stateStore.removeContainerToken(cid1);
    expTime2 += 246897531L;
    stateStore.storeContainerToken(cid2, expTime2);
    prevKey = currentKey;
    stateStore.storeContainerTokenPreviousMasterKey(prevKey);
    currentKey = keygen.generateKey();
    stateStore.storeContainerTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    loadedActiveTokens = state.getActiveTokens();
    assertEquals(2, loadedActiveTokens.size());
    assertNull(loadedActiveTokens.get(cid1));
    assertEquals(expTime2, loadedActiveTokens.get(cid2));
    assertEquals(expTime3, loadedActiveTokens.get(cid3));
  }

  @Test
  public void testLogDeleterStorage() throws IOException {
    // test empty when no state
    RecoveredLogDeleterState state = stateStore.loadLogDeleterState();
    assertTrue(state.getLogDeleterMap().isEmpty());

    // store log deleter state
    final ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    LogDeleterProto proto1 = LogDeleterProto.newBuilder()
        .setUser("user1")
        .setDeletionTime(1234)
        .build();
    stateStore.storeLogDeleter(appId1, proto1);

    // restart state store and verify recovered
    restartStateStore();
    state = stateStore.loadLogDeleterState();
    assertEquals(1, state.getLogDeleterMap().size());
    assertEquals(proto1, state.getLogDeleterMap().get(appId1));

    // store another log deleter
    final ApplicationId appId2 = ApplicationId.newInstance(2, 2);
    LogDeleterProto proto2 = LogDeleterProto.newBuilder()
        .setUser("user2")
        .setDeletionTime(5678)
        .build();
    stateStore.storeLogDeleter(appId2, proto2);

    // restart state store and verify recovered
    restartStateStore();
    state = stateStore.loadLogDeleterState();
    assertEquals(2, state.getLogDeleterMap().size());
    assertEquals(proto1, state.getLogDeleterMap().get(appId1));
    assertEquals(proto2, state.getLogDeleterMap().get(appId2));

    // remove a deleter and verify removed after restart and recovery
    stateStore.removeLogDeleter(appId1);
    restartStateStore();
    state = stateStore.loadLogDeleterState();
    assertEquals(1, state.getLogDeleterMap().size());
    assertEquals(proto2, state.getLogDeleterMap().get(appId2));

    // remove last deleter and verify empty after restart and recovery
    stateStore.removeLogDeleter(appId2);
    restartStateStore();
    state = stateStore.loadLogDeleterState();
    assertTrue(state.getLogDeleterMap().isEmpty());
  }

  @Test
  public void testCompactionCycle() throws IOException {
    final DB mockdb = mock(DB.class);
    conf.setInt(YarnConfiguration.NM_RECOVERY_COMPACTION_INTERVAL_SECS, 1);
    NMLeveldbStateStoreService store = new NMLeveldbStateStoreService() {
      @Override
      protected void checkVersion() {}

      @Override
      protected DB openDatabase(Configuration conf) {
        return mockdb;
      }
    };
    store.init(conf);
    store.start();
    verify(mockdb, timeout(10000)).compactRange(
        (byte[]) isNull(), (byte[]) isNull());
    store.close();
  }

  private static class NMTokenSecretManagerForTest extends
      BaseNMTokenSecretManager {
    public MasterKey generateKey() {
      return createNewMasterKey().getMasterKey();
    }
  }

  private static class ContainerTokenKeyGeneratorForTest extends
      BaseContainerTokenSecretManager {
    public ContainerTokenKeyGeneratorForTest(Configuration conf) {
      super(conf);
    }

    public MasterKey generateKey() {
      return createNewMasterKey().getMasterKey();
    }
  }
}
