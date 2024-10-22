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

import static org.assertj.core.api.Assertions.assertThat;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredApplicationsState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerTokensState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerType;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredDeletionServiceState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLogAggregatorState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLogDeleterState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredNMTokensState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.BaseNMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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

  private List<RecoveredContainerState> loadContainersState(
      RecoveryIterator<RecoveredContainerState> it) throws IOException {
    List<RecoveredContainerState> containers =
        new ArrayList<RecoveredContainerState>();
    while (it.hasNext()) {
      RecoveredContainerState rcs = it.next();
      containers.add(rcs);
    }
    return containers;
  }

  private List<ContainerManagerApplicationProto> loadApplicationProtos(
      RecoveryIterator<ContainerManagerApplicationProto> it)
      throws IOException {
    List<ContainerManagerApplicationProto> applicationProtos =
        new ArrayList<ContainerManagerApplicationProto>();
    while (it.hasNext()) {
      applicationProtos.add(it.next());
    }
    return applicationProtos;
  }

  private List<DeletionServiceDeleteTaskProto> loadDeletionTaskProtos(
      RecoveryIterator<DeletionServiceDeleteTaskProto> it) throws IOException {
    List<DeletionServiceDeleteTaskProto> deleteTaskProtos =
        new ArrayList<DeletionServiceDeleteTaskProto>();
    while (it.hasNext()) {
      deleteTaskProtos.add(it.next());
    }
    return deleteTaskProtos;
  }

  private Map<String, RecoveredUserResources> loadUserResources(
      RecoveryIterator<Map.Entry<String, RecoveredUserResources>> it)
      throws IOException {
    Map<String, RecoveredUserResources> userResources =
        new HashMap<String, RecoveredUserResources>();
    while (it.hasNext()) {
      Map.Entry<String, RecoveredUserResources> entry = it.next();
      userResources.put(entry.getKey(), entry.getValue());
    }
    return userResources;
  }

  private Map<ApplicationAttemptId, MasterKey> loadNMTokens(
      RecoveryIterator<Map.Entry<ApplicationAttemptId, MasterKey>> it)
      throws IOException {
    Map<ApplicationAttemptId, MasterKey> nmTokens =
        new HashMap<ApplicationAttemptId, MasterKey>();
    while (it.hasNext()) {
      Map.Entry<ApplicationAttemptId, MasterKey> entry = it.next();
      nmTokens.put(entry.getKey(), entry.getValue());
    }
    return nmTokens;
  }

  private Map<ContainerId, Long> loadContainerTokens(
      RecoveryIterator<Map.Entry<ContainerId, Long>> it) throws IOException {
    Map<ContainerId, Long> containerTokens =
        new HashMap<ContainerId, Long>();
    while (it.hasNext()) {
      Map.Entry<ContainerId, Long> entry = it.next();
      containerTokens.put(entry.getKey(), entry.getValue());
    }
    return containerTokens;
  }

  private List<LocalizedResourceProto> loadCompletedResources(
      RecoveryIterator<LocalizedResourceProto> it) throws IOException {
    List<LocalizedResourceProto> completedResources =
        new ArrayList<LocalizedResourceProto>();
    while (it != null && it.hasNext()) {
      completedResources.add(it.next());
    }
    return completedResources;
  }

  private Map<LocalResourceProto, Path> loadStartedResources(
      RecoveryIterator <Map.Entry<LocalResourceProto, Path>> it)
      throws IOException {
    Map<LocalResourceProto, Path> startedResources =
        new HashMap<LocalResourceProto, Path>();
    while (it != null &&it.hasNext()) {
      Map.Entry<LocalResourceProto, Path> entry = it.next();
      startedResources.put(entry.getKey(), entry.getValue());
    }
    return startedResources;
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
    assertTrue(loadCompletedResources(pubts.getCompletedResourcesIterator())
        .isEmpty());
    assertTrue(loadStartedResources(pubts.getStartedResourcesIterator())
        .isEmpty());
    assertTrue(loadUserResources(state.getIterator()).isEmpty());
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
    List<ContainerManagerApplicationProto> apps =
        loadApplicationProtos(state.getIterator());
    assertTrue(apps.isEmpty());

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
    apps = loadApplicationProtos(state.getIterator());
    assertEquals(1, apps.size());
    assertEquals(appProto1, apps.get(0));

    // add a new app
    final ApplicationId appId2 = ApplicationId.newInstance(1234, 2);
    builder = ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId2).getProto());
    builder.setUser("user2");
    ContainerManagerApplicationProto appProto2 = builder.build();
    stateStore.storeApplication(appId2, appProto2);
    restartStateStore();
    state = stateStore.loadApplicationsState();
    apps = loadApplicationProtos(state.getIterator());
    assertEquals(2, apps.size());
    assertTrue(apps.contains(appProto1));
    assertTrue(apps.contains(appProto2));

    // test removing an application
    stateStore.removeApplication(appId2);
    restartStateStore();
    state = stateStore.loadApplicationsState();
    apps = loadApplicationProtos(state.getIterator());
    assertEquals(1, apps.size());
    assertEquals(appProto1, apps.get(0));
  }


  @Test
  public void testContainerStorageWhenContainerIsRequested()
      throws IOException {
    final ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    final RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(0, rcs.getVersion());
    assertEquals(containerParams.getContainerStartTime().longValue(),
        rcs.getStartTime());
    assertEquals(RecoveredContainerStatus.REQUESTED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerParams.getContainerRequest(), rcs.getStartRequest());
    assertTrue(rcs.getDiagnostics().isEmpty());
    assertEquals(containerParams.getContainerResource(), rcs.getCapability());
  }



  @Test
  public void testContainerStorageWhenContainerIsQueued()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    StartContainerRequest containerReq = containerParams.getContainerRequest();
    Resource containerResource = containerParams.getContainerResource();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);

    stateStore.storeContainerQueued(containerId);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.QUEUED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertTrue(rcs.getDiagnostics().isEmpty());
    assertEquals(containerResource, rcs.getCapability());
  }

  @Test
  public void testContainerStorageWhenContainerIsLaunched()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    StartContainerRequest containerReq = containerParams.getContainerRequest();
    Resource containerResource = containerParams.getContainerResource();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);
    stateStore.storeContainerQueued(containerId);

    StringBuilder diags = launchContainerWithDiagnostics(containerId);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());
    assertEquals(diags.toString(), rcs.getDiagnostics());
    assertEquals(containerResource, rcs.getCapability());
  }

  @Test
  public void testContainerStorageWhenContainerIsPaused()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    StartContainerRequest containerReq = containerParams.getContainerRequest();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);
    stateStore.storeContainerQueued(containerId);

    stateStore.storeContainerPaused(containerId);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.PAUSED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(containerReq, rcs.getStartRequest());

    // Resume the container
    stateStore.removeContainerPaused(containerId);
    restartStateStore();
    recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
  }

  @Test
  public void testContainerStorageWhenContainerSizeIncreased()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);
    stateStore.storeContainerQueued(containerId);
    launchContainerWithDiagnostics(containerId);

    increaseContainerSize(containerId);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(0, rcs.getVersion());
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(Resource.newInstance(2468, 4), rcs.getCapability());
  }

  @Test
  public void testContainerStorageWhenContainerMarkedAsKilled()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);
    stateStore.storeContainerQueued(containerId);
    StringBuilder diags = launchContainerWithDiagnostics(containerId);
    ContainerTokenIdentifier updateTokenIdentifier =
        increaseContainerSize(containerId);

    markContainerAsKilled(containerId, diags);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.LAUNCHED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertTrue(rcs.getKilled());
    ContainerTokenIdentifier tokenReadFromRequest = BuilderUtils
        .newContainerTokenIdentifier(rcs.getStartRequest()
            .getContainerToken());
    assertEquals(updateTokenIdentifier, tokenReadFromRequest);
    assertEquals(diags.toString(), rcs.getDiagnostics());
  }

  @Test
  public void testContainerStorageWhenContainerCompleted()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();
    ApplicationAttemptId appAttemptId = containerParams.getAppAttemptId();

    storeNewContainerRecordWithoutStartContainerRequest(appAttemptId);
    stateStore.storeContainerQueued(containerId);
    StringBuilder diags = launchContainerWithDiagnostics(containerId);
    markContainerAsKilled(containerId, diags);

    // add yet more diags, mark container completed
    diags.append("some final diags");
    stateStore.storeContainerDiagnostics(containerId, diags);
    stateStore.storeContainerCompleted(containerId, 21);
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.COMPLETED, rcs.getStatus());
    assertEquals(21, rcs.getExitCode());
    assertTrue(rcs.getKilled());
    assertEquals(diags.toString(), rcs.getDiagnostics());
  }

  @Test
  public void testContainerStorage() throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();

    // remaining retry attempts, work dir and log dir are stored
    stateStore.storeContainerRemainingRetryAttempts(containerId, 6);
    stateStore.storeContainerWorkDir(containerId, "/test/workdir");
    stateStore.storeContainerLogDir(containerId, "/test/logdir");
    restartStateStore();

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(6, rcs.getRemainingRetryAttempts());
    assertEquals("/test/workdir", rcs.getWorkDir());
    assertEquals("/test/logdir", rcs.getLogDir());
    validateRetryAttempts(containerId);
  }

  @Test
  public void testContainerStorageWhenContainerRemoved()
      throws IOException {
    ContainerStateConstructParams containerParams =
        storeContainerInStateStore();
    ContainerId containerId = containerParams.getContainerId();

    // remove the container and verify not recovered
    stateStore.removeContainer(containerId);
    restartStateStore();
    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertTrue(recoveredContainers.isEmpty());
    // recover again to check remove clears all containers
    restartStateStore();
    NMStateStoreService nmStoreSpy = spy(stateStore);
    loadContainersState(nmStoreSpy.getContainerStateIterator());
    verify(nmStoreSpy, times(0)).removeContainer(any(ContainerId.class));
  }

  private ContainerStateConstructParams storeContainerInStateStore()
      throws IOException {
    // test empty when no state
    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertTrue(recoveredContainers.isEmpty());

    // create a container request
    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 4);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 5);
    Resource containerResource = Resource.newInstance(1024, 2);
    StartContainerRequest containerReq =
        createContainerRequest(containerId, containerResource);

    long anyContainerStartTime = 1573155078494L;
    stateStore.storeContainer(containerId, 0, anyContainerStartTime,
        containerReq);

    // verify the container version key is not stored for new containers
    DB db = stateStore.getDB();
    assertNull("version key present for new container", db.get(bytes(
        stateStore.getContainerVersionKey(containerId.toString()))));

    return new ContainerStateConstructParams()
        .setContainerRequest(containerReq)
        .setContainerResource(containerResource)
        .setContainerStartTime(anyContainerStartTime)
        .setAppAttemptId(appAttemptId)
        .setContainerId(containerId);
  }

  private static class ContainerStateConstructParams {
    private StartContainerRequest containerRequest;
    private Resource containerResource;
    private Long containerStartTime;
    private ApplicationAttemptId appAttemptId;
    private ContainerId containerId;

    public ApplicationAttemptId getAppAttemptId() {
      return appAttemptId;
    }
    public ContainerStateConstructParams setAppAttemptId(ApplicationAttemptId
        theAppAttemptId) {
      this.appAttemptId = theAppAttemptId;
      return this;
    }
    public ContainerId getContainerId() {
      return containerId;
    }
    public ContainerStateConstructParams setContainerId(ContainerId
        theContainerId) {
      this.containerId = theContainerId;
      return this;
    }

    public StartContainerRequest getContainerRequest() {
      return containerRequest;
    }
    public ContainerStateConstructParams setContainerRequest(
        StartContainerRequest theContainerRequest) {
      this.containerRequest = theContainerRequest;
      return this;
    }

    public Resource getContainerResource() {
      return containerResource;
    }

    public ContainerStateConstructParams setContainerResource(
        Resource theContainerResource) {
      this.containerResource = theContainerResource;
      return this;
    }

    public Long getContainerStartTime() {
      return containerStartTime;
    }

    public ContainerStateConstructParams setContainerStartTime(
        Long theContainerStartTime) {
      this.containerStartTime = theContainerStartTime;
      return this;
    }
  }

  private void markContainerAsKilled(ContainerId containerId,
      StringBuilder diags) throws IOException {
    // mark the container killed, add some more diags
    diags.append("some more diags for container");
    stateStore.storeContainerDiagnostics(containerId, diags);
    stateStore.storeContainerKilled(containerId);
  }

  private ContainerTokenIdentifier increaseContainerSize(
      ContainerId containerId) throws IOException {
    ContainerTokenIdentifier updateTokenIdentifier =
        new ContainerTokenIdentifier(containerId, "host", "user",
            Resource.newInstance(2468, 4), 9876543210L, 42, 2468,
            Priority.newInstance(7), 13579);
    stateStore
        .storeContainerUpdateToken(containerId, updateTokenIdentifier);
    return updateTokenIdentifier;
  }

  private StringBuilder launchContainerWithDiagnostics(ContainerId containerId)
      throws IOException {
    StringBuilder diags = new StringBuilder();
    stateStore.storeContainerLaunched(containerId);
    diags.append("some diags for container");
    stateStore.storeContainerDiagnostics(containerId, diags);
    return diags;
  }

  private void storeNewContainerRecordWithoutStartContainerRequest(
      ApplicationAttemptId appAttemptId) throws IOException {
    // store a new container record without StartContainerRequest
    ContainerId containerId1 = ContainerId.newContainerId(appAttemptId, 6);
    stateStore.storeContainerLaunched(containerId1);

    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    // check whether the new container record is discarded
    assertEquals(1, recoveredContainers.size());
  }

  private void validateRetryAttempts(ContainerId containerId)
      throws IOException {
    // store finishTimeForRetryAttempts
    List<Long> finishTimeForRetryAttempts = Arrays.asList(1462700529039L,
        1462700529050L, 1462700529120L);
    stateStore.storeContainerRestartTimes(containerId,
        finishTimeForRetryAttempts);
    restartStateStore();
    RecoveredContainerState rcs =
        loadContainersState(stateStore.getContainerStateIterator()).get(0);
    List<Long> recoveredRestartTimes = rcs.getRestartTimes();
    assertEquals(1462700529039L, (long)recoveredRestartTimes.get(0));
    assertEquals(1462700529050L, (long)recoveredRestartTimes.get(1));
    assertEquals(1462700529120L, (long)recoveredRestartTimes.get(2));
  }

  private StartContainerRequest createContainerRequest(
          ContainerId containerId, Resource res) {
    return createContainerRequestInternal(containerId, res);
  }

  private StartContainerRequest createContainerRequestInternal(ContainerId
          containerId, Resource res) {
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
        ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3}));
    ByteBuffer containerTokens =
        ByteBuffer.wrap(new byte[] {0x7, 0x8, 0x9, 0xa});
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, "viewuser");
    acls.put(ApplicationAccessType.MODIFY_APP, "moduser");
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, env, containerCmds, serviceData, containerTokens,
        acls);
    Resource containerRsrc = Resource.newInstance(1357, 3);

    if (res != null) {
      containerRsrc = res;
    }
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
  public void testLocalTrackerStateIterator() throws IOException {
    String user1 = "somebody";
    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    ApplicationId appId2 = ApplicationId.newInstance(2, 2);

    String user2 = "someone";
    ApplicationId appId3 = ApplicationId.newInstance(3, 3);

    // start and finish local resource for applications
    Path appRsrcPath1 = new Path("hdfs://some/app/resource1");
    LocalResourcePBImpl rsrcPb1 = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath1),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto1 = rsrcPb1.getProto();
    Path appRsrcLocalPath1 = new Path("/some/local/dir/for/apprsrc1");
    Path appRsrcPath2 = new Path("hdfs://some/app/resource2");
    LocalResourcePBImpl rsrcPb2 = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath2),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto2 = rsrcPb2.getProto();
    Path appRsrcLocalPath2 = new Path("/some/local/dir/for/apprsrc2");
    Path appRsrcPath3 = new Path("hdfs://some/app/resource3");
    LocalResourcePBImpl rsrcPb3 = (LocalResourcePBImpl)
        LocalResource.newInstance(
            URL.fromPath(appRsrcPath3),
            LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
            123L, 456L);
    LocalResourceProto appRsrcProto3 = rsrcPb3.getProto();
    Path appRsrcLocalPath3 = new Path("/some/local/dir/for/apprsrc2");

    stateStore.startResourceLocalization(user1, appId1, appRsrcProto1,
        appRsrcLocalPath1);
    stateStore.startResourceLocalization(user1, appId2, appRsrcProto2,
        appRsrcLocalPath2);
    stateStore.startResourceLocalization(user2, appId3, appRsrcProto3,
        appRsrcLocalPath3);

    LocalizedResourceProto appLocalizedProto1 =
        LocalizedResourceProto.newBuilder()
            .setResource(appRsrcProto1)
            .setLocalPath(appRsrcLocalPath1.toString())
            .setSize(1234567L)
            .build();
    LocalizedResourceProto appLocalizedProto2 =
        LocalizedResourceProto.newBuilder()
            .setResource(appRsrcProto2)
            .setLocalPath(appRsrcLocalPath2.toString())
            .setSize(1234567L)
            .build();
    LocalizedResourceProto appLocalizedProto3 =
        LocalizedResourceProto.newBuilder()
            .setResource(appRsrcProto3)
            .setLocalPath(appRsrcLocalPath3.toString())
            .setSize(1234567L)
            .build();


    stateStore.finishResourceLocalization(user1, appId1, appLocalizedProto1);
    stateStore.finishResourceLocalization(user1, appId2, appLocalizedProto2);
    stateStore.finishResourceLocalization(user2, appId3, appLocalizedProto3);


    List<LocalizedResourceProto> completedResources =
        new ArrayList<LocalizedResourceProto>();
    Map<LocalResourceProto, Path> startedResources =
        new HashMap<LocalResourceProto, Path>();

    // restart and verify two users exist and two apps completed for user1.
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(2, userResources.size());

    RecoveredUserResources uResource = userResources.get(user1);
    assertEquals(2, uResource.getAppTrackerStates().size());
    LocalResourceTrackerState app1ts =
        uResource.getAppTrackerStates().get(appId1);
    assertNotNull(app1ts);
    completedResources = loadCompletedResources(
        app1ts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        app1ts.getStartedResourcesIterator());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, completedResources.size());
    assertEquals(appLocalizedProto1,
        completedResources.iterator().next());
    LocalResourceTrackerState app2ts =
        uResource.getAppTrackerStates().get(appId2);
    assertNotNull(app2ts);
    completedResources = loadCompletedResources(
        app2ts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        app2ts.getStartedResourcesIterator());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, completedResources.size());
    assertEquals(appLocalizedProto2,
        completedResources.iterator().next());
  }

  @Test
  public void testStartResourceLocalizationForApplicationResource()
      throws IOException {
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

    List<LocalizedResourceProto> completedResources =
        new ArrayList<LocalizedResourceProto>();
    Map<LocalResourceProto, Path> startedResources =
        new HashMap<LocalResourceProto, Path>();

    // restart and verify only app resource is marked in-progress
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    completedResources = loadCompletedResources(
        pubts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        pubts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertTrue(startedResources.isEmpty());
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    completedResources = loadCompletedResources(
        privts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        privts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, rur.getAppTrackerStates().size());
    LocalResourceTrackerState appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    completedResources = loadCompletedResources(
        appts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        appts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertEquals(1, startedResources.size());
    assertEquals(appRsrcLocalPath,
        startedResources.get(appRsrcProto));
  }

  @Test
  public void testStartResourceLocalizationForPublicResources()
      throws IOException {
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
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

    // restart and verify resources are marked in-progress
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    List<LocalizedResourceProto> completedResources = loadCompletedResources(
        pubts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources = loadStartedResources(
        pubts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertEquals(2, startedResources.size());
    assertEquals(pubRsrcLocalPath1,
        startedResources.get(pubRsrcProto1));
    assertEquals(pubRsrcLocalPath2,
        startedResources.get(pubRsrcProto2));
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(0, userResources.size());
  }

  @Test
  public void testStartResourceLocalizationForPrivateResource()
      throws IOException {
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
            URL.fromPath(privRsrcPath),
            LocalResourceType.PATTERN, LocalResourceVisibility.PRIVATE,
            789L, 680L, "*pattern*");
    LocalResourceProto privRsrcProto = rsrcPb.getProto();
    Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
    String user = "somebody";
    stateStore.startResourceLocalization(user, null, privRsrcProto,
        privRsrcLocalPath);

    // restart and verify resources are marked in-progress
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    List<LocalizedResourceProto> completedResources = loadCompletedResources(
        privts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources = loadStartedResources(
        privts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertEquals(1, startedResources.size());
    assertEquals(privRsrcLocalPath,
        startedResources.get(privRsrcProto));
    assertEquals(0, rur.getAppTrackerStates().size());
  }

  @Test
  public void testFinishResourceLocalizationForApplicationResource()
      throws IOException {
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

    List<LocalizedResourceProto> completedResources =
        new ArrayList<LocalizedResourceProto>();
    Map<LocalResourceProto, Path> startedResources =
        new HashMap<LocalResourceProto, Path>();

    // restart and verify only app resource is completed
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    completedResources = loadCompletedResources(
        pubts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        pubts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertTrue(startedResources.isEmpty());
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    completedResources = loadCompletedResources(
        privts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        privts.getStartedResourcesIterator());
    assertTrue(completedResources.isEmpty());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, rur.getAppTrackerStates().size());
    LocalResourceTrackerState appts = rur.getAppTrackerStates().get(appId);
    assertNotNull(appts);
    completedResources = loadCompletedResources(
        appts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        appts.getStartedResourcesIterator());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, completedResources.size());
    assertEquals(appLocalizedProto,
        completedResources.iterator().next());
  }

  @Test
  public void testFinishResourceLocalizationForPublicResources()
      throws IOException {
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
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

    // finish some of the resources
    LocalizedResourceProto pubLocalizedProto1 =
        LocalizedResourceProto.newBuilder()
          .setResource(pubRsrcProto1)
          .setLocalPath(pubRsrcLocalPath1.toString())
          .setSize(pubRsrcProto1.getSize())
          .build();
    stateStore.finishResourceLocalization(null, null, pubLocalizedProto1);

    // restart and verify state
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    List<LocalizedResourceProto> completedResources = loadCompletedResources(
        pubts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources = loadStartedResources(
        pubts.getStartedResourcesIterator());
    assertEquals(1, completedResources.size());
    assertEquals(pubLocalizedProto1,
        completedResources.iterator().next());
    assertEquals(1, startedResources.size());
    assertEquals(pubRsrcLocalPath2,
        startedResources.get(pubRsrcProto2));
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(0, userResources.size());
  }

  @Test
  public void testFinishResourceLocalizationForPrivateResource()
      throws IOException {
    String user = "somebody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    Path privRsrcPath = new Path("hdfs://some/private/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
            URL.fromPath(privRsrcPath),
            LocalResourceType.PATTERN, LocalResourceVisibility.PRIVATE,
            789L, 680L, "*pattern*");
    LocalResourceProto privRsrcProto = rsrcPb.getProto();
    Path privRsrcLocalPath = new Path("/some/local/dir/for/privrsrc");
    stateStore.startResourceLocalization(user, null, privRsrcProto,
        privRsrcLocalPath);

    LocalizedResourceProto privLocalizedProto =
        LocalizedResourceProto.newBuilder()
          .setResource(privRsrcProto)
          .setLocalPath(privRsrcLocalPath.toString())
          .setSize(privRsrcProto.getSize())
          .build();
    stateStore.finishResourceLocalization(user, null, privLocalizedProto);

    // restart and verify state
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    List<LocalizedResourceProto> completedResources = loadCompletedResources(
        pubts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources = loadStartedResources(
        pubts.getStartedResourcesIterator());
    assertEquals(0, completedResources.size());
    assertEquals(0, startedResources.size());
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertEquals(1, userResources.size());
    RecoveredUserResources rur = userResources.get(user);
    LocalResourceTrackerState privts = rur.getPrivateTrackerState();
    assertNotNull(privts);
    completedResources = loadCompletedResources(
        privts.getCompletedResourcesIterator());
    startedResources = loadStartedResources(
        privts.getStartedResourcesIterator());
    assertEquals(1, completedResources.size());
    assertEquals(privLocalizedProto,
        completedResources.iterator().next());
    assertTrue(startedResources.isEmpty());
    assertEquals(0, rur.getAppTrackerStates().size());
    LocalResourceTrackerState appts = rur.getAppTrackerStates().get(appId);
    assertNull(appts);
    assertTrue(startedResources.isEmpty());
    assertEquals(1, completedResources.size());
  }

  @Test
  public void testRemoveLocalizedResourceForApplicationResource()
      throws IOException {
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
  }

  @Test
  public void testRemoveLocalizedResourceForPublicResources()
      throws IOException {
    // add public resources and remove some
    Path pubRsrcPath1 = new Path("hdfs://some/public/resource1");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
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

    // restart and verify state
    restartStateStore();
    RecoveredLocalizationState state = stateStore.loadLocalizationState();
    LocalResourceTrackerState pubts = state.getPublicTrackerState();
    List<LocalizedResourceProto> completedResources =
        loadCompletedResources(pubts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources =
        loadStartedResources(pubts.getStartedResourcesIterator());
    assertTrue(startedResources.isEmpty());
    assertEquals(1, completedResources.size());
    assertEquals(pubLocalizedProto1,
        completedResources.iterator().next());
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertTrue(userResources.isEmpty());
  }

  @Test
  public void testRemoveLocalizedResourceForPrivateResource()
      throws IOException {
    String user = "somebody";

    Path privRsrcPath = new Path("hdfs://some/private/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl) LocalResource
        .newInstance(
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
    List<LocalizedResourceProto> completedResources =
        loadCompletedResources(pubts.getCompletedResourcesIterator());
    Map<LocalResourceProto, Path> startedResources =
        loadStartedResources(pubts.getStartedResourcesIterator());
    assertTrue(startedResources.isEmpty());
    assertEquals(0, completedResources.size());
    Map<String, RecoveredUserResources> userResources =
        loadUserResources(state.getIterator());
    assertTrue(userResources.isEmpty());
  }

  @Test
  public void testDeletionTaskStorage() throws IOException {
    // test empty when no state
    RecoveredDeletionServiceState state =
        stateStore.loadDeletionServiceState();
    List<DeletionServiceDeleteTaskProto> deleteTaskProtos =
        loadDeletionTaskProtos(state.getIterator());
    assertTrue(deleteTaskProtos.isEmpty());

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
    deleteTaskProtos = loadDeletionTaskProtos(state.getIterator());
    assertEquals(1, deleteTaskProtos.size());
    assertEquals(proto, deleteTaskProtos.get(0));

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
    deleteTaskProtos = loadDeletionTaskProtos(state.getIterator());
    assertEquals(2, deleteTaskProtos.size());
    assertTrue(deleteTaskProtos.contains(proto));
    assertTrue(deleteTaskProtos.contains(proto2));


    // delete a task and verify gone after recovery
    stateStore.removeDeletionTask(proto2.getId());
    restartStateStore();
    state =  stateStore.loadDeletionServiceState();
    deleteTaskProtos = loadDeletionTaskProtos(state.getIterator());
    assertEquals(1, deleteTaskProtos.size());
    assertEquals(proto, deleteTaskProtos.get(0));

    // delete the last task and verify none left
    stateStore.removeDeletionTask(proto.getId());
    restartStateStore();
    state = stateStore.loadDeletionServiceState();
    deleteTaskProtos = loadDeletionTaskProtos(state.getIterator());
    assertTrue(deleteTaskProtos.isEmpty());  }

  @Test
  public void testNMTokenStorage() throws IOException {
    // test empty when no state
    RecoveredNMTokensState state = stateStore.loadNMTokensState();
    Map<ApplicationAttemptId, MasterKey> loadedAppKeys =
        loadNMTokens(state.getIterator());
    assertNull(state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(loadedAppKeys.isEmpty());

    // store a master key and verify recovered
    NMTokenSecretManagerForTest secretMgr = new NMTokenSecretManagerForTest();
    MasterKey currentKey = secretMgr.generateKey();
    stateStore.storeNMTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    loadedAppKeys = loadNMTokens(state.getIterator());
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(loadedAppKeys.isEmpty());

    // store a previous key and verify recovered
    MasterKey prevKey = secretMgr.generateKey();
    stateStore.storeNMTokenPreviousMasterKey(prevKey);
    restartStateStore();
    state = stateStore.loadNMTokensState();
    loadedAppKeys = loadNMTokens(state.getIterator());
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    assertTrue(loadedAppKeys.isEmpty());

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
    loadedAppKeys = loadNMTokens(state.getIterator());
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
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
    loadedAppKeys = loadNMTokens(state.getIterator());
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
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
    Map<ContainerId, Long> loadedActiveTokens = loadContainerTokens(state.it);
    assertNull(state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(loadedActiveTokens.isEmpty());

    // store a master key and verify recovered
    ContainerTokenKeyGeneratorForTest keygen =
        new ContainerTokenKeyGeneratorForTest(new YarnConfiguration());
    MasterKey currentKey = keygen.generateKey();
    stateStore.storeContainerTokenCurrentMasterKey(currentKey);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    loadedActiveTokens = loadContainerTokens(state.it);
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertNull(state.getPreviousMasterKey());
    assertTrue(loadedActiveTokens.isEmpty());

    // store a previous key and verify recovered
    MasterKey prevKey = keygen.generateKey();
    stateStore.storeContainerTokenPreviousMasterKey(prevKey);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    loadedActiveTokens = loadContainerTokens(state.it);
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
    assertTrue(loadedActiveTokens.isEmpty());

    // store a few container tokens and verify recovered
    ContainerId cid1 = BuilderUtils.newContainerId(1, 1, 1, 1);
    Long expTime1 = 1234567890L;
    ContainerId cid2 = BuilderUtils.newContainerId(2, 2, 2, 2);
    Long expTime2 = 9876543210L;
    stateStore.storeContainerToken(cid1, expTime1);
    stateStore.storeContainerToken(cid2, expTime2);
    restartStateStore();
    state = stateStore.loadContainerTokensState();
    loadedActiveTokens = loadContainerTokens(state.it);
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
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
    loadedActiveTokens = loadContainerTokens(state.it);
    assertEquals(currentKey, state.getCurrentMasterKey());
    assertEquals(prevKey, state.getPreviousMasterKey());
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
  public void testLogAggregatorStorage() throws IOException {
    // test empty when no state
    RecoveredLogAggregatorState state = stateStore.loadLogAggregatorState();
    assertTrue(state.getLogAggregators().isEmpty());

    // store log deleter state
    final ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId1 = ApplicationAttemptId.newInstance(appId1, 1);
    final ContainerId containerId1 = ContainerId.newContainerId(appAttemptId1, 1);
    stateStore.storeLogAggregator(containerId1);

    // restart state store and verify recovered
    restartStateStore();
    state = stateStore.loadLogAggregatorState();
    assertEquals(1, state.getLogAggregators().size());
    assertEquals(containerId1, state.getLogAggregators().get(0));

    // store another log aggregator
    final ApplicationId appId2 = ApplicationId.newInstance(2, 2);
    ApplicationAttemptId appAttemptId2 = ApplicationAttemptId.newInstance(appId2, 1);
    final ContainerId containerId2 = ContainerId.newContainerId(appAttemptId2, 1);
    stateStore.storeLogAggregator(containerId2);

    // restart state store and verify recovered
    restartStateStore();
    state = stateStore.loadLogAggregatorState();
    assertEquals(2, state.getLogAggregators().size());

    // remove a deleter and verify removed after restart and recovery
    stateStore.removeLogAggregator(containerId1);
    restartStateStore();
    state = stateStore.loadLogAggregatorState();
    assertEquals(1, state.getLogAggregators().size());
    assertEquals(containerId2, state.getLogAggregators().get(0));

    // remove last deleter and verify empty after restart and recovery
    stateStore.removeLogAggregator(containerId2);
    restartStateStore();
    state = stateStore.loadLogAggregatorState();
    assertTrue(state.getLogAggregators().isEmpty());
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
    verify(mockdb, timeout(10000).atLeastOnce()).compactRange(
        (byte[]) isNull(), (byte[]) isNull());
    store.close();
  }

  @Test
  public void testUnexpectedKeyDoesntThrowException() throws IOException {
    // test empty when no state
    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertTrue(recoveredContainers.isEmpty());

    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        4);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 5);
    StartContainerRequest startContainerRequest = storeMockContainer(
        containerId);

    // add a invalid key
    byte[] invalidKey = ("ContainerManager/containers/"
    + containerId.toString() + "/invalidKey1234").getBytes();
    stateStore.getDB().put(invalidKey, new byte[1]);
    restartStateStore();
    recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    assertEquals(RecoveredContainerStatus.REQUESTED, rcs.getStatus());
    assertEquals(ContainerExitStatus.INVALID, rcs.getExitCode());
    assertEquals(false, rcs.getKilled());
    assertEquals(startContainerRequest, rcs.getStartRequest());
    assertTrue(rcs.getDiagnostics().isEmpty());
    assertEquals(RecoveredContainerType.KILL, rcs.getRecoveryType());
    // assert unknown keys are cleaned up finally
    assertNotNull(stateStore.getDB().get(invalidKey));
    stateStore.removeContainer(containerId);
    assertNull(stateStore.getDB().get(invalidKey));
  }

  @Test
  public void testAMRMProxyStorage() throws IOException {
    RecoveredAMRMProxyState state = stateStore.loadAMRMProxyState();
    assertThat(state.getCurrentMasterKey()).isNull();
    assertThat(state.getNextMasterKey()).isNull();
    assertThat(state.getAppContexts()).isEmpty();

    ApplicationId appId1 = ApplicationId.newInstance(1, 1);
    ApplicationId appId2 = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId attemptId1 =
        ApplicationAttemptId.newInstance(appId1, 1);
    ApplicationAttemptId attemptId2 =
        ApplicationAttemptId.newInstance(appId2, 2);
    String key1 = "key1";
    String key2 = "key2";
    byte[] data1 = "data1".getBytes();
    byte[] data2 = "data2".getBytes();

    AMRMProxyTokenSecretManager secretManager =
        new AMRMProxyTokenSecretManager(stateStore);
    secretManager.init(conf);
    // Generate currentMasterKey
    secretManager.start();

    try {
      // Add two applications, each with two data entries
      stateStore.storeAMRMProxyAppContextEntry(attemptId1, key1, data1);
      stateStore.storeAMRMProxyAppContextEntry(attemptId2, key1, data1);
      stateStore.storeAMRMProxyAppContextEntry(attemptId1, key2, data2);
      stateStore.storeAMRMProxyAppContextEntry(attemptId2, key2, data2);

      // restart state store and verify recovered
      restartStateStore();
      secretManager.setNMStateStoreService(stateStore);
      state = stateStore.loadAMRMProxyState();
      assertEquals(state.getCurrentMasterKey(),
          secretManager.getCurrentMasterKeyData().getMasterKey());
      assertThat(state.getNextMasterKey()).isNull();
      assertThat(state.getAppContexts()).hasSize(2);
      // app1
      Map<String, byte[]> map = state.getAppContexts().get(attemptId1);
      assertNotEquals(map, null);
      assertThat(map).hasSize(2);
      assertTrue(Arrays.equals(map.get(key1), data1));
      assertTrue(Arrays.equals(map.get(key2), data2));
      // app2
      map = state.getAppContexts().get(attemptId2);
      assertNotEquals(map, null);
      assertThat(map).hasSize(2);
      assertTrue(Arrays.equals(map.get(key1), data1));
      assertTrue(Arrays.equals(map.get(key2), data2));

      // Generate next master key and remove one entry of app2
      secretManager.rollMasterKey();
      stateStore.removeAMRMProxyAppContextEntry(attemptId2, key1);

      // restart state store and verify recovered
      restartStateStore();
      secretManager.setNMStateStoreService(stateStore);
      state = stateStore.loadAMRMProxyState();
      assertEquals(state.getCurrentMasterKey(),
          secretManager.getCurrentMasterKeyData().getMasterKey());
      assertEquals(state.getNextMasterKey(),
          secretManager.getNextMasterKeyData().getMasterKey());
      assertEquals(state.getAppContexts().size(), 2);
      // app1
      map = state.getAppContexts().get(attemptId1);
      assertThat(map).isNotNull();
      assertThat(map).hasSize(2);
      assertTrue(Arrays.equals(map.get(key1), data1));
      assertTrue(Arrays.equals(map.get(key2), data2));
      // app2
      map = state.getAppContexts().get(attemptId2);
      assertThat(map).isNotNull();
      assertThat(map).hasSize(1);
      assertTrue(Arrays.equals(map.get(key2), data2));

      // Activate next master key and remove all entries of app1
      secretManager.activateNextMasterKey();
      stateStore.removeAMRMProxyAppContext(attemptId1);

      // restart state store and verify recovered
      restartStateStore();
      secretManager.setNMStateStoreService(stateStore);
      state = stateStore.loadAMRMProxyState();
      assertEquals(state.getCurrentMasterKey(),
          secretManager.getCurrentMasterKeyData().getMasterKey());
      assertThat(state.getNextMasterKey()).isNull();
      assertThat(state.getAppContexts()).hasSize(1);
      // app2 only
      map = state.getAppContexts().get(attemptId2);
      assertThat(map).isNotNull();
      assertThat(map).hasSize(1);
      assertTrue(Arrays.equals(map.get(key2), data2));
    } finally {
      secretManager.stop();
    }
  }

  @Test
  public void testStateStoreForResourceMapping() throws IOException {
    // test that stateStore is initially empty
    List<RecoveredContainerState> recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertTrue(recoveredContainers.isEmpty());

    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        4);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 5);
    storeMockContainer(containerId);

    Container container = mock(Container.class);
    when(container.getContainerId()).thenReturn(containerId);
    ResourceMappings resourceMappings = new ResourceMappings();
    when(container.getResourceMappings()).thenReturn(resourceMappings);

    stateStore.storeAssignedResources(container, "gpu",
        Arrays.asList(new GpuDevice(1, 1), new GpuDevice(2, 2),
            new GpuDevice(3, 3)));

    // This will overwrite the above
    List<Serializable> gpuRes1 = Arrays.asList(
        new GpuDevice(1, 1), new GpuDevice(2, 2), new GpuDevice(4, 4));
    stateStore.storeAssignedResources(container, "gpu", gpuRes1);

    List<Serializable> fpgaRes = Arrays.asList(
        new FpgaDevice("testType", 3, 3, "testIPID"),
        new FpgaDevice("testType", 4, 4, "testIPID"),
        new FpgaDevice("testType", 5, 5, "testIPID"),
        new FpgaDevice("testType", 6, 6, "testIPID"));
    stateStore.storeAssignedResources(container, "fpga", fpgaRes);

    List<Serializable> numaRes = Arrays.asList(
        new NumaResourceAllocation("testmemNodeId", 2048, "testCpuNodeId", 10));
    stateStore.storeAssignedResources(container, "numa", numaRes);

    restartStateStore();
    recoveredContainers =
        loadContainersState(stateStore.getContainerStateIterator());
    assertEquals(1, recoveredContainers.size());
    RecoveredContainerState rcs = recoveredContainers.get(0);
    List<Serializable> resources = rcs.getResourceMappings()
        .getAssignedResources("gpu");
    Assert.assertEquals(gpuRes1, resources);
    Assert.assertEquals(gpuRes1, resourceMappings.getAssignedResources("gpu"));

    resources = rcs.getResourceMappings().getAssignedResources("fpga");
    Assert.assertEquals(fpgaRes, resources);
    Assert.assertEquals(fpgaRes, resourceMappings.getAssignedResources("fpga"));

    resources = rcs.getResourceMappings().getAssignedResources("numa");
    Assert.assertEquals(numaRes, resources);
    Assert.assertEquals(numaRes, resourceMappings.getAssignedResources("numa"));
    // test removing numa resources from state store
    stateStore.releaseAssignedResources(containerId, "numa");
    recoveredContainers = loadContainersState(stateStore.getContainerStateIterator());
    resourceMappings = recoveredContainers.get(0).getResourceMappings();
    assertTrue(resourceMappings.getAssignedResources("numa").isEmpty());

    // testing calling deletion of non-existing key doesn't break anything
    try {
      stateStore.releaseAssignedResources(containerId, "numa");
    }catch (RuntimeException e){
      Assert.fail("Should not throw exception while deleting non existing key from statestore");
    }
  }

  @Test
  public void testStateStoreNodeHealth() throws IOException {
    // keep the working DB clean, break a temp DB
    DB keepDB = stateStore.getDB();
    DB myMocked = mock(DB.class);
    stateStore.setDB(myMocked);

    ApplicationId appId = ApplicationId.newInstance(1234, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    DBException toThrow = new DBException();
    Mockito.doThrow(toThrow).when(myMocked).
        put(any(byte[].class), any(byte[].class));
    // write some data
    try {
      // chosen a simple method could be any of the "void" methods
      ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
      stateStore.storeContainerKilled(containerId);
    } catch (IOException ioErr) {
      // Cause should be wrapped DBException
      assertTrue(ioErr.getCause() instanceof DBException);
      // check the store is marked unhealthy
      assertFalse("Statestore should have been unhealthy",
          stateStore.isHealthy());
      return;
    } finally {
      // restore the working DB
      stateStore.setDB(keepDB);
    }
    Assert.fail("Expected exception not thrown");
  }

  @Test
  public void testEmptyRestartTimes() throws IOException {
    List<Long> restartTimes = new ArrayList<>();
    ApplicationId appId = ApplicationId.newInstance(1234, 3);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId,
        4);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 5);
    storeMockContainer(containerId);
    stateStore.storeContainerRestartTimes(containerId,
        restartTimes);
    restartStateStore();
    RecoveredContainerState rcs =
        loadContainersState(stateStore.getContainerStateIterator()).get(0);
    List<Long> recoveredRestartTimes = rcs.getRestartTimes();
    assertTrue(recoveredRestartTimes.isEmpty());
  }

  private StartContainerRequest storeMockContainer(ContainerId containerId)
      throws IOException {
    // create a container request
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
        ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3}));
    ByteBuffer containerTokens = ByteBuffer
        .wrap(new byte[] {0x7, 0x8, 0x9, 0xa});
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, "viewuser");
    acls.put(ApplicationAccessType.MODIFY_APP, "moduser");
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        localResources, env, containerCmds,
        serviceData, containerTokens, acls);
    Resource containerRsrc = Resource.newInstance(1357, 3);
    ContainerTokenIdentifier containerTokenId = new ContainerTokenIdentifier(
        containerId, "host", "user", containerRsrc, 9876543210L, 42, 2468,
        Priority.newInstance(7), 13579);
    Token containerToken = Token.newInstance(containerTokenId.getBytes(),
        ContainerTokenIdentifier.KIND.toString(), "password".getBytes(),
        "tokenservice");
    StartContainerRequest containerReq = StartContainerRequest.newInstance(clc,
        containerToken);
    stateStore.storeContainer(containerId, 0, 0, containerReq);
    return containerReq;
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
