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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredDeletionServiceState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
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
  public void testEmptyState() throws IOException {
    assertTrue(stateStore.canRecover());
    verifyEmptyState();
  }

  @Test
  public void testStartResourceLocalization() throws IOException {
    String user = "somebody";
    ApplicationId appId = ApplicationId.newInstance(1, 1);

    // start a local resource for an application
    Path appRsrcPath = new Path("hdfs://some/app/resource");
    LocalResourcePBImpl rsrcPb = (LocalResourcePBImpl)
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(appRsrcPath),
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
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath1),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto1 = rsrcPb.getProto();
    Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
    stateStore.startResourceLocalization(null, null, pubRsrcProto1,
        pubRsrcLocalPath1);
    Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath2),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto2 = rsrcPb.getProto();
    Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
    stateStore.startResourceLocalization(null, null, pubRsrcProto2,
        pubRsrcLocalPath2);
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(privRsrcPath),
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
            ConverterUtils.getYarnUrlFromPath(appRsrcPath),
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
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath1),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto1 = rsrcPb.getProto();
    Path pubRsrcLocalPath1 = new Path("/some/local/dir/for/pubrsrc1");
    stateStore.startResourceLocalization(null, null, pubRsrcProto1,
        pubRsrcLocalPath1);
    Path pubRsrcPath2 = new Path("hdfs://some/public/resource2");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath2),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
            789L, 135L);
    LocalResourceProto pubRsrcProto2 = rsrcPb.getProto();
    Path pubRsrcLocalPath2 = new Path("/some/local/dir/for/pubrsrc2");
    stateStore.startResourceLocalization(null, null, pubRsrcProto2,
        pubRsrcLocalPath2);
    Path privRsrcPath = new Path("hdfs://some/private/resource");
    rsrcPb = (LocalResourcePBImpl) LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(privRsrcPath),
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
            ConverterUtils.getYarnUrlFromPath(appRsrcPath),
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
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath1),
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
            ConverterUtils.getYarnUrlFromPath(pubRsrcPath2),
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
            ConverterUtils.getYarnUrlFromPath(privRsrcPath),
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
}
