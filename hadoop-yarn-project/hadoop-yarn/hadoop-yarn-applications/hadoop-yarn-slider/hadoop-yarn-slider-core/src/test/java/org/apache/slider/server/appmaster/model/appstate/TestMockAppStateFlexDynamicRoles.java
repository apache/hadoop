/*
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

package org.apache.slider.server.appmaster.model.appstate;

import org.apache.hadoop.fs.Path;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockRoles;
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.avro.LoadedRoleHistory;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * Test that if you have more than one role, the right roles are chosen for
 * release.
 */
public class TestMockAppStateFlexDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockAppStateFlexDynamicRoles.class);

  @Override
  public String getTestName() {
    return "TestMockAppStateFlexDynamicRoles";
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node.
   * @return
   */
  @Override
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(4, 4);
  }

  @Override
  public AppStateBindingInfo buildBindingInfo() throws IOException {
    AppStateBindingInfo bindingInfo = super.buildBindingInfo();
    bindingInfo.releaseSelector = new MostRecentContainerReleaseSelector();
    return bindingInfo;
  }

  @Override
  public Application buildApplication() {
    Application application = super.buildApplication();
    Component component = new Component().name("dynamic-6")
        .numberOfContainers(1L);
    application.getComponents().add(component);

    return application;
  }

  @Before
  public void init()
      throws TriggerClusterTeardownException, SliderInternalStateException {
    createAndStartNodes();
  }

  // TODO does not support adding new components dynamically
  public void testDynamicFlexAddRole() throws Throwable {
    Application application = appState.getClusterStatus();
    Component component = new Component().name("dynamicAdd7")
        .numberOfContainers(1L);
    application.getComponents().add(component);
    appState.updateComponents(Collections.singletonMap(component.getName(),
        component.getNumberOfContainers()));
    createAndStartNodes();
    appState.lookupRoleStatus("dynamicAdd7");
  }

  @Test
  public void testDynamicFlexDropRole() throws Throwable {
    appState.updateComponents(Collections.singletonMap("dynamic-6", 0L));
    //status is retained for future
    appState.lookupRoleStatus("dynamic-6");
  }


  @Test
  public void testHistorySaveFlexLoad() throws Throwable {
    Application application = appState.getClusterStatus();
    RoleHistory roleHistory = appState.getRoleHistory();
    Path history = roleHistory.saveHistory(0x0001);
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    Component component = new Component().name("HistorySaveFlexLoad")
        .numberOfContainers(1L);
    application.getComponents().add(component);

    appState.updateComponents(Collections.singletonMap(component.getName(),
        component.getNumberOfContainers()));
    createAndStartNodes();
    LoadedRoleHistory loadedRoleHistory =
        historyWriter.read(fs, history);
    assertEquals(0, appState.getRoleHistory().rebuild(loadedRoleHistory));
  }

  @Test
  public void testHistoryFlexSaveResetLoad() throws Throwable {
    Application application = appState.getClusterStatus();
    Component component = new Component().name("HistoryFlexSaveLoad")
        .numberOfContainers(1L);
    application.getComponents().add(component);

    appState.updateComponents(Collections.singletonMap(component.getName(),
        component.getNumberOfContainers()));
    createAndStartNodes();
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    RoleHistory roleHistory = appState.getRoleHistory();
    Path history = roleHistory.saveHistory(0x0002);
    //now reset the app state
    File historyWorkDir2 = new File("target/history" + getTestName() +
        "-0002");
    Path historyPath2 = new Path(historyWorkDir2.toURI());
    appState = new MockAppState();
    AppStateBindingInfo binding2 = buildBindingInfo();
    binding2.application = factory.newApplication(0, 0, 0)
        .name(getValidTestName());
    binding2.historyPath = historyPath2;
    appState.buildInstance(binding2);
    // on this read there won't be the right number of roles
    LoadedRoleHistory loadedRoleHistory = historyWriter.read(fs, history);
    assertEquals(0, appState.getRoleHistory().rebuild(loadedRoleHistory));
  }

}
