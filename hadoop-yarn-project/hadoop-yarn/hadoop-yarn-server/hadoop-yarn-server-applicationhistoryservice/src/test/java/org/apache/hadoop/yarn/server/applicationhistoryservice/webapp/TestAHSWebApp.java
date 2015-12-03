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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.apache.hadoop.yarn.webapp.Params.TITLE;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryClientService;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManager;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManagerImpl;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStoreTestUtils;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;

public class TestAHSWebApp extends ApplicationHistoryStoreTestUtils {

  public void setApplicationHistoryStore(ApplicationHistoryStore store) {
    this.store = store;
  }

  @Before
  public void setup() {
    store = new MemoryApplicationHistoryStore();
  }

  @Test
  public void testAppControllerIndex() throws Exception {
    ApplicationHistoryManager ahManager = mock(ApplicationHistoryManager.class);
    Injector injector =
        WebAppTests.createMockInjector(ApplicationHistoryManager.class,
          ahManager);
    AHSController controller = injector.getInstance(AHSController.class);
    controller.index();
    Assert
      .assertEquals("Application History", controller.get(TITLE, "unknown"));
  }

  @Test
  public void testView() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
          mockApplicationHistoryClientService(5, 1, 1));
    AHSView ahsViewInstance = injector.getInstance(AHSView.class);

    ahsViewInstance.render();
    WebAppTests.flushOutput(injector);

    ahsViewInstance.set(YarnWebParams.APP_STATE,
      YarnApplicationState.FAILED.toString());
    ahsViewInstance.render();
    WebAppTests.flushOutput(injector);

    ahsViewInstance.set(YarnWebParams.APP_STATE, StringHelper.cjoin(
      YarnApplicationState.FAILED.toString(), YarnApplicationState.KILLED));
    ahsViewInstance.render();
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAPPViewNaturalSortType() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
            mockApplicationHistoryClientService(5, 1, 1));
    AHSView ahsViewInstance = injector.getInstance(AHSView.class);

    ahsViewInstance.render();
    WebAppTests.flushOutput(injector);
    Map<String, String> moreParams =
        ahsViewInstance.context().requestContext().moreParams();
    String appTableColumnsMeta = moreParams.get("ui.dataTables.apps.init");
    Assert.assertTrue(appTableColumnsMeta.indexOf("natural") != -1);
  }

  @Test
  public void testAboutPage() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
            mockApplicationHistoryClientService(0, 0, 0));
    AboutPage aboutPageInstance = injector.getInstance(AboutPage.class);

    aboutPageInstance.render();
    WebAppTests.flushOutput(injector);

    aboutPageInstance.render();
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAppPage() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
          mockApplicationHistoryClientService(1, 5, 1));
    AppPage appPageInstance = injector.getInstance(AppPage.class);

    appPageInstance.render();
    WebAppTests.flushOutput(injector);

    appPageInstance.set(YarnWebParams.APPLICATION_ID, ApplicationId
      .newInstance(0, 1).toString());
    appPageInstance.render();
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAppPageNaturalSortType() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
            mockApplicationHistoryClientService(1, 5, 1));
    AppPage appPageInstance = injector.getInstance(AppPage.class);

    appPageInstance.render();
    WebAppTests.flushOutput(injector);
    Map<String, String> moreParams =
        appPageInstance.context().requestContext().moreParams();
    String attemptsTableColumnsMeta =
        moreParams.get("ui.dataTables.attempts.init");
    Assert.assertTrue(attemptsTableColumnsMeta.indexOf("natural") != -1);
  }

  @Test
  public void testAppAttemptPage() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
          mockApplicationHistoryClientService(1, 1, 5));
    AppAttemptPage appAttemptPageInstance =
        injector.getInstance(AppAttemptPage.class);

    appAttemptPageInstance.render();
    WebAppTests.flushOutput(injector);

    appAttemptPageInstance.set(YarnWebParams.APPLICATION_ATTEMPT_ID,
      ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1)
        .toString());
    appAttemptPageInstance.render();
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testAppAttemptPageNaturalSortType() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
            mockApplicationHistoryClientService(1, 1, 5));
    AppAttemptPage appAttemptPageInstance =
        injector.getInstance(AppAttemptPage.class);
    appAttemptPageInstance.render();
    WebAppTests.flushOutput(injector);
    Map<String, String> moreParams =
        appAttemptPageInstance.context().requestContext().moreParams();
    String tableColumnsMeta = moreParams.get("ui.dataTables.containers.init");
    Assert.assertTrue(tableColumnsMeta.indexOf("natural") != -1);
  }

  @Test
  public void testContainerPage() throws Exception {
    Injector injector =
        WebAppTests.createMockInjector(ApplicationBaseProtocol.class,
          mockApplicationHistoryClientService(1, 1, 1));
    ContainerPage containerPageInstance =
        injector.getInstance(ContainerPage.class);

    containerPageInstance.render();
    WebAppTests.flushOutput(injector);

    containerPageInstance.set(
      YarnWebParams.CONTAINER_ID,
      ContainerId
        .newContainerId(
          ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1),
          1).toString());
    containerPageInstance.render();
    WebAppTests.flushOutput(injector);
  }

  ApplicationHistoryClientService mockApplicationHistoryClientService(int numApps,
      int numAppAttempts, int numContainers) throws Exception {
    ApplicationHistoryManager ahManager =
        new MockApplicationHistoryManagerImpl(store);
    ApplicationHistoryClientService historyClientService =
        new ApplicationHistoryClientService(ahManager);
    for (int i = 1; i <= numApps; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      writeApplicationStartData(appId);
      for (int j = 1; j <= numAppAttempts; ++j) {
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, j);
        writeApplicationAttemptStartData(appAttemptId);
        for (int k = 1; k <= numContainers; ++k) {
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          writeContainerStartData(containerId);
          writeContainerFinishData(containerId);
        }
        writeApplicationAttemptFinishData(appAttemptId);
      }
      writeApplicationFinishData(appId);
    }
    return historyClientService;
  }

  class MockApplicationHistoryManagerImpl extends ApplicationHistoryManagerImpl {

    public MockApplicationHistoryManagerImpl(ApplicationHistoryStore store) {
      super();
      init(new YarnConfiguration());
      start();
    }

    @Override
    protected ApplicationHistoryStore createApplicationHistoryStore(
        Configuration conf) {
      return store;
    }
  };
}
