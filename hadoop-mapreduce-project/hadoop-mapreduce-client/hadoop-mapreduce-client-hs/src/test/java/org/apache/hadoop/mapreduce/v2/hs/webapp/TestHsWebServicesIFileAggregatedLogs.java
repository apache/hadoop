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
package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.webapp.reader.ContainerLogsInfoMessageBodyReader;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.TestContainerLogsUtils;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Regression tests for MAPREDUCE-7539: singleton {@link HsWebServices} reuses
 * {@link LogAggregationIndexedFileController} across requests, so IFile log
 * reads must not rely on per-instance UUID state from a prior application.
 */
public class TestHsWebServicesIFileAggregatedLogs extends JerseyTestBase {

  private static final Configuration CONF = new YarnConfiguration();
  private static FileSystem fs;

  private static final String LOCAL_ROOT_LOG_DIR = "target/LocalLogsIFile";
  private static final String REMOTE_LOG_ROOT_DIR = "target/logs-ifile/";
  private static final String USER = "fakeUser";
  private static final String FILE_NAME = "syslog";

  private static final ApplicationId APPID_1 = ApplicationId.newInstance(1, 1);
  private static final ApplicationId APPID_2 = ApplicationId.newInstance(10, 2);
  private static final ApplicationAttemptId APP_ATTEMPT_1_1 =
      ApplicationAttemptId.newInstance(APPID_1, 1);
  private static final ApplicationAttemptId APP_ATTEMPT_2_2 =
      ApplicationAttemptId.newInstance(APPID_2, 2);

  private static final ContainerId CONTAINER_1_1_1 =
      ContainerId.newContainerId(
          ApplicationAttemptId.newInstance(APPID_1, 1), 1);
  private static final ContainerId CONTAINER_2_2_1 =
      ContainerId.newContainerId(
          ApplicationAttemptId.newInstance(APPID_2, 2), 1);

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(HsWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private final class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      HsWebApp webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      MockHistoryContext appContext = new MockHistoryContext(0, 1, 2, 1);
      ApplicationClientProtocol mockProtocol = mock(ApplicationClientProtocol.class);
      try {
        doAnswer(invocationOnMock -> {
          GetApplicationReportRequest request = invocationOnMock.getArgument(0);
          if (request.getApplicationId().equals(APPID_1)) {
            return GetApplicationReportResponse.newInstance(
                newApplicationReport(APPID_1, APP_ATTEMPT_1_1, false));
          } else if (request.getApplicationId().equals(APPID_2)) {
            return GetApplicationReportResponse.newInstance(
                newApplicationReport(APPID_2, APP_ATTEMPT_2_2, false));
          }
          throw new RuntimeException("Unknown applicationId: " + request.getApplicationId());
        }).when(mockProtocol).getApplicationReport(any());
      } catch (Exception ignore) {
        fail("Failed to setup mock protocol");
      }

      HsWebServices hsWebServices =
          new HsWebServices(appContext, CONF, webApp, mockProtocol);
      try {
        LogServlet logServlet = spy(hsWebServices.getLogServlet());
        doReturn(null).when(logServlet).getNMWebAddressFromRM(any());
        hsWebServices.setLogServlet(logServlet);
      } catch (Exception ignore) {
        fail("Failed to setup LogServlet");
      }

      bind(webApp).to(WebApp.class).named("hsWebApp");
      bind(appContext).to(AppContext.class);
      bind(appContext).to(HistoryContext.class).named("ctx");
      bind(CONF).to(Configuration.class).named("conf");
      bind(mockProtocol).to(ApplicationClientProtocol.class).named("appClient");
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(response).to(HttpServletResponse.class);
      final HttpServletRequest request = mock(HttpServletRequest.class);
      bind(request).to(HttpServletRequest.class);
      hsWebServices.setResponse(response);
      bind(hsWebServices).to(HsWebServices.class);
    }
  }

  @BeforeAll
  public static void setupClass() throws Exception {
    CONF.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    CONF.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_LOG_ROOT_DIR);
    CONF.setStrings(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "IFile");
    CONF.setClass(String.format(
        YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT, "IFile"),
        LogAggregationIndexedFileController.class,
        LogAggregationFileController.class);
    fs = FileSystem.get(CONF);

    Map<ContainerId, String> app1Logs = new HashMap<>();
    app1Logs.put(CONTAINER_1_1_1, "Hello-" + CONTAINER_1_1_1);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(CONF, fs,
        LOCAL_ROOT_LOG_DIR, APPID_1, app1Logs,
        org.apache.hadoop.yarn.api.records.NodeId.newInstance("fakeHost1", 9951),
        FILE_NAME, USER, true);

    Map<ContainerId, String> app2Logs = new HashMap<>();
    app2Logs.put(CONTAINER_2_2_1, "Hello-" + CONTAINER_2_2_1);
    TestContainerLogsUtils.createContainerLogFileInRemoteFS(CONF, fs,
        LOCAL_ROOT_LOG_DIR, APPID_2, app2Logs,
        org.apache.hadoop.yarn.api.records.NodeId.newInstance("fakeHost1", 9951),
        FILE_NAME, USER, false);
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    fs.delete(new Path(REMOTE_LOG_ROOT_DIR), true);
    fs.delete(new Path(LOCAL_ROOT_LOG_DIR), true);
  }

  @Test
  void testGetAggregatedLogsMetaForMultipleAppsWithReusedController() {
    WebTarget r = target().register(ContainerLogsInfoMessageBodyReader.class);

    assertAggregatedLogsMeta(r, APPID_1, CONTAINER_1_1_1);
    assertAggregatedLogsMeta(r, APPID_2, CONTAINER_2_2_1);
  }

  @Test
  void testGetAggregatedLogsMetaReverseOrderWithReusedController() {
    WebTarget r = target().register(ContainerLogsInfoMessageBodyReader.class);

    assertAggregatedLogsMeta(r, APPID_2, CONTAINER_2_2_1);
    assertAggregatedLogsMeta(r, APPID_1, CONTAINER_1_1_1);
  }

  private static void assertAggregatedLogsMeta(WebTarget r,
      ApplicationId appId, ContainerId expectedContainerId) {
    Response response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APP_ID, appId.toString())
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(200, response.getStatus(),
        "aggregated logs meta HTTP status for " + appId);
    List<ContainerLogsInfo> responseList =
        response.readEntity(new GenericType<List<ContainerLogsInfo>>(){});
    assertEquals(1, responseList.size(),
        "aggregated logs meta entry count for " + appId);
    assertEquals(expectedContainerId.toString(),
        responseList.get(0).getContainerId(),
        "aggregated logs container id for " + appId);
  }

  private static ApplicationReport newApplicationReport(ApplicationId appId,
      ApplicationAttemptId appAttemptId, boolean running) {
    return ApplicationReport.newInstance(appId, appAttemptId, USER,
        "fakeQueue", "fakeApplicationName", "localhost", 0, null,
        running ? YarnApplicationState.RUNNING : YarnApplicationState.FINISHED,
        "fake an application report", "", 1000L, 1000L, 1000L, null, null,
        "", 50f, "fakeApplicationType", null);
  }
}
