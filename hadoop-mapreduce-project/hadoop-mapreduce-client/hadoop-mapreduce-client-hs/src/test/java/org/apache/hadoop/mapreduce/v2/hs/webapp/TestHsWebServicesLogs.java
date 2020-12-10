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

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryContext;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.ContainerLogFileInfo;
import org.apache.hadoop.yarn.logaggregation.TestContainerLogsUtils;
import org.apache.hadoop.yarn.server.webapp.LogServlet;
import org.apache.hadoop.yarn.server.webapp.YarnWebServiceParams;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerLogsInfo;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * We created the following aggregated log structure, and test the log
 * related API endpoints of {@link HsWebServices}.
 *
 * application_1 is finished
 *    attempt_1
 *       container_1 finished on node_1 syslog
 *       container_2 finished on node_1 syslog
 *       container_3 finished on node_2 syslog
 *    attempt_2
 *       container_1 finished on node_1 syslog
 *
 * application_2 is running
 *    attempt_1
 *       container_1 finished on node_1 syslog
 *    attempt_2
 *       container_1 finished on node_1 syslog
 *       container_2 running on node_1 syslog
 *       container_3 running on node_2 syslog (with some already aggregated log)
 *
 */
public class TestHsWebServicesLogs extends JerseyTestBase {

  private static Configuration conf = new YarnConfiguration();
  private static FileSystem fs;

  private static final String LOCAL_ROOT_LOG_DIR = "target/LocalLogs";
  private static final String REMOTE_LOG_ROOT_DIR = "target/logs/";

  private static final String USER = "fakeUser";
  private static final String FILE_NAME = "syslog";

  private static final String NM_WEBADDRESS_1 = "test-nm-web-address-1:9999";
  private static final NodeId NM_ID_1 = NodeId.newInstance("fakeHost1", 9951);
  private static final String NM_WEBADDRESS_2 = "test-nm-web-address-2:9999";
  private static final NodeId NM_ID_2 = NodeId.newInstance("fakeHost2", 9952);

  private static final ApplicationId APPID_1 = ApplicationId.newInstance(1, 1);
  private static final ApplicationId APPID_2 = ApplicationId.newInstance(10, 2);

  private static final ApplicationAttemptId APP_ATTEMPT_1_1 =
      ApplicationAttemptId.newInstance(APPID_1, 1);
  private static final ApplicationAttemptId APP_ATTEMPT_1_2 =
      ApplicationAttemptId.newInstance(APPID_1, 2);
  private static final ApplicationAttemptId APP_ATTEMPT_2_1 =
      ApplicationAttemptId.newInstance(APPID_2, 1);
  private static final ApplicationAttemptId APP_ATTEMPT_2_2 =
      ApplicationAttemptId.newInstance(APPID_2, 2);

  private static final ContainerId CONTAINER_1_1_1 =
      ContainerId.newContainerId(APP_ATTEMPT_1_1, 1);
  private static final ContainerId CONTAINER_1_1_2 =
      ContainerId.newContainerId(APP_ATTEMPT_1_1, 2);
  private static final ContainerId CONTAINER_1_1_3 =
      ContainerId.newContainerId(APP_ATTEMPT_1_1, 3);
  private static final ContainerId CONTAINER_1_2_1 =
      ContainerId.newContainerId(APP_ATTEMPT_1_2, 1);
  private static final ContainerId CONTAINER_2_1_1 =
      ContainerId.newContainerId(APP_ATTEMPT_2_1, 1);
  private static final ContainerId CONTAINER_2_2_1 =
      ContainerId.newContainerId(APP_ATTEMPT_2_2, 1);
  private static final ContainerId CONTAINER_2_2_2 =
      ContainerId.newContainerId(APP_ATTEMPT_2_2, 2);
  private static final ContainerId CONTAINER_2_2_3 =
      ContainerId.newContainerId(APP_ATTEMPT_2_2, 3);

  static {
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_LOG_ROOT_DIR);
  }

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      MockHistoryContext appContext = new MockHistoryContext(0, 1, 2, 1);
      HsWebApp webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      ApplicationClientProtocol mockProtocol =
          mock(ApplicationClientProtocol.class);
      try {
        doAnswer(invocationOnMock -> {
          GetApplicationReportRequest request =
              invocationOnMock.getArgument(0);
          // returning the latest application attempt for each application
          if (request.getApplicationId().equals(APPID_1)) {
            return GetApplicationReportResponse.newInstance(
                newApplicationReport(APPID_1, APP_ATTEMPT_1_2, false));
          } else if (request.getApplicationId().equals(APPID_2)) {
            return GetApplicationReportResponse.newInstance(
                newApplicationReport(APPID_2, APP_ATTEMPT_2_2, true));
          }
          throw new RuntimeException(
              "Unknown applicationId: " + request.getApplicationId());
        }).when(mockProtocol).getApplicationReport(any());

        doAnswer(invocationOnMock -> {
          GetContainerReportRequest request = invocationOnMock.getArgument(0);
          ContainerId cId = request.getContainerId();
          // for running containers assign node id and NM web address
          if (cId.equals(CONTAINER_2_2_2)) {
            return GetContainerReportResponse.newInstance(
                newContainerReport(cId, NM_ID_1, NM_WEBADDRESS_1));
          } else if (cId.equals(CONTAINER_2_2_3)) {
            return GetContainerReportResponse.newInstance(
                newContainerReport(cId, NM_ID_2, NM_WEBADDRESS_2));
          }
          // for finished application don't assign node id and NM web address
          return GetContainerReportResponse.newInstance(
              newContainerReport(cId, null, null));
        }).when(mockProtocol).getContainerReport(any());
      } catch (Exception ignore) {
        fail("Failed to setup WebServletModule class");
      }

      HsWebServices hsWebServices =
          new HsWebServices(appContext, conf, webApp, mockProtocol);
      try {
        LogServlet logServlet = hsWebServices.getLogServlet();
        logServlet = spy(logServlet);
        doReturn(null).when(logServlet).getNMWebAddressFromRM(any());
        doReturn(NM_WEBADDRESS_1).when(logServlet).getNMWebAddressFromRM(
            NM_ID_1.toString());
        doReturn(NM_WEBADDRESS_2).when(logServlet).getNMWebAddressFromRM(
            NM_ID_2.toString());
        hsWebServices.setLogServlet(logServlet);
      } catch (Exception ignore) {
        fail("Failed to setup WebServletModule class");
      }

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class).toInstance(hsWebServices);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);
      bind(ApplicationClientProtocol.class).toInstance(mockProtocol);

      serve("/*").with(GuiceContainer.class);
    }
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    fs = FileSystem.get(conf);
    createAggregatedFolders();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  public void setUp() {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  /**
   * Generating aggregated container logs for all containers
   * except CONTAINER_2_2_2, which is still running.
   *
   * @throws Exception if failed to create aggregated log files
   */
  private static void createAggregatedFolders() throws Exception {
    Map<ContainerId, String> contentsApp1 = new HashMap<>();
    contentsApp1.put(CONTAINER_1_1_1, "Hello-" + CONTAINER_1_1_1);
    contentsApp1.put(CONTAINER_1_1_2, "Hello-" + CONTAINER_1_1_2);
    contentsApp1.put(CONTAINER_1_2_1, "Hello-" + CONTAINER_1_2_1);

    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        LOCAL_ROOT_LOG_DIR, APPID_1, contentsApp1, NM_ID_1, FILE_NAME,
        USER, false);

    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        LOCAL_ROOT_LOG_DIR, APPID_1, Collections.singletonMap(CONTAINER_1_1_3,
            "Hello-" + CONTAINER_1_1_3), NM_ID_2, FILE_NAME, USER, false);

    Map<ContainerId, String> contentsApp2 = new HashMap<>();
    contentsApp2.put(CONTAINER_2_1_1, "Hello-" + CONTAINER_2_1_1);
    contentsApp2.put(CONTAINER_2_2_1, "Hello-" + CONTAINER_2_2_1);

    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        LOCAL_ROOT_LOG_DIR, APPID_2, contentsApp2, NM_ID_1, FILE_NAME,
        USER, false);

    TestContainerLogsUtils.createContainerLogFileInRemoteFS(conf, fs,
        LOCAL_ROOT_LOG_DIR, APPID_2, Collections.singletonMap(CONTAINER_2_2_3,
            "Hello-" + CONTAINER_2_2_3), NM_ID_2, FILE_NAME, USER, false);
  }

  public TestHsWebServicesLogs() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    fs.delete(new Path(REMOTE_LOG_ROOT_DIR), true);
    fs.delete(new Path(LOCAL_ROOT_LOG_DIR), true);
  }

  @Test
  public void testGetAggregatedLogsMetaForFinishedApp() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APP_ID, APPID_1.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseList =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    Set<String> expectedIdStrings = Sets.newHashSet(
        CONTAINER_1_1_1.toString(), CONTAINER_1_1_2.toString(),
        CONTAINER_1_1_3.toString(), CONTAINER_1_2_1.toString());

    assertResponseList(responseList, expectedIdStrings, false);

    for (ContainerLogsInfo logsInfo : responseList) {
      String cId = logsInfo.getContainerId();

      assertThat(logsInfo.getLogType()).isEqualTo(
          ContainerLogAggregationType.AGGREGATED.toString());

      if (cId.equals(CONTAINER_1_1_3.toString())) {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_2));
      } else {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_1));
      }

      assertSimpleContainerLogFileInfo(logsInfo, cId);
    }
  }

  @Test
  public void testGetAggregatedLogsMetaForRunningApp() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APP_ID, APPID_2.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseList =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    Set<String> expectedIdStrings = Sets.newHashSet(
        CONTAINER_2_1_1.toString(), CONTAINER_2_2_1.toString(),
        CONTAINER_2_2_3.toString());
    assertResponseList(responseList, expectedIdStrings, true);

    for (ContainerLogsInfo logsInfo : responseList) {
      String cId = logsInfo.getContainerId();

      if (cId.equals(CONTAINER_2_2_3.toString())) {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_2));
      } else {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_1));
      }

      if (logsInfo.getLogType().equals(
          ContainerLogAggregationType.AGGREGATED.toString())) {
        assertSimpleContainerLogFileInfo(logsInfo, cId);
      }
    }
  }

  @Test
  public void testGetAggregatedLogsMetaForFinishedAppAttempt() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(
            YarnWebServiceParams.APPATTEMPT_ID, APP_ATTEMPT_1_1.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseList =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    Set<String> expectedIdStrings = Sets.newHashSet(
        CONTAINER_1_1_1.toString(), CONTAINER_1_1_2.toString(),
        CONTAINER_1_1_3.toString());
    assertResponseList(responseList, expectedIdStrings, false);

    for (ContainerLogsInfo logsInfo : responseList) {
      String cId = logsInfo.getContainerId();

      assertThat(logsInfo.getLogType()).isEqualTo(
          ContainerLogAggregationType.AGGREGATED.toString());

      if (cId.equals(CONTAINER_1_1_3.toString())) {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_2));
      } else {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_1));
      }

      assertSimpleContainerLogFileInfo(logsInfo, cId);
    }
  }

  @Test
  public void testGetAggregatedLogsMetaForRunningAppAttempt() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(
            YarnWebServiceParams.APPATTEMPT_ID, APP_ATTEMPT_2_2.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseList =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    Set<String> expectedIdStrings = Sets.newHashSet(
        CONTAINER_2_2_1.toString(), CONTAINER_2_2_3.toString());
    assertResponseList(responseList, expectedIdStrings, true);

    for (ContainerLogsInfo logsInfo : responseList) {
      String cId = logsInfo.getContainerId();

      if (cId.equals(CONTAINER_2_2_3.toString())) {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_2));
      } else {
        assertThat(logsInfo.getNodeId()).isEqualTo(formatNodeId(NM_ID_1));
      }

      if (logsInfo.getLogType().equals(
          ContainerLogAggregationType.AGGREGATED.toString())) {
        assertSimpleContainerLogFileInfo(logsInfo, cId);
      }
    }
  }

  @Test
  public void testGetContainerLogsForFinishedContainer() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containers")
        .path(CONTAINER_1_1_2.toString()).path("logs")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseText =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    assertThat(responseText.size()).isOne();

    ContainerLogsInfo logsInfo = responseText.get(0);
    assertThat(logsInfo.getLogType()).isEqualTo(
        ContainerLogAggregationType.AGGREGATED.toString());
    assertThat(logsInfo.getContainerId()).isEqualTo(CONTAINER_1_1_2.toString());

    assertSimpleContainerLogFileInfo(logsInfo, CONTAINER_1_1_2.toString());
  }

  @Test
  public void testGetContainerLogsForRunningContainer() throws Exception {
    WebResource r = resource();
    URI requestURI = r.path("ws").path("v1")
        .path("history").path("containers")
        .path(CONTAINER_2_2_2.toString())
        .path("logs")
        .getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertThat(redirectURL).isNotNull();
    assertThat(redirectURL).contains(NM_WEBADDRESS_1,
        "ws/v1/node/containers", CONTAINER_2_2_2.toString(), "/logs");

    // If we specify NM id, we would re-direct the request
    // to this NM's Web Address.
    requestURI = r.path("ws").path("v1")
        .path("history").path("containers")
        .path(CONTAINER_2_2_2.toString())
        .path("logs")
        .queryParam(YarnWebServiceParams.NM_ID, NM_ID_2.toString())
        .getURI();
    redirectURL = getRedirectURL(requestURI.toString());
    assertThat(redirectURL).isNotNull();
    assertThat(redirectURL).contains(NM_WEBADDRESS_2,
        "ws/v1/node/containers", CONTAINER_2_2_2.toString(), "/logs");

    // If this is the redirect request, we would not re-direct the request
    // back and get the aggregated log meta.
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containers")
        .path(CONTAINER_2_2_3.toString())
        .path("logs")
        .queryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE, "true")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    List<ContainerLogsInfo> responseText =
        response.getEntity(new GenericType<List<ContainerLogsInfo>>(){});
    assertThat(responseText.size()).isEqualTo(2);

    ContainerLogsInfo logsInfo1 = responseText.get(0);
    ContainerLogsInfo logsInfo2 = responseText.get(1);

    assertThat(logsInfo1.getContainerId())
        .isEqualTo(CONTAINER_2_2_3.toString());
    assertThat(logsInfo2.getContainerId())
        .isEqualTo(CONTAINER_2_2_3.toString());

    if (logsInfo1.getLogType().equals(
        ContainerLogAggregationType.AGGREGATED.toString())) {
      assertThat(logsInfo2.getLogType()).isEqualTo(
          ContainerLogAggregationType.LOCAL.toString());

      assertSimpleContainerLogFileInfo(logsInfo1, CONTAINER_2_2_3.toString());

      // this information can be only obtained by the NM.
      assertThat(logsInfo2.getContainerLogsInfo()).isNull();
    } else {
      assertThat(logsInfo1.getLogType()).isEqualTo(
          ContainerLogAggregationType.LOCAL.toString());
      assertThat(logsInfo2.getLogType()).isEqualTo(
          ContainerLogAggregationType.AGGREGATED.toString());

      // this information can be only obtained by the NM.
      assertThat(logsInfo1.getContainerLogsInfo()).isNull();

      assertSimpleContainerLogFileInfo(logsInfo2, CONTAINER_2_2_3.toString());
    }
  }

  @Test
  public void testGetContainerLogFileForFinishedContainer() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containerlogs")
        .path(CONTAINER_1_1_2.toString())
        .path(FILE_NAME)
        .accept(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).doesNotContain("Can not find logs",
        "Hello-" + CONTAINER_1_1_1);
    assertThat(responseText).contains("Hello-" + CONTAINER_1_1_2);
  }

  @Test
  public void testNoRedirectForFinishedContainer() throws Exception {
    WebResource r = resource();
    URI requestURI = r.path("ws").path("v1")
        .path("history").path("containerlogs")
        .path(CONTAINER_2_2_1.toString())
        .path(FILE_NAME).getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertThat(redirectURL).isNull();
  }

  /**
   * For local logs we can only check the redirect to the appropriate node.
   */
  @Test
  public void testGetContainerLogFileForRunningContainer() throws Exception {
    WebResource r = resource();
    URI requestURI = r.path("ws").path("v1")
        .path("history").path("containerlogs")
        .path(CONTAINER_2_2_2.toString())
        .path(FILE_NAME).getURI();
    String redirectURL = getRedirectURL(requestURI.toString());
    assertThat(redirectURL).isNotNull();
    assertThat(redirectURL).contains(NM_WEBADDRESS_1, "ws/v1/node/containers",
        "/logs/" + FILE_NAME, CONTAINER_2_2_2.toString());

    // If we specify NM id, we would re-direct the request
    // to this NM's Web Address.
    requestURI = r.path("ws").path("v1")
        .path("history").path("containerlogs")
        .path(CONTAINER_2_2_2.toString()).path(FILE_NAME)
        .queryParam(YarnWebServiceParams.NM_ID, NM_ID_2.toString())
        .getURI();
    redirectURL = getRedirectURL(requestURI.toString());
    assertThat(redirectURL).isNotNull();
    assertThat(redirectURL).contains(NM_WEBADDRESS_2, "ws/v1/node/containers",
        "/logs/" + FILE_NAME, CONTAINER_2_2_2.toString());

    // If this is the redirect request, we would not re-direct the request
    // back and get the aggregated logs.
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containerlogs")
        .path(CONTAINER_2_2_3.toString()).path(FILE_NAME)
        .queryParam(YarnWebServiceParams.REDIRECTED_FROM_NODE, "true")
        .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).isNotNull();

    assertThat(responseText).contains("LogAggregationType: "
        + ContainerLogAggregationType.AGGREGATED, "Hello-" + CONTAINER_2_2_3);
  }

  @Test
  public void testNonExistingAppId() {
    ApplicationId nonExistingApp = ApplicationId.newInstance(99, 99);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APP_ID, nonExistingApp.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        WebApplicationException.class.getSimpleName());
    assertThat(responseText).contains("Can not find");
  }

  @Test
  public void testBadAppId() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APP_ID, "some text")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        BadRequestException.class.getSimpleName());
    assertThat(responseText).contains("Invalid ApplicationId");
  }

  @Test
  public void testNonExistingAppAttemptId() {
    ApplicationId nonExistingApp = ApplicationId.newInstance(99, 99);
    ApplicationAttemptId nonExistingAppAttemptId =
        ApplicationAttemptId.newInstance(nonExistingApp, 1);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APPATTEMPT_ID,
            nonExistingAppAttemptId.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        WebApplicationException.class.getSimpleName());
    assertThat(responseText).contains("Can not find");
  }

  @Test
  public void testBadAppAttemptId() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.APPATTEMPT_ID, "some text")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        BadRequestException.class.getSimpleName());
    assertThat(responseText).contains("Invalid AppAttemptId");
  }

  @Test
  public void testNonExistingContainerId() {
    ApplicationId nonExistingApp = ApplicationId.newInstance(99, 99);
    ApplicationAttemptId nonExistingAppAttemptId =
        ApplicationAttemptId.newInstance(nonExistingApp, 1);
    ContainerId nonExistingContainerId =
        ContainerId.newContainerId(nonExistingAppAttemptId, 1);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.CONTAINER_ID,
            nonExistingContainerId.toString())
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        WebApplicationException.class.getSimpleName());
    assertThat(responseText).contains("Can not find");
  }

  @Test
  public void testBadContainerId() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("aggregatedlogs")
        .queryParam(YarnWebServiceParams.CONTAINER_ID, "some text")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        BadRequestException.class.getSimpleName());
    assertThat(responseText).contains("Invalid ContainerId");
  }

  @Test
  public void testNonExistingContainerMeta() {
    ApplicationId nonExistingApp = ApplicationId.newInstance(99, 99);
    ApplicationAttemptId nonExistingAppAttemptId =
        ApplicationAttemptId.newInstance(nonExistingApp, 1);
    ContainerId nonExistingContainerId =
        ContainerId.newContainerId(nonExistingAppAttemptId, 1);

    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containers")
        .path(nonExistingContainerId.toString()).path("logs")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        WebApplicationException.class.getSimpleName());
    assertThat(responseText).contains("Can not find");
  }

  @Test
  public void testBadContainerForMeta() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1")
        .path("history").path("containers")
        .path("some text").path("logs")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);

    String responseText = response.getEntity(String.class);
    assertThat(responseText).contains(
        BadRequestException.class.getSimpleName());
    assertThat(responseText).contains("Invalid container id");
  }

  private static void assertSimpleContainerLogFileInfo(
      ContainerLogsInfo logsInfo, String cId) {
    assertThat(logsInfo.getContainerLogsInfo()).isNotNull();
    assertThat(logsInfo.getContainerLogsInfo().size()).isEqualTo(1);
    ContainerLogFileInfo fileInfo = logsInfo.getContainerLogsInfo().get(0);
    assertThat(fileInfo.getFileName()).isEqualTo(FILE_NAME);
    assertThat(fileInfo.getFileSize()).isEqualTo(
        String.valueOf(("Hello-" + cId).length()));
  }

  private static void assertResponseList(List<ContainerLogsInfo> responseList,
      Set<String> expectedIdStrings, boolean running) {
    Set<String> actualStrings =
        responseList.stream()
            .map(ContainerLogsInfo::getContainerId)
            .collect(Collectors.toSet());
    assertThat(actualStrings).isEqualTo(expectedIdStrings);

    int expectedSize = expectedIdStrings.size();
    assertThat(responseList.size()).isEqualTo(
        running ? expectedSize * 2 : expectedSize);
  }

  private static String formatNodeId(NodeId nodeId) {
    return nodeId.toString().replace(":", "_");
  }

  private static ApplicationReport newApplicationReport(ApplicationId appId,
      ApplicationAttemptId appAttemptId, boolean running) {
    return ApplicationReport.newInstance(appId, appAttemptId, USER,
        "fakeQueue", "fakeApplicationName", "localhost", 0, null,
        running ? YarnApplicationState.RUNNING : YarnApplicationState.FINISHED,
        "fake an application report", "", 1000L, 1000L, 1000L, null, null,
        "", 50f, "fakeApplicationType", null);
  }

  private static ContainerReport newContainerReport(ContainerId containerId,
      NodeId nodeId, String nmWebAddress) {
    return ContainerReport.newInstance(containerId, null, nodeId,
        Priority.UNDEFINED, 0, 0, null, null, 0, null, nmWebAddress);
  }

  private static String getRedirectURL(String url) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) new URL(url)
        .openConnection();
    // do not automatically follow the redirection
    // otherwise we get too many redirection exceptions
    conn.setInstanceFollowRedirects(false);
    if (conn.getResponseCode() == HttpServletResponse.SC_TEMPORARY_REDIRECT) {
      return conn.getHeaderField("Location");
    }
    return null;
  }
}
