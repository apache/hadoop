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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.toJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;

import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.AfterEach;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonUnmarshaller;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestRMWebServicesReservation extends JerseyTestBase {

  private String webserviceUserName = "testuser";
  private static boolean setAuthFilter = false;
  private static boolean enableRecurrence = false;

  private static MockRM rm;

  private static final int MINIMUM_RESOURCE_DURATION = 100000;
  private static final Clock clock = new UTCClock();
  private static final int MAXIMUM_PERIOD = 86400000;
  private static final int DEFAULT_RECURRENCE = MAXIMUM_PERIOD / 10;
  private static final String TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private static final String FS_ALLOC_FILE = new File(TEST_DIR,
      "test-fs-queues.xml").getAbsolutePath();
  // This is what is used in the test resource files.
  private static final String DEFAULT_QUEUE = "dedicated";
  private static final String LIST_RESERVATION_PATH = "reservation/list";
  private static final String GET_NEW_RESERVATION_PATH =
      "reservation/new-reservation";

  private static JettisonUnmarshaller reservationSubmissionRequestInfoReader;
  static {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(
          ReservationSubmissionRequestInfo.class);
      reservationSubmissionRequestInfoReader = jettisonJaxbContext.createJsonUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }

  private ResourceConfig config;
  private HttpServletRequest hsRequest = mock(HttpServletRequest.class);
  private HttpServletResponse hsResponse = mock(HttpServletResponse.class);

  /*
   * Helper class to allow testing of RM web services which require
   * authorization Add this class as a filter in the Guice injector for the
   * MockRM
   */

  public static class TestRMCustomAuthFilter extends AuthenticationFilter {

    @Override
    protected Properties getConfiguration(String configPrefix,
        FilterConfig filterConfig) throws ServletException {
      Properties props = new Properties();
      Enumeration<?> names = filterConfig.getInitParameterNames();
      while (names.hasMoreElements()) {
        String name = (String) names.nextElement();
        if (name.startsWith(configPrefix)) {
          String value = filterConfig.getInitParameter(name);
          props.put(name.substring(configPrefix.length()), value);
        }
      }
      props.put(AuthenticationFilter.AUTH_TYPE, "simple");
      props.put(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
      return props;
    }

  }

  @Override
  protected Application configure() {
    config = new ResourceConfig();
    config.register(RMWebServices.class);
    config.register(GenericExceptionHandler.class);
    if (setAuthFilter) {
      config.register(TestRMCustomAuthFilter.class);
    }
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    private Configuration conf = new YarnConfiguration();

    @Override
    protected void configure() {
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
      configureScheduler();
      rm = new MockRM(conf);
      bind(rm).to(ResourceManager.class).named("rm");
      bind(conf).to(Configuration.class).named("conf");
      bind(hsRequest).to(HttpServletRequest.class);
      bind(hsResponse).to(HttpServletResponse.class);
    }

    public void configureScheduler() {
    }

    public Configuration getConf() {
      return conf;
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  private class CapTestServletModule extends JerseyBinder {

    CapTestServletModule(boolean flag) {
      if(flag) {
        getConf().setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        getConf().setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      }
    }

    @Override
    public void configureScheduler() {
      getConf().set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
      getConf().setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      CapacitySchedulerConfiguration csconf =
          new CapacitySchedulerConfiguration(getConf());
      String[] queues = { "default", "dedicated" };
      QueuePath dedicatedQueuePath = new QueuePath("root.dedicated");
      csconf.setQueues(new QueuePath("root"), queues);
      csconf.setCapacity(new QueuePath("root.default"), 50.0f);
      csconf.setCapacity(dedicatedQueuePath, 50.0f);
      csconf.setReservable(dedicatedQueuePath, true);
      setConf(csconf);
    }
  }

  private class FairTestServletModule extends JerseyBinder {

    FairTestServletModule(boolean flag) {
      if(flag) {
        getConf().setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        // set the admin acls otherwise all users are considered admins
        // and we can't test authorization
        getConf().setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      }
    }

    @Override
    public void configureScheduler() {
      AllocationFileWriter.create()
          .drfDefaultQueueSchedulingPolicy()
          .addQueue(new AllocationFileQueue.Builder("root")
              .aclAdministerApps("someuser ")
              .subQueue(new AllocationFileQueue.Builder("default")
                  .aclAdministerApps("someuser ").build())
              .subQueue(new AllocationFileQueue.Builder("dedicated")
                  .reservation()
                  .aclAdministerApps("someuser ").build())
              .build())
          .writeToFile(FS_ALLOC_FILE);
      getConf().set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
      getConf().set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    }
  }

  private CapTestServletModule getNoAuthInjectorCap() {
    setAuthFilter = false;
    return new CapTestServletModule(false);
  }

  private CapTestServletModule getSimpleAuthInjectorCap() {
    setAuthFilter = true;
    return new CapTestServletModule(true);
  }

  private FairTestServletModule getNoAuthInjectorFair() {
    setAuthFilter = false;
    return new FairTestServletModule(false);
  }

  private FairTestServletModule getSimpleAuthInjectorFair() {
    setAuthFilter = true;
    return new FairTestServletModule(true);
  }

  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][] {{0, true}, {1, true}, {2, true},
        {3, true}, {0, false}, {1, false}, {2, false}, {3, false}});
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void initTestRMWebServicesReservation(int run, boolean recurrence) throws Exception {
    enableRecurrence = recurrence;
    switch (run) {
    case 0:
    default:
      // No Auth Capacity Scheduler
      config.register(getNoAuthInjectorCap());
      break;
    case 1:
      // Simple Auth Capacity Scheduler
      config.register(getSimpleAuthInjectorCap());
      break;
    case 2:
      // No Auth Fair Scheduler
      config.register(getNoAuthInjectorFair());
      break;
    case 3:
      // Simple Auth Fair Scheduler
      config.register(getSimpleAuthInjectorFair());
      break;
    }
    setUp();
  }

  private boolean isAuthenticationEnabled() {
    return setAuthFilter;
  }

  private WebTarget constructWebResource(WebTarget target, String... paths) {
    for (String path : paths) {
      target = target.path(path);
    }
    if (isAuthenticationEnabled()) {
      target = target.queryParam("user.name", webserviceUserName);
    }
    return target;
  }

  private WebTarget constructWebResource(String... paths) {
    WebTarget target = targetWithJsonObject().path("ws").path("v1").path("cluster");
    return this.constructWebResource(target, paths);
  }

  @AfterEach
  @Override
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSubmitReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId rid = getReservationIdTestHelper(1);
    Response response = reservationSubmissionTestHelper(
        "reservation/submit", MediaType.APPLICATION_JSON, rid);
    if (this.isAuthenticationEnabled()) {
      assertTrue(isHttpSuccessResponse(response));
      verifyReservationCount(1);
    }
    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSubmitDuplicateReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId rid = getReservationIdTestHelper(1);
    long currentTimestamp = clock.getTime() + MINIMUM_RESOURCE_DURATION;
    Response response = reservationSubmissionTestHelper(
        "reservation/submit", MediaType.APPLICATION_JSON, currentTimestamp, "",
        rid);

    // Make sure that the first submission is successful
    if (this.isAuthenticationEnabled()) {
      assertTrue(isHttpSuccessResponse(response));
    }

    response = reservationSubmissionTestHelper(
      "reservation/submit", MediaType.APPLICATION_JSON, currentTimestamp, "",
      rid);

    // Make sure that the second submission is successful
    if (this.isAuthenticationEnabled()) {
      assertTrue(isHttpSuccessResponse(response));
      verifyReservationCount(1);
    }

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSubmitDifferentReservationWithSameId(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId rid = getReservationIdTestHelper(1);
    long currentTimestamp = clock.getTime() + MINIMUM_RESOURCE_DURATION;
    Response response =
        reservationSubmissionTestHelper("reservation/submit", MediaType.APPLICATION_JSON,
        currentTimestamp, "res1", rid);

    // Make sure that the first submission is successful
    if (this.isAuthenticationEnabled()) {
      assertTrue(isHttpSuccessResponse(response));
    }

    // Change the reservation definition.
    response = reservationSubmissionTestHelper(
        "reservation/submit", MediaType.APPLICATION_JSON,
        currentTimestamp + MINIMUM_RESOURCE_DURATION, "res1", rid);

    // Make sure that the second submission is unsuccessful
    if (this.isAuthenticationEnabled()) {
      assertTrue(!isHttpSuccessResponse(response));
      verifyReservationCount(1);
    }

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testFailedSubmitReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    // setup a cluster too small to accept the reservation
    setupCluster(1);

    ReservationId rid = getReservationIdTestHelper(1);
    Response response =
        reservationSubmissionTestHelper("reservation/submit", MediaType.APPLICATION_JSON, rid);

    assertTrue(!isHttpSuccessResponse(response));

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testUpdateReservation(int run, boolean recurrence) throws JSONException, Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId rid = getReservationIdTestHelper(1);
    Response response =
        reservationSubmissionTestHelper("reservation/submit", MediaType.APPLICATION_JSON, rid);
    if (this.isAuthenticationEnabled()) {
      assertTrue(isHttpSuccessResponse(response));
    }
    updateReservationTestHelper("reservation/update", rid,
        MediaType.APPLICATION_JSON);

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testTimeIntervalRequestListReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
        .queryParam("start-time", Long.toString((long) (time * 0.9)))
        .queryParam("end-time", Long.toString(time + (long) (0.9 * MINIMUM_RESOURCE_DURATION)))
        .queryParam("include-resource-allocations", "true")
        .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.
        getJSONObject("reservationListInfo").
        getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testSameTimeIntervalRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    // If authentication is not enabled then id1 and id2 will be null
    if (!this.isAuthenticationEnabled() && id1 == null && id2 == null) {
      return;
    }

    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
        "res_2", id2);

    String timeParam = Long.toString(time + MINIMUM_RESOURCE_DURATION / 2);
    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
        .queryParam("start-time", timeParam)
        .queryParam("end-time", timeParam)
        .queryParam("include-resource-allocations", "true")
        .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations =
        json.getJSONObject("reservationListInfo").getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testInvalidTimeIntervalRequestListReservation(int run, boolean recurrence) throws
      Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
        .queryParam("start-time", "-100")
        .queryParam("end-time", "-100")
        .queryParam("include-resource-allocations", "true")
        .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONArray reservations = json.
        getJSONObject("reservationListInfo").
        getJSONArray("reservations");

    assertEquals(2, reservations.length());

    testRDLHelper(reservations.getJSONObject(0));
    testRDLHelper(reservations.getJSONObject(1));

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testInvalidEndTimeRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", Long.toString((long) (time +
                    MINIMUM_RESOURCE_DURATION * 1.3)))
            .queryParam("end-time", "-1")
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    if (!enableRecurrence) {
      JSONObject reservations = json.
          getJSONObject("reservationListInfo").
          getJSONObject("reservations");

      testRDLHelper(reservations);

      String reservationName = reservations
          .getJSONObject("reservation-definition")
          .getString("reservation-name");
      assertEquals("res_2", reservationName);
    } else {
      // In the case of recurring reservations, both reservations will be
      // picked up by the search interval since it is greater than the period
      // of the reservation.
      JSONArray reservations =
          json.getJSONObject("reservationListInfo").getJSONArray("reservations");
      assertEquals(2, reservations.length());
    }

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testEmptyEndTimeRequestListReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", new Long((long) (time +
                    MINIMUM_RESOURCE_DURATION * 1.3)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    if (!enableRecurrence) {
      JSONObject reservations = json.
          getJSONObject("reservationListInfo").getJSONObject("reservations");

      testRDLHelper(reservations);

      String reservationName = reservations
          .getJSONObject("reservation-definition")
          .getString("reservation-name");
      assertEquals("res_2", reservationName);
    } else {
      // In the case of recurring reservations, both reservations will be
      // picked up by the search interval since it is greater than the period
      // of the reservation.
      JSONArray reservations =
          json.getJSONObject("reservationListInfo").getJSONArray("reservations");
      assertEquals(2, reservations.length());
    }

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testInvalidStartTimeRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", "-1")
            .queryParam("end-time", new Long((long)(time +
                    MINIMUM_RESOURCE_DURATION * 0.9)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.
        getJSONObject("reservationListInfo").
        getJSONObject("reservations");

    testRDLHelper(reservations);

    // only res_1 should fall into the time interval given in the request json.
    String reservationName = reservations
        .getJSONObject("reservation-definition")
        .getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testEmptyStartTimeRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("end-time", new Long((long)(time +
                    MINIMUM_RESOURCE_DURATION * 0.9)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.
        getJSONObject("reservationListInfo").getJSONObject("reservations");

    testRDLHelper(reservations);

    // only res_1 should fall into the time interval given in the request json.
    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testQueueOnlyRequestListReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    assertThat(json.getJSONObject("reservationListInfo")
        .getJSONArray("reservations").length()).isEqualTo(2);
    testRDLHelper(json.getJSONObject("reservationListInfo")
        .getJSONArray("reservations").getJSONObject(0));
    testRDLHelper(json.getJSONObject("reservationListInfo")
         .getJSONArray("reservations").getJSONObject(1));

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testEmptyQueueRequestListReservation(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH);

    testListReservationHelper(target, Response.Status.BAD_REQUEST);

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testNonExistentQueueRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE + "_invalid");

    testListReservationHelper(target, Response.Status.BAD_REQUEST);

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testReservationIdRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    ReservationId id2 = getReservationIdTestHelper(2);

    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);
    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_2", id2);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      target = target.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.
        getJSONObject("reservationListInfo").
        getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testInvalidReservationIdRequestListReservation(int run, boolean recurrence)
      throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);

    reservationSubmissionTestHelper("reservation/submit",
        MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      target = target.queryParam("reservation-id",
              "invalid" + id1.toString());
    }

    JSONObject response = testListReservationHelper(target, Response.Status.NOT_FOUND);

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testIncludeResourceAllocations(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);
    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      target = target.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations =
        json.getJSONObject("reservationListInfo").getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    assertTrue(reservations.has("resource-allocations"));

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testExcludeResourceAllocations(int run, boolean recurrence) throws Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    setupCluster(100);

    ReservationId id1 = getReservationIdTestHelper(1);

    reservationSubmissionTestHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", id1);

    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "false")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      target = target.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(target);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.
        getJSONObject("reservationListInfo").
        getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    assertTrue(!reservations.has("resource-allocations"));

    rm.stop();
  }

  @MethodSource("guiceConfigs")
  @ParameterizedTest
  public void testDeleteReservation(int run, boolean recurrence) throws JSONException, Exception {
    initTestRMWebServicesReservation(run, recurrence);
    rm.start();
    for (int i = 0; i < 100; i++) {
      MockNM amNodeManager =
          rm.registerNode("127.0.0." + i + ":1234", 100 * 1024);
      amNodeManager.nodeHeartbeat(true);
    }

    ReservationId rid = getReservationIdTestHelper(1);

    reservationSubmissionTestHelper("reservation/submit", MediaType
        .APPLICATION_JSON, rid);
    testDeleteReservationHelper("reservation/delete", rid,
        MediaType.APPLICATION_JSON);

    rm.stop();
  }

  /**
   * This method is used when a ReservationId is required. Attempt to use REST
   * API. If authentication is not enabled, ensure that the response status is
   * unauthorized and generate a ReservationId because downstream components
   * require a ReservationId for testing.
   * @param fallbackReservationId the ReservationId to use if authentication
   *                              is not enabled, causing the getNewReservation
   *                              API to fail.
   * @return the object representing the reservation ID.
   */
  private ReservationId getReservationIdTestHelper(int fallbackReservationId)
      throws Exception {
    Thread.sleep(1000);

    if (this.isAuthenticationEnabled()) {
      Principal principal1 = () -> webserviceUserName;
      when(hsRequest.getUserPrincipal()).thenReturn(principal1);
    }

    Response response = constructWebResource(GET_NEW_RESERVATION_PATH)
        .request(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .post(null, Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return ReservationId.newInstance(clock.getTime(), fallbackReservationId);
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json =
        response.
        readEntity(JSONObject.class).
        getJSONObject("new-reservation");

    assertEquals(1, json.length(), "incorrect number of elements");
    ReservationId rid = null;
    try {
      rid = ReservationId.parseReservationId(json.getString("reservation-id"));
    } catch (JSONException j) {
      // failure is possible and is checked outside
    }
    return rid;
  }

  private Response reservationSubmissionTestHelper(String path,
      String media, ReservationId reservationId) throws Exception {
    long arrival = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    return reservationSubmissionTestHelper(path, media, arrival, "res_1",
      reservationId);
  }

  private Response reservationSubmissionTestHelper(String path,
      String media, Long arrival, String reservationName,
      ReservationId reservationId) throws Exception {
    String reservationJson = loadJsonFile("submit-reservation.json");

    String recurrenceExpression = "";
    if (enableRecurrence) {
      recurrenceExpression = String.format(
          "\"recurrence-expression\" : \"%s\",", DEFAULT_RECURRENCE);
    }

    String reservationJsonRequest = String.format(reservationJson,
        reservationId.toString(), arrival, arrival + MINIMUM_RESOURCE_DURATION,
        reservationName, recurrenceExpression);

    return submitAndVerifyReservation(path, media, reservationJsonRequest);
  }

  private Response submitAndVerifyReservation(String path, String media,
      String reservationJson) throws Exception {
    ReservationSubmissionRequestInfo rsci = reservationSubmissionRequestInfoReader.
        unmarshalFromJSON(new StringReader(reservationJson),
        ReservationSubmissionRequestInfo.class);
    Thread.sleep(1000);
    Response response = constructWebResource(path)
        .request(MediaType.APPLICATION_JSON)
        .accept(media)
        .post(Entity.entity(toJson(rsci, ReservationSubmissionRequestInfo.class),
        MediaType.APPLICATION_JSON_TYPE), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
    }

    return response;
  }

  private void updateReservationTestHelper(String path,
      ReservationId reservationId, String media) throws Exception {

    String reservationJson = loadJsonFile("update-reservation.json");

    JettisonUnmarshaller reservationUpdateRequestInfoReader;
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(
          ReservationUpdateRequestInfo.class);
      reservationUpdateRequestInfoReader = jettisonJaxbContext.createJsonUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }

    ReservationUpdateRequestInfo rsci = reservationUpdateRequestInfoReader.
        unmarshalFromJSON(new StringReader(reservationJson),
        ReservationUpdateRequestInfo.class);

    if (this.isAuthenticationEnabled()) {
      // only works when previous submit worked
      if(rsci.getReservationId() == null) {
        throw new IOException("Incorrectly parsed the reservationId");
      }
      rsci.setReservationId(reservationId.toString());
    }

    Thread.sleep(1000);
    Response response = constructWebResource(path)
        .request(MediaType.APPLICATION_JSON)
        .accept(media)
        .post(Entity.entity(toJson(rsci, ReservationUpdateRequestInfo.class),
        MediaType.APPLICATION_JSON_TYPE), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
            response.getMediaType().toString());
    assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());

  }

  private String loadJsonFile(String filename) throws IOException {
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    URL submitURI = cL.getResource(filename);

    String reservationJson =
        FileUtils.readFileToString(new File(submitURI.getFile()));
    return reservationJson;
  }

  private void testDeleteReservationHelper(String path,
      ReservationId reservationId, String media) throws JSONException,
      Exception {

    String reservationJson = loadJsonFile("delete-reservation.json");

    JettisonUnmarshaller reader;
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(
          ReservationDeleteRequestInfo.class);
      reader = jettisonJaxbContext.createJsonUnmarshaller();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }

    ReservationDeleteRequestInfo rsci = reader.
        unmarshalFromJSON(new StringReader(reservationJson),
        ReservationDeleteRequestInfo.class);

    if (this.isAuthenticationEnabled()) {
      // only works when previous submit worked
      if(rsci.getReservationId() == null) {
        throw new IOException("Incorrectly parsed the reservationId");
      }
      rsci.setReservationId(reservationId.toString());
    }

    Thread.sleep(1000);
    Response response = constructWebResource(path)
        .request(MediaType.APPLICATION_JSON)
        .accept(media)
        .post(Entity.entity(toJson(rsci, ReservationDeleteRequestInfo.class),
        MediaType.APPLICATION_JSON_TYPE), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertResponseStatusCode(Response.Status.OK, response.getStatusInfo());
  }

  private void testRDLHelper(JSONObject json) throws JSONException {
    JSONObject requests = json.getJSONObject("reservation-definition")
            .getJSONObject("reservation-requests");
    String type = requests.getString
            ("reservation-request-interpreter");

    assertEquals("0", type);
    assertEquals(60, requests.getJSONArray("reservation-request")
            .getJSONObject(0).getInt("duration"));
  }

  private JSONObject testListReservationHelper(WebTarget target) throws Exception {
    return testListReservationHelper(target, Response.Status.OK);
  }

  private JSONObject testListReservationHelper(WebTarget target, Response.Status
          status) throws Exception {
    Thread.sleep(1000);
    Response response = target.request().get(Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return null;
    }

    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertResponseStatusCode(status, response.getStatusInfo());

    return response.readEntity(JSONObject.class);
  }

  private void verifyReservationCount(int count) throws Exception {
    WebTarget target = constructWebResource(LIST_RESERVATION_PATH)
        .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(target);

    if (count == 1) {
      // If there are any number other than one reservation, this will throw.
      json.getJSONObject("reservationListInfo").getJSONObject("reservations");
    } else {
      JSONArray reservations = json.getJSONArray("reservations");
      assertTrue(reservations.length() == count);
    }
  }

  private boolean isHttpSuccessResponse(Response response) {
    return (response.getStatusInfo().getStatusCode() / 100) == 2;
  }

  private void setupCluster(int nodes) throws Exception {
    for (int i = 0; i < nodes; i++) {
      MockNM amNodeManager =
              rm.registerNode("127.0.0." + i + ":1234", 100 * 1024);
      amNodeManager.nodeHeartbeat(true);
    }
  }
}
