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

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;

@RunWith(Parameterized.class)
public class TestRMWebServicesReservation extends JerseyTest {

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

  private static ObjectReader reader =
      new ObjectMapper().readerFor(ReservationSubmissionRequestInfo.class);

  /*
   * Helper class to allow testing of RM web services which require
   * authorization Add this class as a filter in the Guice injector for the
   * MockRM
   */

  @Singleton
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

  private static abstract class TestServletModule extends ServletModule {
    public Configuration conf = new Configuration();

    public abstract void configureScheduler();

    @Override
    protected void configureServlets() {
      configureScheduler();
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);

      rm = new MockRM(conf);
      bind(ResourceManager.class).toInstance(rm);
      if (setAuthFilter) {
        filter("/*").through(TestRMCustomAuthFilter.class);
      }
    }
  }

  private static class CapTestServletModule extends TestServletModule {
    @Override
    public void configureScheduler() {
      conf.set(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class.getName());
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      CapacitySchedulerConfiguration csconf =
          new CapacitySchedulerConfiguration(conf);
      String[] queues = { "default", "dedicated" };
      QueuePath dedicatedQueuePath = new QueuePath("root.dedicated");
      csconf.setQueues(new QueuePath("root"), queues);
      csconf.setCapacity(new QueuePath("root.default"), 50.0f);
      csconf.setCapacity(dedicatedQueuePath, 50.0f);
      csconf.setReservable(dedicatedQueuePath, true);
      conf = csconf;
    }
  }

  private static class FairTestServletModule extends TestServletModule {
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
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
      conf.set(YarnConfiguration.RM_SCHEDULER, FairScheduler.class.getName());
    }
  }

  private static class NoAuthServletModule extends CapTestServletModule {
    @Override
    protected void configureServlets() {
      setAuthFilter = false;
      super.configureServlets();
    }
  }

  private static class SimpleAuthServletModule extends CapTestServletModule {
    @Override
    protected void configureServlets() {
      setAuthFilter = true;
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      // set the admin acls otherwise all users are considered admins
      // and we can't test authorization
      conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      super.configureServlets();
    }
  }

  private static class FairNoAuthServletModule extends FairTestServletModule {
    @Override
    protected void configureServlets() {
      setAuthFilter = false;
      super.configureServlets();
    }
  }

  private static class FairSimpleAuthServletModule extends
      FairTestServletModule {
    @Override
    protected void configureServlets() {
      setAuthFilter = true;
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      // set the admin acls otherwise all users are considered admins
      // and we can't test authorization
      conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
      super.configureServlets();
    }
  }

  private Injector initNoAuthInjectorCap() {
    return GuiceServletConfig.setInjector(
        Guice.createInjector(new NoAuthServletModule()));
  }

  private Injector initSimpleAuthInjectorCap() {
    return GuiceServletConfig.setInjector(
        Guice.createInjector(new SimpleAuthServletModule()));
  }

  private Injector initNoAuthInjectorFair() {
    return GuiceServletConfig.setInjector(
        Guice.createInjector(new FairNoAuthServletModule()));
  }

  private Injector initSimpleAuthInjectorFair() {
    return GuiceServletConfig.setInjector(
        Guice.createInjector(new FairSimpleAuthServletModule()));
  }

  @Parameters
  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][] {{0, true}, {1, true}, {2, true},
        {3, true}, {0, false}, {1, false}, {2, false}, {3, false}});
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesReservation(int run, boolean recurrence) {
    enableRecurrence = recurrence;
    switch (run) {
    case 0:
    default:
      // No Auth Capacity Scheduler
      initNoAuthInjectorCap();
      break;
    case 1:
      // Simple Auth Capacity Scheduler
      initSimpleAuthInjectorCap();
      break;
    case 2:
      // No Auth Fair Scheduler
      initNoAuthInjectorFair();
      break;
    case 3:
      // Simple Auth Fair Scheduler
      initSimpleAuthInjectorFair();
      break;
    }
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
    WebTarget target = target().path("ws").path("v1").path("cluster");
    return this.constructWebResource(target, paths);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (rm != null) {
      rm.stop();
    }
    super.tearDown();
  }

  @Test
  public void testSubmitReservation() throws Exception {
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

  @Test
  public void testSubmitDuplicateReservation() throws Exception {
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

  @Test
  public void testSubmitDifferentReservationWithSameId() throws Exception {
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

  @Test
  public void testFailedSubmitReservation() throws Exception {
    rm.start();
    // setup a cluster too small to accept the reservation
    setupCluster(1);

    ReservationId rid = getReservationIdTestHelper(1);
    Response response =
        reservationSubmissionTestHelper("reservation/submit", MediaType.APPLICATION_JSON, rid);

    assertTrue(!isHttpSuccessResponse(response));

    rm.stop();
  }

  @Test
  public void testUpdateReservation() throws JSONException, Exception {
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

  @Test
  public void testTimeIntervalRequestListReservation() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @Test
  public void testSameTimeIntervalRequestListReservation() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }
  @Test
  public void testInvalidTimeIntervalRequestListReservation() throws
          Exception {
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

    JSONArray reservations = json.getJSONArray("reservations");

    assertEquals(2, reservations.length());

    testRDLHelper(reservations.getJSONObject(0));
    testRDLHelper(reservations.getJSONObject(1));

    rm.stop();
  }

  @Test
  public void testInvalidEndTimeRequestListReservation() throws Exception {
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
      JSONObject reservations = json.getJSONObject("reservations");

      testRDLHelper(reservations);

      String reservationName = reservations
          .getJSONObject("reservation-definition")
          .getString("reservation-name");
      assertEquals("res_2", reservationName);
    } else {
      // In the case of recurring reservations, both reservations will be
      // picked up by the search interval since it is greater than the period
      // of the reservation.
      JSONArray reservations = json.getJSONArray("reservations");
      assertEquals(2, reservations.length());
    }

    rm.stop();
  }

  @Test
  public void testEmptyEndTimeRequestListReservation() throws Exception {
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
      JSONObject reservations = json.getJSONObject("reservations");

      testRDLHelper(reservations);

      String reservationName = reservations
          .getJSONObject("reservation-definition")
          .getString("reservation-name");
      assertEquals("res_2", reservationName);
    } else {
      // In the case of recurring reservations, both reservations will be
      // picked up by the search interval since it is greater than the period
      // of the reservation.
      JSONArray reservations = json.getJSONArray("reservations");
      assertEquals(2, reservations.length());
    }

    rm.stop();
  }

  @Test
  public void testInvalidStartTimeRequestListReservation() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    // only res_1 should fall into the time interval given in the request json.
    String reservationName = reservations
        .getJSONObject("reservation-definition")
        .getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @Test
  public void testEmptyStartTimeRequestListReservation() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    // only res_1 should fall into the time interval given in the request json.
    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_1");

    rm.stop();
  }

  @Test
  public void testQueueOnlyRequestListReservation() throws Exception {
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

    assertThat(json.getJSONArray("reservations").length()).isEqualTo(2);

    testRDLHelper(json.getJSONArray("reservations").getJSONObject(0));
    testRDLHelper(json.getJSONArray("reservations").getJSONObject(1));

    rm.stop();
  }

  @Test
  public void testEmptyQueueRequestListReservation() throws Exception {
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

  @Test
  public void testNonExistentQueueRequestListReservation() throws Exception {
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

  @Test
  public void testReservationIdRequestListReservation() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    rm.stop();
  }

  @Test
  public void testInvalidReservationIdRequestListReservation() throws
          Exception {
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

  @Test
  public void testIncludeResourceAllocations() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    assertTrue(reservations.has("resource-allocations"));

    rm.stop();
  }

  @Test
  public void testExcludeResourceAllocations() throws Exception {
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

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationId = reservations.getString("reservation-id");
    assertEquals(id1.toString(), reservationId);

    assertTrue(!reservations.has("resource-allocations"));

    rm.stop();
  }

  @Test
  public void testDeleteReservation() throws JSONException, Exception {
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
    Response response = constructWebResource(GET_NEW_RESERVATION_PATH)
        .request(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .get(Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return ReservationId.newInstance(clock.getTime(), fallbackReservationId);
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);

    assertEquals("incorrect number of elements", 1, json.length());
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
    ReservationSubmissionRequestInfo rsci = reader.readValue(reservationJson);

    Thread.sleep(1000);
    Response response = constructWebResource(path)
        .request(MediaType.APPLICATION_JSON)
        .accept(media)
        .post(Entity.json(rsci), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
    }

    return response;
  }

  private void updateReservationTestHelper(String path,
      ReservationId reservationId, String media) throws Exception {

    String reservationJson = loadJsonFile("update-reservation.json");

    ReservationUpdateRequestInfo rsci = reader.readValue(reservationJson);
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
        .post(Entity.json(rsci), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
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

    ReservationDeleteRequestInfo rsci = reader.readValue(reservationJson);
    if (this.isAuthenticationEnabled()) {
      // only works when previous submit worked
      if(rsci.getReservationId() == null) {
        throw new IOException("Incorrectly parsed the reservatinId");
      }
      rsci.setReservationId(reservationId.toString());
    }

    Thread.sleep(1000);
    Response response = constructWebResource(path)
        .request(MediaType.APPLICATION_JSON)
        .accept(media)
        .post(Entity.json(rsci), Response.class);

    if (!this.isAuthenticationEnabled()) {
      assertResponseStatusCode(Response.Status.UNAUTHORIZED, response.getStatusInfo());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
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

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
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
      json.getJSONObject("reservations");
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
