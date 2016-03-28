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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

@RunWith(Parameterized.class)
public class TestRMWebServicesReservation extends JerseyTestBase {

  private String webserviceUserName = "testuser";
  private boolean setAuthFilter = false;

  private static MockRM rm;
  private static Injector injector;

  private static final int MINIMUM_RESOURCE_DURATION = 1000000;
  private static final Clock clock = new UTCClock();
  private static final String TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private static final String FS_ALLOC_FILE = new File(TEST_DIR,
      "test-fs-queues.xml").getAbsolutePath();
  // This is what is used in the test resource files.
  private static final String DEFAULT_QUEUE = "dedicated";
  private static final String LIST_RESERVATION_PATH = "reservation/list";

  public static class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

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

  private abstract class TestServletModule extends ServletModule {
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
      Configuration conf = new Configuration();
      conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
      conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
          YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      CapacitySchedulerConfiguration csconf =
          new CapacitySchedulerConfiguration(conf);
      String[] queues = { "default", "dedicated" };
      csconf.setQueues("root", queues);
      csconf.setCapacity("root.default", 50.0f);
      csconf.setCapacity("root.dedicated", 50.0f);
      csconf.setReservable("root.dedicated", true);

      rm = new MockRM(csconf);
      bind(ResourceManager.class).toInstance(rm);
      if (setAuthFilter) {
        filter("/*").through(TestRMCustomAuthFilter.class);
      }
      serve("/*").with(GuiceContainer.class);
    }
  }

  private class CapTestServletModule extends TestServletModule {
    @Override
    public void configureScheduler() {
      conf.set("yarn.resourcemanager.scheduler.class",
          CapacityScheduler.class.getName());
    }
  }

  private class FairTestServletModule extends TestServletModule {
    @Override
    public void configureScheduler() {
      try {
        PrintWriter out = new PrintWriter(new FileWriter(FS_ALLOC_FILE));
        out.println("<?xml version=\"1.0\"?>");
        out.println("<allocations>");
        out.println("<queue name=\"root\">");
        out.println("  <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  <queue name=\"default\">");
        out.println("    <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  </queue>");
        out.println("  <queue name=\"dedicated\">");
        out.println("    <aclAdministerApps>someuser </aclAdministerApps>");
        out.println("  </queue>");
        out.println("</queue>");
        out.println("</allocations>");
        out.close();
      } catch (IOException e) {
      }
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, FS_ALLOC_FILE);
      conf.set("yarn.resourcemanager.scheduler.class",
          FairScheduler.class.getName());
    }
  }

  private Injector getNoAuthInjectorCap() {
    return Guice.createInjector(new CapTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = false;
        super.configureServlets();
      }
    });
  }

  private Injector getSimpleAuthInjectorCap() {
    return Guice.createInjector(new CapTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = true;
        conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        // set the admin acls otherwise all users are considered admins
        // and we can't test authorization
        conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
        super.configureServlets();
      }
    });
  }

  private Injector getNoAuthInjectorFair() {
    return Guice.createInjector(new FairTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = false;
        super.configureServlets();
      }
    });
  }

  private Injector getSimpleAuthInjectorFair() {
    return Guice.createInjector(new FairTestServletModule() {
      @Override
      protected void configureServlets() {
        setAuthFilter = true;
        conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
        // set the admin acls otherwise all users are considered admins
        // and we can't test authorization
        conf.setStrings(YarnConfiguration.YARN_ADMIN_ACL, "testuser1");
        super.configureServlets();
      }
    });
  }

  @Parameters
  public static Collection<Object[]> guiceConfigs() {
    return Arrays.asList(new Object[][] { { 0 }, { 1 }, { 2 }, { 3 } });
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesReservation(int run) {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .clientConfig(new DefaultClientConfig(JAXBContextResolver.class))
        .contextPath("jersey-guice-filter").servletPath("/").build());
    switch (run) {
    case 0:
    default:
      // No Auth Capacity Scheduler
      injector = getNoAuthInjectorCap();
      break;
    case 1:
      // Simple Auth Capacity Scheduler
      injector = getSimpleAuthInjectorCap();
      break;
    case 2:
      // No Auth Fair Scheduler
      injector = getNoAuthInjectorFair();
      break;
    case 3:
      // Simple Auth Fair Scheduler
      injector = getSimpleAuthInjectorFair();
      break;
    }
  }

  private boolean isAuthenticationEnabled() {
    return setAuthFilter;
  }

  private WebResource constructWebResource(WebResource r, String... paths) {
    WebResource rt = r;
    for (String path : paths) {
      rt = rt.path(path);
    }
    if (isAuthenticationEnabled()) {
      rt = rt.queryParam("user.name", webserviceUserName);
    }
    return rt;
  }

  private WebResource constructWebResource(String... paths) {
    WebResource r = resource();
    WebResource ws = r.path("ws").path("v1").path("cluster");
    return this.constructWebResource(ws, paths);
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
    ReservationId rid =
        testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON);
    if (this.isAuthenticationEnabled()) {
      assertNotNull(rid);
    }
    rm.stop();
  }

  @Test
  public void testFailedSubmitReservation() throws Exception {
    rm.start();
    // setup a cluster too small to accept the reservation
    setupCluster(1);
    ReservationId rid =
        testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON);
    assertNull(rid);
    rm.stop();
  }

  @Test
  public void testUpdateReservation() throws JSONException, Exception {
    rm.start();
    setupCluster(100);
    ReservationId rid =
        testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON);
    if (this.isAuthenticationEnabled()) {
      assertNotNull(rid);
    }
    testUpdateReservationHelper("reservation/update", rid,
        MediaType.APPLICATION_JSON);

    rm.stop();
  }

  @Test
  public void testTimeIntervalRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", Long.toString((long) (time * 0.9)))
            .queryParam("end-time", Long.toString(time + (long) (0.9 *
                    MINIMUM_RESOURCE_DURATION)))
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

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

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    String timeParam = Long.toString(time + MINIMUM_RESOURCE_DURATION / 2);
    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", timeParam)
            .queryParam("end-time", timeParam)
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

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

    ReservationId res1 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    ReservationId res2 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource;
    resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", "-100")
            .queryParam("end-time", "-100")
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

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

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", Long.toString((long) (time +
                    MINIMUM_RESOURCE_DURATION * 1.3)))
            .queryParam("end-time", "-1")
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_2");

    rm.stop();
  }

  @Test
  public void testEmptyEndTimeRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", new Long((long) (time +
                    MINIMUM_RESOURCE_DURATION * 1.3)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    JSONObject reservations = json.getJSONObject("reservations");

    testRDLHelper(reservations);

    String reservationName = reservations.getJSONObject
            ("reservation-definition").getString("reservation-name");
    assertEquals(reservationName, "res_2");

    rm.stop();
  }

  @Test
  public void testInvalidStartTimeRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("start-time", "-1")
            .queryParam("end-time", new Long((long)(time +
                    MINIMUM_RESOURCE_DURATION * 0.9)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

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
  public void testEmptyStartTimeRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    long time = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time, "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, time + MINIMUM_RESOURCE_DURATION,
            "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("end-time", new Long((long)(time +
                    MINIMUM_RESOURCE_DURATION * 0.9)).toString())
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

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

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE);

    JSONObject json = testListReservationHelper(resource);

    if (!this.isAuthenticationEnabled() && json == null) {
      return;
    }

    assertEquals(json.getJSONArray("reservations").length(), 2);

    testRDLHelper(json.getJSONArray("reservations").getJSONObject(0));
    testRDLHelper(json.getJSONArray("reservations").getJSONObject(1));

    rm.stop();
  }

  @Test
  public void testEmptyQueueRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH);

    testListReservationHelper(resource, Status.BAD_REQUEST);

    rm.stop();
  }

  @Test
  public void testNonExistentQueueRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE + "_invalid");

    testListReservationHelper(resource, Status.BAD_REQUEST);

    rm.stop();
  }

  @Test
  public void testReservationIdRequestListReservation() throws Exception {
    rm.start();
    setupCluster(100);

    ReservationId id1 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);
    testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_2", 2);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      resource = resource.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(resource);

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

    ReservationId id1 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      resource = resource.queryParam("reservation-id",
              "invalid" + id1.toString());
    }

    JSONObject response = testListReservationHelper(resource, Status.NOT_FOUND);

    rm.stop();
  }

  @Test
  public void testIncludeResourceAllocations() throws Exception {
    rm.start();
    setupCluster(100);

    ReservationId id1 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "true")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      resource = resource.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(resource);

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

    ReservationId id1 = testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON, clock.getTime(), "res_1", 1);

    WebResource resource = constructWebResource(LIST_RESERVATION_PATH)
            .queryParam("include-resource-allocations", "false")
            .queryParam("queue", DEFAULT_QUEUE);

    if (id1 != null) {
      resource = resource.queryParam("reservation-id", id1.toString());
    }

    JSONObject json = testListReservationHelper(resource);

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
    ReservationId rid =
        testSubmissionReservationHelper("reservation/submit",
            MediaType.APPLICATION_JSON);
    if (this.isAuthenticationEnabled()) {
      assertNotNull(rid);
    }
    testDeleteReservationHelper("reservation/delete", rid,
        MediaType.APPLICATION_JSON);

    rm.stop();
  }

  private ReservationId testSubmissionReservationHelper(String path,
      String media) throws Exception {
    long arrival = clock.getTime() + MINIMUM_RESOURCE_DURATION;

    return testSubmissionReservationHelper(path, media, arrival, "res_1", 1);
  }

  private ReservationId testSubmissionReservationHelper(String path,
      String media, Long arrival, String reservationName, int expectedId)
      throws Exception {
    String reservationJson = loadJsonFile("submit-reservation.json");

    String reservationJsonRequest = String.format(reservationJson, arrival,
            arrival + MINIMUM_RESOURCE_DURATION, reservationName);

    return submitAndVerifyReservation(path, media, reservationJsonRequest,
            expectedId);
  }

  private ReservationId submitAndVerifyReservation(String path, String media,
      String reservationJson, int expectedId) throws Exception {

    JSONJAXBContext jc =
        new JSONJAXBContext(JSONConfiguration.mapped()
            .build(), ReservationSubmissionRequestInfo.class);
    JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
    ReservationSubmissionRequestInfo rsci =
        unmarshaller.unmarshalFromJSON(new StringReader(reservationJson),
            ReservationSubmissionRequestInfo.class);

    Thread.sleep(1000);
    ClientResponse response =
        constructWebResource(path).entity(rsci, MediaType.APPLICATION_JSON)
            .accept(media).post(ClientResponse.class);

    if (!this.isAuthenticationEnabled()) {
      assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
      return null;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);

    assertEquals("incorrect number of elements", 1, json.length());
    ReservationId rid = null;
    try {
      rid = ReservationId.parseReservationId(json.getString("reservation-id"));
      assertEquals("incorrect return value", rid.getId(), expectedId);
    } catch (JSONException j) {
      // failure is possible and is checked outside
    }
    return rid;
  }

  private void testUpdateReservationHelper(String path,
      ReservationId reservationId, String media) throws JSONException,
      Exception {

    String reservationJson = loadJsonFile("update-reservation.json");

    JSONJAXBContext jc =
        new JSONJAXBContext(JSONConfiguration.mapped()
            .build(), ReservationUpdateRequestInfo.class);
    JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
    ReservationUpdateRequestInfo rsci =
        unmarshaller.unmarshalFromJSON(new StringReader(reservationJson),
            ReservationUpdateRequestInfo.class);
    if (this.isAuthenticationEnabled()) {
      // only works when previous submit worked
      if(rsci.getReservationId() == null) {
        throw new IOException("Incorrectly parsed the reservatinId");
      }
      rsci.setReservationId(reservationId.toString());
    }

    Thread.sleep(1000);
    ClientResponse response =
        constructWebResource(path).entity(rsci, MediaType.APPLICATION_JSON)
            .accept(media).post(ClientResponse.class);

    if (!this.isAuthenticationEnabled()) {
      assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(Status.OK, response.getClientResponseStatus());

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

    JSONJAXBContext jc =
        new JSONJAXBContext(JSONConfiguration.mapped()
            .build(), ReservationDeleteRequestInfo.class);
    JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
    ReservationDeleteRequestInfo rsci =
        unmarshaller.unmarshalFromJSON(new StringReader(reservationJson),
            ReservationDeleteRequestInfo.class);
    if (this.isAuthenticationEnabled()) {
      // only works when previous submit worked
      if(rsci.getReservationId() == null) {
        throw new IOException("Incorrectly parsed the reservatinId");
      }
      rsci.setReservationId(reservationId.toString());
    }

    Thread.sleep(1000);
    ClientResponse response =
        constructWebResource(path).entity(rsci, MediaType.APPLICATION_JSON)
            .accept(media).post(ClientResponse.class);

    if (!this.isAuthenticationEnabled()) {
      assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
      return;
    }

    System.out.println("RESPONSE:" + response);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(Status.OK, response.getClientResponseStatus());
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

  private JSONObject testListReservationHelper(WebResource resource) throws
          Exception {
    return testListReservationHelper(resource, Status.OK);
  }

  private JSONObject testListReservationHelper(WebResource resource, Status
          status) throws Exception {
    Thread.sleep(1000);
    ClientResponse response = resource.get(ClientResponse.class);

    if (!this.isAuthenticationEnabled()) {
      assertEquals(Status.UNAUTHORIZED, response.getClientResponseStatus());
      return null;
    }

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(status, response.getClientResponseStatus());

    return response.getEntity(JSONObject.class);
  }

  private void setupCluster(int nodes) throws Exception {
    for (int i = 0; i < nodes; i++) {
      MockNM amNodeManager =
              rm.registerNode("127.0.0." + i + ":1234", 100 * 1024);
      amNodeManager.nodeHeartbeat(true);
    }
  }
}
