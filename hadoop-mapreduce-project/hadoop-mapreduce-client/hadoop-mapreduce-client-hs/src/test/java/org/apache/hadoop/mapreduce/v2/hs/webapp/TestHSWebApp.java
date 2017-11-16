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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.APP_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.ATTEMPT_STATE;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.webapp.TestAMWebApp;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsPage;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHSWebApp {
  private static final Logger LOG = LoggerFactory.getLogger(TestHSWebApp.class);

  @Test public void testAppControllerIndex() {
    MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
    Injector injector = WebAppTests.createMockInjector(AppContext.class, ctx);
    HsController controller = injector.getInstance(HsController.class);
    controller.index();
    assertEquals(ctx.getApplicationID().toString(), controller.get(APP_ID,""));
  }

  @Test public void testJobView() {
    LOG.info("HsJobPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getJobParams(appContext);
    WebAppTests.testPage(HsJobPage.class, AppContext.class, appContext, params);
  }

  @Test
  public void testTasksView() {
    LOG.info("HsTasksPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests.testPage(HsTasksPage.class, AppContext.class, appContext,
        params);
  }

  @Test
  public void testTasksViewNaturalSortType() {
    LOG.info("HsTasksPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    Injector testPage = WebAppTests.testPage(HsTasksPage.class, AppContext.class, appContext, params);
    View viewInstance = testPage.getInstance(HsTasksPage.class);
    Map<String, String> moreParams = viewInstance.context().requestContext().moreParams();
    String appTableColumnsMeta = moreParams.get("ui.dataTables.selector.init");
    Assert.assertTrue(appTableColumnsMeta.indexOf("natural") != -1);
  }

  @Test
  public void testTaskView() {
    LOG.info("HsTaskPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests
        .testPage(HsTaskPage.class, AppContext.class, appContext, params);
  }

  @Test
  public void testTaskViewNaturalSortType() {
    LOG.info("HsTaskPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    Injector testPage = WebAppTests.testPage(HsTaskPage.class, AppContext.class, appContext, params);
    View viewInstance = testPage.getInstance(HsTaskPage.class);
    Map<String, String> moreParams = viewInstance.context().requestContext().moreParams();
    String appTableColumnsMeta = moreParams.get("ui.dataTables.attempts.init");
    Assert.assertTrue(appTableColumnsMeta.indexOf("natural") != -1);
  }

  @Test public void testAttemptsWithJobView() {
    LOG.info("HsAttemptsPage with data");
    MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
    JobId id = ctx.getAllJobs().keySet().iterator().next();
    Map<String, String> params = new HashMap<String,String>();
    params.put(JOB_ID, id.toString());
    params.put(TASK_TYPE, "m");
    params.put(ATTEMPT_STATE, "SUCCESSFUL");
    WebAppTests.testPage(HsAttemptsPage.class, AppContext.class,
        ctx, params);
  }
  
  @Test public void testAttemptsView() {
    LOG.info("HsAttemptsPage");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getTaskParams(appContext);
    WebAppTests.testPage(HsAttemptsPage.class, AppContext.class,
                         appContext, params);
  }
  
  @Test public void testConfView() {
    LOG.info("HsConfPage");
    WebAppTests.testPage(HsConfPage.class, AppContext.class,
                         new MockAppContext(0, 1, 1, 1));
  }
  
  @Test public void testAboutView() {
    LOG.info("HsAboutPage");
    WebAppTests.testPage(HsAboutPage.class, AppContext.class,
                         new MockAppContext(0, 1, 1, 1));
  }
  
  @Test public void testJobCounterView() {
    LOG.info("JobCounterView");
    AppContext appContext = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = TestAMWebApp.getJobParams(appContext);
    WebAppTests.testPage(HsCountersPage.class, AppContext.class,
                         appContext, params);
  }
  
  @Test public void testJobCounterViewForKilledJob() {
    LOG.info("JobCounterViewForKilledJob");
    AppContext appContext = new MockAppContext(0, 1, 1, 1, true);
    Map<String, String> params = TestAMWebApp.getJobParams(appContext);
    WebAppTests.testPage(HsCountersPage.class, AppContext.class,
        appContext, params);
  }
  
  @Test public void testSingleCounterView() {
    LOG.info("HsSingleCounterPage");
    WebAppTests.testPage(HsSingleCounterPage.class, AppContext.class,
                         new MockAppContext(0, 1, 1, 1));
  }
  
  @Test
  public void testLogsView1() throws IOException {
    LOG.info("HsLogsPage");
    Injector injector =
        WebAppTests.testPage(AggregatedLogsPage.class, AppContext.class,
            new MockAppContext(0, 1, 1, 1));
    PrintWriter spyPw = WebAppTests.getPrintWriter(injector);
    verify(spyPw).write("Cannot get container logs without a ContainerId");
    verify(spyPw).write("Cannot get container logs without a NodeId");
    verify(spyPw).write("Cannot get container logs without an app owner");
  }

  @Test
  public void testLogsView2() throws IOException {
    LOG.info("HsLogsPage with data");
    MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = new HashMap<String, String>();

    params.put(CONTAINER_ID, MRApp.newContainerId(1, 1, 333, 1)
        .toString());
    params.put(NM_NODENAME, 
        NodeId.newInstance(MockJobs.NM_HOST, MockJobs.NM_PORT).toString());
    params.put(ENTITY_STRING, "container_10_0001_01_000001");
    params.put(APP_OWNER, "owner");

    Injector injector =
        WebAppTests.testPage(AggregatedLogsPage.class, AppContext.class, ctx,
            params);
    PrintWriter spyPw = WebAppTests.getPrintWriter(injector);
    verify(spyPw).write(
        "Aggregation is not enabled. Try the nodemanager at "
            + MockJobs.NM_HOST + ":" + MockJobs.NM_PORT);
  }

  @Test
  public void testLogsViewSingle() throws IOException {
    LOG.info("HsLogsPage with params for single log and data limits");
    MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = new HashMap<String, String>();

    final Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);

    params.put("start", "-2048");
    params.put("end", "-1024");
    params.put(CONTAINER_LOG_TYPE, "syslog");
    params.put(CONTAINER_ID, MRApp.newContainerId(1, 1, 333, 1)
        .toString());
    params.put(NM_NODENAME,
        NodeId.newInstance(MockJobs.NM_HOST, MockJobs.NM_PORT).toString());
    params.put(ENTITY_STRING, "container_10_0001_01_000001");
    params.put(APP_OWNER, "owner");

    Injector injector =
        WebAppTests.testPage(AggregatedLogsPage.class, AppContext.class, ctx,
            params, new AbstractModule() {
          @Override
          protected void configure() {
            bind(Configuration.class).toInstance(conf);
          }
        });
    PrintWriter spyPw = WebAppTests.getPrintWriter(injector);
    verify(spyPw).write(
        "Logs not available for container_10_0001_01_000001."
            + " Aggregation may not be complete, "
            + "Check back later or try the nodemanager at "
            + MockJobs.NM_HOST + ":" + MockJobs.NM_PORT);
  }

  @Test
  public void testLogsViewBadStartEnd() throws IOException {
    LOG.info("HsLogsPage with bad start/end params");
    MockAppContext ctx = new MockAppContext(0, 1, 1, 1);
    Map<String, String> params = new HashMap<String, String>();

    params.put("start", "foo");
    params.put("end", "bar");
    params.put(CONTAINER_ID, MRApp.newContainerId(1, 1, 333, 1)
        .toString());
    params.put(NM_NODENAME,
        NodeId.newInstance(MockJobs.NM_HOST, MockJobs.NM_PORT).toString());
    params.put(ENTITY_STRING, "container_10_0001_01_000001");
    params.put(APP_OWNER, "owner");

    Injector injector =
        WebAppTests.testPage(AggregatedLogsPage.class, AppContext.class, ctx,
            params);
    PrintWriter spyPw = WebAppTests.getPrintWriter(injector);
    verify(spyPw).write("Invalid log start value: foo");
    verify(spyPw).write("Invalid log end value: bar");
  }
}
  
 
