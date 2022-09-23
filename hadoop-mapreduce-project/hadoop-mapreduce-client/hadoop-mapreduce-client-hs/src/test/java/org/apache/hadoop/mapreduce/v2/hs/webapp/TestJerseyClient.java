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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.MockJobs;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptFinishingMonitor;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.mapreduce.v2.hs.CachedHistoryStorage;
import org.apache.hadoop.mapreduce.v2.hs.ConfigureAware;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryJobs;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.app.AppState;
import org.apache.hadoop.yarn.app.SimpleAppInfo;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.google.inject.util.Providers;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestJerseyClient extends JerseyTest {

  private static JerseyClient client;
  private static Configuration conf = new Configuration();
  private static TestAppContext appContext;
  private static HsWebApp webApp;

  static class TestAppContext extends AbstractService implements HistoryContext, ConfigureAware {
    private final ApplicationAttemptId appAttemptID;
    private final ApplicationId appID;
    private final String user = MockJobs.newUserName();
    private final Map<JobId, Job> partialJobs;
    private final Map<JobId, Job> fullJobs;
    private final long startTime = System.currentTimeMillis();

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts,
        boolean hasFailedTasks) {
      super(TestAppContext.class.getName());
      appID = MockJobs.newAppID(appid);
      appAttemptID = ApplicationAttemptId.newInstance(appID, 0);
      MockHistoryJobs.JobsPair jobs;
      try {
        jobs = MockHistoryJobs.newHistoryJobs(appID, numJobs, numTasks,
            numAttempts, hasFailedTasks);
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
      partialJobs = jobs.partial;
      fullJobs = jobs.full;
    }

    TestAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
      this(appid, numJobs, numTasks, numAttempts, false);
    }

    TestAppContext() {
      this(0, 1, 2, 1);
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }

    @Override
    public CharSequence getUser() {
      return user;
    }

    @Override
    public Job getJob(JobId jobID) {
      return fullJobs.get(jobID);
    }

    public Job getPartialJob(JobId jobID) {
      return partialJobs.get(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return partialJobs; // OK
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler<Event> getEventHandler() {
      return null;
    }

    @Override
    public Clock getClock() {
      return null;
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return null;
    }

    @Override
    public String getApplicationName() {
      return "TestApp";
    }

    @Override
    public long getStartTime() {
      return startTime;
    }


    @Override
    public Set<String> getBlacklistedNodes() {
      return null;
    }

    @Override
    public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
      return null;
    }

    @Override
    public boolean isLastAMRetry() {
      return false;
    }

    @Override
    public boolean hasSuccessfullyUnregistered() {
      return false;
    }

    @Override
    public String getNMHostname() {
      return null;
    }

    @Override
    public Map<JobId, Job> getAllJobs(ApplicationId appId) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public JobsInfo getPartialJobs(Long offset, Long count, String username,
        String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
        JobState jobState) {
      return CachedHistoryStorage.getPartialJobs(this.partialJobs.values(),
          offset, count, username, queue, sBegin, sEnd, fBegin, fEnd, jobState);
    }

    @Override
    public void setHistoryUrl(String historyUrl) {

    }

    @Override
    public String getHistoryUrl() {
      return null;
    }

    @Override
    public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return null;
    }
  }

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      appContext = new TestAppContext();
      webApp = mock(HsWebApp.class);
      when(webApp.name()).thenReturn("hsmockwebapp");

      bind(JAXBContextResolver.class);
      bind(HsWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(WebApp.class).toInstance(webApp);
      bind(AppContext.class).toInstance(appContext);
      bind(HistoryContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);
      bind(ApplicationClientProtocol.class).toProvider(Providers.of(null));

      serve("/*").with(GuiceContainer.class);
    }
  }

  static {
    org.apache.hadoop.yarn.webapp.GuiceServletConfig.setInjector(
        Guice.createInjector(new TestJerseyClient.WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    org.apache.hadoop.yarn.webapp.GuiceServletConfig.setInjector(
        Guice.createInjector(new TestJerseyClient.WebServletModule()));
    client = new JerseyClient();
  }

  @Override
  protected AppDescriptor configure() {
    return new WebAppDescriptor.Builder(
        "org.apache.hadoop.mapreduce.v2.hs.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("test").servletPath("/").build();
  }

  @Test
  public void testFetch(){
    String url = "http://localhost:9998/test/ws/v1/history/mapreduce/jobs";
    try{
      String str = client.fetchJson(url);
      assertNotNull(str);
      System.out.println(str);
    } catch (Exception e){
      fail(e.getMessage());
    }
  }

  @Test
  public void testFetchAs(){
    String url = "http://localhost:9998/test/ws/v1/history/mapreduce/jobs";
    try{
      JobsInfo jobsInfo = client.fetchAs(url, JobsInfo.class);
      assertNotNull(jobsInfo);
      assertNotNull(jobsInfo.getJobs());
      assertEquals(1, jobsInfo.getJobs().size());
      for(org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo info: jobsInfo.getJobs()){
        System.out.println(info.getId());
        String tasksUrl = url + "/" + info.getId() + "/tasks";
        TasksInfo tasksInfo = client.fetchAs(tasksUrl, TasksInfo.class);
        assertNotNull(tasksInfo);
        assertNotNull(tasksInfo.getTask());
        assertTrue(tasksInfo.getTask().size() > 0);
      }
    } catch (Exception e){
      fail(e.getMessage());
    }
  }

  @Test
  public void testConvertJson() {
    String json = "{\"SimpleAppInfo\":{\"id\":50,\"state\":\"COMPLETED\","
        + "\"trackingUrl\":\"http://ip-10-32-151-99.ec2.internal:9046/proxy/"
        + "application_1426670112915_0050/jobhistory/job/job_1426670112915_0050\"}}";
    SimpleAppInfo info = client.convert(json, SimpleAppInfo.class);
    assertNotNull(info);
    assertEquals(50, info.getId());
    assertEquals(AppState.COMPLETED, info.getState());
    assertEquals(
        "http://ip-10-32-151-99.ec2.internal:9046/proxy/"
            + "application_1426670112915_0050/jobhistory/job/job_1426670112915_0050",
        info.getTrackingUrl());
  }
}
