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

package org.apache.hadoop.mapred;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for ClientServiceDelegate.java
 */

@RunWith(value = Parameterized.class)
public class TestClientServiceDelegate {
  private JobID oldJobId = JobID.forName("job_1315895242400_2");
  private org.apache.hadoop.mapreduce.v2.api.records.JobId jobId = TypeConverter
      .toYarn(oldJobId);
  private boolean isAMReachableFromClient;

  public TestClientServiceDelegate(boolean isAMReachableFromClient) {
    this.isAMReachableFromClient = isAMReachableFromClient;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { true }, { false } };
    return Arrays.asList(data);
  }

  @Test
  public void testUnknownAppInRM() throws Exception {
    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenReturn(
        getJobReportResponse());
    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        historyServerProxy, getRMDelegate());

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
  }

  @Test
  public void testRemoteExceptionFromHistoryServer() throws Exception {

    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenThrow(
        new IOException("Job ID doesnot Exist"));

    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))
        .thenReturn(null);

    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        historyServerProxy, rm);

    try {
      clientServiceDelegate.getJobStatus(oldJobId);
      Assert.fail("Invoke should throw exception after retries.");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Job ID doesnot Exist"));
    }
  }

  @Test
  public void testRetriesOnConnectionFailure() throws Exception {

    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenThrow(
        new RuntimeException("1")).thenThrow(new RuntimeException("2"))       
        .thenReturn(getJobReportResponse());

    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))
        .thenReturn(null);

    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        historyServerProxy, rm);

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    verify(historyServerProxy, times(3)).getJobReport(
        any(GetJobReportRequest.class));
  }

  @Test
  public void testRetriesOnAMConnectionFailures() throws Exception {
    if (!isAMReachableFromClient) {
      return;
    }

    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))
      .thenReturn(getRunningApplicationReport("am1", 78));

    // throw exception in 1st, 2nd, 3rd and 4th call of getJobReport, and
    // succeed in the 5th call.
    final MRClientProtocol amProxy = mock(MRClientProtocol.class);
    when(amProxy.getJobReport(any(GetJobReportRequest.class)))
      .thenThrow(new RuntimeException("11"))
      .thenThrow(new RuntimeException("22"))
      .thenThrow(new RuntimeException("33"))
      .thenThrow(new RuntimeException("44")).thenReturn(getJobReportResponse());
    Configuration conf = new YarnConfiguration();
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED,
      !isAMReachableFromClient);
    ClientServiceDelegate clientServiceDelegate =
        new ClientServiceDelegate(conf, rm, oldJobId, null) {
          @Override
          MRClientProtocol instantiateAMProxy(
              final InetSocketAddress serviceAddr) throws IOException {
            super.instantiateAMProxy(serviceAddr);
            return amProxy;
          }
        };

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);

    Assert.assertNotNull(jobStatus);
    // assert maxClientRetry is not decremented.
    Assert.assertEquals(conf.getInt(MRJobConfig.MR_CLIENT_MAX_RETRIES,
      MRJobConfig.DEFAULT_MR_CLIENT_MAX_RETRIES), clientServiceDelegate
      .getMaxClientRetry());
    verify(amProxy, times(5)).getJobReport(any(GetJobReportRequest.class));
  }

  @Test
  public void testHistoryServerNotConfigured() throws Exception {
    //RM doesn't have app report and job History Server is not configured
    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        null, getRMDelegate());
    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertEquals("N/A", jobStatus.getUsername());
    Assert.assertEquals(JobStatus.State.PREP, jobStatus.getState());

    //RM has app report and job History Server is not configured
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    ApplicationReport applicationReport = getFinishedApplicationReport();
    when(rm.getApplicationReport(jobId.getAppId())).thenReturn(
        applicationReport);

    clientServiceDelegate = getClientServiceDelegate(null, rm);
    jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertEquals(applicationReport.getUser(), jobStatus.getUsername());
    Assert.assertEquals(JobStatus.State.SUCCEEDED, jobStatus.getState());
  }
  
  @Test
  public void testJobReportFromHistoryServer() throws Exception {                                 
    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);                           
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenReturn(                      
        getJobReportResponseFromHistoryServer());                                                 
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);                                     
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))                      
    .thenReturn(null);                                                                        
    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(                       
        historyServerProxy, rm);

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("TestJobFilePath", jobStatus.getJobFile());                               
    Assert.assertEquals("http://TestTrackingUrl", jobStatus.getTrackingUrl());                    
    Assert.assertEquals(1.0f, jobStatus.getMapProgress(), 0.0f);
    Assert.assertEquals(1.0f, jobStatus.getReduceProgress(), 0.0f);
  }
  
  @Test
  public void testCountersFromHistoryServer() throws Exception {                                 
    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);                           
    when(historyServerProxy.getCounters(getCountersRequest())).thenReturn(                      
        getCountersResponseFromHistoryServer());
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);                                     
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))                      
        .thenReturn(null);                                                                        
    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(                       
        historyServerProxy, rm);

    Counters counters = TypeConverter.toYarn(clientServiceDelegate.getJobCounters(oldJobId));
    Assert.assertNotNull(counters);
    Assert.assertEquals(1001, counters.getCounterGroup("dummyCounters").getCounter("dummyCounter").getValue());                               
  }

  @Test
  public void testReconnectOnAMRestart() throws IOException {
    //test not applicable when AM not reachable
    //as instantiateAMProxy is not called at all
    if(!isAMReachableFromClient) {
      return;
    }

    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);

    // RM returns AM1 url, null, null and AM2 url on invocations.
    // Nulls simulate the time when AM2 is in the process of restarting.
    ResourceMgrDelegate rmDelegate = mock(ResourceMgrDelegate.class);
    try {
      when(rmDelegate.getApplicationReport(jobId.getAppId())).thenReturn(
          getRunningApplicationReport("am1", 78)).thenReturn(
          getRunningApplicationReport(null, 0)).thenReturn(
          getRunningApplicationReport(null, 0)).thenReturn(
          getRunningApplicationReport("am2", 90));
    } catch (YarnException e) {
      throw new IOException(e);
    }

    GetJobReportResponse jobReportResponse1 = mock(GetJobReportResponse.class);
    when(jobReportResponse1.getJobReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "jobName-firstGen", "user",
            JobState.RUNNING, 0, 0, 0, 0, 0, 0, 0, "anything", null,
            false, ""));

    // First AM returns a report with jobName firstGen and simulates AM shutdown
    // on second invocation.
    MRClientProtocol firstGenAMProxy = mock(MRClientProtocol.class);
    when(firstGenAMProxy.getJobReport(any(GetJobReportRequest.class)))
        .thenReturn(jobReportResponse1).thenThrow(
            new RuntimeException("AM is down!"));

    GetJobReportResponse jobReportResponse2 = mock(GetJobReportResponse.class);
    when(jobReportResponse2.getJobReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "jobName-secondGen", "user",
            JobState.RUNNING, 0, 0, 0, 0, 0, 0, 0, "anything", null,
            false, ""));

    // Second AM generation returns a report with jobName secondGen
    MRClientProtocol secondGenAMProxy = mock(MRClientProtocol.class);
    when(secondGenAMProxy.getJobReport(any(GetJobReportRequest.class)))
        .thenReturn(jobReportResponse2);
    
    ClientServiceDelegate clientServiceDelegate = spy(getClientServiceDelegate(
        historyServerProxy, rmDelegate));
    // First time, connection should be to AM1, then to AM2. Further requests
    // should use the same proxy to AM2 and so instantiateProxy shouldn't be
    // called.
    doReturn(firstGenAMProxy).doReturn(secondGenAMProxy).when(
        clientServiceDelegate).instantiateAMProxy(any(InetSocketAddress.class));

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("jobName-firstGen", jobStatus.getJobName());

    jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("jobName-secondGen", jobStatus.getJobName());

    jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("jobName-secondGen", jobStatus.getJobName());

    verify(clientServiceDelegate, times(2)).instantiateAMProxy(
        any(InetSocketAddress.class));
  }
  
  @Test
  public void testAMAccessDisabled() throws IOException {
    //test only applicable when AM not reachable
    if(isAMReachableFromClient) {
      return;
    }

    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenReturn(                      
        getJobReportResponseFromHistoryServer());                                                 

    ResourceMgrDelegate rmDelegate = mock(ResourceMgrDelegate.class);
    try {
      when(rmDelegate.getApplicationReport(jobId.getAppId())).thenReturn(
          getRunningApplicationReport("am1", 78)).thenReturn(
            getRunningApplicationReport("am1", 78)).thenReturn(
              getRunningApplicationReport("am1", 78)).thenReturn(
          getFinishedApplicationReport());
    } catch (YarnException e) {
      throw new IOException(e);
    }

    ClientServiceDelegate clientServiceDelegate = spy(getClientServiceDelegate(
        historyServerProxy, rmDelegate));

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("N/A", jobStatus.getJobName());
    
    verify(clientServiceDelegate, times(0)).instantiateAMProxy(
        any(InetSocketAddress.class));

    // Should not reach AM even for second and third times too.
    jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("N/A", jobStatus.getJobName());    
    verify(clientServiceDelegate, times(0)).instantiateAMProxy(
        any(InetSocketAddress.class));
    jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
    Assert.assertEquals("N/A", jobStatus.getJobName());    
    verify(clientServiceDelegate, times(0)).instantiateAMProxy(
        any(InetSocketAddress.class));

    // The third time around, app is completed, so should go to JHS
    JobStatus jobStatus1 = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus1);
    Assert.assertEquals("TestJobFilePath", jobStatus1.getJobFile());                               
    Assert.assertEquals("http://TestTrackingUrl", jobStatus1.getTrackingUrl());                    
    Assert.assertEquals(1.0f, jobStatus1.getMapProgress(), 0.0f);
    Assert.assertEquals(1.0f, jobStatus1.getReduceProgress(), 0.0f);
    
    verify(clientServiceDelegate, times(0)).instantiateAMProxy(
        any(InetSocketAddress.class));
  }
  
  @Test
  public void testRMDownForJobStatusBeforeGetAMReport() throws IOException {
    Configuration conf = new YarnConfiguration();
    testRMDownForJobStatusBeforeGetAMReport(conf,
        MRJobConfig.DEFAULT_MR_CLIENT_MAX_RETRIES);
  }

  @Test
  public void testRMDownForJobStatusBeforeGetAMReportWithRetryTimes()
      throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.setInt(MRJobConfig.MR_CLIENT_MAX_RETRIES, 2);
    testRMDownForJobStatusBeforeGetAMReport(conf, conf.getInt(
        MRJobConfig.MR_CLIENT_MAX_RETRIES,
        MRJobConfig.DEFAULT_MR_CLIENT_MAX_RETRIES));
  }
  
  @Test
  public void testRMDownRestoreForJobStatusBeforeGetAMReport()
      throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.setInt(MRJobConfig.MR_CLIENT_MAX_RETRIES, 3);

    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED,
        !isAMReachableFromClient);
    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(any(GetJobReportRequest.class)))
        .thenReturn(getJobReportResponse());
    ResourceMgrDelegate rmDelegate = mock(ResourceMgrDelegate.class);
    try {
      when(rmDelegate.getApplicationReport(jobId.getAppId())).thenThrow(
          new java.lang.reflect.UndeclaredThrowableException(new IOException(
              "Connection refuced1"))).thenThrow(
          new java.lang.reflect.UndeclaredThrowableException(new IOException(
              "Connection refuced2")))
          .thenReturn(getFinishedApplicationReport());
      ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(
          conf, rmDelegate, oldJobId, historyServerProxy);
      JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
      verify(rmDelegate, times(3)).getApplicationReport(
          any(ApplicationId.class));
      Assert.assertNotNull(jobStatus);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  private void testRMDownForJobStatusBeforeGetAMReport(Configuration conf,
      int noOfRetries) throws IOException {
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED,
        !isAMReachableFromClient);
    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    ResourceMgrDelegate rmDelegate = mock(ResourceMgrDelegate.class);
    try {
      when(rmDelegate.getApplicationReport(jobId.getAppId())).thenThrow(
          new java.lang.reflect.UndeclaredThrowableException(new IOException(
              "Connection refuced1"))).thenThrow(
          new java.lang.reflect.UndeclaredThrowableException(new IOException(
              "Connection refuced2"))).thenThrow(
          new java.lang.reflect.UndeclaredThrowableException(new IOException(
              "Connection refuced3")));
      ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(
          conf, rmDelegate, oldJobId, historyServerProxy);
      try {
        clientServiceDelegate.getJobStatus(oldJobId);
        Assert.fail("It should throw exception after retries");
      } catch (IOException e) {
        System.out.println("fail to get job status,and e=" + e.toString());
      }
      verify(rmDelegate, times(noOfRetries)).getApplicationReport(
          any(ApplicationId.class));
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }  

  private GetJobReportRequest getJobReportRequest() {
    GetJobReportRequest request = Records.newRecord(GetJobReportRequest.class);
    request.setJobId(jobId);
    return request;
  }
  
  private GetJobReportResponse getJobReportResponse() {
    GetJobReportResponse jobReportResponse = Records
        .newRecord(GetJobReportResponse.class);
    JobReport jobReport = Records.newRecord(JobReport.class);
    jobReport.setJobId(jobId);
    jobReport.setJobState(JobState.SUCCEEDED);
    jobReportResponse.setJobReport(jobReport);
    return jobReportResponse;
  }

  private GetCountersRequest getCountersRequest() {
    GetCountersRequest request = Records.newRecord(GetCountersRequest.class);
    request.setJobId(jobId);
    return request;
  }

  private ApplicationReport getFinishedApplicationReport() {
    ApplicationId appId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    return ApplicationReport.newInstance(appId, attemptId, "user", "queue",
      "appname", "host", 124, null, YarnApplicationState.FINISHED,
      "diagnostics", "url", 0, 0, FinalApplicationStatus.SUCCEEDED, null,
      "N/A", 0.0f, YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
  }

  private ApplicationReport getRunningApplicationReport(String host, int port) {
    ApplicationId appId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    return ApplicationReport.newInstance(appId, attemptId, "user", "queue",
      "appname", host, port, null, YarnApplicationState.RUNNING, "diagnostics",
      "url", 0, 0, FinalApplicationStatus.UNDEFINED, null, "N/A", 0.0f,
      YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
  }

  private ResourceMgrDelegate getRMDelegate() throws IOException {
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    try {
      when(rm.getApplicationReport(jobId.getAppId())).thenReturn(null);
    } catch (YarnException e) {
      throw new IOException(e);
    }
    return rm;
  }

  private ClientServiceDelegate getClientServiceDelegate(
      MRClientProtocol historyServerProxy, ResourceMgrDelegate rm) {
    Configuration conf = new YarnConfiguration();
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    conf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, !isAMReachableFromClient);
    ClientServiceDelegate clientServiceDelegate = new ClientServiceDelegate(
        conf, rm, oldJobId, historyServerProxy);
    return clientServiceDelegate;
  }

  private GetJobReportResponse getJobReportResponseFromHistoryServer() {
    GetJobReportResponse jobReportResponse = Records                                              
        .newRecord(GetJobReportResponse.class);                                                   
    JobReport jobReport = Records.newRecord(JobReport.class);                                     
    jobReport.setJobId(jobId);                                                                    
    jobReport.setJobState(JobState.SUCCEEDED);                                                    
    jobReport.setMapProgress(1.0f);
    jobReport.setReduceProgress(1.0f);
    jobReport.setJobFile("TestJobFilePath");
    jobReport.setTrackingUrl("http://TestTrackingUrl");
    jobReportResponse.setJobReport(jobReport);
    return jobReportResponse;
  }
  
  private GetCountersResponse getCountersResponseFromHistoryServer() {
    GetCountersResponse countersResponse = Records
        .newRecord(GetCountersResponse.class);
    Counter counter = Records.newRecord(Counter.class);
    CounterGroup counterGroup = Records.newRecord(CounterGroup.class);
    Counters counters = Records.newRecord(Counters.class);
    counter.setDisplayName("dummyCounter");
    counter.setName("dummyCounter");
    counter.setValue(1001);
    counterGroup.setName("dummyCounters");
    counterGroup.setDisplayName("dummyCounters");
    counterGroup.setCounter("dummyCounter", counter);
    counters.setCounterGroup("dummyCounters", counterGroup);
    countersResponse.setCounters(counters);
    return countersResponse;
  }
}
