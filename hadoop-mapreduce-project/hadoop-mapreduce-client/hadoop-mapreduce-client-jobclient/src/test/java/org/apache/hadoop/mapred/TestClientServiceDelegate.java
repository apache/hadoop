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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Test;

/**
 * Tests for ClientServiceDelegate.java
 */

public class TestClientServiceDelegate {
  private JobID oldJobId = JobID.forName("job_1315895242400_2");
  private org.apache.hadoop.mapreduce.v2.api.records.JobId jobId = TypeConverter
      .toYarn(oldJobId);

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
        RPCUtil.getRemoteException("Job ID doesnot Exist"));

    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))
        .thenReturn(null);

    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        historyServerProxy, rm);

    try {
      clientServiceDelegate.getJobStatus(oldJobId);
      Assert.fail("Invoke should throw exception after retries.");
    } catch (YarnRemoteException e) {
      Assert.assertEquals("Job ID doesnot Exist", e.getMessage());
    }
  }

  @Test
  public void testRetriesOnConnectionFailure() throws Exception {

    MRClientProtocol historyServerProxy = mock(MRClientProtocol.class);
    when(historyServerProxy.getJobReport(getJobReportRequest())).thenThrow(
        new RuntimeException("1")).thenThrow(new RuntimeException("2"))
        .thenThrow(new RuntimeException("3"))
        .thenReturn(getJobReportResponse());

    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(TypeConverter.toYarn(oldJobId).getAppId()))
        .thenReturn(null);

    ClientServiceDelegate clientServiceDelegate = getClientServiceDelegate(
        historyServerProxy, rm);

    JobStatus jobStatus = clientServiceDelegate.getJobStatus(oldJobId);
    Assert.assertNotNull(jobStatus);
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
    ApplicationReport applicationReport = getApplicationReport();
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
    Assert.assertEquals(1.0f, jobStatus.getMapProgress());                                        
    Assert.assertEquals(1.0f, jobStatus.getReduceProgress());                                     
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

  private ApplicationReport getApplicationReport() {
    ApplicationReport applicationReport = Records
        .newRecord(ApplicationReport.class);
    applicationReport.setYarnApplicationState(YarnApplicationState.FINISHED);
    applicationReport.setUser("root");
    applicationReport.setHost("N/A");
    applicationReport.setName("N/A");
    applicationReport.setQueue("N/A");
    applicationReport.setStartTime(0);
    applicationReport.setFinishTime(0);
    applicationReport.setTrackingUrl("N/A");
    applicationReport.setDiagnostics("N/A");
    applicationReport.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    return applicationReport;
  }

  private ResourceMgrDelegate getRMDelegate() throws YarnRemoteException {
    ResourceMgrDelegate rm = mock(ResourceMgrDelegate.class);
    when(rm.getApplicationReport(jobId.getAppId())).thenReturn(null);
    return rm;
  }

  private ClientServiceDelegate getClientServiceDelegate(
      MRClientProtocol historyServerProxy, ResourceMgrDelegate rm) {
    Configuration conf = new YarnConfiguration();
    conf.set(MRConfig.FRAMEWORK_NAME, "yarn");
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
    jobReport.setTrackingUrl("TestTrackingUrl");
    jobReportResponse.setJobReport(jobReport);
    return jobReportResponse;
  }
}
