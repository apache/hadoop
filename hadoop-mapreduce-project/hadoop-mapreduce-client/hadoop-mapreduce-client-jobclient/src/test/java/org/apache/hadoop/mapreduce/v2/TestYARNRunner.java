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

package org.apache.hadoop.mapreduce.v2;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test YarnRunner and make sure the client side plugin works 
 * fine
 */
public class TestYARNRunner extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestYARNRunner.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
 
  private YARNRunner yarnRunner;
  private ResourceMgrDelegate resourceMgrDelegate;
  private YarnConfiguration conf;
  private ClientCache clientCache;
  private ApplicationId appId;
  private JobID jobId;
  private File testWorkDir = 
      new File("target", TestYARNRunner.class.getName());
  private ApplicationSubmissionContext submissionContext;
  private  ClientServiceDelegate clientDelegate;
  private static final String failString = "Rejected job";
 
  @Before
  public void setUp() throws Exception {
    resourceMgrDelegate = mock(ResourceMgrDelegate.class);
    conf = new YarnConfiguration();
    clientCache = new ClientCache(conf, resourceMgrDelegate);
    clientCache = spy(clientCache);
    yarnRunner = new YARNRunner(conf, resourceMgrDelegate, clientCache);
    yarnRunner = spy(yarnRunner);
    submissionContext = mock(ApplicationSubmissionContext.class);
    doAnswer(
        new Answer<ApplicationSubmissionContext>() {
          @Override
          public ApplicationSubmissionContext answer(InvocationOnMock invocation)
              throws Throwable {
            return submissionContext;
          }
        }
        ).when(yarnRunner).createApplicationSubmissionContext(any(Configuration.class),
            any(String.class), any(Credentials.class));
    
    appId = recordFactory.newRecordInstance(ApplicationId.class);
    appId.setClusterTimestamp(System.currentTimeMillis());
    appId.setId(1);
    jobId = TypeConverter.fromYarn(appId);
    if (testWorkDir.exists()) {
      FileContext.getLocalFSFileContext().delete(new Path(testWorkDir.toString()), true);
    }
    testWorkDir.mkdirs();
   }
  
  
  @Test
  public void testJobKill() throws Exception {
    clientDelegate = mock(ClientServiceDelegate.class);
    when(clientDelegate.getJobStatus(any(JobID.class))).thenReturn(new 
        org.apache.hadoop.mapreduce.JobStatus(jobId, 0f, 0f, 0f, 0f, 
            State.PREP, JobPriority.HIGH, "tmp", "tmp", "tmp", "tmp"));
    when(clientDelegate.killJob(any(JobID.class))).thenReturn(true);
    doAnswer(
        new Answer<ClientServiceDelegate>() {
          @Override
          public ClientServiceDelegate answer(InvocationOnMock invocation)
              throws Throwable {
            return clientDelegate;
          }
        }
        ).when(clientCache).getClient(any(JobID.class));
    yarnRunner.killJob(jobId);
    verify(resourceMgrDelegate).killApplication(appId);
    when(clientDelegate.getJobStatus(any(JobID.class))).thenReturn(new 
        org.apache.hadoop.mapreduce.JobStatus(jobId, 0f, 0f, 0f, 0f, 
            State.RUNNING, JobPriority.HIGH, "tmp", "tmp", "tmp", "tmp"));
    yarnRunner.killJob(jobId);
    verify(clientDelegate).killJob(jobId);
  }
  
  @Test
  public void testJobSubmissionFailure() throws Exception {
    when(resourceMgrDelegate.submitApplication(any(ApplicationSubmissionContext.class))).
    thenReturn(appId);
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getApplicationId()).thenReturn(appId);
    when(report.getDiagnostics()).thenReturn(failString);
    when(report.getState()).thenReturn(ApplicationState.FAILED);
    when(resourceMgrDelegate.getApplicationReport(appId)).thenReturn(report);
    Credentials credentials = new Credentials();
    File jobxml = new File(testWorkDir, "job.xml");
    OutputStream out = new FileOutputStream(jobxml);
    conf.writeXml(out);
    out.close();
    try {
      yarnRunner.submitJob(jobId, testWorkDir.getAbsolutePath().toString(), credentials); 
    } catch(IOException io) {
      LOG.info("Logging exception:", io);
      assertTrue(io.getLocalizedMessage().contains(failString));
    }
  }
  
  @Test
  public void testResourceMgrDelegate() throws Exception {
    /* we not want a mock of resourcemgr deleagte */
    ClientRMProtocol clientRMProtocol = mock(ClientRMProtocol.class);
    ResourceMgrDelegate delegate = new ResourceMgrDelegate(conf, clientRMProtocol);
    /* make sure kill calls finish application master */
    when(clientRMProtocol.forceKillApplication(any(KillApplicationRequest.class)))
    .thenReturn(null);
    delegate.killApplication(appId);
    verify(clientRMProtocol).forceKillApplication(any(KillApplicationRequest.class));
    
    /* make sure getalljobs calls get all applications */
    when(clientRMProtocol.getAllApplications(any(GetAllApplicationsRequest.class))).
    thenReturn(recordFactory.newRecordInstance(GetAllApplicationsResponse.class));
    delegate.getAllJobs();
    verify(clientRMProtocol).getAllApplications(any(GetAllApplicationsRequest.class));
    
    /* make sure getapplication report is called */
    when(clientRMProtocol.getApplicationReport(any(GetApplicationReportRequest.class)))
    .thenReturn(recordFactory.newRecordInstance(GetApplicationReportResponse.class));
    delegate.getApplicationReport(appId);
    verify(clientRMProtocol).getApplicationReport(any(GetApplicationReportRequest.class));
    
    /* make sure metrics is called */
    GetClusterMetricsResponse clusterMetricsResponse = recordFactory.newRecordInstance
        (GetClusterMetricsResponse.class);
    clusterMetricsResponse.setClusterMetrics(recordFactory.newRecordInstance(
        YarnClusterMetrics.class));
    when(clientRMProtocol.getClusterMetrics(any(GetClusterMetricsRequest.class)))
    .thenReturn(clusterMetricsResponse);
    delegate.getClusterMetrics();
    verify(clientRMProtocol).getClusterMetrics(any(GetClusterMetricsRequest.class));
    
    when(clientRMProtocol.getClusterNodes(any(GetClusterNodesRequest.class))).
    thenReturn(recordFactory.newRecordInstance(GetClusterNodesResponse.class));
    delegate.getActiveTrackers();
    verify(clientRMProtocol).getClusterNodes(any(GetClusterNodesRequest.class));
    
    GetNewApplicationIdResponse newAppIdResponse = recordFactory.newRecordInstance(
        GetNewApplicationIdResponse.class);
    newAppIdResponse.setApplicationId(appId);
    when(clientRMProtocol.getNewApplicationId(any(GetNewApplicationIdRequest.class))).
    thenReturn(newAppIdResponse);
    delegate.getNewJobID();
    verify(clientRMProtocol).getNewApplicationId(any(GetNewApplicationIdRequest.class));
    
    GetQueueInfoResponse queueInfoResponse = recordFactory.newRecordInstance(
        GetQueueInfoResponse.class);
    queueInfoResponse.setQueueInfo(recordFactory.newRecordInstance(QueueInfo.class));
    when(clientRMProtocol.getQueueInfo(any(GetQueueInfoRequest.class))).
    thenReturn(queueInfoResponse);
    delegate.getQueues();
    verify(clientRMProtocol).getQueueInfo(any(GetQueueInfoRequest.class));
    
    GetQueueUserAclsInfoResponse aclResponse = recordFactory.newRecordInstance(
        GetQueueUserAclsInfoResponse.class);
    when(clientRMProtocol.getQueueUserAcls(any(GetQueueUserAclsInfoRequest.class)))
    .thenReturn(aclResponse);
    delegate.getQueueAclsForCurrentUser();
    verify(clientRMProtocol).getQueueUserAcls(any(GetQueueUserAclsInfoRequest.class));
  }
}
