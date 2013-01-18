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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
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
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
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
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FAILED);
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
    /* we not want a mock of resource mgr delegate */
    final ClientRMProtocol clientRMProtocol = mock(ClientRMProtocol.class);
    ResourceMgrDelegate delegate = new ResourceMgrDelegate(conf) {
      @Override
      public synchronized void start() {
        this.rmClient = clientRMProtocol;
      }
    };
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
    
    GetNewApplicationResponse newAppResponse = recordFactory.newRecordInstance(
        GetNewApplicationResponse.class);
    newAppResponse.setApplicationId(appId);
    when(clientRMProtocol.getNewApplication(any(GetNewApplicationRequest.class))).
    thenReturn(newAppResponse);
    delegate.getNewJobID();
    verify(clientRMProtocol).getNewApplication(any(GetNewApplicationRequest.class));
    
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
  
  @Test
  public void testHistoryServerToken() throws Exception {
    //Set the master principal in the config
    conf.set(YarnConfiguration.RM_PRINCIPAL,"foo@LOCAL");

    final String masterPrincipal = Master.getMasterPrincipal(conf);

    final MRClientProtocol hsProxy = mock(MRClientProtocol.class);
    when(hsProxy.getDelegationToken(any(GetDelegationTokenRequest.class))).thenAnswer(
        new Answer<GetDelegationTokenResponse>() {
          public GetDelegationTokenResponse answer(InvocationOnMock invocation) {
            GetDelegationTokenRequest request =
                (GetDelegationTokenRequest)invocation.getArguments()[0];
            // check that the renewer matches the cluster's RM principal
            assertEquals(masterPrincipal, request.getRenewer() );

            DelegationToken token =
                recordFactory.newRecordInstance(DelegationToken.class);
            // none of these fields matter for the sake of the test
            token.setKind("");
            token.setService("");
            token.setIdentifier(ByteBuffer.allocate(0));
            token.setPassword(ByteBuffer.allocate(0));
            GetDelegationTokenResponse tokenResponse =
                recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
            tokenResponse.setDelegationToken(token);
            return tokenResponse;
          }
        });
    
    UserGroupInformation.createRemoteUser("someone").doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            yarnRunner = new YARNRunner(conf, null, null);
            yarnRunner.getDelegationTokenFromHS(hsProxy);
            verify(hsProxy).
              getDelegationToken(any(GetDelegationTokenRequest.class));
            return null;
          }
        });
  }

  @Test
  public void testAMAdminCommandOpts() throws Exception {
    JobConf jobConf = new JobConf();
    
    jobConf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, "-Djava.net.preferIPv4Stack=true");
    jobConf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-Xmx1024m");
    
    YARNRunner yarnRunner = new YARNRunner(jobConf);
    
    File jobxml = new File(testWorkDir, MRJobConfig.JOB_CONF_FILE);
    OutputStream out = new FileOutputStream(jobxml);
    conf.writeXml(out);
    out.close();
    
    File jobsplit = new File(testWorkDir, MRJobConfig.JOB_SPLIT);
    out = new FileOutputStream(jobsplit);
    out.close();
    
    File jobsplitmetainfo = new File(testWorkDir, MRJobConfig.JOB_SPLIT_METAINFO);
    out = new FileOutputStream(jobsplitmetainfo);
    out.close();
    
    File appTokens = new File(testWorkDir, MRJobConfig.APPLICATION_TOKENS_FILE);
    out = new FileOutputStream(appTokens);
    out.close();
    
    ApplicationSubmissionContext submissionContext = 
        yarnRunner.createApplicationSubmissionContext(jobConf, testWorkDir.toString(), new Credentials());
    
    ContainerLaunchContext containerSpec = submissionContext.getAMContainerSpec();
    List<String> commands = containerSpec.getCommands();
    
    int index = 0;
    int adminIndex = 0;
    int adminPos = -1;
    int userIndex = 0;
    int userPos = -1;
    
    for(String command : commands) {
      if(command != null) {
        adminPos = command.indexOf("-Djava.net.preferIPv4Stack=true");
        if(adminPos >= 0)
          adminIndex = index;
        
        userPos = command.indexOf("-Xmx1024m");
        if(userPos >= 0)
          userIndex = index;
      }
      
      index++;
    }
    
    // Check both admin java opts and user java opts are in the commands
    assertTrue("AM admin command opts not in the commands.", adminPos > 0);
    assertTrue("AM user command opts not in the commands.", userPos > 0);
    
    // Check the admin java opts is before user java opts in the commands
    if(adminIndex == userIndex) {
      assertTrue("AM admin command opts is after user command opts.", adminPos < userPos);
    } else {
      assertTrue("AM admin command opts is after user command opts.", adminIndex < userIndex);
    }
  }
  @Test
  public void testWarnCommandOpts() throws Exception {
    Logger logger = Logger.getLogger(YARNRunner.class);
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    Layout layout = new SimpleLayout();
    Appender appender = new WriterAppender(layout, bout);
    logger.addAppender(appender);
    
    JobConf jobConf = new JobConf();
    
    jobConf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, "-Djava.net.preferIPv4Stack=true -Djava.library.path=foo");
    jobConf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-Xmx1024m -Djava.library.path=bar");
    
    YARNRunner yarnRunner = new YARNRunner(jobConf);
    
    File jobxml = new File(testWorkDir, MRJobConfig.JOB_CONF_FILE);
    OutputStream out = new FileOutputStream(jobxml);
    conf.writeXml(out);
    out.close();
    
    File jobsplit = new File(testWorkDir, MRJobConfig.JOB_SPLIT);
    out = new FileOutputStream(jobsplit);
    out.close();
    
    File jobsplitmetainfo = new File(testWorkDir, MRJobConfig.JOB_SPLIT_METAINFO);
    out = new FileOutputStream(jobsplitmetainfo);
    out.close();
    
    File appTokens = new File(testWorkDir, MRJobConfig.APPLICATION_TOKENS_FILE);
    out = new FileOutputStream(appTokens);
    out.close();
    
    @SuppressWarnings("unused")
    ApplicationSubmissionContext submissionContext = 
        yarnRunner.createApplicationSubmissionContext(jobConf, testWorkDir.toString(), new Credentials());
   
    String logMsg = bout.toString();
    assertTrue(logMsg.contains("WARN - Usage of -Djava.library.path in " + 
    		"yarn.app.mapreduce.am.admin-command-opts can cause programs to no " +
        "longer function if hadoop native libraries are used. These values " + 
    		"should be set as part of the LD_LIBRARY_PATH in the app master JVM " +
        "env using yarn.app.mapreduce.am.admin.user.env config settings."));
    assertTrue(logMsg.contains("WARN - Usage of -Djava.library.path in " + 
        "yarn.app.mapreduce.am.command-opts can cause programs to no longer " +
        "function if hadoop native libraries are used. These values should " +
        "be set as part of the LD_LIBRARY_PATH in the app master JVM env " +
        "using yarn.app.mapreduce.am.env config settings."));
  }
}
