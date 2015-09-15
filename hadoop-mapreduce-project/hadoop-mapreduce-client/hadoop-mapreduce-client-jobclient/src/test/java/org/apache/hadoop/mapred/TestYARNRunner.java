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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.After;
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

  // prefix before <LOG_DIR>/profile.out
  private static final String PROFILE_PARAMS =
      MRJobConfig.DEFAULT_TASK_PROFILE_PARAMS.substring(0,
          MRJobConfig.DEFAULT_TASK_PROFILE_PARAMS.lastIndexOf("%"));

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

    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    jobId = TypeConverter.fromYarn(appId);
    if (testWorkDir.exists()) {
      FileContext.getLocalFSFileContext().delete(new Path(testWorkDir.toString()), true);
    }
    testWorkDir.mkdirs();
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(testWorkDir);
  }

  @Test(timeout=20000)
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

    when(clientDelegate.getJobStatus(any(JobID.class))).thenReturn(null);
    when(resourceMgrDelegate.getApplicationReport(any(ApplicationId.class)))
        .thenReturn(
            ApplicationReport.newInstance(appId, null, "tmp", "tmp", "tmp",
                "tmp", 0, null, YarnApplicationState.FINISHED, "tmp", "tmp",
                0l, 0l, FinalApplicationStatus.SUCCEEDED, null, null, 0f,
                "tmp", null));
    yarnRunner.killJob(jobId);
    verify(clientDelegate).killJob(jobId);
  }

  @Test(timeout=60000)
  public void testJobKillTimeout() throws Exception {
    long timeToWaitBeforeHardKill =
        10000 + MRJobConfig.DEFAULT_MR_AM_HARD_KILL_TIMEOUT_MS;
    conf.setLong(MRJobConfig.MR_AM_HARD_KILL_TIMEOUT_MS,
        timeToWaitBeforeHardKill);
    clientDelegate = mock(ClientServiceDelegate.class);
    doAnswer(
        new Answer<ClientServiceDelegate>() {
          @Override
          public ClientServiceDelegate answer(InvocationOnMock invocation)
              throws Throwable {
            return clientDelegate;
          }
        }
      ).when(clientCache).getClient(any(JobID.class));
    when(clientDelegate.getJobStatus(any(JobID.class))).thenReturn(new
        org.apache.hadoop.mapreduce.JobStatus(jobId, 0f, 0f, 0f, 0f,
            State.RUNNING, JobPriority.HIGH, "tmp", "tmp", "tmp", "tmp"));
    long startTimeMillis = System.currentTimeMillis();
    yarnRunner.killJob(jobId);
    assertTrue("killJob should have waited at least " + timeToWaitBeforeHardKill
        + " ms.", System.currentTimeMillis() - startTimeMillis
                  >= timeToWaitBeforeHardKill);
  }

  @Test(timeout=20000)
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

  @Test(timeout=20000)
  public void testResourceMgrDelegate() throws Exception {
    /* we not want a mock of resource mgr delegate */
    final ApplicationClientProtocol clientRMProtocol = mock(ApplicationClientProtocol.class);
    ResourceMgrDelegate delegate = new ResourceMgrDelegate(conf) {
      @Override
      protected void serviceStart() throws Exception {
        assertTrue(this.client instanceof YarnClientImpl);
        ((YarnClientImpl) this.client).setRMClient(clientRMProtocol);
      }
    };
    /* make sure kill calls finish application master */
    when(clientRMProtocol.forceKillApplication(any(KillApplicationRequest.class)))
    .thenReturn(KillApplicationResponse.newInstance(true));
    delegate.killApplication(appId);
    verify(clientRMProtocol).forceKillApplication(any(KillApplicationRequest.class));

    /* make sure getalljobs calls get all applications */
    when(clientRMProtocol.getApplications(any(GetApplicationsRequest.class))).
    thenReturn(recordFactory.newRecordInstance(GetApplicationsResponse.class));
    delegate.getAllJobs();
    verify(clientRMProtocol).getApplications(any(GetApplicationsRequest.class));

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

  @Test(timeout=20000)
  public void testGetHSDelegationToken() throws Exception {
    try {
      Configuration conf = new Configuration();

      // Setup mock service
      InetSocketAddress mockRmAddress = new InetSocketAddress("localhost", 4444);
      Text rmTokenSevice = SecurityUtil.buildTokenService(mockRmAddress);

      InetSocketAddress mockHsAddress = new InetSocketAddress("localhost", 9200);
      Text hsTokenSevice = SecurityUtil.buildTokenService(mockHsAddress);

      // Setup mock rm token
      RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(
          new Text("owner"), new Text("renewer"), new Text("real"));
      Token<RMDelegationTokenIdentifier> token = new Token<RMDelegationTokenIdentifier>(
          new byte[0], new byte[0], tokenIdentifier.getKind(), rmTokenSevice);
      token.setKind(RMDelegationTokenIdentifier.KIND_NAME);

      // Setup mock history token
      org.apache.hadoop.yarn.api.records.Token historyToken =
          org.apache.hadoop.yarn.api.records.Token.newInstance(new byte[0],
            MRDelegationTokenIdentifier.KIND_NAME.toString(), new byte[0],
            hsTokenSevice.toString());
      GetDelegationTokenResponse getDtResponse =
          Records.newRecord(GetDelegationTokenResponse.class);
      getDtResponse.setDelegationToken(historyToken);

      // mock services
      MRClientProtocol mockHsProxy = mock(MRClientProtocol.class);
      doReturn(mockHsAddress).when(mockHsProxy).getConnectAddress();
      doReturn(getDtResponse).when(mockHsProxy).getDelegationToken(
          any(GetDelegationTokenRequest.class));

      ResourceMgrDelegate rmDelegate = mock(ResourceMgrDelegate.class);
      doReturn(rmTokenSevice).when(rmDelegate).getRMDelegationTokenService();

      ClientCache clientCache = mock(ClientCache.class);
      doReturn(mockHsProxy).when(clientCache).getInitializedHSProxy();

      Credentials creds = new Credentials();

      YARNRunner yarnRunner = new YARNRunner(conf, rmDelegate, clientCache);

      // No HS token if no RM token
      yarnRunner.addHistoryToken(creds);
      verify(mockHsProxy, times(0)).getDelegationToken(
          any(GetDelegationTokenRequest.class));

      // No HS token if RM token, but secirity disabled.
      creds.addToken(new Text("rmdt"), token);
      yarnRunner.addHistoryToken(creds);
      verify(mockHsProxy, times(0)).getDelegationToken(
          any(GetDelegationTokenRequest.class));

      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      UserGroupInformation.setConfiguration(conf);
      creds = new Credentials();

      // No HS token if no RM token, security enabled
      yarnRunner.addHistoryToken(creds);
      verify(mockHsProxy, times(0)).getDelegationToken(
          any(GetDelegationTokenRequest.class));

      // HS token if RM token present, security enabled
      creds.addToken(new Text("rmdt"), token);
      yarnRunner.addHistoryToken(creds);
      verify(mockHsProxy, times(1)).getDelegationToken(
          any(GetDelegationTokenRequest.class));

      // No additional call to get HS token if RM and HS token present
      yarnRunner.addHistoryToken(creds);
      verify(mockHsProxy, times(1)).getDelegationToken(
          any(GetDelegationTokenRequest.class));
    } finally {
      // Back to defaults.
      UserGroupInformation.setConfiguration(new Configuration());
    }
  }

  @Test(timeout=20000)
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

            org.apache.hadoop.yarn.api.records.Token token =
                recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.Token.class);
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

  @Test(timeout=20000)
  public void testAMAdminCommandOpts() throws Exception {
    JobConf jobConf = new JobConf();
    
    jobConf.set(MRJobConfig.MR_AM_ADMIN_COMMAND_OPTS, "-Djava.net.preferIPv4Stack=true");
    jobConf.set(MRJobConfig.MR_AM_COMMAND_OPTS, "-Xmx1024m");
    
    YARNRunner yarnRunner = new YARNRunner(jobConf);
    
    ApplicationSubmissionContext submissionContext =
        buildSubmitContext(yarnRunner, jobConf);
    
    ContainerLaunchContext containerSpec = submissionContext.getAMContainerSpec();
    List<String> commands = containerSpec.getCommands();
    
    int index = 0;
    int adminIndex = 0;
    int adminPos = -1;
    int userIndex = 0;
    int userPos = -1;
    int tmpDirPos = -1;

    for(String command : commands) {
      if(command != null) {
        assertFalse("Profiler should be disabled by default",
            command.contains(PROFILE_PARAMS));
        adminPos = command.indexOf("-Djava.net.preferIPv4Stack=true");
        if(adminPos >= 0)
          adminIndex = index;
        
        userPos = command.indexOf("-Xmx1024m");
        if(userPos >= 0)
          userIndex = index;

        tmpDirPos = command.indexOf("-Djava.io.tmpdir=");
      }
      
      index++;
    }

    // Check java.io.tmpdir opts are set in the commands
    assertTrue("java.io.tmpdir is not set for AM", tmpDirPos > 0);

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
  @Test(timeout=20000)
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
    
    @SuppressWarnings("unused")
    ApplicationSubmissionContext submissionContext =
        buildSubmitContext(yarnRunner, jobConf);
   
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

  @Test(timeout=20000)
  public void testAMProfiler() throws Exception {
    JobConf jobConf = new JobConf();

    jobConf.setBoolean(MRJobConfig.MR_AM_PROFILE, true);

    YARNRunner yarnRunner = new YARNRunner(jobConf);

    ApplicationSubmissionContext submissionContext =
        buildSubmitContext(yarnRunner, jobConf);

    ContainerLaunchContext containerSpec = submissionContext.getAMContainerSpec();
    List<String> commands = containerSpec.getCommands();

    for(String command : commands) {
      if (command != null) {
        if (command.contains(PROFILE_PARAMS)) {
          return;
        }
      }
    }
    throw new IllegalStateException("Profiler opts not found!");
  }

  @Test
  public void testAMStandardEnv() throws Exception {
    final String ADMIN_LIB_PATH = "foo";
    final String USER_LIB_PATH = "bar";
    final String USER_SHELL = "shell";
    JobConf jobConf = new JobConf();

    jobConf.set(MRJobConfig.MR_AM_ADMIN_USER_ENV, "LD_LIBRARY_PATH=" +
        ADMIN_LIB_PATH);
    jobConf.set(MRJobConfig.MR_AM_ENV, "LD_LIBRARY_PATH="
        + USER_LIB_PATH);
    jobConf.set(MRJobConfig.MAPRED_ADMIN_USER_SHELL, USER_SHELL);

    YARNRunner yarnRunner = new YARNRunner(jobConf);
    ApplicationSubmissionContext appSubCtx =
        buildSubmitContext(yarnRunner, jobConf);

    // make sure PWD is first in the lib path
    ContainerLaunchContext clc = appSubCtx.getAMContainerSpec();
    Map<String, String> env = clc.getEnvironment();
    String libPath = env.get(Environment.LD_LIBRARY_PATH.name());
    assertNotNull("LD_LIBRARY_PATH not set", libPath);
    String cps = jobConf.getBoolean(
        MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
        MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM)
        ? ApplicationConstants.CLASS_PATH_SEPARATOR : File.pathSeparator;
    assertEquals("Bad AM LD_LIBRARY_PATH setting",
        MRApps.crossPlatformifyMREnv(conf, Environment.PWD)
        + cps + ADMIN_LIB_PATH + cps + USER_LIB_PATH, libPath);

    // make sure SHELL is set
    String shell = env.get(Environment.SHELL.name());
    assertNotNull("SHELL not set", shell);
    assertEquals("Bad SHELL setting", USER_SHELL, shell);
  }

  private ApplicationSubmissionContext buildSubmitContext(
      YARNRunner yarnRunner, JobConf jobConf) throws IOException {
    File jobxml = new File(testWorkDir, MRJobConfig.JOB_CONF_FILE);
    OutputStream out = new FileOutputStream(jobxml);
    conf.writeXml(out);
    out.close();

    File jobsplit = new File(testWorkDir, MRJobConfig.JOB_SPLIT);
    out = new FileOutputStream(jobsplit);
    out.close();

    File jobsplitmetainfo = new File(testWorkDir,
        MRJobConfig.JOB_SPLIT_METAINFO);
    out = new FileOutputStream(jobsplitmetainfo);
    out.close();

    return yarnRunner.createApplicationSubmissionContext(jobConf,
        testWorkDir.toString(), new Credentials());
  }
}
