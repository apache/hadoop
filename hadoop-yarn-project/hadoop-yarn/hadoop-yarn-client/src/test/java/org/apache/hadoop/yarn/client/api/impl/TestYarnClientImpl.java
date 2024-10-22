/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager
        .ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class is to test class {@link YarnClientImpl ).
 */
public class TestYarnClientImpl extends ParameterizedSchedulerTestBase {

  protected static final String YARN_RM = "yarn-rm@EXAMPLE.COM";

  public TestYarnClientImpl(SchedulerType type) throws IOException {
    super(type);
  }

  @Before
  public void setup() {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Test
  public void testStartWithTimelineV15() {
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.5f);
    YarnClientImpl client = (YarnClientImpl) YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    client.stop();
  }

  @Test
  public void testAsyncAPIPollTimeout() {
    testAsyncAPIPollTimeoutHelper(null, false);
    testAsyncAPIPollTimeoutHelper(0L, true);
    testAsyncAPIPollTimeoutHelper(1L, true);
  }

  private void testAsyncAPIPollTimeoutHelper(Long valueForTimeout,
          boolean expectedTimeoutEnforcement) {
    YarnClientImpl client = new YarnClientImpl();
    try {
      Configuration conf = getConf();
      if (valueForTimeout != null) {
        conf.setLong(
                YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS,
                valueForTimeout);
      }

      client.init(conf);

      Assert.assertEquals(
              expectedTimeoutEnforcement, client.enforceAsyncAPITimeout());
    } finally {
      IOUtils.closeStream(client);
    }
  }

  @Test
  public void testBestEffortTimelineDelegationToken()
          throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    conf.set(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE,
            KerberosAuthenticationHandler.TYPE);
    YarnClientImpl client = spy(new YarnClientImpl() {

      @Override
      TimelineClient createTimelineClient() throws IOException, YarnException {
        timelineClient = mock(TimelineClient.class);
        when(timelineClient.getDelegationToken(any()))
                .thenThrow(new RuntimeException("Best effort test exception"));
        return timelineClient;
      }
    });

    client.init(conf);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_CLIENT_BEST_EFFORT,
            true);
    client.serviceInit(conf);
    client.getTimelineDelegationToken();

    try {
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_CLIENT_BEST_EFFORT, false);
      client.serviceInit(conf);
      client.getTimelineDelegationToken();
      Assert.fail("Get delegation token should have thrown an exception");
    } catch (IOException e) {
      // Success
    }
  }

  // Validates if YarnClientImpl automatically adds HDFS Delegation
  // token for Log Aggregation Path in a cluster setup with fs.DefaultFS
  // set to LocalFileSystem and Log Aggregation Path set to HDFS.
  @Test
  public void testAutomaitcLogAggregationDelegationToken()
      throws Exception {
    Configuration conf = getConf();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    conf.set(YarnConfiguration.RM_PRINCIPAL, YARN_RM);
    String remoteRootLogPath = "/tmp/app-logs";

    MiniDFSCluster hdfsCluster = null;
    try {
      // Step 1: Start a MiniDFSCluster for Log Aggregation Path
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(1).build();

      Path remoteRootLogDir = new Path(remoteRootLogPath);

      FileSystem fs = hdfsCluster.getFileSystem();
      fs.mkdirs(remoteRootLogDir);
      conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
          fs.getFileStatus(remoteRootLogDir).getPath().toString());

      // Step 2: Prepare a Mock FileSystem which returns Delegation Token
      // when YarnClientImpl invokes
      DelegationTokenIdentifier hdfsDT = new DelegationTokenIdentifier(new Text(
          "test"), new Text(YARN_RM), null);
      final Token<DelegationTokenIdentifier> dToken =
          new Token<>(hdfsDT.getBytes(), new byte[0], hdfsDT.getKind(),
          new Text());

      FileSystem mockFs = mock(FileSystem.class);
      doAnswer(new Answer<Token<?>[]>() {
        @Override
        public Token<?>[] answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          ((Credentials) args[1]).addToken(hdfsDT.getKind(), dToken);
          return new Token[]{dToken};
        }
      }).when(mockFs).addDelegationTokens(any(), any());

      FileSystemTestHelper.addFileSystemForTesting(fs.getUri(),
          hdfsConfig, mockFs);

      // Step 3: Prepare a Mock YarnClientImpl
      YarnClientImpl client = spy(new YarnClientImpl() {

        @Override
        protected void serviceStart() {
          rmClient = mock(ApplicationClientProtocol.class);
        }

        @Override
        protected void serviceStop() {
        }

        @Override
        public ApplicationReport getApplicationReport(ApplicationId appId) {
          ApplicationReport report = mock(ApplicationReport.class);
          when(report.getYarnApplicationState())
              .thenReturn(YarnApplicationState.RUNNING);
          return report;
        }

        @Override
        public boolean isSecurityEnabled() {
          return true;
        }
      });

      client.init(conf);
      client.start();

      // Step 4: Prepare a ApplicationSubmissionContext and submit the app
      ApplicationSubmissionContext context =
          mock(ApplicationSubmissionContext.class);
      ApplicationId applicationId = ApplicationId.newInstance(0, 1);
      when(context.getApplicationId()).thenReturn(applicationId);

      DataOutputBuffer dob = new DataOutputBuffer();
      Credentials credentials = new Credentials();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

      ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
          null, null, null, null, tokens, null);
      when(context.getAMContainerSpec()).thenReturn(clc);

      client.submitApplication(context);

      // Step 5: Verify automatic addition of HDFS DT for log aggregation path
      credentials = new Credentials();
      DataInputByteBuffer dibb = new DataInputByteBuffer();
      tokens = clc.getTokens();
      if (tokens != null) {
        dibb.reset(tokens);
        credentials.readTokenStorageStream(dibb);
        tokens.rewind();
      }
      Collection<Token<? extends TokenIdentifier>> dTokens =
           credentials.getAllTokens();
      Assert.assertEquals("Failed to place token for Log Aggregation Path",
          1, dTokens.size());
      Assert.assertEquals("Wrong Token for Log Aggregation",
          hdfsDT.getKind(), dTokens.iterator().next().getKind());

    } finally {
      if (hdfsCluster != null) {
        hdfsCluster.shutdown();
      }
    }
  }

  @Test
  public void testAutomaticTimelineDelegationTokenLoading()
          throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    conf.set(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE,
            KerberosAuthenticationHandler.TYPE);
    TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier();
    final Token<TimelineDelegationTokenIdentifier> dToken =
            new Token<>(
                    timelineDT.getBytes(), new byte[0], timelineDT.getKind(), new Text());
    // create a mock client
    YarnClientImpl client = spy(new YarnClientImpl() {

      @Override
      TimelineClient createTimelineClient() throws IOException, YarnException {
        timelineClient = mock(TimelineClient.class);
        when(timelineClient.getDelegationToken(any())).thenReturn(dToken);
        return timelineClient;
      }


      @Override
      protected void serviceStart() {
        rmClient = mock(ApplicationClientProtocol.class);
      }

      @Override
      protected void serviceStop() {
      }

      @Override
      public ApplicationReport getApplicationReport(ApplicationId appId) {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getYarnApplicationState())
                .thenReturn(YarnApplicationState.RUNNING);
        return report;
      }

      @Override
      public boolean isSecurityEnabled() {
        return true;
      }
    });
    client.init(conf);
    client.start();
    try {
      // when i == 0, timeline DT already exists, no need to get one more
      // when i == 1, timeline DT doesn't exist, need to get one more
      for (int i = 0; i < 2; ++i) {
        ApplicationSubmissionContext context =
                mock(ApplicationSubmissionContext.class);
        ApplicationId applicationId = ApplicationId.newInstance(0, i + 1);
        when(context.getApplicationId()).thenReturn(applicationId);
        DataOutputBuffer dob = new DataOutputBuffer();
        Credentials credentials = new Credentials();
        if (i == 0) {
          credentials.addToken(client.timelineService, dToken);
        }
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
                null, null, null, null, tokens, null);
        when(context.getAMContainerSpec()).thenReturn(clc);
        client.submitApplication(context);
        if (i == 0) {
          // GetTimelineDelegationToken shouldn't be called
          verify(client, never()).getTimelineDelegationToken();
        }
        // In either way, token should be there
        credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        tokens = clc.getTokens();
        if (tokens != null) {
          dibb.reset(tokens);
          credentials.readTokenStorageStream(dibb);
          tokens.rewind();
        }
        Collection<Token<? extends TokenIdentifier>> dTokens =
                credentials.getAllTokens();
        Assert.assertEquals(1, dTokens.size());
        Assert.assertEquals(dToken, dTokens.iterator().next());
      }
    } finally {
      client.stop();
    }
  }

  @Test
  public void testParseTimelineDelegationTokenRenewer() {
    // Client side
    YarnClientImpl client = (YarnClientImpl) YarnClient.createYarnClient();
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.set(YarnConfiguration.RM_PRINCIPAL, "rm/_HOST@EXAMPLE.COM");
    conf.set(
            YarnConfiguration.RM_ADDRESS, "localhost:8188");
    try {
      client.init(conf);
      client.start();
      Assert.assertEquals("rm/localhost@EXAMPLE.COM", client.timelineDTRenewer);
    } finally {
      client.stop();
    }
  }

  @Test
  public void testNMEnvWhitelistExportEnabled() {
    Configuration conf = getConf();
    conf.set(YarnConfiguration.NM_ENV_WHITELIST_EXPORT_ENABLED, "true");
    conf.set(YarnConfiguration.NM_ENV_WHITELIST, "TEST_KEY_1");

    // Set property for TEST_KEY_1
    System.setProperty("TEST_KEY_1", "TEST_VALUE_1");
    System.setProperty("TEST_KEY_2", "TEST_VALUE_2");

    // Prepare a Mock YarnClientImpl
    YarnClientImpl client = spy(new YarnClientImpl() {

      @Override
      protected void serviceStart() {
        rmClient = mock(ApplicationClientProtocol.class);
      }

      @Override
      protected void serviceStop() {
      }

      @Override
      public ApplicationReport getApplicationReport(ApplicationId appId) {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getYarnApplicationState())
            .thenReturn(YarnApplicationState.RUNNING);
        return report;
      }
    });
    conf.set(
        YarnConfiguration.RM_ADDRESS, "localhost:8188");

    // Prepare a ApplicationSubmissionContext and submit the app
    ApplicationSubmissionContext context =
        mock(ApplicationSubmissionContext.class);
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    when(context.getApplicationId()).thenReturn(applicationId);

    Map<String, String> env = new HashMap<>();
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        null, env, null, null, null, null);
    when(context.getAMContainerSpec()).thenReturn(clc);

    try {
      client.init(conf);
      client.start();
      client.submitApplication(context);
      Assert.assertEquals("TEST_VALUE_1",
          context.getAMContainerSpec().getEnvironment().get("TEST_KEY_1"));
      Assert.assertNotEquals("TEST_VALUE_2",
          context.getAMContainerSpec().getEnvironment().get("TEST_KEY_2"));
    } catch (YarnException | IOException e) {
      e.printStackTrace();
    } finally {
      client.stop();
    }
  }
}
