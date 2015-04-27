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
package org.apache.hadoop.mapreduce.v2.app.local;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncrease;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

public class TestLocalContainerAllocator {

  @Test
  public void testRMConnectionRetry() throws Exception {
    // verify the connection exception is thrown
    // if we haven't exhausted the retry interval
    ApplicationMasterProtocol mockScheduler =
        mock(ApplicationMasterProtocol.class);
    when(mockScheduler.allocate(isA(AllocateRequest.class)))
      .thenThrow(RPCUtil.getRemoteException(new IOException("forcefail")));
    Configuration conf = new Configuration();
    LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(mockScheduler);
    lca.init(conf);
    lca.start();
    try {
      lca.heartbeat();
      Assert.fail("heartbeat was supposed to throw");
    } catch (YarnException e) {
      // YarnException is expected
    } finally {
      lca.stop();
    }

    // verify YarnRuntimeException is thrown when the retry interval has expired
    conf.setLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS, 0);
    lca = new StubbedLocalContainerAllocator(mockScheduler);
    lca.init(conf);
    lca.start();
    try {
      lca.heartbeat();
      Assert.fail("heartbeat was supposed to throw");
    } catch (YarnRuntimeException e) {
      // YarnRuntimeException is expected
    } finally {
      lca.stop();
    }
  }

  @Test
  public void testAllocResponseId() throws Exception {
    ApplicationMasterProtocol scheduler = new MockScheduler();
    Configuration conf = new Configuration();
    LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(scheduler);
    lca.init(conf);
    lca.start();

    // do two heartbeats to verify the response ID is being tracked
    lca.heartbeat();
    lca.heartbeat();
    lca.close();
  }

  @Test
  public void testAMRMTokenUpdate() throws Exception {
    Configuration conf = new Configuration();
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1, 1), 1);
    AMRMTokenIdentifier oldTokenId = new AMRMTokenIdentifier(attemptId, 1);
    AMRMTokenIdentifier newTokenId = new AMRMTokenIdentifier(attemptId, 2);
    Token<AMRMTokenIdentifier> oldToken = new Token<AMRMTokenIdentifier>(
        oldTokenId.getBytes(), "oldpassword".getBytes(), oldTokenId.getKind(),
        new Text());
    Token<AMRMTokenIdentifier> newToken = new Token<AMRMTokenIdentifier>(
        newTokenId.getBytes(), "newpassword".getBytes(), newTokenId.getKind(),
        new Text());

    MockScheduler scheduler = new MockScheduler();
    scheduler.amToken = newToken;

    final LocalContainerAllocator lca =
        new StubbedLocalContainerAllocator(scheduler);
    lca.init(conf);
    lca.start();

    UserGroupInformation testUgi = UserGroupInformation.createUserForTesting(
        "someuser", new String[0]);
    testUgi.addToken(oldToken);
    testUgi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            lca.heartbeat();
            return null;
          }
    });
    lca.close();

    // verify there is only one AMRM token in the UGI and it matches the
    // updated token from the RM
    int tokenCount = 0;
    Token<? extends TokenIdentifier> ugiToken = null;
    for (Token<? extends TokenIdentifier> token : testUgi.getTokens()) {
      if (AMRMTokenIdentifier.KIND_NAME.equals(token.getKind())) {
        ugiToken = token;
        ++tokenCount;
      }
    }

    Assert.assertEquals("too many AMRM tokens", 1, tokenCount);
    Assert.assertArrayEquals("token identifier not updated",
        newToken.getIdentifier(), ugiToken.getIdentifier());
    Assert.assertArrayEquals("token password not updated",
        newToken.getPassword(), ugiToken.getPassword());
    Assert.assertEquals("AMRM token service not updated",
        new Text(ClientRMProxy.getAMRMTokenService(conf)),
        ugiToken.getService());
  }

  private static class StubbedLocalContainerAllocator
    extends LocalContainerAllocator {
    private ApplicationMasterProtocol scheduler;

    public StubbedLocalContainerAllocator(ApplicationMasterProtocol scheduler) {
      super(mock(ClientService.class), createAppContext(),
          "nmhost", 1, 2, null);
      this.scheduler = scheduler;
    }

    @Override
    protected void register() {
    }

    @Override
    protected void unregister() {
    }

    @Override
    protected void startAllocatorThread() {
      allocatorThread = new Thread();
    }

    @Override
    protected ApplicationMasterProtocol createSchedulerProxy() {
      return scheduler;
    }

    private static AppContext createAppContext() {
      ApplicationId appId = ApplicationId.newInstance(1, 1);
      ApplicationAttemptId attemptId =
          ApplicationAttemptId.newInstance(appId, 1);
      Job job = mock(Job.class);
      @SuppressWarnings("rawtypes")
      EventHandler eventHandler = mock(EventHandler.class);
      AppContext ctx = mock(AppContext.class);
      when(ctx.getApplicationID()).thenReturn(appId);
      when(ctx.getApplicationAttemptId()).thenReturn(attemptId);
      when(ctx.getJob(isA(JobId.class))).thenReturn(job);
      when(ctx.getClusterInfo()).thenReturn(
        new ClusterInfo(Resource.newInstance(10240, 1)));
      when(ctx.getEventHandler()).thenReturn(eventHandler);
      return ctx;
    }
  }

  private static class MockScheduler implements ApplicationMasterProtocol {
    int responseId = 0;
    Token<AMRMTokenIdentifier> amToken = null;

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      Assert.assertEquals("response ID mismatch",
          responseId, request.getResponseId());
      ++responseId;
      org.apache.hadoop.yarn.api.records.Token yarnToken = null;
      if (amToken != null) {
        yarnToken = org.apache.hadoop.yarn.api.records.Token.newInstance(
            amToken.getIdentifier(), amToken.getKind().toString(),
            amToken.getPassword(), amToken.getService().toString());
      }
      return AllocateResponse.newInstance(responseId,
          Collections.<ContainerStatus>emptyList(),
          Collections.<Container>emptyList(),
          Collections.<NodeReport>emptyList(),
          Resources.none(), null, 1, null,
          Collections.<NMToken>emptyList(),
          yarnToken,
          Collections.<ContainerResourceIncrease>emptyList(),
          Collections.<ContainerResourceDecrease>emptyList());
    }
  }
}
