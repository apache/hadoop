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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestApplicationMasterLauncher {

  private static final Log LOG = LogFactory
      .getLog(TestApplicationMasterLauncher.class);

  private static final class MyContainerManagerImpl implements
      ContainerManager {

    boolean launched = false;
    boolean cleanedup = false;
    String attemptIdAtContainerManager = null;
    String containerIdAtContainerManager = null;
    String nmHostAtContainerManager = null;
    int nmPortAtContainerManager;
    int nmHttpPortAtContainerManager;
    long submitTimeAtContainerManager;
    int maxAppAttempts;

    @Override
    public StartContainerResponse
        startContainer(StartContainerRequest request)
            throws YarnRemoteException {
      LOG.info("Container started by MyContainerManager: " + request);
      launched = true;
      Map<String, String> env =
          request.getContainerLaunchContext().getEnvironment();
      ContainerId containerId =
          request.getContainer().getId();
      containerIdAtContainerManager = containerId.toString();
      attemptIdAtContainerManager =
          containerId.getApplicationAttemptId().toString();
      nmHostAtContainerManager = request.getContainer().getNodeId().getHost();
      nmPortAtContainerManager =
          request.getContainer().getNodeId().getPort();
      nmHttpPortAtContainerManager =
          Integer.parseInt(request.getContainer().getNodeHttpAddress()
              .split(":")[1]);
      submitTimeAtContainerManager =
          Long.parseLong(env.get(ApplicationConstants.APP_SUBMIT_TIME_ENV));
      maxAppAttempts =
          Integer.parseInt(env.get(ApplicationConstants.MAX_APP_ATTEMPTS_ENV));
      return null;
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest request)
        throws YarnRemoteException {
      LOG.info("Container cleaned up by MyContainerManager");
      cleanedup = true;
      return null;
    }

    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest request) throws YarnRemoteException {
      return null;
    }

  }

  @Test
  public void testAMLaunchAndCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MyContainerManagerImpl containerManager = new MyContainerManagerImpl();
    MockRMWithCustomAMLauncher rm = new MockRMWithCustomAMLauncher(
        containerManager);
    rm.start();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);

    RMApp app = rm.submitApp(2000);

    // kick the scheduling
    nm1.nodeHeartbeat(true);

    int waitCount = 0;
    while (containerManager.launched == false && waitCount++ < 20) {
      LOG.info("Waiting for AM Launch to happen..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(containerManager.launched);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId appAttemptId = attempt.getAppAttemptId();
    Assert.assertEquals(appAttemptId.toString(),
        containerManager.attemptIdAtContainerManager);
    Assert.assertEquals(app.getSubmitTime(),
        containerManager.submitTimeAtContainerManager);
    Assert.assertEquals(app.getRMAppAttempt(appAttemptId)
        .getMasterContainer().getId()
        .toString(), containerManager.containerIdAtContainerManager);
    Assert.assertEquals(nm1.getNodeId().getHost(),
        containerManager.nmHostAtContainerManager);
    Assert.assertEquals(nm1.getNodeId().getPort(),
        containerManager.nmPortAtContainerManager);
    Assert.assertEquals(nm1.getHttpPort(),
        containerManager.nmHttpPortAtContainerManager);
    Assert.assertEquals(YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS,
        containerManager.maxAppAttempts);

    MockAM am = new MockAM(rm.getRMContext(), rm
        .getApplicationMasterService(), appAttemptId);
    am.registerAppAttempt();
    am.unregisterAppAttempt();

    //complete the AM container to finish the app normally
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am.waitForState(RMAppAttemptState.FINISHED);

    waitCount = 0;
    while (containerManager.cleanedup == false && waitCount++ < 20) {
      LOG.info("Waiting for AM Cleanup to happen..");
      Thread.sleep(1000);
    }
    Assert.assertTrue(containerManager.cleanedup);

    am.waitForState(RMAppAttemptState.FINISHED);
    rm.stop();
  }
}
