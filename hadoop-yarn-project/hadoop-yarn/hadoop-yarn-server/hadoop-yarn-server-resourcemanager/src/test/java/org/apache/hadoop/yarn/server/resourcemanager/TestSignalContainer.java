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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestSignalContainer {

  private static final Log LOG = LogFactory
    .getLog(TestSignalContainer.class);

  @Test
  public void testSignalRequestDeliveryToNM() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    FairScheduler fs = null;
    if (rm.getResourceScheduler().getClass() == FairScheduler.class) {
      fs = (FairScheduler)rm.getResourceScheduler();
    }
    rm.start();

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();

    //request for containers
    final int request = 2;
    am.allocate("h1" , 1000, request, new ArrayList<ContainerId>());

    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = new ArrayList<>(request);
    int waitCount = 0;
    while (conts.size() < request && waitCount++ < 200) {
      LOG.info("Got " + conts.size() + " containers. Waiting to get "
           + request);
      Thread.sleep(100);
      List<Container> allocation = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers();
      conts.addAll(allocation);
      if (fs != null) {
        nm1.nodeHeartbeat(true);
      }
    }
    Assert.assertEquals(request, conts.size());

    for(Container container : conts) {
      rm.signalToContainer(container.getId(),
          SignalContainerCommand.OUTPUT_THREAD_DUMP);
    }

    NodeHeartbeatResponse resp;
    List<SignalContainerRequest> contsToSignal;
    int signaledConts = 0;

    waitCount = 0;
    while ( signaledConts < request && waitCount++ < 200) {
      LOG.info("Waiting to get signalcontainer events.. signaledConts: "
          + signaledConts);
      resp = nm1.nodeHeartbeat(true);
      contsToSignal = resp.getContainersToSignalList();
      signaledConts += contsToSignal.size();
      Thread.sleep(100);
    }

    // Verify NM receives the expected number of signal container requests.
    Assert.assertEquals(request, signaledConts);

    am.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt.getAppAttemptId(), 1, ContainerState.COMPLETE);
    rm.waitForState(am.getApplicationAttemptId(), RMAppAttemptState.FINISHED);

    rm.stop();
  }
}