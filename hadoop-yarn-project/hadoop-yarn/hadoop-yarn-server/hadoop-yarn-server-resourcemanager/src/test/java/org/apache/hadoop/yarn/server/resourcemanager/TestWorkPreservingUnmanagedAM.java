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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.slf4j.event.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test UAM handling in RM.
 */
public class TestWorkPreservingUnmanagedAM
    extends ParameterizedSchedulerTestBase {

  private YarnConfiguration conf;

  public TestWorkPreservingUnmanagedAM(SchedulerType type) throws IOException {
    super(type);
  }

  @Before
  public void setup() {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  /**
   * Test UAM work preserving restart. When the keepContainersAcrossAttempt flag
   * is on, we allow UAM to directly register again and move on without getting
   * the applicationAlreadyRegistered exception.
   */
  protected void testUAMRestart(boolean keepContainers) throws Exception {
    // start RM
    MockRM rm = new MockRM();
    rm.start();
    MockNM nm =
        new MockNM("127.0.0.1:1234", 15120, rm.getResourceTrackerService());
    nm.registerNode();

    // create app and launch the UAM
    boolean unamanged = true;
    int maxAttempts = 1;
    boolean waitForAccepted = true;
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(200, rm)
        .withAppName("")
        .withUser(UserGroupInformation.getCurrentUser().getShortUserName())
        .withAcls(null)
        .withUnmanagedAM(unamanged)
        .withQueue(null)
        .withMaxAppAttempts(maxAttempts)
        .withCredentials(null)
        .withAppType(null)
        .withWaitForAppAcceptedState(waitForAccepted)
        .withKeepContainers(keepContainers)
        .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);

    MockAM am = MockRM.launchUAM(app, rm, nm);

    // Register for the first time
    am.registerAppAttempt();

    // Allocate two containers to UAM
    int numContainers = 3;
    List<Container> conts = am.allocate("127.0.0.1", 1000, numContainers,
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(100);
    }

    // Release one container
    List<ContainerId> releaseList =
        Collections.singletonList(conts.get(0).getId());
    List<ContainerStatus> finishedConts =
        am.allocate(new ArrayList<ResourceRequest>(), releaseList)
            .getCompletedContainersStatuses();
    while (finishedConts.size() < releaseList.size()) {
      nm.nodeHeartbeat(true);
      finishedConts
          .addAll(am
              .allocate(new ArrayList<ResourceRequest>(),
                  new ArrayList<ContainerId>())
              .getCompletedContainersStatuses());
      Thread.sleep(100);
    }

    // Register for the second time
    RegisterApplicationMasterResponse response = null;
    try {
      response = am.registerAppAttempt(false);
    } catch (InvalidApplicationMasterRequestException e) {
      Assert.assertEquals(false, keepContainers);
      return;
    }
    Assert.assertEquals("RM should not allow second register"
        + " for UAM without keep container flag ", true, keepContainers);

    // Expecting the two running containers previously
    Assert.assertEquals(2, response.getContainersFromPreviousAttempts().size());
    Assert.assertEquals(1, response.getNMTokensFromPreviousAttempts().size());

    // Allocate one more containers to UAM, just to be safe
    numContainers = 1;
    am.allocate("127.0.0.1", 1000, numContainers, new ArrayList<ContainerId>());
    nm.nodeHeartbeat(true);
    conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() < numContainers) {
      nm.nodeHeartbeat(true);
      conts.addAll(am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(100);
    }

    rm.stop();
  }

  @Test(timeout = 600000)
  public void testUAMRestartKeepContainers() throws Exception {
    testUAMRestart(true);
  }

  @Test(timeout = 600000)
  public void testUAMRestartNoKeepContainers() throws Exception {
    testUAMRestart(false);
  }

}
