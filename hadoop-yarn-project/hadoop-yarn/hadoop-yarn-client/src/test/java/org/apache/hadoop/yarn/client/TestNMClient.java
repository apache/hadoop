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

package org.apache.hadoop.yarn.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNMClient {
  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;
  YarnClientImpl yarnClient = null;
  AMRMClientImpl<ContainerRequest> rmClient = null;
  NMClientImpl nmClient = null;
  List<NodeReport> nodeReports = null;
  ApplicationAttemptId attemptId = null;
  int nodeCount = 3;

  @Before
  public void setup() throws YarnException, IOException {
    // start minicluster
    conf = new YarnConfiguration();
    yarnCluster =
        new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    assertNotNull(yarnCluster);
    assertEquals(STATE.STARTED, yarnCluster.getServiceState());

    // start rm client
    yarnClient = new YarnClientImpl();
    yarnClient.init(conf);
    yarnClient.start();
    assertNotNull(yarnClient);
    assertEquals(STATE.STARTED, yarnClient.getServiceState());

    // get node info
    nodeReports = yarnClient.getNodeReports();

    // submit new app
    GetNewApplicationResponse newApp = yarnClient.getNewApplication();
    ApplicationId appId = newApp.getApplicationId();

    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);
    // set the application id
    appContext.setApplicationId(appId);
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Priority.newInstance(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    // unmanaged AM
    appContext.setUnmanagedAM(true);
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    int iterationsLeft = 30;
    while (iterationsLeft > 0) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        break;
      }
      sleep(1000);
      --iterationsLeft;
    }
    if (iterationsLeft == 0) {
      fail("Application hasn't bee started");
    }

    // start am rm client
    rmClient = new AMRMClientImpl<ContainerRequest>(attemptId);
    rmClient.init(conf);
    rmClient.start();
    assertNotNull(rmClient);
    assertEquals(STATE.STARTED, rmClient.getServiceState());

    // start am nm client
    nmClient = new NMClientImpl();
    nmClient.init(conf);
    nmClient.start();
    assertNotNull(nmClient);
    assertEquals(STATE.STARTED, nmClient.getServiceState());
  }

  @After
  public void tearDown() {
    rmClient.stop();
    yarnClient.stop();
    yarnCluster.stop();
  }

  private void stopNmClient(boolean stopContainers) {
    assertNotNull("Null nmClient", nmClient);
    // leave one unclosed
    assertEquals(1, nmClient.startedContainers.size());
    // default true
    assertTrue(nmClient.cleanupRunningContainers.get());
    nmClient.cleanupRunningContainersOnStop(stopContainers);
    assertEquals(stopContainers, nmClient.cleanupRunningContainers.get());
    nmClient.stop();
  }

  @Test (timeout = 60000)
  public void testNMClientNoCleanupOnStop()
      throws YarnException, IOException {

    rmClient.registerApplicationMaster("Host", 10000, "");

    testContainerManagement(nmClient, allocateContainers(rmClient, 5));

    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                                         null, null);
    // don't stop the running containers
    stopNmClient(false);
    assertFalse(nmClient.startedContainers. isEmpty());
    //now cleanup
    nmClient.cleanupRunningContainers();
    assertEquals(0, nmClient.startedContainers.size());
  }

  @Test (timeout = 60000)
  public void testNMClient()
      throws YarnException, IOException {

    rmClient.registerApplicationMaster("Host", 10000, "");

    testContainerManagement(nmClient, allocateContainers(rmClient, 5));

    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        null, null);
    // stop the running containers on close
    assertFalse(nmClient.startedContainers.isEmpty());
    nmClient.cleanupRunningContainersOnStop(true);
    assertTrue(nmClient.cleanupRunningContainers.get());
    nmClient.stop();
  }

  private Set<Container> allocateContainers(
      AMRMClientImpl<ContainerRequest> rmClient, int num)
      throws YarnException, IOException {
    // setup container request
    Resource capability = Resource.newInstance(1024, 0);
    Priority priority = Priority.newInstance(0);
    String node = nodeReports.get(0).getNodeId().getHost();
    String rack = nodeReports.get(0).getRackName();
    String[] nodes = new String[] {node};
    String[] racks = new String[] {rack};

    for (int i = 0; i < num; ++i) {
      rmClient.addContainerRequest(new ContainerRequest(capability, nodes,
          racks, priority, 1));
    }

    int containersRequestedAny = rmClient.remoteRequestsTable.get(priority)
        .get(ResourceRequest.ANY).get(capability).remoteRequest
        .getNumContainers();

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 2;
    Set<Container> containers = new TreeSet<Container>();
    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft > 0) {
      AllocateResponse allocResponse = rmClient.allocate(0.1f);

      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      for(Container container : allocResponse.getAllocatedContainers()) {
        containers.add(container);
      }
      if(allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(1000);
      }

      --iterationsLeft;
    }
    return containers;
  }

  private void testContainerManagement(NMClientImpl nmClient,
      Set<Container> containers) throws YarnException, IOException {
    int size = containers.size();
    int i = 0;
    for (Container container : containers) {
      // getContainerStatus shouldn't be called before startContainer,
      // otherwise, NodeManager cannot find the container
      try {
        nmClient.getContainerStatus(container.getId(), container.getNodeId(),
            container.getContainerToken());
        fail("Exception is expected");
      } catch (YarnException e) {
        assertTrue("The thrown exception is not expected",
            e.getMessage().contains("is not handled by this NodeManager"));
      }

      // stopContainer shouldn't be called before startContainer,
      // otherwise, an exception will be thrown
      try {
        nmClient.stopContainer(container.getId(), container.getNodeId(),
            container.getContainerToken());
        fail("Exception is expected");
      } catch (YarnException e) {
        if (!e.getMessage()
              .contains("is either not started yet or already stopped")) {
          throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e).initCause(
              e));
        }
      }

      Credentials ts = new Credentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens =
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      ContainerLaunchContext clc =
          Records.newRecord(ContainerLaunchContext.class);
      clc.setTokens(securityTokens);
      try {
        nmClient.startContainer(container, clc);
      } catch (YarnException e) {
        throw (AssertionError)
          (new AssertionError("Exception is not expected: " + e).initCause(e));
      }

      // leave one container unclosed
      if (++i < size) {
        // NodeManager may still need some time to make the container started
        testGetContainerStatus(container, i, ContainerState.RUNNING, "",
            -1000);

        try {
          nmClient.stopContainer(container.getId(), container.getNodeId(),
              container.getContainerToken());
        } catch (YarnException e) {
          throw (AssertionError)
            (new AssertionError("Exception is not expected: " + e)
               .initCause(e));
        }

        // getContainerStatus can be called after stopContainer
        testGetContainerStatus(container, i, ContainerState.COMPLETE,
            "Container killed by the ApplicationMaster.", 143);
      }
    }
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void testGetContainerStatus(Container container, int index,
      ContainerState state, String diagnostics, int exitStatus)
          throws YarnException, IOException {
    while (true) {
      try {
        ContainerStatus status = nmClient.getContainerStatus(
            container.getId(), container.getNodeId(),
                container.getContainerToken());
        // NodeManager may still need some time to get the stable
        // container status
        if (status.getState() == state) {
          assertEquals(container.getId(), status.getContainerId());
          assertTrue("" + index + ": " + status.getDiagnostics(),
              status.getDiagnostics().contains(diagnostics));
          assertEquals(exitStatus, status.getExitStatus());
          break;
        }
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
