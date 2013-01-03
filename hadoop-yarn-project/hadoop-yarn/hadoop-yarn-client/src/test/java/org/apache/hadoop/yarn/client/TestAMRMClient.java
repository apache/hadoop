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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
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
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.Records;

public class TestAMRMClient {
  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;
  YarnClientImpl yarnClient = null;
  List<NodeReport> nodeReports = null;
  ApplicationAttemptId attemptId = null;
  int nodeCount = 3;
  
  @Before
  public void setup() throws YarnRemoteException {
    // start minicluster
    conf = new YarnConfiguration();
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = new YarnClientImpl();
    yarnClient.init(conf);
    yarnClient.start();

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
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
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
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        break;
      }
    }
  }
  
  @After
  public void tearDown() {
    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }

  @Test (timeout=60000)
  public void testAMRMClient() throws YarnRemoteException {
    AMRMClientImpl amClient = null;
    try {
      // start am rm client
      amClient = new AMRMClientImpl(attemptId);
      amClient.init(conf);
      amClient.start();

      amClient.registerApplicationMaster("Host", 10000, "");

      testAllocation(amClient);

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }
  
  
  private void testAllocation(final AMRMClientImpl amClient)  
      throws YarnRemoteException {
    // setup container request
    final Resource capability = Records.newRecord(Resource.class);
    final Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);
    capability.setMemory(1024);
    String node = nodeReports.get(0).getNodeId().getHost();
    String rack = nodeReports.get(0).getRackName();
    final String[] nodes = { node };
    final String[] racks = { rack };
    
    assertTrue(amClient.ask.size() == 0);
    assertTrue(amClient.release.size() == 0);
    
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority, 1));
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority, 3));
    amClient.removeContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority, 2));
    
    int containersRequestedNode = amClient.remoteRequestsTable.get(priority)
        .get(node).get(capability).getNumContainers();
    int containersRequestedRack = amClient.remoteRequestsTable.get(priority)
        .get(rack).get(capability).getNumContainers();
    int containersRequestedAny = amClient.remoteRequestsTable.get(priority)
        .get(AMRMClient.ANY).get(capability).getNumContainers();

    assertTrue(containersRequestedNode == 2);
    assertTrue(containersRequestedRack == 2);
    assertTrue(containersRequestedAny == 2);
    assertTrue(amClient.ask.size() == 3);
    assertTrue(amClient.release.size() == 0);

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    int iterationsLeft = 2;
    Set<ContainerId> releases = new TreeSet<ContainerId>();
    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertTrue(amClient.ask.size() == 0);
      assertTrue(amClient.release.size() == 0);
      
      assertTrue(nodeCount == amClient.getClusterNodeCount());
      AMResponse amResponse = allocResponse.getAMResponse();
      allocatedContainerCount += amResponse.getAllocatedContainers().size();
      for(Container container : amResponse.getAllocatedContainers()) {
        ContainerId rejectContainerId = container.getId();
        releases.add(rejectContainerId);
        amClient.releaseAssignedContainer(rejectContainerId);
      }
      if(allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(1000);
      }
    }

    assertTrue(allocatedContainerCount == containersRequestedAny);
    assertTrue(amClient.release.size() == 2);
    assertTrue(amClient.ask.size() == 0);
    
    // need to tell the AMRMClient that we dont need these resources anymore
    amClient.removeContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority, 2));
    assertTrue(amClient.ask.size() == 3);
    // send 0 container count request for resources that are no longer needed
    ResourceRequest snoopRequest = amClient.ask.iterator().next();
    assertTrue(snoopRequest.getNumContainers() == 0);
    
    // test RPC exception handling
    amClient.addContainerRequest(new ContainerRequest(capability, nodes,
        racks, priority, 2));
    snoopRequest = amClient.ask.iterator().next();
    assertTrue(snoopRequest.getNumContainers() == 2);
    
    AMRMProtocol realRM = amClient.rmClient;
    try {
      AMRMProtocol mockRM = mock(AMRMProtocol.class);
      when(mockRM.allocate(any(AllocateRequest.class))).thenAnswer(
          new Answer<AllocateResponse>() {
            public AllocateResponse answer(InvocationOnMock invocation)
                throws Exception {
              amClient.removeContainerRequest(new ContainerRequest(capability,
                  nodes, racks, priority, 2));
              throw new Exception();
            }
          });
      amClient.rmClient = mockRM;
      amClient.allocate(0.1f);
    }catch (Exception ioe) {}
    finally {
      amClient.rmClient = realRM;
    }

    assertTrue(amClient.release.size() == 2);
    assertTrue(amClient.ask.size() == 3);
    snoopRequest = amClient.ask.iterator().next();
    // verify that the remove request made in between makeRequest and allocate 
    // has not been lost
    assertTrue(snoopRequest.getNumContainers() == 0);
    
    iterationsLeft = 2;
    // do a few iterations to ensure RM is not going send new containers
    while(!releases.isEmpty() || iterationsLeft-- > 0) {
      // inform RM of rejection
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      AMResponse amResponse = allocResponse.getAMResponse();
      // RM did not send new containers because AM does not need any
      assertTrue(amResponse.getAllocatedContainers().size() == 0);
      if(amResponse.getCompletedContainersStatuses().size() > 0) {
        for(ContainerStatus cStatus : amResponse.getCompletedContainersStatuses()) {
          if(releases.contains(cStatus.getContainerId())) {
            assertTrue(cStatus.getState() == ContainerState.COMPLETE);
            assertTrue(cStatus.getExitStatus() == -100);
            releases.remove(cStatus.getContainerId());
          }
        }
      }
      if(iterationsLeft > 0) {
        // sleep to make sure NM's heartbeat
        sleep(1000);
      }
    }
    
    assertTrue(amClient.ask.size() == 0);
    assertTrue(amClient.release.size() == 0);
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
