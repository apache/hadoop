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

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

public class MockAM {

  private static final Logger LOG = Logger.getLogger(MockAM.class);

  private volatile int responseId = 0;
  private final ApplicationAttemptId attemptId;
  private RMContext context;
  private ApplicationMasterProtocol amRMProtocol;
  private UserGroupInformation ugi;
  private volatile AllocateResponse lastResponse;

  private final List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
  private final List<ContainerId> releases = new ArrayList<ContainerId>();

  public MockAM(RMContext context, ApplicationMasterProtocol amRMProtocol,
      ApplicationAttemptId attemptId) {
    this.context = context;
    this.amRMProtocol = amRMProtocol;
    this.attemptId = attemptId;
  }

  public void setAMRMProtocol(ApplicationMasterProtocol amRMProtocol,
      RMContext context) {
    this.context = context;
    this.amRMProtocol = amRMProtocol;
  }

  /**
   * Wait until an attempt has reached a specified state.
   * The timeout is 40 seconds.
   * @param finalState the attempt state waited
   * @throws InterruptedException
   *         if interrupted while waiting for the state transition
   */
  private void waitForState(RMAppAttemptState finalState)
      throws InterruptedException {
    RMApp app = context.getRMApps().get(attemptId.getApplicationId());
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    MockRM.waitForState(attempt, finalState);
  }

  public RegisterApplicationMasterResponse registerAppAttempt()
      throws Exception {
    return registerAppAttempt(true);
  }

  public RegisterApplicationMasterResponse registerAppAttempt(boolean wait)
      throws Exception {
    if (wait) {
      waitForState(RMAppAttemptState.LAUNCHED);
    }
    responseId = 0;
    final RegisterApplicationMasterRequest req =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    req.setHost("");
    req.setRpcPort(1);
    req.setTrackingUrl("");
    if (ugi == null) {
      ugi = UserGroupInformation.createRemoteUser(
          attemptId.toString());
      Token<AMRMTokenIdentifier> token =
          context.getRMApps().get(attemptId.getApplicationId())
              .getRMAppAttempt(attemptId).getAMRMToken();
      ugi.addTokenIdentifier(token.decodeIdentifier());
    }
    try {
      return ugi
        .doAs(
            new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
          @Override
          public RegisterApplicationMasterResponse run() throws Exception {
            return amRMProtocol.registerApplicationMaster(req);
          }
        });
    } catch (UndeclaredThrowableException e) {
      throw (Exception) e.getCause();
    }
  }

  public void addRequests(String[] hosts, int memory, int priority,
      int containers) throws Exception {
    addRequests(hosts, memory, priority, containers, 0L);
  }

  public void addRequests(String[] hosts, int memory, int priority,
      int containers, long allocationRequestId) throws Exception {
    requests.addAll(
        createReq(hosts, memory, priority, containers, allocationRequestId));
  }

  public AllocateResponse schedule() throws Exception {
    AllocateResponse response = allocate(requests, releases);
    requests.clear();
    releases.clear();
    return response;
  }

  public void addContainerToBeReleased(ContainerId containerId) {
    releases.add(containerId);
  }
  
  public AllocateResponse allocate(
      String host, int memory, int numContainers,
      List<ContainerId> releases) throws Exception {
    return allocate(host, memory, numContainers, releases, null);
  }
  
  public AllocateResponse allocate(
      String host, int memory, int numContainers,
      List<ContainerId> releases, String labelExpression) throws Exception {
    return allocate(host, memory, numContainers, 1, releases, labelExpression);
  }
  
  public AllocateResponse allocate(
      String host, int memory, int numContainers, int priority,
      List<ContainerId> releases, String labelExpression) throws Exception {
    List<ResourceRequest> reqs =
        createReq(new String[] { host }, memory, priority, numContainers,
            labelExpression, 0L);
    return allocate(reqs, releases);
  }
  
  public List<ResourceRequest> createReq(String[] hosts, int memory,
      int priority, int containers, long allocationRequestId) throws Exception {
    return createReq(hosts, memory, priority, containers, null,
        allocationRequestId);
  }

  public List<ResourceRequest> createReq(String[] hosts, int memory,
      int priority, int containers, String labelExpression,
      long allocationRequestId) throws Exception {
    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
    if (hosts != null) {
      for (String host : hosts) {
        // only add host/rack request when asked host isn't ANY
        if (!host.equals(ResourceRequest.ANY)) {
          ResourceRequest hostReq =
              createResourceReq(host, memory, priority, containers,
                  labelExpression);
          hostReq.setAllocationRequestId(allocationRequestId);
          reqs.add(hostReq);
          ResourceRequest rackReq =
              createResourceReq("/default-rack", memory, priority, containers,
                  labelExpression);
          rackReq.setAllocationRequestId(allocationRequestId);
          reqs.add(rackReq);
        }
      }
    }

    ResourceRequest offRackReq = createResourceReq(ResourceRequest.ANY, memory,
        priority, containers, labelExpression);
    offRackReq.setAllocationRequestId(allocationRequestId);
    reqs.add(offRackReq);
    return reqs;
  }
  
  public ResourceRequest createResourceReq(String resource, int memory, int priority,
      int containers) throws Exception {
    return createResourceReq(resource, memory, priority, containers, null);
  }

  public ResourceRequest createResourceReq(String resource, int memory,
      int priority, int containers, String labelExpression) throws Exception {
    return createResourceReq(resource, memory, priority, containers,
        labelExpression, ExecutionTypeRequest.newInstance());
  }

  public ResourceRequest createResourceReq(String resource, int memory,
      int priority, int containers, String labelExpression,
      ExecutionTypeRequest executionTypeRequest) throws Exception {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setResourceName(resource);
    req.setNumContainers(containers);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(priority);
    req.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemorySize(memory);
    req.setCapability(capability);
    if (labelExpression != null) {
      req.setNodeLabelExpression(labelExpression);
    }
    req.setExecutionTypeRequest(executionTypeRequest);
    return req;

  }

  public AllocateResponse allocate(
      List<ResourceRequest> resourceRequest, List<ContainerId> releases)
      throws Exception {
    final AllocateRequest req =
        AllocateRequest.newInstance(0, 0F, resourceRequest,
          releases, null);
    return allocate(req);
  }
  
  public AllocateResponse sendContainerResizingRequest(
      List<UpdateContainerRequest> updateRequests) throws Exception {
    final AllocateRequest req = AllocateRequest.newInstance(0, 0F, null, null,
        null, updateRequests);
    return allocate(req);
  }

  public AllocateResponse allocate(AllocateRequest allocateRequest)
            throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        context.getRMApps().get(attemptId.getApplicationId())
            .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    lastResponse = doAllocateAs(ugi, allocateRequest);
    return lastResponse;
  }

  public AllocateResponse doAllocateAs(UserGroupInformation ugi,
      final AllocateRequest req) throws Exception {
    req.setResponseId(++responseId);
    try {
      return ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {
        @Override
        public AllocateResponse run() throws Exception {
          return amRMProtocol.allocate(req);
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw (Exception) e.getCause();
    }
  }
  
  public AllocateResponse doHeartbeat() throws Exception {
    return allocate(null, null);
  }

  public void unregisterAppAttempt() throws Exception {
    waitForState(RMAppAttemptState.RUNNING);
    unregisterAppAttempt(true);
  }

  public void unregisterAppAttempt(boolean waitForStateRunning)
      throws Exception {
    final FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
            FinalApplicationStatus.SUCCEEDED, "", "");
    unregisterAppAttempt(req, waitForStateRunning);
  }

  public void unregisterAppAttempt(final FinishApplicationMasterRequest req,
      boolean waitForStateRunning) throws Exception {
    if (waitForStateRunning) {
      waitForState(RMAppAttemptState.RUNNING);
    }
    if (ugi == null) {
      ugi =  UserGroupInformation.createRemoteUser(attemptId.toString());
      Token<AMRMTokenIdentifier> token =
          context.getRMApps()
              .get(attemptId.getApplicationId())
              .getRMAppAttempt(attemptId).getAMRMToken();
      ugi.addTokenIdentifier(token.decodeIdentifier());
    }
    try {
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          amRMProtocol.finishApplicationMaster(req);
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw (Exception) e.getCause();
    }
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return this.attemptId;
  }

  public List<Container> allocateAndWaitForContainers(int nContainer,
      int memory, MockNM nm) throws Exception {
    return allocateAndWaitForContainers("ANY", nContainer, memory, nm);
  }

  public List<Container> allocateAndWaitForContainers(String host,
      int nContainer, int memory, MockNM nm) throws Exception {
    // AM request for containers
    allocate(host, memory, nContainer, null);
    // kick the scheduler
    nm.nodeHeartbeat(true);
    List<Container> conts = allocate(new ArrayList<ResourceRequest>(), null)
        .getAllocatedContainers();
    while (conts.size() < nContainer) {
      nm.nodeHeartbeat(true);
      conts.addAll(allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    return conts;
  }
}
