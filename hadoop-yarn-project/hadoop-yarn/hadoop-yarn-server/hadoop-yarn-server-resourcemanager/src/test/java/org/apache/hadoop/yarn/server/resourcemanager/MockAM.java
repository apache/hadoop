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

import org.junit.Assert;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

public class MockAM {

  private volatile int responseId = 0;
  private final ApplicationAttemptId attemptId;
  private final RMContext context;
  private ApplicationMasterProtocol amRMProtocol;

  private final List<ResourceRequest> requests = new ArrayList<ResourceRequest>();
  private final List<ContainerId> releases = new ArrayList<ContainerId>();

  public MockAM(RMContext context, ApplicationMasterProtocol amRMProtocol,
      ApplicationAttemptId attemptId) {
    this.context = context;
    this.amRMProtocol = amRMProtocol;
    this.attemptId = attemptId;
  }
  
  void setAMRMProtocol(ApplicationMasterProtocol amRMProtocol) {
    this.amRMProtocol = amRMProtocol;
  }

  public void waitForState(RMAppAttemptState finalState) throws Exception {
    RMApp app = context.getRMApps().get(attemptId.getApplicationId());
    RMAppAttempt attempt = app.getRMAppAttempt(attemptId);
    int timeoutSecs = 0;
    while (!finalState.equals(attempt.getAppAttemptState())
        && timeoutSecs++ < 40) {
      System.out
          .println("AppAttempt : " + attemptId + " State is : " 
              + attempt.getAppAttemptState()
              + " Waiting for state : " + finalState);
      Thread.sleep(1000);
    }
    System.out.println("AppAttempt State is : " + attempt.getAppAttemptState());
    Assert.assertEquals("AppAttempt state is not correct (timedout)",
        finalState, attempt.getAppAttemptState());
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
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        context.getRMApps().get(attemptId.getApplicationId())
          .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    try {
      return ugi
        .doAs(new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
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
    requests.addAll(createReq(hosts, memory, priority, containers));
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
    List<ResourceRequest> reqs = createReq(new String[]{host}, memory, 1, numContainers);
    return allocate(reqs, releases);
  }

  public List<ResourceRequest> createReq(String[] hosts, int memory, int priority,
      int containers) throws Exception {
    List<ResourceRequest> reqs = new ArrayList<ResourceRequest>();
    for (String host : hosts) {
      ResourceRequest hostReq = createResourceReq(host, memory, priority,
          containers);
      reqs.add(hostReq);
      ResourceRequest rackReq = createResourceReq("/default-rack", memory,
          priority, containers);
      reqs.add(rackReq);
    }

    ResourceRequest offRackReq = createResourceReq(ResourceRequest.ANY, memory,
        priority, containers);
    reqs.add(offRackReq);
    return reqs;

  }

  public ResourceRequest createResourceReq(String resource, int memory, int priority,
      int containers) throws Exception {
    ResourceRequest req = Records.newRecord(ResourceRequest.class);
    req.setResourceName(resource);
    req.setNumContainers(containers);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(priority);
    req.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    req.setCapability(capability);
    return req;
  }

  public AllocateResponse allocate(
      List<ResourceRequest> resourceRequest, List<ContainerId> releases)
      throws Exception {
    final AllocateRequest req =
        AllocateRequest.newInstance(++responseId, 0F, resourceRequest,
          releases, null);
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        context.getRMApps().get(attemptId.getApplicationId())
          .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
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

  public AllocateResponse allocate(AllocateRequest allocateRequest)
            throws Exception {
    final AllocateRequest req = allocateRequest;
    req.setResponseId(++responseId);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        context.getRMApps().get(attemptId.getApplicationId())
            .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
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

  public void unregisterAppAttempt() throws Exception {
    waitForState(RMAppAttemptState.RUNNING);
    final FinishApplicationMasterRequest req =
        FinishApplicationMasterRequest.newInstance(
          FinalApplicationStatus.SUCCEEDED, "", "");
    unregisterAppAttempt(req,true);
  }

  public void unregisterAppAttempt(final FinishApplicationMasterRequest req,
      boolean waitForStateRunning) throws Exception {
    if (waitForStateRunning) {
      waitForState(RMAppAttemptState.RUNNING);
    }
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(attemptId.toString());
    Token<AMRMTokenIdentifier> token =
        context.getRMApps().get(attemptId.getApplicationId())
            .getRMAppAttempt(attemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        amRMProtocol.finishApplicationMaster(req);
        return null;
      }
    });
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return this.attemptId;
  }
}
