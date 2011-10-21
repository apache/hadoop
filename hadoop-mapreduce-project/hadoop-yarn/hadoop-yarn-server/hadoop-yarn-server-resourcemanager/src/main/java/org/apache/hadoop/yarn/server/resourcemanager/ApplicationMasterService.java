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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.AbstractService;

@Private
public class ApplicationMasterService extends AbstractService implements
    AMRMProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  private ApplicationTokenSecretManager appTokenManager;
  private InetSocketAddress masterServiceAddress;
  private Server server;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AMResponse> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AMResponse>();
  private final AMResponse reboot = recordFactory.newRecordInstance(AMResponse.class);
  private final RMContext rmContext;

  public ApplicationMasterService(RMContext rmContext,
      ApplicationTokenSecretManager appTokenManager, YarnScheduler scheduler) {
    super(ApplicationMasterService.class.getName());
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.appTokenManager = appTokenManager;
    this.rScheduler = scheduler;
    this.reboot.setReboot(true);
//    this.reboot.containers = new ArrayList<Container>();
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration conf) {
    String bindAddress =
      conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);
    masterServiceAddress =  NetUtils.createSocketAddr(bindAddress,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT,
      YarnConfiguration.RM_SCHEDULER_ADDRESS);
    super.init(conf);
  }

  @Override
  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
      rpc.getServer(AMRMProtocol.class, this, masterServiceAddress,
          conf, this.appTokenManager,
          conf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT, 
              YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
    this.server.start();
    super.start();
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnRemoteException {

    ApplicationAttemptId applicationAttemptId = request
        .getApplicationAttemptId();
    ApplicationId appID = applicationAttemptId.getApplicationId();
    AMResponse lastResponse = responseMap.get(applicationAttemptId);
    if (lastResponse == null) {
      String message = "Application doesn't exist in cache "
          + applicationAttemptId;
      LOG.error(message);
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, message, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throw RPCUtil.getRemoteException(message);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lastResponse) {

      LOG.info("AM registration " + applicationAttemptId);
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptRegistrationEvent(applicationAttemptId, request
              .getHost(), request.getRpcPort(), request.getTrackingUrl()));

      RMAuditLogger.logSuccess(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "ApplicationMasterService", appID,
          applicationAttemptId);

      // Pick up min/max resource from scheduler...
      RegisterApplicationMasterResponse response = recordFactory
          .newRecordInstance(RegisterApplicationMasterResponse.class);
      response.setMinimumResourceCapability(rScheduler
          .getMinimumResourceCapability());
      response.setMaximumResourceCapability(rScheduler
          .getMaximumResourceCapability());
      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnRemoteException {

    ApplicationAttemptId applicationAttemptId = request
        .getApplicationAttemptId();
    AMResponse lastResponse = responseMap.get(applicationAttemptId);
    if (lastResponse == null) {
      String message = "Application doesn't exist in cache "
          + applicationAttemptId;
      LOG.error(message);
      throw RPCUtil.getRemoteException(message);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lastResponse) {

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptUnregistrationEvent(applicationAttemptId, request
              .getTrackingUrl(), request.getFinalApplicationStatus(), request
              .getDiagnostics()));

      FinishApplicationMasterResponse response = recordFactory
          .newRecordInstance(FinishApplicationMasterResponse.class);
      return response;
    }
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnRemoteException {

    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponse allocateResponse = recordFactory
        .newRecordInstance(AllocateResponse.class);
    AMResponse lastResponse = responseMap.get(appAttemptId);
    if (lastResponse == null) {
      LOG.error("AppAttemptId doesnt exist in cache " + appAttemptId);
      allocateResponse.setAMResponse(reboot);
      return allocateResponse;
    }
    if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
      /* old heartbeat */
      allocateResponse.setAMResponse(lastResponse);
      return allocateResponse;
    } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
      LOG.error("Invalid responseid from appAttemptId " + appAttemptId);
      // Oh damn! Sending reboot isn't enough. RM state is corrupted. TODO:
      allocateResponse.setAMResponse(reboot);
      return allocateResponse;
    }

    // Allow only one thread in AM to do heartbeat at a time.
    synchronized (lastResponse) { // BUG TODO: Locking order is screwed.

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request
              .getProgress()));

      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();

      // Send new requests to appAttempt.
      Allocation allocation =
          this.rScheduler.allocate(appAttemptId, ask, release);

      RMApp app = this.rmContext.getRMApps().get(appAttemptId.getApplicationId());
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);

      AMResponse response = recordFactory.newRecordInstance(AMResponse.class);
      response.setAllocatedContainers(allocation.getContainers());
      response.setCompletedContainersStatuses(appAttempt
          .pullJustFinishedContainers());
      response.setResponseId(lastResponse.getResponseId() + 1);
      response.setAvailableResources(allocation.getResourceLimit());
      responseMap.put(appAttemptId, response);
      allocateResponse.setAMResponse(response);
      return allocateResponse;
    }
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AMResponse response = recordFactory.newRecordInstance(AMResponse.class);
    response.setResponseId(0);
    responseMap.put(attemptId, response);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    AMResponse lastResponse = responseMap.get(attemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        responseMap.remove(attemptId);
      }
    }
  }

  @Override
  public void stop() {
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }
}
