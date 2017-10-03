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

package org.apache.hadoop.yarn.sls.appmaster;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerWrapper;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public abstract class AMSimulator extends TaskRunner.Task {
  // resource manager
  protected ResourceManager rm;
  // main
  protected SLSRunner se;
  // application
  protected ApplicationId appId;
  protected ApplicationAttemptId appAttemptId;
  protected String oldAppId;    // jobId from the jobhistory file
  // record factory
  protected final static RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);
  // response queue
  protected final BlockingQueue<AllocateResponse> responseQueue;
  private int responseId = 0;
  // user name
  protected String user;  
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  private long baselineTimeMS;
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS;
  // whether tracked in Metrics
  protected boolean isTracked;
  // progress
  protected int totalContainers;
  protected int finishedContainers;

  // waiting for AM container
  volatile boolean isAMContainerRunning = false;
  volatile Container amContainer;
  
  private static final Logger LOG = LoggerFactory.getLogger(AMSimulator.class);

  private Resource amContainerResource;

  private ReservationSubmissionRequest reservationRequest;

  public AMSimulator() {
    this.responseQueue = new LinkedBlockingQueue<>();
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public void init(int heartbeatInterval,
      List<ContainerSimulator> containerList, ResourceManager resourceManager,
      SLSRunner slsRunnner, long startTime, long finishTime, String simUser,
      String simQueue, boolean tracked, String oldApp,
      ReservationSubmissionRequest rr, long baseTimeMS,
      Resource amContainerResource) {
    super.init(startTime, startTime + 1000000L * heartbeatInterval,
        heartbeatInterval);
    this.user = simUser;
    this.rm = resourceManager;
    this.se = slsRunnner;
    this.queue = simQueue;
    this.oldAppId = oldApp;
    this.isTracked = tracked;
    this.baselineTimeMS = baseTimeMS;
    this.traceStartTimeMS = startTime;
    this.traceFinishTimeMS = finishTime;
    this.reservationRequest = rr;
    this.amContainerResource = amContainerResource;
  }

  /**
   * register with RM
   */
  @Override
  public void firstStep() throws Exception {
    simulateStartTimeMS = System.currentTimeMillis() - baselineTimeMS;

    ReservationId reservationId = null;

    // submit a reservation if one is required, exceptions naturally happen
    // when the reservation does not fit, catch, log, and move on running job
    // without reservation.
    try {
      reservationId = submitReservationWhenSpecified();
    } catch (UndeclaredThrowableException y) {
      LOG.warn("Unable to place reservation: " + y.getMessage());
    }

    // submit application, waiting until ACCEPTED
    submitApp(reservationId);

    // track app metrics
    trackApp();
  }

  public synchronized void notifyAMContainerLaunched(Container masterContainer)
      throws Exception {
    this.amContainer = masterContainer;
    this.appAttemptId = masterContainer.getId().getApplicationAttemptId();
    registerAM();
    isAMContainerRunning = true;
  }

  private ReservationId submitReservationWhenSpecified()
      throws IOException, InterruptedException {
    if (reservationRequest != null) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws YarnException, IOException {
          rm.getClientRMService().submitReservation(reservationRequest);
          LOG.info("RESERVATION SUCCESSFULLY SUBMITTED "
              + reservationRequest.getReservationId());
          return null;

        }
      });
      return reservationRequest.getReservationId();
    } else {
      return null;
    }
  }

  @Override
  public void middleStep() throws Exception {
    if (isAMContainerRunning) {
      // process responses in the queue
      processResponseQueue();

      // send out request
      sendContainerRequest();

      // check whether finish
      checkStop();
    }
  }

  @Override
  public void lastStep() throws Exception {
    LOG.info("Application {} is shutting down.", appId);
    // unregister tracking
    if (isTracked) {
      untrackApp();
    }

    // Finish AM container
    if (amContainer != null) {
      LOG.info("AM container = {} reported to finish", amContainer.getId());
      se.getNmMap().get(amContainer.getNodeId()).cleanupContainer(
          amContainer.getId());
    } else {
      LOG.info("AM container is null");
    }

    if (null == appAttemptId) {
      // If appAttemptId == null, AM is not launched from RM's perspective, so
      // it's unnecessary to finish am as well
      return;
    }

    // unregister application master
    final FinishApplicationMasterRequest finishAMRequest = recordFactory
                  .newRecordInstance(FinishApplicationMasterRequest.class);
    finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps().get(appId)
        .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        rm.getApplicationMasterService()
            .finishApplicationMaster(finishAMRequest);
        return null;
      }
    });

    simulateFinishTimeMS = System.currentTimeMillis() - baselineTimeMS;
    // record job running information
    SchedulerMetrics schedulerMetrics =
            ((SchedulerWrapper)rm.getResourceScheduler()).getSchedulerMetrics();
    if (schedulerMetrics != null) {
      schedulerMetrics.addAMRuntime(appId, traceStartTimeMS, traceFinishTimeMS,
              simulateStartTimeMS, simulateFinishTimeMS);
    }
  }
  
  protected ResourceRequest createResourceRequest(
          Resource resource, String host, int priority, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }
  
  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
      List<ContainerId> toRelease) {
    AllocateRequest allocateRequest =
            recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setResponseId(responseId++);
    allocateRequest.setAskList(ask);
    allocateRequest.setReleaseList(toRelease);
    return allocateRequest;
  }
  
  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
    return createAllocateRequest(ask, new ArrayList<ContainerId>());
  }

  protected abstract void processResponseQueue() throws Exception;
  
  protected abstract void sendContainerRequest() throws Exception;
  
  protected abstract void checkStop();
  
  private void submitApp(ReservationId reservationId)
          throws YarnException, InterruptedException, IOException {
    // ask for new application
    GetNewApplicationRequest newAppRequest =
        Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse newAppResponse = 
        rm.getClientRMService().getNewApplication(newAppRequest);
    appId = newAppResponse.getApplicationId();
    
    // submit the application
    final SubmitApplicationRequest subAppRequest =
        Records.newRecord(SubmitApplicationRequest.class);
    ApplicationSubmissionContext appSubContext = 
        Records.newRecord(ApplicationSubmissionContext.class);
    appSubContext.setApplicationId(appId);
    appSubContext.setMaxAppAttempts(1);
    appSubContext.setQueue(queue);
    appSubContext.setPriority(Priority.newInstance(0));
    ContainerLaunchContext conLauContext = 
        Records.newRecord(ContainerLaunchContext.class);
    conLauContext.setApplicationACLs(new HashMap<>());
    conLauContext.setCommands(new ArrayList<>());
    conLauContext.setEnvironment(new HashMap<>());
    conLauContext.setLocalResources(new HashMap<>());
    conLauContext.setServiceData(new HashMap<>());
    appSubContext.setAMContainerSpec(conLauContext);
    appSubContext.setResource(amContainerResource);

    if(reservationId != null) {
      appSubContext.setReservationID(reservationId);
    }

    subAppRequest.setApplicationSubmissionContext(appSubContext);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws YarnException, IOException {
        rm.getClientRMService().submitApplication(subAppRequest);
        return null;
      }
    });
    LOG.info("Submit a new application {}", appId);
  }

  private void registerAM()
          throws YarnException, IOException, InterruptedException {
    // register application master
    final RegisterApplicationMasterRequest amRegisterRequest =
            Records.newRecord(RegisterApplicationMasterRequest.class);
    amRegisterRequest.setHost("localhost");
    amRegisterRequest.setRpcPort(1000);
    amRegisterRequest.setTrackingUrl("localhost:1000");

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps().get(appId)
        .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());

    ugi.doAs(
            new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
      @Override
      public RegisterApplicationMasterResponse run() throws Exception {
        return rm.getApplicationMasterService()
                .registerApplicationMaster(amRegisterRequest);
      }
    });

    LOG.info("Register the application master for application {}", appId);
  }

  private void trackApp() {
    if (isTracked) {
      SchedulerMetrics schedulerMetrics =
          ((SchedulerWrapper)rm.getResourceScheduler()).getSchedulerMetrics();
      if (schedulerMetrics != null) {
        schedulerMetrics.addTrackedApp(appId, oldAppId);
      }
    }
  }
  public void untrackApp() {
    if (isTracked) {
      SchedulerMetrics schedulerMetrics =
          ((SchedulerWrapper)rm.getResourceScheduler()).getSchedulerMetrics();
      if (schedulerMetrics != null) {
        schedulerMetrics.removeTrackedApp(oldAppId);
      }
    }
  }
  
  protected List<ResourceRequest> packageRequests(
          List<ContainerSimulator> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
    Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
    ResourceRequest anyRequest = null;
    for (ContainerSimulator cs : csList) {
      if (cs.getHostname() != null) {
        String[] rackHostNames = SLSUtils.getRackHostName(cs.getHostname());
        // check rack local
        String rackname = "/" + rackHostNames[0];
        if (rackLocalRequestMap.containsKey(rackname)) {
          rackLocalRequestMap.get(rackname).setNumContainers(
              rackLocalRequestMap.get(rackname).getNumContainers() + 1);
        } else {
          ResourceRequest request =
              createResourceRequest(cs.getResource(), rackname, priority, 1);
          rackLocalRequestMap.put(rackname, request);
        }
        // check node local
        String hostname = rackHostNames[1];
        if (nodeLocalRequestMap.containsKey(hostname)) {
          nodeLocalRequestMap.get(hostname).setNumContainers(
              nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
        } else {
          ResourceRequest request =
              createResourceRequest(cs.getResource(), hostname, priority, 1);
          nodeLocalRequestMap.put(hostname, request);
        }
      }
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(
                cs.getResource(), ResourceRequest.ANY, priority, 1);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.addAll(nodeLocalRequestMap.values());
    ask.addAll(rackLocalRequestMap.values());
    if (anyRequest != null) {
      ask.add(anyRequest);
    }
    return ask;
  }

  public String getQueue() {
    return queue;
  }
  public String getAMType() {
    return amtype;
  }
  public long getDuration() {
    return simulateFinishTimeMS - simulateStartTimeMS;
  }
  public int getNumTasks() {
    return totalContainers;
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptId;
  }
}
