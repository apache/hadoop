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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
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
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.AMDefinition;
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
  private static final long FINISH_TIME_NOT_INITIALIZED = Long.MIN_VALUE;
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
  private String user;
  // nodelabel expression
  private String nodeLabelExpression;
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  private long baselineTimeMS;
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS = FINISH_TIME_NOT_INITIALIZED;
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

  private Map<ApplicationId, AMSimulator> appIdToAMSim;

  private Set<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

  public AMSimulator() {
    this.responseQueue = new LinkedBlockingQueue<>();
  }

  public void init(AMDefinition amDef, ResourceManager rm, SLSRunner slsRunner,
      boolean tracked, long baselineTimeMS, long heartbeatInterval,
      Map<ApplicationId, AMSimulator> appIdToAMSim) {
    long startTime = amDef.getJobStartTime();
    long endTime = startTime + 1000000L * heartbeatInterval;
    super.init(startTime, endTime, heartbeatInterval);

    this.user = amDef.getUser();
    this.queue = amDef.getQueue();
    this.oldAppId = amDef.getOldAppId();
    this.amContainerResource = amDef.getAmResource();
    this.nodeLabelExpression = amDef.getLabelExpression();
    this.traceStartTimeMS = amDef.getJobStartTime();
    this.traceFinishTimeMS = amDef.getJobFinishTime();
    this.rm = rm;
    this.se = slsRunner;
    this.isTracked = tracked;
    this.baselineTimeMS = baselineTimeMS;
    this.appIdToAMSim = appIdToAMSim;
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

    // add submitted app to mapping
    appIdToAMSim.put(appId, this);

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

  protected void setReservationRequest(ReservationSubmissionRequest rr){
    this.reservationRequest = rr;
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
    if (simulateFinishTimeMS != FINISH_TIME_NOT_INITIALIZED) {
      // The finish time is already recorded.
      // Different value from zero means lastStep was called before.
      // We want to prevent lastStep to be called more than once.
      // See YARN-10427 for more details.
      LOG.warn("Method AMSimulator#lastStep was already called. " +
          "Skipping execution of method for application: {}", appId);
      return;
    }

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

    // Clear runningApps for ranNodes of this app
    for (NodeId nodeId : ranNodes) {
      se.getNmMap().get(nodeId).finishApplication(getApplicationId());
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

  protected ResourceRequest createResourceRequest(Resource resource,
      ExecutionType executionType, String host, int priority, long
      allocationId, int numContainers) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    request.setExecutionTypeRequest(
        ExecutionTypeRequest.newInstance(executionType));
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    request.setAllocationRequestId(allocationId);
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

  public abstract void initReservation(
      ReservationId reservationId, long deadline, long now);

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
    if (nodeLabelExpression != null) {
      appSubContext.setNodeLabelExpression(nodeLabelExpression);
    }

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
    Map<Long, Map<String, ResourceRequest>> rackLocalRequests =
        new HashMap<>();
    Map<Long, Map<String, ResourceRequest>> nodeLocalRequests =
        new HashMap<>();
    Map<Long, ResourceRequest> anyRequests = new HashMap<>();
    for (ContainerSimulator cs : csList) {
      long allocationId = cs.getAllocationId();
      ResourceRequest anyRequest = anyRequests.get(allocationId);
      if (cs.getHostname() != null) {
        Map<String, ResourceRequest> rackLocalRequestMap;
        if (rackLocalRequests.containsKey(allocationId)) {
          rackLocalRequestMap = rackLocalRequests.get(allocationId);
        } else {
          rackLocalRequestMap = new HashMap<>();
          rackLocalRequests.put(allocationId, rackLocalRequestMap);
        }
        String[] rackHostNames = SLSUtils.getRackHostName(cs.getHostname());
        // check rack local
        String rackname = "/" + rackHostNames[0];
        if (rackLocalRequestMap.containsKey(rackname)) {
          rackLocalRequestMap.get(rackname).setNumContainers(
              rackLocalRequestMap.get(rackname).getNumContainers() + 1);
        } else {
          ResourceRequest request = createResourceRequest(cs.getResource(),
              cs.getExecutionType(), rackname, priority,
              cs.getAllocationId(), 1);
          rackLocalRequestMap.put(rackname, request);
        }
        // check node local
        Map<String, ResourceRequest> nodeLocalRequestMap;
        if (nodeLocalRequests.containsKey(allocationId)) {
          nodeLocalRequestMap = nodeLocalRequests.get(allocationId);
        } else {
          nodeLocalRequestMap = new HashMap<>();
          nodeLocalRequests.put(allocationId, nodeLocalRequestMap);
        }
        String hostname = rackHostNames[1];
        if (nodeLocalRequestMap.containsKey(hostname)) {
          nodeLocalRequestMap.get(hostname).setNumContainers(
              nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
        } else {
          ResourceRequest request = createResourceRequest(cs.getResource(),
              cs.getExecutionType(), hostname, priority,
              cs.getAllocationId(), 1);
          nodeLocalRequestMap.put(hostname, request);
        }
      }
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(cs.getResource(),
            cs.getExecutionType(), ResourceRequest.ANY, priority,
            cs.getAllocationId(), 1);
        anyRequests.put(allocationId, anyRequest);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    for (Map<String, ResourceRequest> nodeLocalRequestMap :
        nodeLocalRequests.values()) {
      ask.addAll(nodeLocalRequestMap.values());
    }
    for (Map<String, ResourceRequest> rackLocalRequestMap :
        rackLocalRequests.values()) {
      ask.addAll(rackLocalRequestMap.values());
    }
    ask.addAll(anyRequests.values());
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

  public Set<NodeId> getRanNodes() {
    return this.ranNodes;
  }
}
