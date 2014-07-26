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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
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
import org.apache.hadoop.yarn.api.protocolrecords
        .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;

import org.apache.hadoop.yarn.api.protocolrecords
        .RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords
        .RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;

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
  protected int RESPONSE_ID = 1;
  // user name
  protected String user;  
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS;
  // whether tracked in Metrics
  protected boolean isTracked;
  // progress
  protected int totalContainers;
  protected int finishedContainers;
  
  protected final Logger LOG = Logger.getLogger(AMSimulator.class);
  
  public AMSimulator() {
    this.responseQueue = new LinkedBlockingQueue<AllocateResponse>();
  }

  public void init(int id, int heartbeatInterval, 
      List<ContainerSimulator> containerList, ResourceManager rm, SLSRunner se,
      long traceStartTime, long traceFinishTime, String user, String queue, 
      boolean isTracked, String oldAppId) {
    super.init(traceStartTime, traceStartTime + 1000000L * heartbeatInterval,
            heartbeatInterval);
    this.user = user;
    this.rm = rm;
    this.se = se;
    this.user = user;
    this.queue = queue;
    this.oldAppId = oldAppId;
    this.isTracked = isTracked;
    this.traceStartTimeMS = traceStartTime;
    this.traceFinishTimeMS = traceFinishTime;
  }

  /**
   * register with RM
   */
  @Override
  public void firstStep() throws Exception {
    simulateStartTimeMS = System.currentTimeMillis() - 
                          SLSRunner.getRunner().getStartTimeMS();

    // submit application, waiting until ACCEPTED
    submitApp();

    // register application master
    registerAM();

    // track app metrics
    trackApp();
  }

  @Override
  public void middleStep() throws Exception {
    // process responses in the queue
    processResponseQueue();
    
    // send out request
    sendContainerRequest();
    
    // check whether finish
    checkStop();
  }

  @Override
  public void lastStep() throws Exception {
    LOG.info(MessageFormat.format("Application {0} is shutting down.", appId));
    // unregister tracking
    if (isTracked) {
      untrackApp();
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

    simulateFinishTimeMS = System.currentTimeMillis() -
        SLSRunner.getRunner().getStartTimeMS();
    // record job running information
    ((ResourceSchedulerWrapper)rm.getResourceScheduler())
         .addAMRuntime(appId, 
                      traceStartTimeMS, traceFinishTimeMS, 
                      simulateStartTimeMS, simulateFinishTimeMS);
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
    allocateRequest.setResponseId(RESPONSE_ID ++);
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
  
  private void submitApp()
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
    conLauContext.setApplicationACLs(
        new HashMap<ApplicationAccessType, String>());
    conLauContext.setCommands(new ArrayList<String>());
    conLauContext.setEnvironment(new HashMap<String, String>());
    conLauContext.setLocalResources(new HashMap<String, LocalResource>());
    conLauContext.setServiceData(new HashMap<String, ByteBuffer>());
    appSubContext.setAMContainerSpec(conLauContext);
    appSubContext.setUnmanagedAM(true);
    subAppRequest.setApplicationSubmissionContext(appSubContext);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws YarnException {
        rm.getClientRMService().submitApplication(subAppRequest);
        return null;
      }
    });
    LOG.info(MessageFormat.format("Submit a new application {0}", appId));
    
    // waiting until application ACCEPTED
    RMApp app = rm.getRMContext().getRMApps().get(appId);
    while(app.getState() != RMAppState.ACCEPTED) {
      Thread.sleep(10);
    }

    // Waiting until application attempt reach LAUNCHED
    // "Unmanaged AM must register after AM attempt reaches LAUNCHED state"
    this.appAttemptId = rm.getRMContext().getRMApps().get(appId)
        .getCurrentAppAttempt().getAppAttemptId();
    RMAppAttempt rmAppAttempt = rm.getRMContext().getRMApps().get(appId)
        .getCurrentAppAttempt();
    while (rmAppAttempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED) {
      Thread.sleep(10);
    }
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

    LOG.info(MessageFormat.format(
            "Register the application master for application {0}", appId));
  }

  private void trackApp() {
    if (isTracked) {
      ((ResourceSchedulerWrapper) rm.getResourceScheduler())
              .addTrackedApp(appAttemptId, oldAppId);
    }
  }
  public void untrackApp() {
    if (isTracked) {
      ((ResourceSchedulerWrapper) rm.getResourceScheduler())
              .removeTrackedApp(appAttemptId, oldAppId);
    }
  }
  
  protected List<ResourceRequest> packageRequests(
          List<ContainerSimulator> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
    Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
    ResourceRequest anyRequest = null;
    for (ContainerSimulator cs : csList) {
      String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
      // check rack local
      String rackname = rackHostNames[0];
      if (rackLocalRequestMap.containsKey(rackname)) {
        rackLocalRequestMap.get(rackname).setNumContainers(
            rackLocalRequestMap.get(rackname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), rackname, priority, 1);
        rackLocalRequestMap.put(rackname, request);
      }
      // check node local
      String hostname = rackHostNames[1];
      if (nodeLocalRequestMap.containsKey(hostname)) {
        nodeLocalRequestMap.get(hostname).setNumContainers(
            nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), hostname, priority, 1);
        nodeLocalRequestMap.put(hostname, request);
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
}
