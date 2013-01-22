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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.client.ClientTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFinishedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStoredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RMAppAttemptImpl implements RMAppAttempt, Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppAttemptImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);
  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private final StateMachine<RMAppAttemptState,
                             RMAppAttemptEventType,
                             RMAppAttemptEvent> stateMachine;

  private final RMContext rmContext;
  private final EventHandler eventHandler;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ApplicationAttemptId applicationAttemptId;
  private ClientToken clientToken;
  private final ApplicationSubmissionContext submissionContext;

  //nodes on while this attempt's containers ran
  private final Set<NodeId> ranNodes =
    new HashSet<NodeId>();
  private final List<ContainerStatus> justFinishedContainers =
    new ArrayList<ContainerStatus>();
  private Container masterContainer;

  private float progress = 0;
  private String host = "N/A";
  private int rpcPort;
  private String origTrackingUrl = "N/A";
  private String proxiedTrackingUrl = "N/A";
  private long startTime = 0;

  // Set to null initially. Will eventually get set 
  // if an RMAppAttemptUnregistrationEvent occurs
  private FinalApplicationStatus finalStatus = null;
  private final StringBuilder diagnostics = new StringBuilder();

  private Configuration conf;

  private static final StateMachineFactory<RMAppAttemptImpl,
                                           RMAppAttemptState,
                                           RMAppAttemptEventType,
                                           RMAppAttemptEvent>
       stateMachineFactory  = new StateMachineFactory<RMAppAttemptImpl,
                                            RMAppAttemptState,
                                            RMAppAttemptEventType,
                                     RMAppAttemptEvent>(RMAppAttemptState.NEW)

       // Transitions from NEW State
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.SUBMITTED,
          RMAppAttemptEventType.START, new AttemptStartedTransition())
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.REGISTERED,
          new UnexpectedAMRegisteredTransition())
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.RECOVERED, 
          RMAppAttemptEventType.RECOVER)
          
      // Transitions from SUBMITTED state
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.APP_REJECTED, new AppRejectedTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, 
          EnumSet.of(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING,
                     RMAppAttemptState.SCHEDULED),
          RMAppAttemptEventType.APP_ACCEPTED, 
          new ScheduleTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.REGISTERED,
          new UnexpectedAMRegisteredTransition())
          
       // Transitions from SCHEDULED State
      .addTransition(RMAppAttemptState.SCHEDULED,
                     RMAppAttemptState.ALLOCATED_SAVING,
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          new AMContainerAllocatedTransition())
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))
          
       // Transitions from ALLOCATED_SAVING State
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.ATTEMPT_SAVED, new AttemptStoredTransition())
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED_SAVING,
          RMAppAttemptEventType.CONTAINER_ACQUIRED, 
          new ContainerAcquiredTransition())
       // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))
      
       // Transitions from LAUNCHED_UNMANAGED_SAVING State
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.ATTEMPT_SAVED, 
          new UnmanagedAMAttemptSavedTransition())
      // attempt should not try to register in this state
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.FAILED,
          RMAppAttemptEventType.REGISTERED,
          new UnexpectedAMRegisteredTransition())
      // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new BaseFinalTransition(RMAppAttemptState.KILLED))

       // Transitions from ALLOCATED State
      .addTransition(RMAppAttemptState.ALLOCATED,
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.CONTAINER_ACQUIRED,
          new ContainerAcquiredTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.LAUNCHED, new AMLaunchedTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.LAUNCH_FAILED, new LaunchFailedTransition())
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL, new KillAllocatedAMTransition())
          
       // Transitions from LAUNCHED State
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.REGISTERED, new AMRegisteredTransition())
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new AMContainerCrashedTransition())
      .addTransition(
          RMAppAttemptState.LAUNCHED, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.LAUNCHED, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))

       // Transitions from RUNNING State
      .addTransition(RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED),
          RMAppAttemptEventType.UNREGISTERED, new AMUnregisteredTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.STATUS_UPDATE, new StatusUpdateTransition())
      .addTransition(RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
          RMAppAttemptEventType.CONTAINER_ALLOCATED)
      .addTransition(
                RMAppAttemptState.RUNNING, RMAppAttemptState.RUNNING,
                RMAppAttemptEventType.CONTAINER_ACQUIRED,
                new ContainerAcquiredTransition())
      .addTransition(
          RMAppAttemptState.RUNNING,
          EnumSet.of(RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new ContainerFinishedTransition())
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.FAILED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FAILED))
      .addTransition(
          RMAppAttemptState.RUNNING, RMAppAttemptState.KILLED,
          RMAppAttemptEventType.KILL,
          new FinalTransition(RMAppAttemptState.KILLED))

      // Transitions from FAILED State
      .addTransition(
          RMAppAttemptState.FAILED,
          RMAppAttemptState.FAILED,
          EnumSet.of(
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_FINISHED))

      // Transitions from FINISHING State
      .addTransition(RMAppAttemptState.FINISHING,
          EnumSet.of(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED),
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new AMFinishingContainerFinishedTransition())
      .addTransition(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHED,
          RMAppAttemptEventType.EXPIRE,
          new FinalTransition(RMAppAttemptState.FINISHED))
      .addTransition(RMAppAttemptState.FINISHING, RMAppAttemptState.FINISHING,
          EnumSet.of(
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.STATUS_UPDATE,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.KILL))

      // Transitions from FINISHED State
      .addTransition(
          RMAppAttemptState.FINISHED,
          RMAppAttemptState.FINISHED,
          EnumSet.of(
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.KILL))

      // Transitions from KILLED State
      .addTransition(
          RMAppAttemptState.KILLED,
          RMAppAttemptState.KILLED,
          EnumSet.of(RMAppAttemptEventType.APP_ACCEPTED,
              RMAppAttemptEventType.APP_REJECTED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.REGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.ATTEMPT_SAVED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.STATUS_UPDATE))
              
      // Transitions from RECOVERED State
      .addTransition(
          RMAppAttemptState.RECOVERED,
          RMAppAttemptState.RECOVERED,
          EnumSet.of(RMAppAttemptEventType.START,
              RMAppAttemptEventType.APP_ACCEPTED,
              RMAppAttemptEventType.APP_REJECTED,
              RMAppAttemptEventType.EXPIRE,
              RMAppAttemptEventType.LAUNCHED,
              RMAppAttemptEventType.LAUNCH_FAILED,
              RMAppAttemptEventType.REGISTERED,
              RMAppAttemptEventType.CONTAINER_ALLOCATED,
              RMAppAttemptEventType.CONTAINER_ACQUIRED,
              RMAppAttemptEventType.ATTEMPT_SAVED,
              RMAppAttemptEventType.CONTAINER_FINISHED,
              RMAppAttemptEventType.UNREGISTERED,
              RMAppAttemptEventType.KILL,
              RMAppAttemptEventType.STATUS_UPDATE))
    .installTopology();

  public RMAppAttemptImpl(ApplicationAttemptId appAttemptId,
      RMContext rmContext, YarnScheduler scheduler,
      ApplicationMasterService masterService,
      ApplicationSubmissionContext submissionContext,
      Configuration conf) {
    this.conf = conf;
    this.applicationAttemptId = appAttemptId;
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.submissionContext = submissionContext;
    this.scheduler = scheduler;
    this.masterService = masterService;

    if (UserGroupInformation.isSecurityEnabled()) {

      this.rmContext.getClientToAMTokenSecretManager().registerApplication(
        appAttemptId);

      Token<ClientTokenIdentifier> token =
          new Token<ClientTokenIdentifier>(new ClientTokenIdentifier(
            appAttemptId), this.rmContext.getClientToAMTokenSecretManager());
      this.clientToken =
          BuilderUtils.newClientToken(token.getIdentifier(), token.getKind()
            .toString(), token.getPassword(), token.getService().toString());
    }

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.proxiedTrackingUrl = generateProxyUriWithoutScheme();
    
    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public ApplicationAttemptId getAppAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  public ApplicationSubmissionContext getSubmissionContext() {
    return this.submissionContext;
  } 

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    this.readLock.lock();
    try {
      return this.finalStatus;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public RMAppAttemptState getAppAttemptState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getHost() {
    this.readLock.lock();

    try {
      return this.host;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public int getRpcPort() {
    this.readLock.lock();

    try {
      return this.rpcPort;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public String getTrackingUrl() {
    this.readLock.lock();
    try {
      return this.proxiedTrackingUrl;
    } finally {
      this.readLock.unlock();
    }
  }
  
  @Override
  public String getOriginalTrackingUrl() {
    this.readLock.lock();
    try {
      return this.origTrackingUrl;
    } finally {
      this.readLock.unlock();
    }    
  }
  
  @Override
  public String getWebProxyBase() {
    this.readLock.lock();
    try {
      return ProxyUriUtils.getPath(applicationAttemptId.getApplicationId());
    } finally {
      this.readLock.unlock();
    }    
  }
  
  private String generateProxyUriWithoutScheme() {
    return generateProxyUriWithoutScheme(null);
  }
  
  private String generateProxyUriWithoutScheme(
      final String trackingUriWithoutScheme) {
    this.readLock.lock();
    try {
      URI trackingUri = StringUtils.isEmpty(trackingUriWithoutScheme) ? null :
        ProxyUriUtils.getUriFromAMUrl(trackingUriWithoutScheme);
      String proxy = YarnConfiguration.getProxyHostAndPort(conf);
      URI proxyUri = ProxyUriUtils.getUriFromAMUrl(proxy);
      URI result = ProxyUriUtils.getProxyUri(trackingUri, proxyUri,
          applicationAttemptId.getApplicationId());
      //We need to strip off the scheme to have it match what was there before
      return result.toASCIIString().substring(HttpConfig.getSchemePrefix().length());
    } catch (URISyntaxException e) {
      LOG.warn("Could not proxify "+trackingUriWithoutScheme,e);
      return trackingUriWithoutScheme;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ClientToken getClientToken() {
    return this.clientToken;
  }

  @Override
  public String getDiagnostics() {
    this.readLock.lock();

    try {
      return this.diagnostics.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  public void setDiagnostics(String message) {
    this.writeLock.lock();

    try {
      this.diagnostics.append(message);
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    this.readLock.lock();

    try {
      return this.progress;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> getJustFinishedContainers() {
    this.readLock.lock();
    try {
      return this.justFinishedContainers;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerStatus> pullJustFinishedContainers() {
    this.writeLock.lock();

    try {
      List<ContainerStatus> returnList = new ArrayList<ContainerStatus>(
          this.justFinishedContainers.size());
      returnList.addAll(this.justFinishedContainers);
      this.justFinishedContainers.clear();
      return returnList;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Set<NodeId> getRanNodes() {
    return ranNodes;
  }

  @Override
  public Container getMasterContainer() {
    this.readLock.lock();

    try {
      return this.masterContainer;
    } finally {
      this.readLock.unlock();
    }
  }

  private void setMasterContainer(Container container) {
    masterContainer = container;
  }

  @Override
  public void handle(RMAppAttemptEvent event) {

    this.writeLock.lock();

    try {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      LOG.debug("Processing event for " + appAttemptID + " of type "
          + event.getType());
      final RMAppAttemptState oldState = getAppAttemptState();
      try {
        /* keep the master in sync with the state machine */
        this.stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        /* TODO fail the application on the failed transition */
      }

      if (oldState != getAppAttemptState()) {
        LOG.info(appAttemptID + " State change from " + oldState + " to "
            + getAppAttemptState());
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public ApplicationResourceUsageReport getApplicationResourceUsageReport() {
    this.readLock.lock();
    
    try {
      int numUsedContainers = 0;
      int numReservedContainers = 0;
      Resource currentConsumption = Resources.createResource(0, 0);
      Resource reservedResources = Resources.createResource(0, 0);
      
      SchedulerAppReport schedApp = 
          scheduler.getSchedulerAppInfo(this.getAppAttemptId());
      Collection<RMContainer> liveContainers;
      Collection<RMContainer> reservedContainers;
      if (schedApp != null) {
        liveContainers = schedApp.getLiveContainers();
        reservedContainers = schedApp.getReservedContainers();
        if (liveContainers != null) {
          numUsedContainers = liveContainers.size();
          for (RMContainer lc : liveContainers) {
            Resources.addTo(currentConsumption, lc.getContainer().getResource());
          }
        }
        if (reservedContainers != null) {
          numReservedContainers = reservedContainers.size();
          for (RMContainer rc : reservedContainers) {
            Resources.addTo(reservedResources, rc.getContainer().getResource());
          }
        }
      }

      return BuilderUtils.newApplicationResourceUsageReport(
          numUsedContainers, numReservedContainers,
          currentConsumption, reservedResources,
          Resources.add(currentConsumption, reservedResources));
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void recover(RMState state) {
    ApplicationState appState = 
        state.getApplicationState().get(getAppAttemptId().getApplicationId());
    ApplicationAttemptState attemptState = appState.getAttempt(getAppAttemptId());
    assert attemptState != null;
    setMasterContainer(attemptState.getMasterContainer());
    LOG.info("Recovered attempt: AppId: " + getAppAttemptId().getApplicationId() 
             + " AttemptId: " + getAppAttemptId()
             + " MasterContainer: " + masterContainer);
    setDiagnostics("Attempt recovered after RM restart");
    handle(new RMAppAttemptEvent(getAppAttemptId(), 
                                 RMAppAttemptEventType.RECOVER));
  }
  
  private static class BaseTransition implements
      SingleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent> {

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
    }

  }

  private static final class AttemptStartedTransition extends BaseTransition {
	@Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      appAttempt.startTime = System.currentTimeMillis();

      // Register with the ApplicationMasterService
      appAttempt.masterService
          .registerAppAttempt(appAttempt.applicationAttemptId);

      // Add the application to the scheduler
      appAttempt.eventHandler.handle(
          new AppAddedSchedulerEvent(appAttempt.applicationAttemptId,
              appAttempt.submissionContext.getQueue(),
              appAttempt.submissionContext.getUser()));
    }
  }

  private static final class AppRejectedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptRejectedEvent rejectedEvent = (RMAppAttemptRejectedEvent) event;

      // Tell the AMS. Unregister from the ApplicationMasterService
      appAttempt.masterService
          .unregisterAttempt(appAttempt.applicationAttemptId);
      
      // Save the diagnostic message
      String message = rejectedEvent.getMessage();
      appAttempt.setDiagnostics(message);

      // Send the rejection event to app
      appAttempt.eventHandler.handle(
          new RMAppRejectedEvent(
              rejectedEvent.getApplicationAttemptId().getApplicationId(),
              message)
          );
    }
  }

  private static final List<ContainerId> EMPTY_CONTAINER_RELEASE_LIST =
      new ArrayList<ContainerId>();
  private static final List<ResourceRequest> EMPTY_CONTAINER_REQUEST_LIST =
    new ArrayList<ResourceRequest>();

  private static final class ScheduleTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {
    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      if (!appAttempt.submissionContext.getUnmanagedAM()) {
        // Send the acceptance to the app
        appAttempt.eventHandler.handle(new RMAppEvent(event
            .getApplicationAttemptId().getApplicationId(),
            RMAppEventType.APP_ACCEPTED));

        // Request a container for the AM.
        ResourceRequest request = BuilderUtils.newResourceRequest(
            AM_CONTAINER_PRIORITY, "*", appAttempt.submissionContext
                .getAMContainerSpec().getResource(), 1);

        Allocation amContainerAllocation = appAttempt.scheduler.allocate(
            appAttempt.applicationAttemptId,
            Collections.singletonList(request), EMPTY_CONTAINER_RELEASE_LIST);
        if (amContainerAllocation != null
            && amContainerAllocation.getContainers() != null) {
          assert (amContainerAllocation.getContainers().size() == 0);
        }
        return RMAppAttemptState.SCHEDULED;
      } else {
        // RM not allocating container. AM is self launched. 
        RMStateStore store = appAttempt.rmContext.getStateStore();
        // save state and then go to LAUNCHED state
        appAttempt.storeAttempt(store);
        return RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING;
      }
    }
  }

  private static final class AMContainerAllocatedTransition 
                                                      extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                                                     RMAppAttemptEvent event) {
      // Acquire the AM container from the scheduler.
      Allocation amContainerAllocation = appAttempt.scheduler.allocate(
          appAttempt.applicationAttemptId, EMPTY_CONTAINER_REQUEST_LIST,
          EMPTY_CONTAINER_RELEASE_LIST);

      // Set the masterContainer
      appAttempt.setMasterContainer(amContainerAllocation.getContainers().get(
                                                                           0));

      RMStateStore store = appAttempt.rmContext.getStateStore();
      appAttempt.storeAttempt(store);
    }
  }
  
  private static final class AttemptStoredTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                                                    RMAppAttemptEvent event) {
      appAttempt.checkAttemptStoreError(event);
      appAttempt.launchAttempt();
    }
  }
  
  private static class BaseFinalTransition extends BaseTransition {

    private final RMAppAttemptState finalAttemptState;

    public BaseFinalTransition(RMAppAttemptState finalAttemptState) {
      this.finalAttemptState = finalAttemptState;
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      ApplicationAttemptId appAttemptId = appAttempt.getAppAttemptId();

      // Tell the AMS. Unregister from the ApplicationMasterService
      appAttempt.masterService.unregisterAttempt(appAttemptId);

      // Tell the application and the scheduler
      ApplicationId applicationId = appAttemptId.getApplicationId();
      RMAppEvent appEvent = null;
      switch (finalAttemptState) {
        case FINISHED:
        {
          appEvent = new RMAppFinishedAttemptEvent(applicationId,
              appAttempt.getDiagnostics());
        }
        break;
        case KILLED:
        {
          appEvent =
              new RMAppFailedAttemptEvent(applicationId,
                  RMAppEventType.ATTEMPT_KILLED,
                  "Application killed by user.");
        }
        break;
        case FAILED:
        {
          appEvent =
              new RMAppFailedAttemptEvent(applicationId,
                  RMAppEventType.ATTEMPT_FAILED,
                  appAttempt.getDiagnostics());
        }
        break;
        default:
        {
          LOG.error("Cannot get this state!! Error!!");
        }
        break;
      }

      appAttempt.eventHandler.handle(appEvent);
      appAttempt.eventHandler.handle(new AppRemovedSchedulerEvent(appAttemptId,
        finalAttemptState));

      // Remove the AppAttempt from the ApplicationTokenSecretManager
      appAttempt.rmContext.getApplicationTokenSecretManager()
        .applicationMasterFinished(appAttemptId);
    }
  }

  private static class AMLaunchedTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      // Register with AMLivelinessMonitor
      appAttempt.attemptLaunched();
    }
  }
  
  private static final class UnmanagedAMAttemptSavedTransition 
                                                extends AMLaunchedTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
                            RMAppAttemptEvent event) {
      appAttempt.checkAttemptStoreError(event);
      // Send the acceptance to the app
      // Ideally this should have been done when the scheduler accepted the app.
      // But its here because until the attempt is saved the client should not
      // launch the unmanaged AM. Client waits for the app status to be accepted
      // before doing so. So we have to delay the accepted state until we have 
      // completed storing the attempt
      appAttempt.eventHandler.handle(new RMAppEvent(event
          .getApplicationAttemptId().getApplicationId(),
          RMAppEventType.APP_ACCEPTED));
      
      super.transition(appAttempt, event);
    }    
  }

  private static final class LaunchFailedTransition extends BaseFinalTransition {

    public LaunchFailedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Use diagnostic from launcher
      RMAppAttemptLaunchFailedEvent launchFaileEvent
        = (RMAppAttemptLaunchFailedEvent) event;
      appAttempt.diagnostics.append(launchFaileEvent.getMessage());

      // Tell the app, scheduler
      super.transition(appAttempt, event);

    }
  }

  private static final class KillAllocatedAMTransition extends
      BaseFinalTransition {
    public KillAllocatedAMTransition() {
      super(RMAppAttemptState.KILLED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      // Tell the application and scheduler
      super.transition(appAttempt, event);

      // Tell the launcher to cleanup.
      appAttempt.eventHandler.handle(new AMLauncherEvent(
          AMLauncherEventType.CLEANUP, appAttempt));

    }
  }

  private static final class AMRegisteredTransition extends BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptRegistrationEvent registrationEvent
          = (RMAppAttemptRegistrationEvent) event;
      appAttempt.host = registrationEvent.getHost();
      appAttempt.rpcPort = registrationEvent.getRpcport();
      appAttempt.origTrackingUrl = registrationEvent.getTrackingurl();
      appAttempt.proxiedTrackingUrl = 
        appAttempt.generateProxyUriWithoutScheme(appAttempt.origTrackingUrl);

      // Let the app know
      appAttempt.eventHandler.handle(new RMAppEvent(appAttempt
          .getAppAttemptId().getApplicationId(),
          RMAppEventType.ATTEMPT_REGISTERED));
    }
  }

  private static final class AMContainerCrashedTransition extends
      BaseFinalTransition {

    public AMContainerCrashedTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent finishEvent =
          ((RMAppAttemptContainerFinishedEvent)event);

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());

      // Setup diagnostic message
      ContainerStatus status = finishEvent.getContainerStatus();
      appAttempt.diagnostics.append("AM Container for " +
          appAttempt.getAppAttemptId() + " exited with " +
          " exitCode: " + status.getExitStatus() +
          " due to: " +  status.getDiagnostics() + "." +
          "Failing this attempt.");

      // Tell the app, scheduler
      super.transition(appAttempt, finishEvent);
    }
  }

  private static class FinalTransition extends BaseFinalTransition {

    public FinalTransition(RMAppAttemptState finalAttemptState) {
      super(finalAttemptState);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      appAttempt.progress = 1.0f;

      // Tell the app and the scheduler
      super.transition(appAttempt, event);

      // UnRegister from AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().unregister(
          appAttempt.getAppAttemptId());
      appAttempt.rmContext.getAMFinishingMonitor().unregister(
          appAttempt.getAppAttemptId());

      
      // Unregister from the ClientTokenSecretManager
      if (UserGroupInformation.isSecurityEnabled()) {
        appAttempt.rmContext.getClientToAMTokenSecretManager()
          .unRegisterApplication(appAttempt.getAppAttemptId());
      }

      if(!appAttempt.submissionContext.getUnmanagedAM()) {
        // Tell the launcher to cleanup.
        appAttempt.eventHandler.handle(new AMLauncherEvent(
            AMLauncherEventType.CLEANUP, appAttempt));
      }
    }
  }
  
  private static class UnexpectedAMRegisteredTransition extends
      BaseFinalTransition {

    public UnexpectedAMRegisteredTransition() {
      super(RMAppAttemptState.FAILED);
    }

    @Override
    public void transition(RMAppAttemptImpl appAttempt, RMAppAttemptEvent event) {
      assert appAttempt.submissionContext.getUnmanagedAM();
      appAttempt
          .setDiagnostics("Unmanaged AM must register after AM attempt reaches LAUNCHED state.");
      super.transition(appAttempt, event);
    }

  }

  private static final class StatusUpdateTransition extends
      BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptStatusupdateEvent statusUpdateEvent
        = (RMAppAttemptStatusupdateEvent) event;

      // Update progress
      appAttempt.progress = statusUpdateEvent.getProgress();

      // Ping to AMLivelinessMonitor
      appAttempt.rmContext.getAMLivelinessMonitor().receivedPing(
          statusUpdateEvent.getApplicationAttemptId());
    }
  }

  private static final class AMUnregisteredTransition implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptId = appAttempt.getAppAttemptId();

      appAttempt.rmContext.getAMLivelinessMonitor().unregister(appAttemptId);

      // Remove the AppAttempt from the ApplicationTokenSecretManager
      appAttempt.rmContext.getApplicationTokenSecretManager()
        .applicationMasterFinished(appAttemptId);

      appAttempt.progress = 1.0f;

      RMAppAttemptUnregistrationEvent unregisterEvent
        = (RMAppAttemptUnregistrationEvent) event;
      appAttempt.diagnostics.append(unregisterEvent.getDiagnostics());
      appAttempt.origTrackingUrl = unregisterEvent.getTrackingUrl();
      appAttempt.proxiedTrackingUrl = 
        appAttempt.generateProxyUriWithoutScheme(appAttempt.origTrackingUrl);
      appAttempt.finalStatus = unregisterEvent.getFinalApplicationStatus();

      // Tell the app
      if (appAttempt.getSubmissionContext().getUnmanagedAM()) {
        // Unmanaged AMs have no container to wait for, so they skip
        // the FINISHING state and go straight to FINISHED.
        new FinalTransition(RMAppAttemptState.FINISHED).transition(
            appAttempt, event);
        return RMAppAttemptState.FINISHED;
      }
      appAttempt.rmContext.getAMFinishingMonitor().register(appAttemptId);
      ApplicationId applicationId =
          appAttempt.getAppAttemptId().getApplicationId();
      appAttempt.eventHandler.handle(
          new RMAppEvent(applicationId, RMAppEventType.ATTEMPT_FINISHING));
      return RMAppAttemptState.FINISHING;
    }
  }

  private static final class ContainerAcquiredTransition extends
      BaseTransition {
    @Override
    public void transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {
      RMAppAttemptContainerAcquiredEvent acquiredEvent
        = (RMAppAttemptContainerAcquiredEvent) event;
      appAttempt.ranNodes.add(acquiredEvent.getContainer().getNodeId());
    }
  }

  private static final class ContainerFinishedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent
        = (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the AmContainer? If the finished container is same as
      // the AMContainer, AppAttempt fails
      if (appAttempt.masterContainer != null
          && appAttempt.masterContainer.getId().equals(
              containerStatus.getContainerId())) {
        // container associated with AM. must not be unmanaged 
        assert appAttempt.submissionContext.getUnmanagedAM() == false;
        // Setup diagnostic message
        appAttempt.diagnostics.append("AM Container for " +
            appAttempt.getAppAttemptId() + " exited with " +
            " exitCode: " + containerStatus.getExitStatus() +
            " due to: " +  containerStatus.getDiagnostics() + "." +
            "Failing this attempt.");

        // When the AM dies, the trackingUrl is left pointing to the AM's URL,
        // which shows up in the scheduler UI as a broken link.  Direct the
        // user to the app page on the RM so they can see the status and logs.
        appAttempt.origTrackingUrl = pjoin(
            YarnConfiguration.getRMWebAppHostAndPort(appAttempt.conf),
            "cluster", "app", appAttempt.getAppAttemptId().getApplicationId());
        appAttempt.proxiedTrackingUrl = appAttempt.origTrackingUrl;

        new FinalTransition(RMAppAttemptState.FAILED).transition(
            appAttempt, containerFinishedEvent);
        return RMAppAttemptState.FAILED;
      }

      // Normal container.

      // Put it in completedcontainers list
      appAttempt.justFinishedContainers.add(containerStatus);
      return RMAppAttemptState.RUNNING;
    }
  }

  private static final class AMFinishingContainerFinishedTransition
      implements
      MultipleArcTransition<RMAppAttemptImpl, RMAppAttemptEvent, RMAppAttemptState> {

    @Override
    public RMAppAttemptState transition(RMAppAttemptImpl appAttempt,
        RMAppAttemptEvent event) {

      RMAppAttemptContainerFinishedEvent containerFinishedEvent
        = (RMAppAttemptContainerFinishedEvent) event;
      ContainerStatus containerStatus =
          containerFinishedEvent.getContainerStatus();

      // Is this container the ApplicationMaster container?
      if (appAttempt.masterContainer.getId().equals(
          containerStatus.getContainerId())) {
        new FinalTransition(RMAppAttemptState.FINISHED).transition(
            appAttempt, containerFinishedEvent);
        return RMAppAttemptState.FINISHED;
      }

      // Normal container.
      appAttempt.justFinishedContainers.add(containerStatus);
      return RMAppAttemptState.FINISHING;
    }
  }

  @Override
  public long getStartTime() {
    this.readLock.lock();
    try {
      return this.startTime;
    } finally {
      this.readLock.unlock();
    }
  }
  
  private void launchAttempt(){
    // Send event to launch the AM Container
    eventHandler.handle(new AMLauncherEvent(AMLauncherEventType.LAUNCH, this));
  }
  
  private void attemptLaunched() {
    // Register with AMLivelinessMonitor
    rmContext.getAMLivelinessMonitor().register(getAppAttemptId());
  }
  
  private void checkAttemptStoreError(RMAppAttemptEvent event) {
    RMAppAttemptStoredEvent storeEvent = (RMAppAttemptStoredEvent) event;
    if(storeEvent.getStoredException() != null)
    {
      // This needs to be handled for HA and give up master status if we got
      // fenced
      LOG.error("Failed to store attempt: " + getAppAttemptId(),
                storeEvent.getStoredException());
      ExitUtil.terminate(1, storeEvent.getStoredException());
    }
  }
  
  private void storeAttempt(RMStateStore store) {
    // store attempt data in a non-blocking manner to prevent dispatcher
    // thread starvation and wait for state to be saved
    LOG.info("Storing attempt: AppId: " + 
              getAppAttemptId().getApplicationId() 
              + " AttemptId: " + 
              getAppAttemptId()
              + " MasterContainer: " + masterContainer);
    store.storeApplicationAttempt(this);
  }
}
