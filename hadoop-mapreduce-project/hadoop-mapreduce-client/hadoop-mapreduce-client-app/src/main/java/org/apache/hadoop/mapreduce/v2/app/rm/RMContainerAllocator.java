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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  static final Logger LOG = LoggerFactory.getLogger(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  static final Priority PRIORITY_FAST_FAIL_MAP;
  static final Priority PRIORITY_REDUCE;
  static final Priority PRIORITY_MAP;
  static final Priority PRIORITY_OPPORTUNISTIC_MAP;

  @VisibleForTesting
  public static final String RAMPDOWN_DIAGNOSTIC = "Reducer preempted "
      + "to make room for pending map attempts";

  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
    PRIORITY_OPPORTUNISTIC_MAP =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
            Priority.class);
    PRIORITY_OPPORTUNISTIC_MAP.setPriority(19);
  }
  
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Lifecycle of map
  scheduled->assigned->completed
  
  Lifecycle of reduce
  pending->scheduled->assigned->completed
  
  Maps are scheduled as soon as their requests are received. Reduces are 
  added to the pending and are ramped up (added to scheduled) based 
  on completed maps and current availability in the cluster.
  */
  
  //reduces which are not yet scheduled
  private final LinkedList<ContainerRequest> pendingReduces = 
    new LinkedList<ContainerRequest>();

  //holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests;

  //holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();

  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int lastCompletedTasks = 0;
  
  private boolean recalculateReduceSchedule = false;
  private Resource mapResourceRequest = Resources.none();
  private Resource reduceResourceRequest = Resources.none();
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;

  // Mapper allocation timeout, after which a reducer is forcibly preempted
  private long reducerUnconditionalPreemptionDelayMs;

  // Duration to wait before preempting a reducer when there is NO room
  private long reducerNoHeadroomPreemptionDelayMs = 0;

  private float reduceSlowStart = 0;
  private int maxRunningMaps = 0;
  private int maxRunningReduces = 0;
  private long retryInterval;
  private long retrystartTime;
  private Clock clock;

  private final AMPreemptionPolicy preemptionPolicy;

  @VisibleForTesting
  protected BlockingQueue<ContainerAllocatorEvent> eventQueue
    = new LinkedBlockingQueue<ContainerAllocatorEvent>();

  private ScheduleStats scheduleStats = new ScheduleStats();

  private String mapNodeLabelExpression;

  private String reduceNodeLabelExpression;

  public RMContainerAllocator(ClientService clientService, AppContext context,
      AMPreemptionPolicy preemptionPolicy) {
    super(clientService, context);
    this.preemptionPolicy = preemptionPolicy;
    this.stopped = new AtomicBoolean(false);
    this.clock = context.getClock();
    this.assignedRequests = createAssignedRequests();
  }

  protected AssignedRequests createAssignedRequests() {
    return new AssignedRequests();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    reducerUnconditionalPreemptionDelayMs = 1000 * conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC);
    reducerNoHeadroomPreemptionDelayMs = conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_PREEMPT_DELAY_SEC) * 1000;//sec -> ms
    maxRunningMaps = conf.getInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_MAP_LIMIT);
    maxRunningReduces = conf.getInt(MRJobConfig.JOB_RUNNING_REDUCE_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_REDUCE_LIMIT);
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    mapNodeLabelExpression = conf.get(MRJobConfig.MAP_NODE_LABEL_EXP);
    reduceNodeLabelExpression = conf.get(MRJobConfig.REDUCE_NODE_LABEL_EXP);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
    this.scheduledRequests.setNumOpportunisticMapsPercent(
        conf.getInt(MRJobConfig.MR_NUM_OPPORTUNISTIC_MAPS_PERCENT,
            MRJobConfig.DEFAULT_MR_NUM_OPPORTUNISTIC_MAPS_PERCENT));
    LOG.info(this.scheduledRequests.getNumOpportunisticMapsPercent() +
        "% of the mappers will be scheduled using OPPORTUNISTIC containers");
  }

  @Override
  protected void serviceStart() throws Exception {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        ContainerAllocatorEvent event;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            eventHandler.handle(new JobEvent(getJob().getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers != null && allocatedContainers.size() > 0) {
      scheduledRequests.assign(allocatedContainers);
    }

    int completedMaps = getJob().getCompletedMaps();
    int completedTasks = completedMaps + getJob().getCompletedReduces();
    if ((lastCompletedTasks != completedTasks) ||
          (scheduledRequests.maps.size() > 0)) {
      lastCompletedTasks = completedTasks;
      recalculateReduceSchedule = true;
    }

    if (recalculateReduceSchedule) {
      boolean reducerPreempted = preemptReducesIfNeeded();

      if (!reducerPreempted) {
        // Only schedule new reducers if no reducer preemption happens for
        // this heartbeat
        scheduleReduces(getJob().getTotalMaps(), completedMaps,
            scheduledRequests.maps.size(), scheduledRequests.reduces.size(),
            assignedRequests.maps.size(), assignedRequests.reduces.size(),
            mapResourceRequest, reduceResourceRequest, pendingReduces.size(),
            maxReduceRampupLimit, reduceSlowStart);
      }

      recalculateReduceSchedule = false;
    }

    scheduleStats.updateAndLogIfChanged("After Scheduling: ");
  }

  @Override
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    super.serviceStop();
    scheduleStats.log("Final Stats: ");
  }

  @Private
  @VisibleForTesting
  AssignedRequests getAssignedRequests() {
    return assignedRequests;
  }

  @Private
  @VisibleForTesting
  ScheduledRequests getScheduledRequests() {
    return scheduledRequests;
  }

  @Private
  @VisibleForTesting
  int getNumOfPendingReduces() {
    return pendingReduces.size();
  }

  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      boolean isMap = reqEvent.getAttemptID().getTaskId().getTaskType().
          equals(TaskType.MAP);
      if (isMap) {
        handleMapContainerRequest(reqEvent);
      } else {
        handleReduceContainerRequest(reqEvent);
      }
      
    } else if (
      event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
  
      LOG.info("Processing the event " + event.toString());

      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        ContainerId containerId = assignedRequests.get(aId);
        if (containerId != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          pendingRelease.add(containerId);
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
      preemptionPolicy.handleCompletedContainer(event.getAttemptID());
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
      // propagate failures to preemption policy to discard checkpoints for
      // failed tasks
      preemptionPolicy.handleFailedContainer(event.getAttemptID());
    }
  }

  @SuppressWarnings({ "unchecked" })
  private void handleReduceContainerRequest(ContainerRequestEvent reqEvent) {
    assert(reqEvent.getAttemptID().getTaskId().getTaskType().equals(
        TaskType.REDUCE));

    Resource supportedMaxContainerCapability = getMaxContainerCapability();
    JobId jobId = getJob().getID();

    if (reduceResourceRequest.equals(Resources.none())) {
      reduceResourceRequest = reqEvent.getCapability();
      eventHandler.handle(new JobHistoryEvent(jobId,
          new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.REDUCE,
              reduceResourceRequest.getMemorySize())));
      LOG.info("reduceResourceRequest:" + reduceResourceRequest);
    }

    boolean reduceContainerRequestAccepted = true;
    if (reduceResourceRequest.getMemorySize() >
        supportedMaxContainerCapability.getMemorySize()
        ||
        reduceResourceRequest.getVirtualCores() >
        supportedMaxContainerCapability.getVirtualCores()) {
      reduceContainerRequestAccepted = false;
    }

    if (reduceContainerRequestAccepted) {
      // set the resources
      reqEvent.getCapability().setVirtualCores(
          reduceResourceRequest.getVirtualCores());
      reqEvent.getCapability().setMemorySize(
          reduceResourceRequest.getMemorySize());

      if (reqEvent.getEarlierAttemptFailed()) {
        //previously failed reducers are added to the front for fail fast
        pendingReduces.addFirst(new ContainerRequest(reqEvent,
            PRIORITY_REDUCE, reduceNodeLabelExpression));
      } else {
        //reduces are added to pending queue and are slowly ramped up
        pendingReduces.add(new ContainerRequest(reqEvent,
            PRIORITY_REDUCE, reduceNodeLabelExpression));
      }
    } else {
      String diagMsg = "REDUCE capability required is more than the " +
          "supported max container capability in the cluster. Killing" +
          " the Job. reduceResourceRequest: " + reduceResourceRequest +
          " maxContainerCapability:" + supportedMaxContainerCapability;
      LOG.info(diagMsg);
      eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
      eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    }
  }

  @SuppressWarnings({ "unchecked" })
  private void handleMapContainerRequest(ContainerRequestEvent reqEvent) {
    assert(reqEvent.getAttemptID().getTaskId().getTaskType().equals(
        TaskType.MAP));

    Resource supportedMaxContainerCapability = getMaxContainerCapability();
    JobId jobId = getJob().getID();

    if (mapResourceRequest.equals(Resources.none())) {
      mapResourceRequest = reqEvent.getCapability();
      eventHandler.handle(new JobHistoryEvent(jobId,
          new NormalizedResourceEvent(
              org.apache.hadoop.mapreduce.TaskType.MAP,
              mapResourceRequest.getMemorySize())));
      LOG.info("mapResourceRequest:" + mapResourceRequest);
    }

    boolean mapContainerRequestAccepted = true;
    if (mapResourceRequest.getMemorySize() >
        supportedMaxContainerCapability.getMemorySize()
        ||
        mapResourceRequest.getVirtualCores() >
        supportedMaxContainerCapability.getVirtualCores()) {
      mapContainerRequestAccepted = false;
    }

    if(mapContainerRequestAccepted) {
      // set the resources
      reqEvent.getCapability().setMemorySize(
          mapResourceRequest.getMemorySize());
      reqEvent.getCapability().setVirtualCores(
          mapResourceRequest.getVirtualCores());
      scheduledRequests.addMap(reqEvent); //maps are immediately scheduled
    } else {
      String diagMsg = "The required MAP capability is more than the " +
          "supported max container capability in the cluster. Killing" +
          " the Job. mapResourceRequest: " + mapResourceRequest +
          " maxContainerCapability:" + supportedMaxContainerCapability;
      LOG.info(diagMsg);
      eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
      eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
    }
  }

  private static String getHost(String contMgrAddress) {
    String host = contMgrAddress;
    String[] hostport = host.split(":");
    if (hostport.length == 2) {
      host = hostport[0];
    }
    return host;
  }

  @Private
  @VisibleForTesting
  synchronized void setReduceResourceRequest(Resource res) {
    this.reduceResourceRequest = res;
  }

  @Private
  @VisibleForTesting
  synchronized void setMapResourceRequest(Resource res) {
    this.mapResourceRequest = res;
  }

  @Private
  @VisibleForTesting
  boolean preemptReducesIfNeeded() {
    if (reduceResourceRequest.equals(Resources.none())) {
      return false; // no reduces
    }

    if (assignedRequests.maps.size() > 0) {
      // there are assigned mappers
      return false;
    }

    if (scheduledRequests.maps.size() <= 0) {
      // there are no pending requests for mappers
      return false;
    }

    // At this point:
    // we have pending mappers and all assigned resources are taken by reducers
    if (reducerUnconditionalPreemptionDelayMs >= 0) {
      // Unconditional preemption is enabled.
      // If mappers are pending for longer than the configured threshold,
      // preempt reducers irrespective of what the headroom is.
      if (preemptReducersForHangingMapRequests(
          reducerUnconditionalPreemptionDelayMs)) {
        return true;
      }
    }

    // The pending mappers haven't been waiting for too long. Let us see if
    // there are enough resources for a mapper to run. This is calculated by
    // excluding scheduled reducers from headroom and comparing it against
    // resources required to run one mapper.
    Resource scheduledReducesResource = Resources.multiply(
         reduceResourceRequest, scheduledRequests.reduces.size());
    Resource availableResourceForMap =
         Resources.subtract(getAvailableResources(), scheduledReducesResource);
    if (ResourceCalculatorUtils.computeAvailableContainers(availableResourceForMap,
        mapResourceRequest, getSchedulerResourceTypes()) > 0) {
       // Enough room to run a mapper
      return false;
    }

    // Available resources are not enough to run mapper. See if we should hold
    // off before preempting reducers and preempt if okay.
    return preemptReducersForHangingMapRequests(reducerNoHeadroomPreemptionDelayMs);
  }

  private boolean preemptReducersForHangingMapRequests(long pendingThreshold) {
    int hangingMapRequests = getNumHangingRequests(
        pendingThreshold, scheduledRequests.maps);
    if (hangingMapRequests > 0) {
      preemptReducer(hangingMapRequests);
      return true;
    }
    return false;
  }

  private void clearAllPendingReduceRequests() {
    rampDownReduces(Integer.MAX_VALUE);
  }

  private void preemptReducer(int hangingMapRequests) {
    clearAllPendingReduceRequests();

    // preempt for making space for at least one map
    int preemptionReduceNumForOneMap =
        ResourceCalculatorUtils.divideAndCeilContainers(mapResourceRequest,
            reduceResourceRequest, getSchedulerResourceTypes());
    int preemptionReduceNumForPreemptionLimit =
        ResourceCalculatorUtils.divideAndCeilContainers(
            Resources.multiply(getResourceLimit(), maxReducePreemptionLimit),
            reduceResourceRequest, getSchedulerResourceTypes());
    int preemptionReduceNumForAllMaps =
        ResourceCalculatorUtils.divideAndCeilContainers(
            Resources.multiply(mapResourceRequest, hangingMapRequests),
            reduceResourceRequest, getSchedulerResourceTypes());
    int toPreempt =
        Math.min(Math.max(preemptionReduceNumForOneMap,
                preemptionReduceNumForPreemptionLimit),
            preemptionReduceNumForAllMaps);

    LOG.info("Going to preempt " + toPreempt
        + " due to lack of space for maps");
    assignedRequests.preemptReduce(toPreempt);
  }

  private int getNumHangingRequests(long allocationDelayThresholdMs,
      Map<TaskAttemptId, ContainerRequest> requestMap) {
    if (allocationDelayThresholdMs <= 0)
      return requestMap.size();
    int hangingRequests = 0;
    long currTime = clock.getTime();
    for (ContainerRequest request: requestMap.values()) {
      long delay = currTime - request.requestTimeMs;
      if (delay > allocationDelayThresholdMs)
        hangingRequests++;
    }
    return hangingRequests;
  }
  
  @Private
  public void scheduleReduces(
      int totalMaps, int completedMaps,
      int scheduledMaps, int scheduledReduces,
      int assignedMaps, int assignedReduces,
      Resource mapResourceReqt, Resource reduceResourceReqt,
      int numPendingReduces,
      float maxReduceRampupLimit, float reduceSlowStart) {
    
    if (numPendingReduces == 0) {
      return;
    }
    
    // get available resources for this job
    Resource headRoom = getAvailableResources();

    LOG.info("Recalculating schedule, headroom=" + headRoom);
    
    //check for slow start
    if (!getIsReduceStarted()) {//not set yet
      int completedMapsForReduceSlowstart = (int)Math.ceil(reduceSlowStart * 
                      totalMaps);
      if(completedMaps < completedMapsForReduceSlowstart) {
        LOG.info("Reduce slow start threshold not met. " +
              "completedMapsForReduceSlowstart " + 
            completedMapsForReduceSlowstart);
        return;
      } else {
        LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
        setIsReduceStarted(true);
      }
    }
    
    //if all maps are assigned, then ramp up all reduces irrespective of the
    //headroom
    if (scheduledMaps == 0 && numPendingReduces > 0) {
      LOG.info("All maps assigned. " +
          "Ramping up all remaining reduces:" + numPendingReduces);
      scheduleAllReduces();
      return;
    }

    float completedMapPercent = 0f;
    if (totalMaps != 0) {//support for 0 maps
      completedMapPercent = (float)completedMaps/totalMaps;
    } else {
      completedMapPercent = 1;
    }
    
    Resource netScheduledMapResource =
        Resources.multiply(mapResourceReqt, (scheduledMaps + assignedMaps));

    Resource netScheduledReduceResource =
        Resources.multiply(reduceResourceReqt,
          (scheduledReduces + assignedReduces));

    Resource finalMapResourceLimit;
    Resource finalReduceResourceLimit;

    // ramp up the reduces based on completed map percentage
    Resource totalResourceLimit = getResourceLimit();

    Resource idealReduceResourceLimit =
        Resources.multiply(totalResourceLimit,
          Math.min(completedMapPercent, maxReduceRampupLimit));
    Resource ideaMapResourceLimit =
        Resources.subtract(totalResourceLimit, idealReduceResourceLimit);

    // check if there aren't enough maps scheduled, give the free map capacity
    // to reduce.
    // Even when container number equals, there may be unused resources in one
    // dimension
    if (ResourceCalculatorUtils.computeAvailableContainers(ideaMapResourceLimit,
      mapResourceReqt, getSchedulerResourceTypes()) >= (scheduledMaps + assignedMaps)) {
      // enough resource given to maps, given the remaining to reduces
      Resource unusedMapResourceLimit =
          Resources.subtract(ideaMapResourceLimit, netScheduledMapResource);
      finalReduceResourceLimit =
          Resources.add(idealReduceResourceLimit, unusedMapResourceLimit);
      finalMapResourceLimit =
          Resources.subtract(totalResourceLimit, finalReduceResourceLimit);
    } else {
      finalMapResourceLimit = ideaMapResourceLimit;
      finalReduceResourceLimit = idealReduceResourceLimit;
    }

    LOG.info("completedMapPercent " + completedMapPercent
        + " totalResourceLimit:" + totalResourceLimit
        + " finalMapResourceLimit:" + finalMapResourceLimit
        + " finalReduceResourceLimit:" + finalReduceResourceLimit
        + " netScheduledMapResource:" + netScheduledMapResource
        + " netScheduledReduceResource:" + netScheduledReduceResource);

    int rampUp =
        ResourceCalculatorUtils.computeAvailableContainers(Resources.subtract(
                finalReduceResourceLimit, netScheduledReduceResource),
            reduceResourceReqt, getSchedulerResourceTypes());

    if (rampUp > 0) {
      rampUp = Math.min(rampUp, numPendingReduces);
      LOG.info("Ramping up " + rampUp);
      rampUpReduces(rampUp);
    } else if (rampUp < 0) {
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledReduces);
      LOG.info("Ramping down " + rampDown);
      rampDownReduces(rampDown);
    }
  }

  @Private
  public void scheduleAllReduces() {
    for (ContainerRequest req : pendingReduces) {
      scheduledRequests.addReduce(req);
    }
    pendingReduces.clear();
  }
  
  @Private
  public void rampUpReduces(int rampUp) {
    //more reduce to be scheduled
    for (int i = 0; i < rampUp; i++) {
      ContainerRequest request = pendingReduces.removeFirst();
      scheduledRequests.addReduce(request);
    }
  }
  
  @Private
  public void rampDownReduces(int rampDown) {
    //remove from the scheduled and move back to pending
    while (rampDown > 0) {
      ContainerRequest request = scheduledRequests.removeReduce();
      if (request == null) {
        return;
      }
      pendingReduces.add(request);
      rampDown--;
    }
  }
  
  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    applyConcurrentTaskLimits();

    // will be null the first time
    Resource headRoom = Resources.clone(getAvailableResources());
    AllocateResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try
     * to contact the RM.
     */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (ApplicationAttemptNotFoundException e ) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
        JobEventType.JOB_AM_REBOOT));
      throw new RMContainerAllocationException(
        "Resource Manager doesn't recognize AttemptId: "
            + this.getContext().getApplicationAttemptId(), e);
    } catch (ApplicationMasterNotRegisteredException e) {
      LOG.info("ApplicationMaster is out of sync with ResourceManager,"
          + " hence resync and send outstanding requests.");
      // RM may have restarted, re-register with RM.
      lastResponseID = 0;
      register();
      addOutstandingRequestOnResync();
      return null;
    } catch (InvalidLabelResourceRequestException e) {
      // If Invalid label exception is received means the requested label doesnt
      // have access so killing job in this case.
      String diagMsg = "Requested node-label-expression is invalid: "
          + StringUtils.stringifyException(e);
      LOG.info(diagMsg);
      JobId jobId = this.getJob().getID();
      eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
      eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
      throw e;
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.JOB_AM_REBOOT));
        throw new RMContainerAllocationException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    Resource newHeadRoom = getAvailableResources();
    List<Container> newContainers = response.getAllocatedContainers();
    // Setting NMTokens
    if (response.getNMTokens() != null) {
      for (NMToken nmToken : response.getNMTokens()) {
        NMTokenCache.setNMToken(nmToken.getNodeId().toString(),
            nmToken.getToken());
      }
    }

    // Setting AMRMToken
    if (response.getAMRMToken() != null) {
      updateAMRMToken(response.getAMRMToken());
    }

    List<ContainerStatus> finishedContainers =
        response.getCompletedContainersStatuses();

    // propagate preemption requests
    final PreemptionMessage preemptReq = response.getPreemptionMessage();
    if (preemptReq != null) {
      preemptionPolicy.preempt(
          new PreemptionContext(assignedRequests), preemptReq);
    }

    if (newContainers.size() + finishedContainers.size() > 0
        || !headRoom.equals(newHeadRoom)) {
      //something changed
      recalculateReduceSchedule = true;
      if (LOG.isDebugEnabled() && !headRoom.equals(newHeadRoom)) {
        LOG.debug("headroom=" + newHeadRoom);
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Container cont : newContainers) {
        LOG.debug("Received new Container :" + cont);
      }
    }

    //Called on each allocation. Will know about newly blacklisted/added hosts.
    computeIgnoreBlacklisting();

    handleUpdatedNodes(response);
    handleJobPriorityChange(response);
    // Handle receiving the timeline collector address and token for this app.
    MRAppMaster.RunningAppContext appContext =
        (MRAppMaster.RunningAppContext)this.getContext();
    if (appContext.getTimelineV2Client() != null) {
      appContext.getTimelineV2Client().
          setTimelineCollectorInfo(response.getCollectorInfo());
    }
    for (ContainerStatus cont : finishedContainers) {
      processFinishedContainer(cont);
    }
    return newContainers;
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  void processFinishedContainer(ContainerStatus container) {
    LOG.info("Received completed container " + container.getContainerId());
    TaskAttemptId attemptID = assignedRequests.get(container.getContainerId());
    if (attemptID == null) {
      LOG.error("Container complete event for unknown container "
          + container.getContainerId());
    } else {
      pendingRelease.remove(container.getContainerId());
      assignedRequests.remove(attemptID);

      // Send the diagnostics
      String diagnostic = StringInterner.weakIntern(container.getDiagnostics());
      eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
          diagnostic));

      // send the container completed event to Task attempt
      eventHandler.handle(createContainerFinishedEvent(container, attemptID));

      preemptionPolicy.handleCompletedContainer(attemptID);
    }
  }

  private void applyConcurrentTaskLimits() {
    int numScheduledMaps = scheduledRequests.maps.size();
    if (maxRunningMaps > 0 && numScheduledMaps > 0 &&
        getJob().getTotalMaps() > maxRunningMaps) {
      int maxRequestedMaps = Math.max(0,
          maxRunningMaps - assignedRequests.maps.size());
      int numScheduledFailMaps = scheduledRequests.earlierFailedMaps.size();
      int failedMapRequestLimit = Math.min(maxRequestedMaps,
          numScheduledFailMaps);
      int normalMapRequestLimit = Math.min(
          maxRequestedMaps - failedMapRequestLimit,
          numScheduledMaps - numScheduledFailMaps);
      setRequestLimit(PRIORITY_FAST_FAIL_MAP, mapResourceRequest,
          failedMapRequestLimit);
      setRequestLimit(PRIORITY_MAP, mapResourceRequest, normalMapRequestLimit);
      setRequestLimit(PRIORITY_OPPORTUNISTIC_MAP, mapResourceRequest,
          normalMapRequestLimit);
    }

    int numScheduledReduces = scheduledRequests.reduces.size();
    if (maxRunningReduces > 0 && numScheduledReduces > 0 &&
        getJob().getTotalReduces() > maxRunningReduces) {
      int maxRequestedReduces = Math.max(0,
          maxRunningReduces - assignedRequests.reduces.size());
      int reduceRequestLimit = Math.min(maxRequestedReduces,
          numScheduledReduces);
      setRequestLimit(PRIORITY_REDUCE, reduceResourceRequest,
          reduceRequestLimit);
    }
  }

  private boolean canAssignMaps() {
    return (maxRunningMaps <= 0
        || assignedRequests.maps.size() < maxRunningMaps);
  }

  private boolean canAssignReduces() {
    return (maxRunningReduces <= 0
        || assignedRequests.reduces.size() < maxRunningReduces);
  }

  private void updateAMRMToken(Token token) throws IOException {
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        new org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>(token
          .getIdentifier().array(), token.getPassword().array(), new Text(
          token.getKind()), new Text(token.getService()));
    UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
    currentUGI.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(getConfig()));
  }

  @VisibleForTesting
  public TaskAttemptEvent createContainerFinishedEvent(ContainerStatus cont,
      TaskAttemptId attemptId) {
    TaskAttemptEvent event;
    switch (cont.getExitStatus()) {
    case ContainerExitStatus.ABORTED:
    case ContainerExitStatus.PREEMPTED:
    case ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER:
      // killed by YARN
      event = new TaskAttemptEvent(attemptId, TaskAttemptEventType.TA_KILL);
      break;
    default:
      event = new TaskAttemptEvent(attemptId,
          TaskAttemptEventType.TA_CONTAINER_COMPLETED);
    }
    return event;
  }
  
  @SuppressWarnings("unchecked")
  private void handleUpdatedNodes(AllocateResponse response) {
    // send event to the job about on updated nodes
    List<NodeReport> updatedNodes = response.getUpdatedNodes();
    if (!updatedNodes.isEmpty()) {

      // send event to the job to act upon completed tasks
      eventHandler.handle(new JobUpdatedNodesEvent(getJob().getID(),
          updatedNodes));

      // act upon running tasks
      HashSet<NodeId> unusableNodes = new HashSet<NodeId>();
      for (NodeReport nr : updatedNodes) {
        NodeState nodeState = nr.getNodeState();
        if (nodeState.isUnusable()) {
          unusableNodes.add(nr.getNodeId());
        }
      }
      for (int i = 0; i < 2; ++i) {
        HashMap<TaskAttemptId, Container> taskSet = i == 0 ? assignedRequests.maps
            : assignedRequests.reduces;
        // kill running containers
        for (Map.Entry<TaskAttemptId, Container> entry : taskSet.entrySet()) {
          TaskAttemptId tid = entry.getKey();
          NodeId taskAttemptNodeId = entry.getValue().getNodeId();
          if (unusableNodes.contains(taskAttemptNodeId)) {
            LOG.info("Killing taskAttempt:" + tid
                + " because it is running on unusable node:"
                + taskAttemptNodeId);
            // If map, reschedule next task attempt.
            boolean rescheduleNextAttempt = (i == 0) ? true : false;
            eventHandler.handle(new TaskAttemptKillEvent(tid,
                "TaskAttempt killed because it ran on unusable node"
                    + taskAttemptNodeId, rescheduleNextAttempt));
          }
        }
      }
    }
  }

  void handleJobPriorityChange(AllocateResponse response) {
    Priority applicationPriority = response.getApplicationPriority();
    if (null != applicationPriority) {
      Priority priorityFromResponse = Priority
          .newInstance(applicationPriority.getPriority());
      // Update the job priority to Job directly.
      getJob().setJobPriority(priorityFromResponse);
    }
  }

  @Private
  public Resource getResourceLimit() {
    Resource headRoom = getAvailableResources();
    Resource assignedMapResource =
        Resources.multiply(mapResourceRequest, assignedRequests.maps.size());
    Resource assignedReduceResource =
        Resources.multiply(reduceResourceRequest,
          assignedRequests.reduces.size());
    return Resources.add(headRoom,
      Resources.add(assignedMapResource, assignedReduceResource));
  }

  @Private
  @VisibleForTesting
  class ScheduledRequests {
    
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    
    /** Maps from a host to a list of Map tasks with data on the host */
    private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    @VisibleForTesting
    final Map<TaskAttemptId, ContainerRequest> maps =
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    int mapsMod100 = 0;
    int numOpportunisticMapsPercent = 0;

    void setNumOpportunisticMapsPercent(int numMaps) {
      this.numOpportunisticMapsPercent = numMaps;
    }

    int getNumOpportunisticMapsPercent() {
      return this.numOpportunisticMapsPercent;
    }

    @VisibleForTesting
    final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces =
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    boolean remove(TaskAttemptId tId) {
      ContainerRequest req = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        req = maps.remove(tId);
      } else {
        req = reduces.remove(tId);
      }
      
      if (req == null) {
        return false;
      } else {
        decContainerReq(req);
        return true;
      }
    }
    
    ContainerRequest removeReduce() {
      Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
      if (it.hasNext()) {
        Entry<TaskAttemptId, ContainerRequest> entry = it.next();
        it.remove();
        decContainerReq(entry.getValue());
        return entry.getValue();
      }
      return null;
    }
    
    void addMap(ContainerRequestEvent event) {
      ContainerRequest request = null;
      
      if (event.getEarlierAttemptFailed()) {
        earlierFailedMaps.add(event.getAttemptID());
        request =
            new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP,
                mapNodeLabelExpression);
        LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
        // If its an earlier Failed attempt, do not retry as OPPORTUNISTIC
        maps.put(event.getAttemptID(), request);
        addContainerReq(request);
      } else {
        if (mapsMod100 < numOpportunisticMapsPercent) {
          request =
              new ContainerRequest(event, PRIORITY_OPPORTUNISTIC_MAP,
                  mapNodeLabelExpression);
          maps.put(event.getAttemptID(), request);
          addOpportunisticResourceRequest(request.priority, request.capability);
        } else {
          request =
              new ContainerRequest(event, PRIORITY_MAP, mapNodeLabelExpression);
          for (String host : event.getHosts()) {
            LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
            if (list == null) {
              list = new LinkedList<TaskAttemptId>();
              mapsHostMapping.put(host, list);
            }
            list.add(event.getAttemptID());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Added attempt req to host " + host);
            }
          }
          for (String rack : event.getRacks()) {
            LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
            if (list == null) {
              list = new LinkedList<TaskAttemptId>();
              mapsRackMapping.put(rack, list);
            }
            list.add(event.getAttemptID());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Added attempt req to rack " + rack);
            }
          }
          maps.put(event.getAttemptID(), request);
          addContainerReq(request);
        }
        mapsMod100++;
        mapsMod100 %= 100;
      }
    }
    
    
    void addReduce(ContainerRequest req) {
      reduces.put(req.attemptID, req);
      addContainerReq(req);
    }
    
    // this method will change the list of allocatedContainers.
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      int reducePending = reduces.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated.getId()
              + " with priority " + allocated.getPriority() + " to NM "
              + allocated.getNodeId());
        }
        
        // check if allocated container meets memory requirements 
        // and whether we have any scheduled tasks that need 
        // a container to be assigned
        boolean isAssignable = true;
        Priority priority = allocated.getPriority();
        Resource allocatedResource = allocated.getResource();
        if (PRIORITY_FAST_FAIL_MAP.equals(priority) 
            || PRIORITY_MAP.equals(priority)
            || PRIORITY_OPPORTUNISTIC_MAP.equals(priority)) {
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              mapResourceRequest, getSchedulerResourceTypes()) <= 0
              || maps.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a map as either "
                + " container memory less than required " + mapResourceRequest
                + " or no pending map tasks - maps.isEmpty=" 
                + maps.isEmpty()); 
            isAssignable = false; 
          }
        } 
        else if (PRIORITY_REDUCE.equals(priority)) {
          if (ResourceCalculatorUtils.computeAvailableContainers(allocatedResource,
              reduceResourceRequest, getSchedulerResourceTypes()) <= 0
              || (reducePending <= 0)) {
            LOG.info("Cannot assign container " + allocated
                + " for a reduce as either "
                + " container memory less than required " + reduceResourceRequest
                + " or no pending reduce tasks.");
            isAssignable = false;
          } else {
            reducePending--;
          }
        } else {
          LOG.warn("Container allocated at unwanted priority: " + priority + 
              ". Returning to RM...");
          isAssignable = false;
        }
        
        if(!isAssignable) {
          // release container if we could not assign it 
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
        
        // do not assign if allocated container is on a  
        // blacklisted host
        String allocatedHost = allocated.getNodeId().getHost();
        if (isNodeBlacklisted(allocatedHost)) {
          // we need to request for a new container 
          // and release the current one
          LOG.info("Got allocated container on a blacklisted "
              + " host "+allocatedHost
              +". Releasing container " + allocated);

          // find the request matching this allocated container 
          // and replace it with a new one 
          ContainerRequest toBeReplacedReq = 
              getContainerReqToReplace(allocated);
          if (toBeReplacedReq != null) {
            LOG.info("Placing a new container request for task attempt " 
                + toBeReplacedReq.attemptID);
            ContainerRequest newReq = 
                getFilteredContainerRequest(toBeReplacedReq);
            decContainerReq(toBeReplacedReq);
            if (toBeReplacedReq.attemptID.getTaskId().getTaskType() ==
                TaskType.MAP) {
              maps.put(newReq.attemptID, newReq);
            }
            else {
              reduces.put(newReq.attemptID, newReq);
            }
            addContainerReq(newReq);
          }
          else {
            LOG.info("Could not map allocated container to a valid request."
                + " Releasing allocated container " + allocated);
          }
          
          // release container if we could not assign it 
          containerNotAssigned(allocated);
          it.remove();
          continue;
        }
      }

      assignContainers(allocatedContainers);
       
      // release container if we could not assign it 
      it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        LOG.info("Releasing unassigned container " + allocated);
        containerNotAssigned(allocated);
      }
    }
    
    @SuppressWarnings("unchecked")
    private void containerAssigned(Container allocated, 
                                    ContainerRequest assigned) {
      // Update resource requests
      decContainerReq(assigned);

      // send the container-assigned event to task attempt
      eventHandler.handle(new TaskAttemptContainerAssignedEvent(
          assigned.attemptID, allocated, applicationACLs));

      assignedRequests.add(allocated, assigned.attemptID);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Assigned container (" + allocated + ") "
            + " to task " + assigned.attemptID + " on node "
            + allocated.getNodeId().toString());
      }
    }
    
    private void containerNotAssigned(Container allocated) {
      containersReleased++;
      pendingRelease.add(allocated.getId());
      release(allocated.getId());      
    }
    
    private ContainerRequest assignWithoutLocality(Container allocated) {
      ContainerRequest assigned = null;
      
      Priority priority = allocated.getPriority();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Assigning container " + allocated + " to fast fail map");
        assigned = assignToFailedMap(allocated);
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to reduce");
        }
        assigned = assignToReduce(allocated);
      }
        
      return assigned;
    }
        
    private void assignContainers(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      while (it.hasNext()) {
        Container allocated = it.next();
        ContainerRequest assigned = assignWithoutLocality(allocated);
        if (assigned != null) {
          containerAssigned(allocated, assigned);
          it.remove();
        }
      }

      assignMapsWithLocality(allocatedContainers);
    }
    
    private ContainerRequest getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequest toBeReplaced = null;
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
        Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
        while (toBeReplaced == null && iter.hasNext()) {
          toBeReplaced = maps.get(iter.next());
        }
        LOG.info("Found replacement: " + toBeReplaced);
        return toBeReplaced;
      }
      else if (PRIORITY_MAP.equals(priority)
          || PRIORITY_OPPORTUNISTIC_MAP.equals(priority)) {
        LOG.info("Replacing MAP container " + allocated.getId());
        // allocated container was for a map
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        if (list != null && list.size() > 0) {
          TaskAttemptId tId = list.removeLast();
          if (maps.containsKey(tId)) {
            toBeReplaced = maps.remove(tId);
          }
        }
        else {
          TaskAttemptId tId = maps.keySet().iterator().next();
          toBeReplaced = maps.remove(tId);          
        }        
      }
      else if (PRIORITY_REDUCE.equals(priority)) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        toBeReplaced = reduces.remove(tId);    
      }
      LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }
    
    
    @SuppressWarnings("unchecked")
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0
          && canAssignMaps()) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();      
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          eventHandler.handle(jce);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    private ContainerRequest assignToReduce(Container allocated) {
      ContainerRequest assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0 && canAssignReduces()) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
    
    @SuppressWarnings("unchecked")
    private void assignMapsWithLocality(List<Container> allocatedContainers) {
      // try to assign to all nodes first to match node local
      Iterator<Container> it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0 && canAssignMaps()){
        Container allocated = it.next();        
        Priority priority = allocated.getPriority();
        assert (PRIORITY_MAP.equals(priority)
            || PRIORITY_OPPORTUNISTIC_MAP.equals(priority));
        if (!PRIORITY_OPPORTUNISTIC_MAP.equals(priority)) {
          // "if (maps.containsKey(tId))" below should be almost always true.
          // hence this while loop would almost always have O(1) complexity
          String host = allocated.getNodeId().getHost();
          LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
          while (list != null && list.size() > 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Host matched to the request list " + host);
            }
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              ContainerRequest assigned = maps.remove(tId);
              containerAssigned(allocated, assigned);
              it.remove();
              JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId()
                      .getJobId());
              jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
              eventHandler.handle(jce);
              hostLocalAssigned++;
              if (LOG.isDebugEnabled()) {
                LOG.debug("Assigned based on host match " + host);
              }
              break;
            }
          }
        }
      }
      
      // try to match all rack local
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0 && canAssignMaps()){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert (PRIORITY_MAP.equals(priority)
            || PRIORITY_OPPORTUNISTIC_MAP.equals(priority));
        if (!PRIORITY_OPPORTUNISTIC_MAP.equals(priority)) {
          // "if (maps.containsKey(tId))" below should be almost always true.
          // hence this while loop would almost always have O(1) complexity
          String host = allocated.getNodeId().getHost();
          String rack = RackResolver.resolve(host).getNetworkLocation();
          LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
          while (list != null && list.size() > 0) {
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              ContainerRequest assigned = maps.remove(tId);
              containerAssigned(allocated, assigned);
              it.remove();
              JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId()
                      .getJobId());
              jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
              eventHandler.handle(jce);
              rackLocalAssigned++;
              if (LOG.isDebugEnabled()) {
                LOG.debug("Assigned based on rack match " + rack);
              }
              break;
            }
          }
        }
      }
      
      // assign remaining
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0 && canAssignMaps()){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert (PRIORITY_MAP.equals(priority)
            || PRIORITY_OPPORTUNISTIC_MAP.equals(priority));
        TaskAttemptId tId = maps.keySet().iterator().next();
        ContainerRequest assigned = maps.remove(tId);
        containerAssigned(allocated, assigned);
        it.remove();
        JobCounterUpdateEvent jce =
          new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
        eventHandler.handle(jce);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigned based on * match");
        }
      }
    }
  }

  @Private
  @VisibleForTesting
  class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    @VisibleForTesting
    final LinkedHashMap<TaskAttemptId, Container> maps =
      new LinkedHashMap<TaskAttemptId, Container>();
    @VisibleForTesting
    final LinkedHashMap<TaskAttemptId, Container> reduces =
      new LinkedHashMap<TaskAttemptId, Container>();
    @VisibleForTesting
    final Set<TaskAttemptId> preemptionWaitingReduces =
      new HashSet<TaskAttemptId>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, container);
      } else {
        reduces.put(tId, container);
      }
    }

    @SuppressWarnings("unchecked")
    void preemptReduce(int toPreempt) {
      List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>
        (reduces.keySet());
      //sort reduces on progress
      Collections.sort(reduceList,
          new Comparator<TaskAttemptId>() {
        @Override
        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
          return Float.compare(
              getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress(),
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress());
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptKillEvent(id, RAMPDOWN_DIAGNOSTIC));
      }
    }
    
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId).getId();
      } else {
        containerId = reduces.remove(tId).getId();
        if (containerId != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (containerId != null) {
        containerToAttemptMap.remove(containerId);
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    ContainerId get(TaskAttemptId tId) {
      Container taskContainer;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        taskContainer = maps.get(tId);
      } else {
        taskContainer = reduces.get(tId);
      }

      if (taskContainer == null) {
        return null;
      } else {
        return taskContainer.getId();
      }
    }
  }

  private class ScheduleStats {
    int numPendingReduces;
    int numScheduledMaps;
    int numScheduledReduces;
    int numAssignedMaps;
    int numAssignedReduces;
    int numCompletedMaps;
    int numCompletedReduces;
    int numContainersAllocated;
    int numContainersReleased;

    public void updateAndLogIfChanged(String msgPrefix) {
      boolean changed = false;

      // synchronized to fix findbug warnings
      synchronized (RMContainerAllocator.this) {
        changed |= (numPendingReduces != pendingReduces.size());
        numPendingReduces = pendingReduces.size();
        changed |= (numScheduledMaps != scheduledRequests.maps.size());
        numScheduledMaps = scheduledRequests.maps.size();
        changed |= (numScheduledReduces != scheduledRequests.reduces.size());
        numScheduledReduces = scheduledRequests.reduces.size();
        changed |= (numAssignedMaps != assignedRequests.maps.size());
        numAssignedMaps = assignedRequests.maps.size();
        changed |= (numAssignedReduces != assignedRequests.reduces.size());
        numAssignedReduces = assignedRequests.reduces.size();
        changed |= (numCompletedMaps != getJob().getCompletedMaps());
        numCompletedMaps = getJob().getCompletedMaps();
        changed |= (numCompletedReduces != getJob().getCompletedReduces());
        numCompletedReduces = getJob().getCompletedReduces();
        changed |= (numContainersAllocated != containersAllocated);
        numContainersAllocated = containersAllocated;
        changed |= (numContainersReleased != containersReleased);
        numContainersReleased = containersReleased;
      }

      if (changed) {
        log(msgPrefix);
      }
    }

    public void log(String msgPrefix) {
        LOG.info(msgPrefix + "PendingReds:" + numPendingReduces +
        " ScheduledMaps:" + numScheduledMaps +
        " ScheduledReds:" + numScheduledReduces +
        " AssignedMaps:" + numAssignedMaps +
        " AssignedReds:" + numAssignedReduces +
        " CompletedMaps:" + numCompletedMaps +
        " CompletedReds:" + numCompletedReduces +
        " ContAlloc:" + numContainersAllocated +
        " ContRel:" + numContainersReleased +
        " HostLocal:" + hostLocalAssigned +
        " RackLocal:" + rackLocalAssigned);
    }
  }

  static class PreemptionContext extends AMPreemptionPolicy.Context {
    final AssignedRequests reqs;

    PreemptionContext(AssignedRequests reqs) {
      this.reqs = reqs;
    }
    @Override
    public TaskAttemptId getTaskAttempt(ContainerId container) {
      return reqs.get(container);
    }

    @Override
    public List<Container> getContainers(TaskType t){
      if(TaskType.REDUCE.equals(t))
        return new ArrayList<Container>(reqs.reduces.values());
      if(TaskType.MAP.equals(t))
        return new ArrayList<Container>(reqs.maps.values());
      return null;
    }

  }

}
