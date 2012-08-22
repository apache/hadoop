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

package org.apache.hadoop.mapreduce.v2.app2.rm;

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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventKillRequest;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerAssignTAEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerMap;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerState;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerTASucceededEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventTaskAttemptEnded;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventTaskAttemptSucceeded;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends AbstractService
    implements ContainerAllocator {

// TODO XXX: Factor in MAPREDUCE-4437. Reduce scheduling needs to be looked into IAC

  static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  private static final Priority PRIORITY_FAST_FAIL_MAP;
  private static final Priority PRIORITY_REDUCE;
  private static final Priority PRIORITY_MAP;

  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
  }
  
  private final AppContext appContext;
  private final Clock clock;
  private Job job;
  private final JobId jobId;
  private final RMContainerRequestor requestor;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AMContainerMap containerMap;
  
  //TODO XXX Make Configurable.
  // Run the scheduler if it hasn't run for this interval.
  private long scheduleInterval = 1000l;
  
  Timer scheduleTimer;
  ScheduleTimerTask scheduleTimerTask;
  private long lastScheduleTime = 0l;
  
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
  private final LinkedList<ContainerRequestInfo> pendingReduces = 
    new LinkedList<ContainerRequestInfo>();

  //holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();
  
  // Populated whenever a container is available - from any source.
  private List<ContainerId> availableContainerIds = new LinkedList<ContainerId>();
  
  private final Map<TaskAttemptId, AMSchedulerTALaunchRequestEvent> 
      attemptToLaunchRequestMap = new HashMap<TaskAttemptId, AMSchedulerTALaunchRequestEvent>();
  
  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  
  private boolean recalculateReduceSchedule = false;
  private int mapResourceReqt;//memory
  private int reduceResourceReqt;//memory
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  private float reduceSlowStart = 0;
  
  // TODO XXX: Remove this. Temporary for testing.
  private boolean shouldReUse;

  BlockingQueue<AMSchedulerEvent> eventQueue
    = new LinkedBlockingQueue<AMSchedulerEvent>();

  @SuppressWarnings("rawtypes")
  public RMContainerAllocator(RMContainerRequestor requestor,
      AppContext appContext) {
    super("RMContainerAllocator");
    this.requestor = requestor;
    this.appContext = appContext;
    this.clock = appContext.getClock();
    this.eventHandler = appContext.getEventHandler();
    ApplicationId appId = appContext.getApplicationID();
    // JobId should not be required here. 
    // Currently used for error notification, clc construction, etc. Should not be  
    JobID id = TypeConverter.fromYarn(appId);
    JobId jobId = TypeConverter.toYarn(id);
    this.jobId = jobId;
    
    this.containerMap = appContext.getAllContainers();
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    shouldReUse = conf.getBoolean("am.scheduler.shouldReuse", false);
    LOG.info("XXX: ShouldReUse: " + shouldReUse);
    RackResolver.init(conf);
  }

  @Override
  public void start() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM.
            eventHandler.handle(new JobEvent(job.getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    
    scheduleTimer = new Timer("AMSchedulerTimer", true);
    scheduleTimerTask = new ScheduleTimerTask();
    scheduleTimer.scheduleAtFixedRate(scheduleTimerTask, scheduleInterval, scheduleInterval);
    this.job = appContext.getJob(jobId);
    
    super.start();
  }

  @Override
  public void stop() {
    this.stopEventHandling = true;
    if (eventHandlingThread != null)
      eventHandlingThread.interrupt();
    super.stop();
    if (scheduleTimerTask != null) {
      scheduleTimerTask.stop();
    }
    LOG.info("Final Scheduler Stats: " + getStat());
  }
  
  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    eventHandler.handle(event);
  }
  
  private Job getJob() {
    return this.job;
  }

  private class ScheduleTimerTask extends TimerTask {
    private volatile boolean shouldRun = true;

    @Override
    public void run() {
      // TODO XXX XXX: Reduces are not being shceduled. Forcing them via this for now. Figure out when reduce schedule should be recomputed.
      // TODO XXX. Does this need to be stopped before the service stop()
      if (clock.getTime() - lastScheduleTime > scheduleInterval && shouldRun) {
        handle(new AMSchedulerEventContainersAllocated(
            Collections.<ContainerId> emptyList(), true));
        // Sending a false. Just try to flush available containers.
        // The decision to schedule reduces may need to be based on available containers.
      }
    }

    public void stop() {
      shouldRun = false;
      this.cancel();
    }
  }

  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  public void handle(AMSchedulerEvent event) {
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
      throw new YarnException(e);
    }
  }
  
  // TODO XXX: Before and after makeRemoteRequest statistics.

protected synchronized void handleEvent(AMSchedulerEvent sEvent) {
    
    switch(sEvent.getType()) {
    // TODO XXX: recalculateReduceSchedule may need to bet set on other events - not just containerAllocated.
    case S_TA_LAUNCH_REQUEST:
      handleTaLaunchRequest((AMSchedulerTALaunchRequestEvent) sEvent);
      // Add to queue of pending tasks.
      break;
    case S_TA_STOP_REQUEST: //Effectively means a failure.
      handleTaStopRequest((AMSchedulerTAStopRequestEvent)sEvent);
      break;
    case S_TA_SUCCEEDED:
      handleTaSucceededRequest((AMSchedulerTASucceededEvent)sEvent);
      break;
    case S_TA_ENDED:
      // TODO XXX XXX: Not generated yet. Depends on E05 etc. Also look at TaskAttempt transitions.
      break;
    case S_CONTAINERS_ALLOCATED:
      handleContainersAllocated((AMSchedulerEventContainersAllocated) sEvent);
      break;
    // No HEALTH_CHANGE events. Not modifying the table based on these.
    case S_CONTAINER_COMPLETED: // Maybe use this to reschedule reduces ?
      break;
    // Node State Change Event. May want to withdraw requests related to the node, and put
    // in fresh requests.
      
    // Similarly for the case where a  node gets blacklisted.
    default:
      break;
    }
  }

  private void handleTaLaunchRequest(AMSchedulerTALaunchRequestEvent event) {
    // Add to queue of pending tasks.
    LOG.info("Processing the event " + event.toString());
    attemptToLaunchRequestMap.put(event.getAttemptID(), event);
    if (event.getAttemptID().getTaskId().getTaskType() == TaskType.MAP) {
      mapResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.MAP, mapResourceReqt);
      event.getCapability().setMemory(mapResourceReqt);
      scheduledRequests.addMap(event);
    } else { // Reduce
      reduceResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.REDUCE, reduceResourceReqt);
      event.getCapability().setMemory(reduceResourceReqt);
      if (event.isRescheduled()) {
        pendingReduces.addFirst(new ContainerRequestInfo(new ContainerRequest(
            event, PRIORITY_REDUCE), event));
      } else {
        pendingReduces.addLast(new ContainerRequestInfo(new ContainerRequest(
            event, PRIORITY_REDUCE), event));
      }
    }
  }

  private void handleTaStopRequest(AMSchedulerTAStopRequestEvent event) {
    LOG.info("Processing the event " + event.toString());
    TaskAttemptId aId = event.getAttemptID();
    attemptToLaunchRequestMap.remove(aId);
    // XXX Not very efficient. List / check type.
    boolean removed = pendingReduces.remove(aId);
    if (!removed) {
      removed = scheduledRequests.remove(aId);
      if (!removed) {
        // Maybe assigned.
        ContainerId containerId = assignedRequests.getContainerId(aId);
        if (containerId != null) {
          // Ask the container to stop.
          sendEvent(new AMContainerEvent(containerId, AMContainerEventType.C_STOP_REQUEST));
          // Inform the Node - the task has asked to be STOPPED / has already stopped.
          sendEvent(new AMNodeEventTaskAttemptEnded(containerMap.get(containerId).getContainer().getNodeId(), containerId, event.getAttemptID(), event.failed()));
        } else {
          LOG.warn("Received a STOP request for absent taskAttempt: " + event.getAttemptID());
        }
      }
    }
  }
  
  private void handleTaSucceededRequest(AMSchedulerTASucceededEvent event) {
    // TODO XXX Part of re-use.
    // TODO XXX Also may change after state machines are finalized.
    // XXX: Maybe send the request to the task before sending it to the scheduler - the scheduler can then
    //  query the task to figure out whether the taskAttempt is the successfulAttempt - and whether to count it towards the reduce ramp up.
    //  Otherwise -> Job.getCompletedMaps() - will give an out of date picture, since the scheduler event will always be generated before the TaskCompleted event to the job.
    
    LOG.info("Processing the event " + event.toString());
    attemptToLaunchRequestMap.remove(event.getAttemptID());
    ContainerId containerId = assignedRequests.remove(event.getAttemptID());
    if (containerId != null) { // TODO Should not be null. Confirm.
      sendEvent(new AMContainerTASucceededEvent(containerId, event.getAttemptID()));
      sendEvent(new AMNodeEventTaskAttemptSucceeded(containerMap.get(containerId).getContainer().getNodeId(), containerId, event.getAttemptID()));
      containerAvailable(containerId);
    } else {
      LOG.warn("Received TaskAttemptSucceededEvent for unmapped TaskAttempt: "
          + event.getAttemptID() + ". Full event: " + event);
    }
  }
  
  // TODO XXX: Deal with node blacklisting.
  
  private void handleContainersAllocated(AMSchedulerEventContainersAllocated event) {
    // TODO XXX: Maybe have an event from the Requestor -> saying AllocationChanged -> listOfNewContainers, listOfFinishedContainers (finished containers goes to Containers directly, but should always come in from the RM)
    // TODO XXX
    /*
     * Start allocating containers. Match requests to capabilities. 
     * Send out Container_START / Container_TA_ASSIGNED events.
     */
    // TODO XXX: Logging of the assigned containerIds.

    LOG.info("Processing the event " + event.toString());
    availableContainerIds.addAll(event.getContainerIds());
    if (event.didHeadroomChange() || event.getContainerIds().size() > 0) {
      // TODO XXX -> recaulculateReduceSchedule in case of released containers
      // .... would imply CONTAINER_COMPLETED messages are required by the Scheduler.
      // ContainerReleased == headroomChange ?
      recalculateReduceSchedule = true;
    }
    schedule();
  }

  
  
  // TODO Override for re-use.
  protected synchronized void assignContainers() {
    if (availableContainerIds.size() > 0) {
      LOG.info("Before Assign: " + getStat());
      scheduledRequests.assign(availableContainerIds);
      availableContainerIds.clear();
      LOG.info("After Assign: " + getStat());
      
    }
  }
  
  // TODO Override for re-use.
  protected void requestContainers() {
    // Nothign else here. All requests are sent to the Requester immediately.
    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces(
          getJob().getTotalMaps(), getJob().getCompletedMaps(),
          scheduledRequests.maps.size(), scheduledRequests.reduces.size(), 
          assignedRequests.maps.size(), assignedRequests.reduces.size(),
          mapResourceReqt, reduceResourceReqt,
          pendingReduces.size(), 
          maxReduceRampupLimit, reduceSlowStart);
      recalculateReduceSchedule = false;
    }
  }

  /* availableContainerIds contains the currently available containers.
   * Should be cleared appropriately. 
   */
  private synchronized void schedule() {
    assignContainers();
    requestContainers();
  }

  protected void containerAvailable(ContainerId containerId) {
    if (shouldReUse) {
      availableContainerIds.add(containerId);
      handle(new AMSchedulerEventContainersAllocated(
          Collections.<ContainerId> emptyList(), true));
    } else {
      sendEvent(new AMContainerEvent(containerId,
          AMContainerEventType.C_STOP_REQUEST));
    }
  }

  // TODO Override for container re-use.
//  protected void containerAvailable(ContainerId containerId) {
//    // For now releasing the container.
//    // allocatedContainerIds.add(containerId);
//    sendEvent(new AMContainerEvent(containerId,
//        AMContainerEventType.C_STOP_REQUEST));
//    // XXX A release should not be required. Only required when a container
//    // cannot be assigned, or if there's an explicit request to stop the container,
//    // in which case the release request will go out from the container itself.
//  }

  @SuppressWarnings("unchecked")
  private int maybeComputeNormalizedRequestForType(
      AMSchedulerTALaunchRequestEvent event, TaskType taskType,
      int prevComputedSize) {
    if (prevComputedSize == 0) {
      int supportedMaxContainerCapability = requestor
          .getMaxContainerCapability().getMemory();
      prevComputedSize = event.getCapability().getMemory();
      int minSlotMemSize = requestor.getMinContainerCapability().getMemory();
      prevComputedSize = (int) Math.ceil((float) prevComputedSize
          / minSlotMemSize)
          * minSlotMemSize;
      eventHandler.handle(new JobHistoryEvent(job.getID(),
          new NormalizedResourceEvent(TypeConverter.fromYarn(taskType),
              prevComputedSize)));
      LOG.info(taskType + "ResourceReqt:" + prevComputedSize);
      if (prevComputedSize > supportedMaxContainerCapability) {
        String diagMsg = taskType
            + " capability required is more than the supported "
            + "max container capability in the cluster. Killing the Job. "
            + taskType + "ResourceReqt: " + prevComputedSize
            + " maxContainerCapability:" + supportedMaxContainerCapability;
        LOG.info(diagMsg);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(job.getID(), diagMsg));
        eventHandler.handle(new JobEvent(job.getID(), JobEventType.JOB_KILL));
      }
    }
    return prevComputedSize;
  }

  private void preemptReducesIfNeeded() {
    if (reduceResourceReqt == 0) {
      return; //no reduces
    }
    //check if reduces have taken over the whole cluster and there are 
    //unassigned maps
    if (scheduledRequests.maps.size() > 0) {
      int memLimit = getMemLimit();
      int availableMemForMap = memLimit - ((assignedRequests.reduces.size() -
          assignedRequests.preemptionWaitingReduces.size()) * reduceResourceReqt);
      //availableMemForMap must be sufficient to run atleast 1 map
      if (availableMemForMap < mapResourceReqt) {
        //to make sure new containers are given to maps and not reduces
        //ramp down all scheduled reduces if any
        //(since reduces are scheduled at higher priority than maps)
        LOG.info("Ramping down all scheduled reduces:" + scheduledRequests.reduces.size());
        for (ContainerRequestInfo req : scheduledRequests.reduces.values()) {
          pendingReduces.add(req);
        }
        scheduledRequests.reduces.clear();
        
        //preempt for making space for atleast one map
        int premeptionLimit = Math.max(mapResourceReqt, 
            (int) (maxReducePreemptionLimit * memLimit));
        
        int preemptMem = Math.min(scheduledRequests.maps.size() * mapResourceReqt, 
            premeptionLimit);
        
        int toPreempt = (int) Math.ceil((float) preemptMem/reduceResourceReqt);
        toPreempt = Math.min(toPreempt, assignedRequests.reduces.size());
        
        LOG.info("Going to preempt " + toPreempt);
        assignedRequests.preemptReduce(toPreempt);
      }
    }
  }
  
  @Private
  public void scheduleReduces(
      int totalMaps, int completedMaps,
      int scheduledMaps, int scheduledReduces,
      int assignedMaps, int assignedReduces,
      int mapResourceReqt, int reduceResourceReqt,
      int numPendingReduces,
      float maxReduceRampupLimit, float reduceSlowStart) {
    
    if (numPendingReduces == 0) {
      return;
    }
    
    LOG.info("Recalculating schedule...");
    
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
    
    int netScheduledMapMem = 
        (scheduledMaps + assignedMaps) * mapResourceReqt;

    int netScheduledReduceMem = 
        (scheduledReduces + assignedReduces) * reduceResourceReqt;

    int finalMapMemLimit = 0;
    int finalReduceMemLimit = 0;
    
    // ramp up the reduces based on completed map percentage
    int totalMemLimit = getMemLimit();
    int idealReduceMemLimit = 
        Math.min(
            (int)(completedMapPercent * totalMemLimit),
            (int) (maxReduceRampupLimit * totalMemLimit));
    int idealMapMemLimit = totalMemLimit - idealReduceMemLimit;

    // check if there aren't enough maps scheduled, give the free map capacity
    // to reduce
    if (idealMapMemLimit > netScheduledMapMem) {
      int unusedMapMemLimit = idealMapMemLimit - netScheduledMapMem;
      finalReduceMemLimit = idealReduceMemLimit + unusedMapMemLimit;
      finalMapMemLimit = totalMemLimit - finalReduceMemLimit;
    } else {
      finalMapMemLimit = idealMapMemLimit;
      finalReduceMemLimit = idealReduceMemLimit;
    }
    
    LOG.info("completedMapPercent " + completedMapPercent +
        " totalMemLimit:" + totalMemLimit +
        " finalMapMemLimit:" + finalMapMemLimit +
        " finalReduceMemLimit:" + finalReduceMemLimit + 
        " netScheduledMapMem:" + netScheduledMapMem +
        " netScheduledReduceMem:" + netScheduledReduceMem);
    
    int rampUp = 
        (finalReduceMemLimit - netScheduledReduceMem) / reduceResourceReqt;
    
    if (rampUp > 0) {
      rampUp = Math.min(rampUp, numPendingReduces);
      LOG.info("Ramping up " + rampUp);
      rampUpReduces(rampUp);
    } else if (rampUp < 0){
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledReduces);
      LOG.info("Ramping down " + rampDown);
      rampDownReduces(rampDown);
    }
  }

  @Private
  public void scheduleAllReduces() {
    for (ContainerRequestInfo req : pendingReduces) {
      scheduledRequests.addReduce(req);
    }
    pendingReduces.clear();
  }
  
  @Private
  public void rampUpReduces(int rampUp) {
    //more reduce to be scheduled
    for (int i = 0; i < rampUp; i++) {
      ContainerRequestInfo request = pendingReduces.removeFirst();
      scheduledRequests.addReduce(request);
    }
  }
  
  @Private
  public void rampDownReduces(int rampDown) {
    //remove from the scheduled and move back to pending
    for (int i = 0; i < rampDown; i++) {
      ContainerRequestInfo request = scheduledRequests.removeReduce();
      pendingReduces.add(request);
    }
  }
  
  /**
   * Synchronized to avoid findbugs warnings
   */
  private synchronized String getStat() {
    return "PendingReduces:" + pendingReduces.size() +
        " ScheduledMaps:" + scheduledRequests.maps.size() +
        " ScheduledReduces:" + scheduledRequests.reduces.size() +
        " AssignedMaps:" + assignedRequests.maps.size() + 
        " AssignedReduces:" + assignedRequests.reduces.size() +
        " completedMaps:" + getJob().getCompletedMaps() + 
        " completedReduces:" + getJob().getCompletedReduces() +
        " containersAllocated:" + containersAllocated +
        " containersReleased:" + containersReleased +
        " hostLocalAssigned:" + hostLocalAssigned + 
        " rackLocalAssigned:" + rackLocalAssigned +
        " availableResources(headroom):" + requestor.getAvailableResources();
  }



  // TODO XXX XXX Get rid of the JobUpdatedNodesEvent. Taken care of by Nodes.
  


  @Private
  public int getMemLimit() {
    int headRoom = requestor.getAvailableResources() != null ? requestor.getAvailableResources().getMemory() : 0;
    return headRoom + assignedRequests.maps.size() * mapResourceReqt + 
       assignedRequests.reduces.size() * reduceResourceReqt;
  }
  
  private class ScheduledRequests {
    
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    
    /** Maps from a host to a list of Map tasks with data on the host */
    private final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();
    private final Map<TaskAttemptId, ContainerRequestInfo> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerRequestInfo>();
    
    private final LinkedHashMap<TaskAttemptId, ContainerRequestInfo> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerRequestInfo>();
    
    
    boolean remove(TaskAttemptId tId) {
      ContainerRequestInfo req = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        req = maps.remove(tId);
      } else {
        req = reduces.remove(tId);
      }
      // TODO XXX: Remove from mapsHostMapping and mapsRackMapping
      
      if (req == null) {
        return false;
      } else {
        requestor.decContainerReq(req.getContainerRequest());
        return true;
      }
    }
    
    ContainerRequestInfo removeReduce() {
      Iterator<Entry<TaskAttemptId, ContainerRequestInfo>> it = reduces.entrySet().iterator();
      if (it.hasNext()) {
        Entry<TaskAttemptId, ContainerRequestInfo> entry = it.next();
        it.remove();
        requestor.decContainerReq(entry.getValue().getContainerRequest());
        return entry.getValue();
      }
      return null;
    }
    
    void addMap(AMSchedulerTALaunchRequestEvent event) {
      ContainerRequest request = null;
      
      if (event.isRescheduled()) {
        earlierFailedMaps.add(event.getAttemptID());
        request = new ContainerRequest(event, PRIORITY_FAST_FAIL_MAP);
        LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
      } else {
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
       for (String rack: event.getRacks()) {
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
       request = new ContainerRequest(event, PRIORITY_MAP);
      }
//      ContainerRequestInfo csInfo = new ContainerRequestInfo(request, event.getAttemptID());
      maps.put(event.getAttemptID(), new ContainerRequestInfo(request, event));
      requestor.addContainerReq(request);
    }
    
    
    void addReduce(ContainerRequestInfo req) {
      reduces.put(req.getAttemptId(), req);
      requestor.addContainerReq(req.getContainerRequest());
    }
    
    @SuppressWarnings("unchecked")
    // TODO XXX: Simplify this entire code path. Split into functions.
    private void assign(List<ContainerId> allocatedContainerIds) {
      Iterator<ContainerId> it = allocatedContainerIds.iterator();
      LOG.info("Got allocated containers " + allocatedContainerIds.size());
      containersAllocated += allocatedContainerIds.size();
      while (it.hasNext()) {
        ContainerId containerId = it.next();
        Container allocated = containerMap.get(containerId).getContainer();
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
        int allocatedMemory = allocated.getResource().getMemory();
        if (PRIORITY_FAST_FAIL_MAP.equals(priority) 
            || PRIORITY_MAP.equals(priority)) {
          if (allocatedMemory < mapResourceReqt
              || maps.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a map as either "
                + " container memory less than required " + mapResourceReqt
                + " or no pending map tasks - maps.isEmpty=" 
                + maps.isEmpty()); 
            isAssignable = false; 
          }
        } 
        else if (PRIORITY_REDUCE.equals(priority)) {
          if (allocatedMemory < reduceResourceReqt
              || reduces.isEmpty()) {
            LOG.info("Cannot assign container " + allocated 
                + " for a reduce as either "
                + " container memory less than required " + reduceResourceReqt
                + " or no pending reduce tasks - reduces.isEmpty=" 
                + reduces.isEmpty()); 
            isAssignable = false;
          }
        }          
        
//        boolean blackListed = false;
        boolean nodeUsable = true;
        ContainerRequestInfo assigned = null;
        
        if (isAssignable) {
          // do not assign if allocated container is on a  
          // blacklisted host
          String allocatedHost = allocated.getNodeId().getHost();
          // TODO XXX: Modify the Request table as and when containers are allocated on bad hosts, as against updating the table as soon as a node is blacklisted / lost. 
          // Blakclisted nodes should likely be removed immediately.
          
          // TODO Differentiation between blacklisted versus unusable nodes ?
          //blackListed = appContext.getAllNodes().isHostBlackListed(allocatedHost);
          nodeUsable = appContext.getNode(allocated.getNodeId()).isUsable();
          
          if (!nodeUsable) {
            // we need to request for a new container 
            // and release the current one
            LOG.info("Got allocated container on an unusable "
                + " host "+allocatedHost
                +". Releasing container " + allocated);

            // find the request matching this allocated container 
            // and replace it with a new one 
            ContainerRequestInfo toBeReplacedReq = 
                getContainerReqToReplace(allocated);
            
            // TODO XXX: Requirement here is to be able to figure out the taskAttemptId for which this request was put. If that's being replaced, update corresponding maps with info.
            // Effectively a RequestInfo to attemptId map - or a structure which includes both.
            
            if (toBeReplacedReq != null) {
              LOG.info("Placing a new container request for task attempt " 
                  + toBeReplacedReq.getAttemptId());
              ContainerRequestInfo newReq = 
                  getFilteredContainerRequest(toBeReplacedReq);
              requestor.decContainerReq(toBeReplacedReq.getContainerRequest());
              if (toBeReplacedReq.getAttemptId().getTaskId().getTaskType() ==
                  TaskType.MAP) {
                maps.put(newReq.getAttemptId(), newReq);
              }
              else {
                reduces.put(newReq.getAttemptId(), newReq);
              }
              requestor.addContainerReq(newReq.getContainerRequest());
            }
            else {
              LOG.info("Could not map allocated container to a valid request."
                  + " Releasing allocated container " + allocated);
            }
          }
          else {
            assigned = assign(allocated);
            if (assigned != null) {
              // Update resource requests
              requestor.decContainerReq(assigned.getContainerRequest());

              // TODO Maybe: ApplicationACLs should be populated into the appContext from the RMCommunicator.
              
              
              // TODO XXX: Launch only if not already running.
              // TODO XXX: Change this event to be more specific.
              if (appContext.getContainer(containerId).getState() == AMContainerState.ALLOCATED) {
                eventHandler.handle(new AMContainerLaunchRequestEvent(containerId, attemptToLaunchRequestMap.get(assigned.getAttemptId()), requestor.getApplicationAcls(), getJob().getID()));
              }
              eventHandler.handle(new AMContainerAssignTAEvent(containerId, assigned.getAttemptId(), attemptToLaunchRequestMap.get(assigned.getAttemptId()).getRemoteTask()));
              // TODO XXX: If re-using, get rid of one request.

              assignedRequests.add(allocated, assigned.getAttemptId());

              if (LOG.isDebugEnabled()) {
                LOG.info("Assigned container (" + allocated + ") "
                    + " to task " + assigned.getAttemptId() + " on node "
                    + allocated.getNodeId().toString());
              }
            }
            else {
              //not assigned to any request, release the container
              LOG.info("Releasing unassigned and invalid container " 
                  + allocated + ". RM has gone crazy, someone go look!"
                  + " Hey RM, if you are so rich, go donate to non-profits!");
            }
          }
        }
        
        // release container if it was blacklisted 
        // or if we could not assign it 
        if (!nodeUsable || assigned == null) {
          containersReleased++;
          sendEvent(new AMContainerEvent(containerId, AMContainerEventType.C_STOP_REQUEST));
        }
      }
    }
    
    // TODO XXX: Check whether the node is bad before an assign.
    
    private ContainerRequestInfo assign(Container allocated) {
      ContainerRequestInfo assigned = null;
      
      Priority priority = allocated.getPriority();
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Assigning container " + allocated + " to fast fail map");
        assigned = assignToFailedMap(allocated);
      } else if (PRIORITY_REDUCE.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to reduce");
        }
        assigned = assignToReduce(allocated);
      } else if (PRIORITY_MAP.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to map");
        }
        assigned = assignToMap(allocated);
      } else {
        LOG.warn("Container allocated at unwanted priority: " + priority + 
            ". Returning to RM...");
      }
        
      return assigned;
    }
    
    private ContainerRequestInfo getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequestInfo toBeReplaced = null;
      if (PRIORITY_FAST_FAIL_MAP.equals(priority)) {
        LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
        Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
        while (toBeReplaced == null && iter.hasNext()) {
          toBeReplaced = maps.get(iter.next());
        }
        LOG.info("Found replacement: " + toBeReplaced);
        return toBeReplaced;
      }
      else if (PRIORITY_MAP.equals(priority)) {
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
    private ContainerRequestInfo assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequestInfo assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();      
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.getAttemptId().getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          eventHandler.handle(jce);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    private ContainerRequestInfo assignToReduce(Container allocated) {
      ContainerRequestInfo assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
    
    @SuppressWarnings("unchecked")
    private ContainerRequestInfo assignToMap(Container allocated) {
    //try to assign to maps if present 
      //first by host, then by rack, followed by *
      ContainerRequestInfo assigned = null;
      while (assigned == null && maps.size() > 0) {
        String host = allocated.getNodeId().getHost();
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        while (list != null && list.size() > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Host matched to the request list " + host);
          }
          TaskAttemptId tId = list.removeFirst();
          if (maps.containsKey(tId)) {
            assigned = maps.remove(tId);
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(tId.getTaskId().getJobId());
            // TODO XXX: Move these counter updated to go out from the TaskAttempt.
            jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            hostLocalAssigned++;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on host match " + host);
            }
            break;
          }
        }
        if (assigned == null) {
          String rack = RackResolver.resolve(host).getNetworkLocation();
          list = mapsRackMapping.get(rack);
          while (list != null && list.size() > 0) {
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              assigned = maps.remove(tId);
              JobCounterUpdateEvent jce =
                new JobCounterUpdateEvent(tId.getTaskId().getJobId());
              jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
              eventHandler.handle(jce);
              rackLocalAssigned++;
              if (LOG.isDebugEnabled()) {
                LOG.debug("Assigned based on rack match " + rack);
              }
              break;
            }
          }
          if (assigned == null && maps.size() > 0) {
            TaskAttemptId tId = maps.keySet().iterator().next();
            assigned = maps.remove(tId);
            JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(tId.getTaskId().getJobId());
            jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
            eventHandler.handle(jce);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Assigned based on * match");
            }
            break;
          }
        }
      }
      return assigned;
    }
  }

  private class AssignedRequests {
    private final LinkedHashMap<TaskAttemptId, Container> maps = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final LinkedHashMap<TaskAttemptId, Container> reduces = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final Set<TaskAttemptId> preemptionWaitingReduces = 
      new HashSet<TaskAttemptId>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getId().toString() + " to " + tId);
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
          float p = getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress() -
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress();
          return p >= 0 ? 1 : -1;
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptEventKillRequest(id, "Pre-empting reduce"));
      }
    }
    
    ContainerId getContainerId(TaskAttemptId taId) {
      ContainerId containerId = null;
      if (taId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.get(taId).getId();
      } else {
        containerId = reduces.get(taId).getId();
      }
      return containerId;
    }
    
    // TODO XXX Check where all this is being used.
    // Old code was removing when CONTAINER_COMPLETED was received fromthe RM.
    ContainerId remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId).getId();
      } else {
        containerId = reduces.remove(tId).getId();
        if (containerId != null) {
          // TODO XXX -> Revisit remove(), semantics change.
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      return containerId;
    }
  }

  protected ContainerRequestInfo getFilteredContainerRequest(
      ContainerRequestInfo origRequestInfo) {
    ContainerRequest orig = origRequestInfo.getContainerRequest();
    ArrayList<String> newHosts = new ArrayList<String>();
    for (String host : orig.hosts) {
      if (!appContext.getAllNodes().isHostBlackListed(host)) {
        newHosts.add(host);
      }
    }
    String[] hosts = newHosts.toArray(new String[newHosts.size()]);
    ContainerRequestInfo newReq = new ContainerRequestInfo(
        new ContainerRequest(orig.capability, hosts, orig.racks, orig.priority),
        origRequestInfo.launchRequestEvent);
    return newReq;
  }

  private static class ContainerRequestInfo {
    ContainerRequestInfo(ContainerRequest containerRequest,
        AMSchedulerTALaunchRequestEvent launchRequestEvent) {
      this.containerRequest = containerRequest;
      this.launchRequestEvent = launchRequestEvent;
    }

    ContainerRequest containerRequest;
    AMSchedulerTALaunchRequestEvent launchRequestEvent;

    TaskAttemptId getAttemptId() {
      return launchRequestEvent.getAttemptID();
    }

    ContainerRequest getContainerRequest() {
      return this.containerRequest;
    }
  }
}
