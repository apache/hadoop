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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  private static final Priority PRIORITY_FAST_FAIL_MAP;
  private static final Priority PRIORITY_REDUCE;
  private static final Priority PRIORITY_MAP;

  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
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
  private final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();
  
  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int lastCompletedTasks = 0;
  
  private boolean recalculateReduceSchedule = false;
  private int mapResourceReqt;//memory
  private int reduceResourceReqt;//memory
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  private float reduceSlowStart = 0;
  private long retryInterval;
  private long retrystartTime;

  BlockingQueue<ContainerAllocatorEvent> eventQueue
    = new LinkedBlockingQueue<ContainerAllocatorEvent>();

  private ScheduleStats scheduleStats = new ScheduleStats();

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
    this.stopped = new AtomicBoolean(false);
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
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }

  @Override
  public void start() {
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
    super.start();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers.size() > 0) {
      scheduledRequests.assign(allocatedContainers);
    }

    int completedMaps = getJob().getCompletedMaps();
    int completedTasks = completedMaps + getJob().getCompletedReduces();
    if (lastCompletedTasks != completedTasks) {
      lastCompletedTasks = completedTasks;
      recalculateReduceSchedule = true;
    }

    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces(
          getJob().getTotalMaps(), completedMaps,
          scheduledRequests.maps.size(), scheduledRequests.reduces.size(), 
          assignedRequests.maps.size(), assignedRequests.reduces.size(),
          mapResourceReqt, reduceResourceReqt,
          pendingReduces.size(), 
          maxReduceRampupLimit, reduceSlowStart);
      recalculateReduceSchedule = false;
    }

    scheduleStats.updateAndLogIfChanged("After Scheduling: ");
  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    eventHandlingThread.interrupt();
    super.stop();
    scheduleStats.log("Final Stats: ");
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
      throw new YarnException(e);
    }
  }

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      JobId jobId = getJob().getID();
      int supportedMaxContainerCapability =
          getMaxContainerCapability().getMemory();
      if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {
        if (mapResourceReqt == 0) {
          mapResourceReqt = reqEvent.getCapability().getMemory();
          int minSlotMemSize = getMinContainerCapability().getMemory();
          mapResourceReqt = (int) Math.ceil((float) mapResourceReqt/minSlotMemSize)
              * minSlotMemSize;
          eventHandler.handle(new JobHistoryEvent(jobId, 
              new NormalizedResourceEvent(org.apache.hadoop.mapreduce.TaskType.MAP,
              mapResourceReqt)));
          LOG.info("mapResourceReqt:"+mapResourceReqt);
          if (mapResourceReqt > supportedMaxContainerCapability) {
            String diagMsg = "MAP capability required is more than the supported " +
            "max container capability in the cluster. Killing the Job. mapResourceReqt: " + 
            mapResourceReqt + " maxContainerCapability:" + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        //set the rounded off memory
        reqEvent.getCapability().setMemory(mapResourceReqt);
        scheduledRequests.addMap(reqEvent);//maps are immediately scheduled
      } else {
        if (reduceResourceReqt == 0) {
          reduceResourceReqt = reqEvent.getCapability().getMemory();
          int minSlotMemSize = getMinContainerCapability().getMemory();
          //round off on slotsize
          reduceResourceReqt = (int) Math.ceil((float) 
              reduceResourceReqt/minSlotMemSize) * minSlotMemSize;
          eventHandler.handle(new JobHistoryEvent(jobId, 
              new NormalizedResourceEvent(
                  org.apache.hadoop.mapreduce.TaskType.REDUCE,
              reduceResourceReqt)));
          LOG.info("reduceResourceReqt:"+reduceResourceReqt);
          if (reduceResourceReqt > supportedMaxContainerCapability) {
            String diagMsg = "REDUCE capability required is more than the " +
            		"supported max container capability in the cluster. Killing the " +
            		"Job. reduceResourceReqt: " + reduceResourceReqt +
            		" maxContainerCapability:" + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        //set the rounded off memory
        reqEvent.getCapability().setMemory(reduceResourceReqt);
        if (reqEvent.getEarlierAttemptFailed()) {
          //add to the front of queue for fail fast
          pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
          //reduces are added to pending and are slowly ramped up
        }
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
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
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
        for (ContainerRequest req : scheduledRequests.reduces.values()) {
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
    
    int headRoom = getAvailableResources() != null ?
        getAvailableResources().getMemory() : 0;
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
    for (int i = 0; i < rampDown; i++) {
      ContainerRequest request = scheduledRequests.removeReduce();
      pendingReduces.add(request);
    }
  }
  
  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    AMResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try
     * to contact the RM.
     */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    if (response.getReboot()) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
                                       JobEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: " +
                               this.getContext().getApplicationID());
    }
    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    List<Container> newContainers = response.getAllocatedContainers();
    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    if (newContainers.size() + finishedContainers.size() > 0 || headRoom != newHeadRoom) {
      //something changed
      recalculateReduceSchedule = true;
      if (LOG.isDebugEnabled() && headRoom != newHeadRoom) {
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
    
    for (ContainerStatus cont : finishedContainers) {
      LOG.info("Received completed container " + cont.getContainerId());
      TaskAttemptId attemptID = assignedRequests.get(cont.getContainerId());
      if (attemptID == null) {
        LOG.error("Container complete event for unknown container id "
            + cont.getContainerId());
      } else {
        assignedRequests.remove(attemptID);
        
        // send the container completed event to Task attempt
        eventHandler.handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
        // Send the diagnostics
        String diagnostics = StringInterner.weakIntern(cont.getDiagnostics());
        eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
            diagnostics));
      }
    }
    return newContainers;
  }

  @Private
  public int getMemLimit() {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
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
    private final Map<TaskAttemptId, ContainerRequest> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
    private final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces = 
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
      maps.put(event.getAttemptID(), request);
      addContainerReq(request);
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
        LOG.info("Releasing unassigned and invalid container " 
            + allocated + ". RM may have assignment issues");
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

      assignedRequests.add(allocated.getId(), assigned.attemptID);

      if (LOG.isDebugEnabled()) {
        LOG.info("Assigned container (" + allocated + ") "
            + " to task " + assigned.attemptID + " on node "
            + allocated.getNodeId().toString());
      }
    }
    
    private void containerNotAssigned(Container allocated) {
      containersReleased++;
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
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0) {
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
      if (assigned == null && reduces.size() > 0) {
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
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();        
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
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
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
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
      
      // try to match all rack local
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
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
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
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
      
      // assign remaining
      it = allocatedContainers.iterator();
      while(it.hasNext() && maps.size() > 0){
        Container allocated = it.next();
        Priority priority = allocated.getPriority();
        assert PRIORITY_MAP.equals(priority);
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

  private class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, ContainerId> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerId>();
    private final LinkedHashMap<TaskAttemptId, ContainerId> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerId>();
    private final Set<TaskAttemptId> preemptionWaitingReduces = 
      new HashSet<TaskAttemptId>();
    
    void add(ContainerId containerId, TaskAttemptId tId) {
      LOG.info("Assigned container " + containerId.toString() + " to " + tId);
      containerToAttemptMap.put(containerId, tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, containerId);
      } else {
        reduces.put(tId, containerId);
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
        eventHandler.handle(new TaskAttemptEvent(id, TaskAttemptEventType.TA_KILL));
      }
    }
    
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId);
      } else {
        containerId = reduces.remove(tId);
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
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        return maps.get(tId);
      } else {
        return reduces.get(tId);
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
}
