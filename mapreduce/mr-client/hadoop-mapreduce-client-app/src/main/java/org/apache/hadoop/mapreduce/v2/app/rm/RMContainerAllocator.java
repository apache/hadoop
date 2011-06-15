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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AMConstants;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  private static final Priority PRIORITY_FAST_FAIL_MAP;
  private static final Priority PRIORITY_REDUCE;
  private static final Priority PRIORITY_MAP;
  
  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
  }
  
  /*
  Vocabulory Used: 
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
  
  private boolean recalculateReduceSchedule = false;
  private int mapResourceReqt;//memory
  private int reduceResourceReqt;//memory
  private int completedMaps = 0;
  private int completedReduces = 0;
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  private float reduceSlowStart = 0;

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        AMConstants.REDUCE_RAMPUP_UP_LIMIT, 
        AMConstants.DEFAULT_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        AMConstants.REDUCE_PREEMPTION_LIMIT,
        AMConstants.DEFAULT_REDUCE_PREEMPTION_LIMIT);
    RackResolver.init(conf);
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    LOG.info("Before Allocation: " + getStat());
    List<Container> allocatedContainers = getResources();
    LOG.info("After Allocation: " + getStat());
    if (allocatedContainers.size() > 0) {
      LOG.info("Before Assign: " + getStat());
      scheduledRequests.assign(allocatedContainers);
      LOG.info("After Assign: " + getStat());
    }
    
    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces();
      recalculateReduceSchedule = false;
    }
  }

  @Override
  public void stop() {
    super.stop();
    LOG.info("Final Stats: " + getStat());
  }

  @Override
  public synchronized void handle(ContainerAllocatorEvent event) {
    LOG.info("Processing the event " + event.toString());
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {
        if (mapResourceReqt == 0) {
          mapResourceReqt = reqEvent.getCapability().getMemory();
          int minSlotMemSize = getMinContainerCapability().getMemory();
          mapResourceReqt = (int) Math.ceil((float) mapResourceReqt/minSlotMemSize) * minSlotMemSize;
          LOG.info("mapResourceReqt:"+mapResourceReqt);
          if (mapResourceReqt > getMaxContainerCapability().getMemory()) {
            String diagMsg = "MAP capability required is more than the supported " +
            "max container capability in the cluster. Killing the Job. mapResourceReqt: " + 
            mapResourceReqt + " maxContainerCapability:" + getMaxContainerCapability().getMemory();
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                getJob().getID(), diagMsg));
            eventHandler.handle(new JobEvent(getJob().getID(), JobEventType.JOB_KILL));
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
          reduceResourceReqt = (int) Math.ceil((float) reduceResourceReqt/minSlotMemSize) * minSlotMemSize;
          LOG.info("reduceResourceReqt:"+reduceResourceReqt);
          if (reduceResourceReqt > getMaxContainerCapability().getMemory()) {
            String diagMsg = "REDUCE capability required is more than the supported " +
            "max container capability in the cluster. Killing the Job. reduceResourceReqt: " + 
            reduceResourceReqt + " maxContainerCapability:" + getMaxContainerCapability().getMemory();
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                getJob().getID(), diagMsg));
            eventHandler.handle(new JobEvent(getJob().getID(), JobEventType.JOB_KILL));
          }
        }
        //set the rounded off memory
        reqEvent.getCapability().setMemory(reduceResourceReqt);
        if (reqEvent.getEarlierAttemptFailed()) {
          //add to the front of queue for fail fast
          pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));//reduces are added to pending and are slowly ramped up
        }
      }
      
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        Container container = assignedRequests.get(aId);
        if (container != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          release(container);
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
    int memLimit = getMemLimit();
    if (scheduledRequests.maps.size() > 0) {
      int availableMemForMap = memLimit - ((assignedRequests.reduces.size() -
          assignedRequests.preemptionWaitingReduces.size()) * reduceResourceReqt);
      //availableMemForMap must be sufficient to run atleast 1 map
      if (availableMemForMap < mapResourceReqt) {
        //preempt for making space for atleast one map
        int premeptionLimit = Math.max(mapResourceReqt - availableMemForMap, 
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

  private void scheduleReduces() {
    
    if (pendingReduces.size() == 0) {
      return;
    }
    
    LOG.info("Recalculating schedule...");
    
    int totalMaps = assignedRequests.maps.size() + completedMaps + scheduledRequests.maps.size();
    
    //check for slow start
    if (!reduceStarted) {//not set yet
      int completedMapsForReduceSlowstart = (int)Math.ceil(reduceSlowStart * 
                      totalMaps);
      if(completedMaps < completedMapsForReduceSlowstart) {
        LOG.info("Reduce slow start threshold not met. " +
              "completedMapsForReduceSlowstart " + completedMapsForReduceSlowstart);
        return;
      } else {
        LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
        reduceStarted = true;
      }
    }
    
    float completedMapPercent = 0f;
    if (totalMaps != 0) {//support for 0 maps
      completedMapPercent = (float)completedMaps/totalMaps;
    } else {
      completedMapPercent = 1;
    }
    
    int netScheduledMapMem = scheduledRequests.maps.size() * mapResourceReqt
        + assignedRequests.maps.size() * mapResourceReqt;

    int netScheduledReduceMem = scheduledRequests.reduces.size()
        * reduceResourceReqt + assignedRequests.reduces.size()
        * reduceResourceReqt;

    int finalMapMemLimit = 0;
    int finalReduceMemLimit = 0;
    
    // ramp up the reduces based on completed map percentage
    int totalMemLimit = getMemLimit();
    int idealReduceMemLimit = Math.min((int)(completedMapPercent * totalMemLimit),
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
    
    int rampUp = (finalReduceMemLimit - netScheduledReduceMem)
        / reduceResourceReqt;
    
    if (rampUp > 0) {
      rampUp = Math.min(rampUp, pendingReduces.size());
      LOG.info("Ramping up " + rampUp);
      //more reduce to be scheduled
      for (int i = 0; i < rampUp; i++) {
        ContainerRequest request = pendingReduces.removeFirst();
        scheduledRequests.addReduce(request);
      }
    } else if (rampUp < 0){
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledRequests.reduces.size());
      LOG.info("Ramping down " + rampDown);
      //remove from the scheduled and move back to pending
      for (int i = 0; i < rampDown; i++) {
        ContainerRequest request = scheduledRequests.removeReduce();
        pendingReduces.add(request);
      }
    }
  }

  private String getStat() {
    return "PendingReduces:" + pendingReduces.size() +
        " ScheduledMaps:" + scheduledRequests.maps.size() +
        " ScheduledReduces:" + scheduledRequests.reduces.size() +
        " AssignedMaps:" + assignedRequests.maps.size() + 
        " AssignedReduces:" + assignedRequests.reduces.size() +
        " completedMaps:" + completedMaps +
        " completedReduces:" + completedReduces +
        " containersAllocated:" + containersAllocated +
        " containersReleased:" + containersReleased +
        " hostLocalAssigned:" + hostLocalAssigned + 
        " rackLocalAssigned:" + rackLocalAssigned +
        " availableResources(headroom):" + getAvailableResources();
  }
  
  private List<Container> getResources() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    List<Container> allContainers = makeRemoteRequest();
    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    if (allContainers.size() > 0 || headRoom != newHeadRoom) {
      //something changed
      recalculateReduceSchedule = true;
    }
    
    List<Container> allocatedContainers = new ArrayList<Container>();
    for (Container cont : allContainers) {
      if (cont.getState() != ContainerState.COMPLETE) {
        allocatedContainers.add(cont);
        LOG.debug("Received Container :" + cont);
      } else {
        LOG.info("Received completed container " + cont);
        TaskAttemptId attemptID = assignedRequests.get(cont.getId());
        if (attemptID == null) {
          LOG.error("Container complete event for unknown container id " +
              cont.getId());
        } else {
          assignedRequests.remove(attemptID);
          if (attemptID.getTaskId().getTaskType().equals(TaskType.MAP)) {
            completedMaps++;
          } else {
            completedReduces++;
          }
          //send the container completed event to Task attempt
          eventHandler.handle(new TaskAttemptEvent(attemptID,
              TaskAttemptEventType.TA_CONTAINER_COMPLETED));
        }
      }
      LOG.debug("Received Container :" + cont);
    }
    return allocatedContainers;
  }

  private int getMemLimit() {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    return headRoom + assignedRequests.maps.size() * mapResourceReqt + 
       assignedRequests.reduces.size() * reduceResourceReqt;
  }
  
  private class ScheduledRequests {
    
    private final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    
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
      } else {
        for (String host : event.getHosts()) {
          LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
          if (list == null) {
            list = new LinkedList<TaskAttemptId>();
            mapsHostMapping.put(host, list);
          }
          list.add(event.getAttemptID());
          LOG.info("Added attempt req to host " + host);
       }
       for (String rack: event.getRacks()) {
         LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
         if (list == null) {
           list = new LinkedList<TaskAttemptId>();
           mapsRackMapping.put(rack, list);
         }
         list.add(event.getAttemptID());
         LOG.info("Added attempt req to rack " + rack);
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
    
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        LOG.info("Assiging container " + allocated);
        ContainerRequest assigned = assign(allocated);
          
        if (assigned != null) {
          // Update resource requests
          decContainerReq(assigned);

          // send the container-assigned event to task attempt
          eventHandler.handle(new TaskAttemptContainerAssignedEvent(
              assigned.attemptID, allocated));

          assignedRequests.add(allocated, assigned.attemptID);
          
          LOG.info("Assigned container (" + allocated + ") " +
              " to task " + assigned.attemptID +
              " on node " + allocated.getContainerManagerAddress());
        } else {
          //not assigned to any request, release the container
          LOG.info("Releasing unassigned container " + allocated);
          containersReleased++;
          release(allocated);
        }
      }
    }
    
    private ContainerRequest assign(Container allocated) {
      ContainerRequest assigned = null;
      
      if (mapResourceReqt != reduceResourceReqt) {
        //assign based on size
        LOG.info("Assigning based on container size");
        if (allocated.getResource().getMemory() == mapResourceReqt) {
          assigned = assignToFailedMap(allocated);
          if (assigned == null) {
            assigned = assignToMap(allocated);
          }
        } else if (allocated.getResource().getMemory() == reduceResourceReqt) {
          assigned = assignToReduce(allocated);
        }
        
        return assigned;
      }
      
      //container can be given to either map or reduce
      //assign based on priority
      
      //try to assign to earlierFailedMaps if present
      assigned = assignToFailedMap(allocated);
      
      if (assigned == null) {
        assigned = assignToReduce(allocated);
      }
      
      //try to assign to maps if present
      if (assigned == null) {
        assigned = assignToMap(allocated);
      }
      
      return assigned;
    }
    
    
    private ContainerRequest assignToFailedMap(Container allocated) {
      //try to assign to earlierFailedMaps if present
      ContainerRequest assigned = null;
      while (assigned == null && earlierFailedMaps.size() > 0 && 
          allocated.getResource().getMemory() >= mapResourceReqt) {
        TaskAttemptId tId = earlierFailedMaps.removeFirst();
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          LOG.info("Assigned from earlierFailedMaps");
          break;
        }
      }
      return assigned;
    }
    
    private ContainerRequest assignToReduce(Container allocated) {
      ContainerRequest assigned = null;
      //try to assign to reduces if present
      if (assigned == null && reduces.size() > 0
          && allocated.getResource().getMemory() >= reduceResourceReqt) {
        TaskAttemptId tId = reduces.keySet().iterator().next();
        assigned = reduces.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
    
    private ContainerRequest assignToMap(Container allocated) {
    //try to assign to maps if present 
      //first by host, then by rack, followed by *
      ContainerRequest assigned = null;
      while (assigned == null && maps.size() > 0
          && allocated.getResource().getMemory() >= mapResourceReqt) {
        String host = getHost(allocated.getContainerManagerAddress());
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        while (list != null && list.size() > 0) {
          LOG.info("Host matched to the request list " + host);
          TaskAttemptId tId = list.removeFirst();
          if (maps.containsKey(tId)) {
            assigned = maps.remove(tId);
            hostLocalAssigned++;
            LOG.info("Assigned based on host match " + host);
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
              rackLocalAssigned++;
              LOG.info("Assigned based on rack match " + rack);
              break;
            }
          }
          if (assigned == null && maps.size() > 0) {
            TaskAttemptId tId = maps.keySet().iterator().next();
            assigned = maps.remove(tId);
            LOG.info("Assigned based on * match");
            break;
          }
        }
      }
      return assigned;
    }
  }

  private class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, Container> maps = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final LinkedHashMap<TaskAttemptId, Container> reduces = 
      new LinkedHashMap<TaskAttemptId, Container>();
    private final Set<TaskAttemptId> preemptionWaitingReduces = 
      new HashSet<TaskAttemptId>();
    
    void add(Container container, TaskAttemptId tId) {
      LOG.info("Assigned container " + container.getContainerManagerAddress() 
          + " to " + tId);
      containerToAttemptMap.put(container.getId(), tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, container);
      } else {
        reduces.put(tId, container);
      }
    }

    void preemptReduce(int toPreempt) {
      List<TaskAttemptId> reduceList = new ArrayList(reduces.keySet());
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
      Container container = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        container = maps.remove(tId);
      } else {
        container = reduces.remove(tId);
        if (container != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (container != null) {
        containerToAttemptMap.remove(container.getId());
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    Container get(TaskAttemptId tId) {
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        return maps.get(tId);
      } else {
        return reduces.get(tId);
      }
    }
  }
}
