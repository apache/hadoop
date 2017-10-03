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
package org.apache.hadoop.mapreduce.v2.app.rm.preemption;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This policy works in combination with an implementation of task
 * checkpointing. It computes the tasks to be preempted in response to the RM
 * request for preemption. For strict requests, it maps containers to
 * corresponding tasks; for fungible requests, it attempts to pick the best
 * containers to preempt (reducers in reverse allocation order). The
 * TaskAttemptListener will interrogate this policy when handling a task
 * heartbeat to check whether the task should be preempted or not. When handling
 * fungible requests, the policy discount the RM ask by the amount of currently
 * in-flight preemptions (i.e., tasks that are checkpointing).
 *
 * This class it is also used to maintain the list of checkpoints for existing
 * tasks. Centralizing this functionality here, allows us to have visibility on
 * preemption and checkpoints in a single location, thus coordinating preemption
 * and checkpoint management decisions in a single policy.
 */
public class CheckpointAMPreemptionPolicy implements AMPreemptionPolicy {

  // task attempts flagged for preemption
  private final Set<TaskAttemptId> toBePreempted;

  private final Set<TaskAttemptId> countedPreemptions;

  private final Map<TaskId,TaskCheckpointID> checkpoints;

  private final Map<TaskAttemptId,Resource> pendingFlexiblePreemptions;

  @SuppressWarnings("rawtypes")
  private EventHandler eventHandler;

  static final Logger LOG = LoggerFactory
      .getLogger(CheckpointAMPreemptionPolicy.class);

  public CheckpointAMPreemptionPolicy() {
    this(Collections.synchronizedSet(new HashSet<TaskAttemptId>()),
         Collections.synchronizedSet(new HashSet<TaskAttemptId>()),
         Collections.synchronizedMap(new HashMap<TaskId,TaskCheckpointID>()),
         Collections.synchronizedMap(new HashMap<TaskAttemptId,Resource>()));
  }

  CheckpointAMPreemptionPolicy(Set<TaskAttemptId> toBePreempted,
      Set<TaskAttemptId> countedPreemptions,
      Map<TaskId,TaskCheckpointID> checkpoints,
      Map<TaskAttemptId,Resource> pendingFlexiblePreemptions) {
    this.toBePreempted = toBePreempted;
    this.countedPreemptions = countedPreemptions;
    this.checkpoints = checkpoints;
    this.pendingFlexiblePreemptions = pendingFlexiblePreemptions;
  }

  @Override
  public void init(AppContext context) {
    this.eventHandler = context.getEventHandler();
  }

  @Override
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {

    if (preemptionRequests != null) {

      // handling non-negotiable preemption

      StrictPreemptionContract cStrict = preemptionRequests.getStrictContract();
      if (cStrict != null
          && cStrict.getContainers() != null
          && cStrict.getContainers().size() > 0) {
        LOG.info("strict preemption :" +
            preemptionRequests.getStrictContract().getContainers().size() +
            " containers to kill");

        // handle strict preemptions. These containers are non-negotiable
        for (PreemptionContainer c :
            preemptionRequests.getStrictContract().getContainers()) {
          ContainerId reqCont = c.getId();
          TaskAttemptId reqTask = ctxt.getTaskAttempt(reqCont);
          if (reqTask != null) {
            // ignore requests for preempting containers running maps
            if (org.apache.hadoop.mapreduce.v2.api.records.TaskType.REDUCE
                .equals(reqTask.getTaskId().getTaskType())) {
              toBePreempted.add(reqTask);
              LOG.info("preempting " + reqCont + " running task:" + reqTask);
            } else {
              LOG.info("NOT preempting " + reqCont + " running task:" + reqTask);
            }
          }
        }
      }

      // handling negotiable preemption
      PreemptionContract cNegot = preemptionRequests.getContract();
      if (cNegot != null
          && cNegot.getResourceRequest() != null
          && cNegot.getResourceRequest().size() > 0
          && cNegot.getContainers() != null
          && cNegot.getContainers().size() > 0) {

        LOG.info("negotiable preemption :" +
            preemptionRequests.getContract().getResourceRequest().size() +
            " resourceReq, " +
            preemptionRequests.getContract().getContainers().size() +
            " containers");
        // handle fungible preemption. Here we only look at the total amount of
        // resources to be preempted and pick enough of our containers to
        // satisfy that. We only support checkpointing for reducers for now.
        List<PreemptionResourceRequest> reqResources =
          preemptionRequests.getContract().getResourceRequest();

        // compute the total amount of pending preemptions (to be discounted
        // from current request)
        int pendingPreemptionRam = 0;
        int pendingPreemptionCores = 0;
        for (Resource r : pendingFlexiblePreemptions.values()) {
          pendingPreemptionRam += r.getMemorySize();
          pendingPreemptionCores += r.getVirtualCores();
        }

        // discount preemption request based on currently pending preemption
        for (PreemptionResourceRequest rr : reqResources) {
          ResourceRequest reqRsrc = rr.getResourceRequest();
          if (!ResourceRequest.ANY.equals(reqRsrc.getResourceName())) {
            // For now, only respond to aggregate requests and ignore locality
            continue;
          }

          LOG.info("ResourceRequest:" + reqRsrc);
          int reqCont = reqRsrc.getNumContainers();
          long reqMem = reqRsrc.getCapability().getMemorySize();
          long totalMemoryToRelease = reqCont * reqMem;
          int reqCores = reqRsrc.getCapability().getVirtualCores();
          int totalCoresToRelease = reqCont * reqCores;

          // remove
          if (pendingPreemptionRam > 0) {
            // if goes negative we simply exit
            totalMemoryToRelease -= pendingPreemptionRam;
            // decrement pending resources if zero or negatve we will
            // ignore it while processing next PreemptionResourceRequest
            pendingPreemptionRam -= totalMemoryToRelease;
          }
          if (pendingPreemptionCores > 0) {
            totalCoresToRelease -= pendingPreemptionCores;
            pendingPreemptionCores -= totalCoresToRelease;
          }

          // reverse order of allocation (for now)
          List<Container> listOfCont = ctxt.getContainers(TaskType.REDUCE);
          Collections.sort(listOfCont, new Comparator<Container>() {
            @Override
            public int compare(final Container o1, final Container o2) {
              return o2.getId().compareTo(o1.getId());
            }
          });

          // preempt reducers first
          for (Container cont : listOfCont) {
            if (totalMemoryToRelease <= 0 && totalCoresToRelease<=0) {
              break;
            }
            TaskAttemptId reduceId = ctxt.getTaskAttempt(cont.getId());
            int cMem = (int) cont.getResource().getMemorySize();
            int cCores = cont.getResource().getVirtualCores();

            if (!toBePreempted.contains(reduceId)) {
              totalMemoryToRelease -= cMem;
              totalCoresToRelease -= cCores;
                toBePreempted.add(reduceId);
                pendingFlexiblePreemptions.put(reduceId, cont.getResource());
            }
            LOG.info("ResourceRequest:" + reqRsrc + " satisfied preempting "
                + reduceId);
          }
          // if map was preemptable we would do add them to toBePreempted here
        }
      }
    }
  }

  @Override
  public void handleFailedContainer(TaskAttemptId attemptID) {
    toBePreempted.remove(attemptID);
    checkpoints.remove(attemptID.getTaskId());
  }

  @Override
  public void handleCompletedContainer(TaskAttemptId attemptID){
    LOG.info(" task completed:" + attemptID);
    toBePreempted.remove(attemptID);
    pendingFlexiblePreemptions.remove(attemptID);
  }

  @Override
  public boolean isPreempted(TaskAttemptId yarnAttemptID) {
    if (toBePreempted.contains(yarnAttemptID)) {
      updatePreemptionCounters(yarnAttemptID);
      return true;
    }
    return false;
  }

  @Override
  public void reportSuccessfulPreemption(TaskAttemptId taskAttemptID) {
    // ignore
  }

  @Override
  public TaskCheckpointID getCheckpointID(TaskId taskId) {
    return checkpoints.get(taskId);
  }

  @Override
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid) {
    checkpoints.put(taskId, cid);
    if (cid != null) {
      updateCheckpointCounters(taskId, cid);
    }
  }

  @SuppressWarnings({ "unchecked" })
  private void updateCheckpointCounters(TaskId taskId, TaskCheckpointID cid) {
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINTS, 1);
    eventHandler.handle(jce);
    jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINT_BYTES, cid.getCheckpointBytes());
    eventHandler.handle(jce);
    jce = new JobCounterUpdateEvent(taskId.getJobId());
    jce.addCounterUpdate(JobCounter.CHECKPOINT_TIME, cid.getCheckpointTime());
    eventHandler.handle(jce);

  }

  @SuppressWarnings({ "unchecked" })
  private void updatePreemptionCounters(TaskAttemptId yarnAttemptID) {
    if (!countedPreemptions.contains(yarnAttemptID)) {
      countedPreemptions.add(yarnAttemptID);
      JobCounterUpdateEvent jce = new JobCounterUpdateEvent(yarnAttemptID
          .getTaskId().getJobId());
      jce.addCounterUpdate(JobCounter.TASKS_REQ_PREEMPT, 1);
      eventHandler.handle(jce);
    }
  }

}
